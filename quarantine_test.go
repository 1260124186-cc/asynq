// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	asynqerrors "github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/rdb"
	h "github.com/hibiken/asynq/internal/testutil"
)

// stubGate is a manually controlled quarantineGate used to drive processor tests.
type stubGate struct{ on atomic.Bool }

func (g *stubGate) IsQuarantined() bool { return g.on.Load() }

func newTestQuarantiner(t *testing.T, r *rdb.RDB, nodeID, serverID string, failThreshold, recoveryThreshold int) *quarantiner {
	t.Helper()
	return newQuarantiner(quarantinerParams{
		logger:            testLogger,
		broker:            r,
		nodeID:            nodeID,
		serverID:          serverID,
		failureThreshold:  failThreshold,
		recoveryThreshold: recoveryThreshold,
		interval:          50 * time.Millisecond,
	})
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func TestQuarantiner_GateTransitions(t *testing.T) {
	client := setup(t)
	defer client.Close()
	r := rdb.NewRDB(client)

	const (
		nodeID, serverID        = "comp-node-1", "srv-1"
		failThreshold, recovery = 3, 2
	)
	q := newTestQuarantiner(t, r, nodeID, serverID, failThreshold, recovery)
	var wg sync.WaitGroup
	q.start(&wg)
	defer func() { q.shutdown(); wg.Wait() }()

	if q.IsQuarantined() {
		t.Fatal("gate should start healthy")
	}

	for i := int64(1); i <= failThreshold; i++ {
		q.reportFailure(failureHeartbeat, heartbeatEventKey(serverID, i))
	}
	waitUntil(t, 2*time.Second, q.IsQuarantined, "gate to become quarantined after failures")

	// Distinct duplicate of an already counted event must not change anything.
	before, _ := r.GetNodeQuarantine(nodeID)
	q.reportFailure(failureHeartbeat, heartbeatEventKey(serverID, 1))
	time.Sleep(100 * time.Millisecond)
	after, _ := r.GetNodeQuarantine(nodeID)
	if after.FailCount != before.FailCount || after.Version != before.Version {
		t.Errorf("duplicate event changed state: before=%+v after=%+v", before, after)
	}
	if !q.IsQuarantined() {
		t.Fatal("gate must remain quarantined after duplicate report")
	}

	for i := 0; i < recovery; i++ {
		q.recordHeartbeat()
	}
	waitUntil(t, 2*time.Second, func() bool { return !q.IsQuarantined() }, "gate to release after stable heartbeats")
}

func TestQuarantiner_ReconcileLoadsPersistedState(t *testing.T) {
	client := setup(t)
	defer client.Close()
	r := rdb.NewRDB(client)

	const nodeID, oldServer, newServer = "comp-node-2", "srv-old", "srv-new"
	ctx := context.Background()
	// A previous process isolated the node and then died (liveness marker gone).
	if _, _, err := r.ReportNodeFailure(ctx, nodeID, oldServer, "evt-1", 1); err != nil {
		t.Fatal(err)
	}
	client.Del(ctx, base.NodeQuarantineAliveKey(nodeID, oldServer))

	// A restarted process builds a fresh quarantiner; its first reconciliation must
	// pick up the persisted isolation even before any report is processed.
	q := newTestQuarantiner(t, r, nodeID, newServer, 3, 3)
	q.reconcile()
	if !q.IsQuarantined() {
		t.Fatal("restarted node must inherit persisted quarantined state")
	}
}

func TestQuarantiner_ReconcilePicksUpManualClear(t *testing.T) {
	client := setup(t)
	defer client.Close()
	r := rdb.NewRDB(client)

	const nodeID, serverID = "comp-node-3", "srv-1"
	ctx := context.Background()
	r.ReportNodeFailure(ctx, nodeID, serverID, "evt-1", 1)

	q := newTestQuarantiner(t, r, nodeID, serverID, 1, 1)
	q.reconcile()
	if !q.IsQuarantined() {
		t.Fatal("expected quarantined gate")
	}

	if err := r.ClearNodeQuarantine(ctx, nodeID, "ops", "network recovered"); err != nil {
		t.Fatal(err)
	}
	q.reconcile()
	if q.IsQuarantined() {
		t.Fatal("gate must release after manual clear is reconciled")
	}

	audit, err := r.ListNodeQuarantineAudit(nodeID)
	if err != nil || len(audit) == 0 {
		t.Fatalf("audit missing: %v", err)
	}
	last := audit[len(audit)-1]
	if last.Action != base.QuarantineActionManualClear || last.Operator != "ops" {
		t.Errorf("manual clear not recorded correctly: %+v", last)
	}
}

func TestQuarantiner_ConflictKeepsGateUnchanged(t *testing.T) {
	client := setup(t)
	defer client.Close()
	r := rdb.NewRDB(client)

	const nodeID, owner, intruder = "comp-node-4", "srv-owner", "srv-intruder"
	ctx := context.Background()
	r.ReportNodeFailure(ctx, nodeID, owner, "evt-1", 3)
	r.RecordNodeHeartbeat(ctx, nodeID, owner, 3) // owner alive

	// The intruder's reconciliation must fail with a conflict and leave its gate alone.
	q := newTestQuarantiner(t, r, nodeID, intruder, 3, 3)
	q.quarantined.Store(true) // simulate a pre-existing local isolation decision
	q.reconcile()
	if !q.IsQuarantined() {
		t.Fatal("conflict must not flip the pre-existing gate")
	}

	// Direct conflict through the report path surfaces the sentinel error.
	_, _, err := r.ReportNodeFailure(ctx, nodeID, intruder, "evt-2", 3)
	if !asynqerrors.Is(err, asynqerrors.ErrNodeIdentityConflict) {
		t.Fatalf("err = %v, want ErrNodeIdentityConflict", err)
	}
}

func TestProcessor_SkipsDequeueWhileQuarantined(t *testing.T) {
	client := setup(t)
	defer client.Close()
	rdbClient := rdb.NewRDB(client)

	starting := make(chan *workerInfo)
	finished := make(chan *base.TaskMessage)
	syncCh := make(chan *syncRequest)
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go fakeHeartbeater(starting, finished, done)
	go fakeSyncer(syncCh, done)

	var (
		mu        sync.Mutex
		processed []*Task
	)
	handler := func(ctx context.Context, task *Task) error {
		mu.Lock()
		processed = append(processed, task)
		mu.Unlock()
		return nil
	}

	gate := &stubGate{}
	p := newProcessor(processorParams{
		logger:            testLogger,
		broker:            rdbClient,
		baseCtxFn:         context.Background,
		retryDelayFunc:    DefaultRetryDelayFunc,
		taskCheckInterval: 50 * time.Millisecond,
		isFailureFunc:     defaultIsFailureFunc,
		syncCh:            syncCh,
		cancelations:      base.NewCancelations(),
		concurrency:       10,
		queues:            defaultQueueConfig,
		strictPriority:    false,
		shutdownTimeout:   defaultShutdownTimeout,
		gate:              gate,
		starting:          starting,
		finished:          finished,
	})
	p.handler = HandlerFunc(handler)

	msg := h.NewTaskMessage("quarantined-task", nil)
	h.SeedPendingQueue(t, client, []*base.TaskMessage{msg}, base.DefaultQueueName)

	// While quarantined, the processor must not pull the available task.
	gate.on.Store(true)
	var wg sync.WaitGroup
	p.start(&wg)
	time.Sleep(400 * time.Millisecond)
	mu.Lock()
	gotDuringQuarantine := len(processed)
	mu.Unlock()
	if gotDuringQuarantine != 0 {
		t.Fatalf("processed %d tasks while quarantined, want 0", gotDuringQuarantine)
	}

	// Once released, the same processor resumes consuming the queue.
	gate.on.Store(false)
	waitUntil(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(processed) == 1
	}, "processor to dequeue and process the task after release")

	p.shutdown()
	wg.Wait()
}
