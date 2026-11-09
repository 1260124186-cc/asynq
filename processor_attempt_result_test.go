// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
	h "github.com/hibiken/asynq/internal/testutil"
)

// TestProcessorAttemptResultLineage verifies end to end that every execution
// attempt keeps an independent result record, failed attempts are retained, and
// only the successful attempt is marked final.
func TestProcessorAttemptResultLineage(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)

	msg := h.NewTaskMessage("lineage_task", nil)
	msg.Retry = 5
	msg.Retention = 3600 // keep the completed task so its lineage remains inspectable
	h.SeedPendingQueue(t, r, []*base.TaskMessage{msg}, base.DefaultQueueName)

	var mu sync.Mutex
	attempts := 0
	p := newProcessorForTest(t, rdbClient, HandlerFunc(func(ctx context.Context, task *Task) error {
		mu.Lock()
		attempts++
		n := attempts
		mu.Unlock()

		// Every attempt reports its own result before returning.
		data := []byte(fmt.Sprintf("result-%d", n))
		if _, err := task.ResultWriter().Write(data); err != nil {
			return fmt.Errorf("write result: %w", err)
		}
		if n < 3 {
			return fmt.Errorf("simulated failure #%d", n)
		}
		return nil
	}))
	p.retryDelayFunc = func(n int, err error, task *Task) time.Duration {
		return 20 * time.Millisecond
	}

	// The processor alone never re-queues due retry entries; run a forwarder
	// with a short interval like the real server does.
	fwd := newForwarder(forwarderParams{
		logger:   testLogger,
		broker:   rdbClient,
		queues:   []string{base.DefaultQueueName},
		interval: 100 * time.Millisecond,
	})
	var fwdWg sync.WaitGroup
	fwd.start(&fwdWg)
	defer fwd.shutdown()

	p.start(&sync.WaitGroup{})
	time.Sleep(3 * time.Second)
	p.shutdown()

	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	info, err := inspector.GetTaskInfo(base.DefaultQueueName, msg.ID)
	if err != nil {
		t.Fatalf("GetTaskInfo failed: %v", err)
	}
	if info.State != TaskStateCompleted {
		t.Fatalf("task state = %s, want %s", info.State, TaskStateCompleted)
	}
	if string(info.Result) != "result-3" {
		t.Errorf("TaskInfo.Result = %q, want result-3", info.Result)
	}

	records, err := inspector.ListTaskAttemptResults(base.DefaultQueueName, msg.ID)
	if err != nil {
		t.Fatalf("ListTaskAttemptResults failed: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("got %d records, want 3: %+v", len(records), records)
	}
	want := []struct {
		attempt int
		status  AttemptStatus
		data    string
		final   bool
	}{
		{1, AttemptStatusRecorded, "result-1", false},
		{2, AttemptStatusRecorded, "result-2", false},
		{3, AttemptStatusFinal, "result-3", true},
	}
	for i, w := range want {
		got := records[i]
		if got.Attempt != w.attempt || got.Status != w.status ||
			string(got.Result) != w.data || got.Final != w.final {
			t.Errorf("record %d = %+v, want attempt=%d status=%s data=%q final=%t",
				i, got, w.attempt, w.status, w.data, w.final)
		}
	}
}

// TestProcessorDuplicateResultWriteIsIdempotent verifies that reporting a
// result repeatedly within one attempt never produces additional records.
func TestProcessorDuplicateResultWriteIsIdempotent(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)

	msg := h.NewTaskMessage("idempotent_result_task", nil)
	msg.Retention = 3600
	h.SeedPendingQueue(t, r, []*base.TaskMessage{msg}, base.DefaultQueueName)

	p := newProcessorForTest(t, rdbClient, HandlerFunc(func(ctx context.Context, task *Task) error {
		w := task.ResultWriter()
		if w.Attempt() != 1 {
			t.Errorf("ResultWriter.Attempt = %d, want 1", w.Attempt())
		}
		if _, err := w.Write([]byte("first-write")); err != nil {
			return err
		}
		if _, err := w.Write([]byte("second-write")); err != nil {
			return err
		}
		return nil
	}))

	p.start(&sync.WaitGroup{})
	time.Sleep(2 * time.Second)
	p.shutdown()

	taskKey := base.TaskKey(base.DefaultQueueName, msg.ID)
	fields := r.HGetAll(context.Background(), taskKey).Val()
	slots := 0
	for field := range fields {
		if _, ok := base.ParseAttemptResultFieldName(field); ok {
			slots++
		}
	}
	if slots != 1 {
		t.Errorf("got %d attempt records after duplicate writes, want 1", slots)
	}
	if marker := fields[base.AttemptResultFinalField]; marker != "1" {
		t.Errorf("final marker = %q, want 1", marker)
	}

	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()
	records, err := inspector.ListTaskAttemptResults(base.DefaultQueueName, msg.ID)
	if err != nil {
		t.Fatalf("ListTaskAttemptResults failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	if !records[0].Final || records[0].Status != AttemptStatusFinal ||
		string(records[0].Result) != "second-write" {
		t.Errorf("record = %+v, want single final record holding the last write", records[0])
	}
}

// TestRecovererBackfillsIncompleteAttempt verifies that after a hard crash
// (lease expired with no result reported), recovery fills the missing attempt
// record instead of leaving a gap, and the next attempt's record is appended.
func TestRecovererBackfillsIncompleteAttempt(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)

	msg := h.NewTaskMessage("crashed_task", nil)
	msg.Retry = 5
	h.SeedActiveQueue(t, r, []*base.TaskMessage{msg}, base.DefaultQueueName)
	h.SeedLease(t, r, []base.Z{
		{Message: msg, Score: time.Now().Add(-time.Minute).Unix()},
	}, base.DefaultQueueName)

	recoverer := newRecoverer(recovererParams{
		logger:         testLogger,
		broker:         rdbClient,
		queues:         []string{base.DefaultQueueName},
		interval:       time.Second,
		retryDelayFunc: func(n int, err error, task *Task) time.Duration { return time.Minute },
		isFailureFunc:  defaultIsFailureFunc,
	})
	var wg sync.WaitGroup
	recoverer.start(&wg)
	time.Sleep(2 * time.Second)
	recoverer.shutdown()

	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	records, err := inspector.ListTaskAttemptResults(base.DefaultQueueName, msg.ID)
	if err != nil {
		t.Fatalf("ListTaskAttemptResults after recovery failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records after recovery, want 1 placeholder", len(records))
	}
	if records[0].Attempt != 1 || records[0].Status != AttemptStatusIncomplete ||
		records[0].Final || len(records[0].Result) != 0 {
		t.Errorf("placeholder = %+v, want incomplete empty record for attempt 1", records[0])
	}

	// The retried run is attempt 2; its record is appended without a gap.
	if _, err := rdbClient.WriteAttemptResult(base.DefaultQueueName, msg.ID, 2, []byte("recovered-result")); err != nil {
		t.Fatalf("WriteAttemptResult attempt 2 failed: %v", err)
	}
	records, err = inspector.ListTaskAttemptResults(base.DefaultQueueName, msg.ID)
	if err != nil {
		t.Fatalf("ListTaskAttemptResults after re-run failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}
	if records[0].Status != AttemptStatusIncomplete || records[0].Attempt != 1 {
		t.Errorf("record 0 = %+v, want incomplete attempt 1", records[0])
	}
	if records[1].Status != AttemptStatusRecorded || records[1].Attempt != 2 ||
		string(records[1].Result) != "recovered-result" {
		t.Errorf("record 1 = %+v, want recorded attempt 2", records[1])
	}
}
