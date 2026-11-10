// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
	h "github.com/hibiken/asynq/internal/testutil"
)

func TestAggregator(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	client := Client{broker: rdbClient}

	tests := []struct {
		desc             string
		gracePeriod      time.Duration
		maxDelay         time.Duration
		maxSize          int
		aggregateFunc    func(gname string, tasks []*Task) *Task
		tasks            []*Task       // tasks to enqueue
		enqueueFrequency time.Duration // time between one enqueue event to another
		waitTime         time.Duration // time to wait
		wantGroups       map[string]map[string][]base.Z
		wantPending      map[string][]*base.TaskMessage
	}{
		{
			desc:        "group older than the grace period should be aggregated",
			gracePeriod: 1 * time.Second,
			maxDelay:    0, // no maxdelay limit
			maxSize:     0, // no maxsize limit
			aggregateFunc: func(gname string, tasks []*Task) *Task {
				return NewTask(gname, nil, MaxRetry(len(tasks))) // use max retry to see how many tasks were aggregated
			},
			tasks: []*Task{
				NewTask("task1", nil, Group("mygroup")),
				NewTask("task2", nil, Group("mygroup")),
				NewTask("task3", nil, Group("mygroup")),
			},
			enqueueFrequency: 300 * time.Millisecond,
			waitTime:         3 * time.Second,
			wantGroups: map[string]map[string][]base.Z{
				"default": {
					"mygroup": {},
				},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					h.NewTaskMessageBuilder().SetType("mygroup").SetRetry(3).Build(),
				},
			},
		},
		{
			desc:        "group older than the max-delay should be aggregated",
			gracePeriod: 2 * time.Second,
			maxDelay:    4 * time.Second,
			maxSize:     0, // no maxsize limit
			aggregateFunc: func(gname string, tasks []*Task) *Task {
				return NewTask(gname, nil, MaxRetry(len(tasks))) // use max retry to see how many tasks were aggregated
			},
			tasks: []*Task{
				NewTask("task1", nil, Group("mygroup")), // time 0
				NewTask("task2", nil, Group("mygroup")), // time 1s
				NewTask("task3", nil, Group("mygroup")), // time 2s
				NewTask("task4", nil, Group("mygroup")), // time 3s
			},
			enqueueFrequency: 1 * time.Second,
			waitTime:         4 * time.Second,
			wantGroups: map[string]map[string][]base.Z{
				"default": {
					"mygroup": {},
				},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					h.NewTaskMessageBuilder().SetType("mygroup").SetRetry(4).Build(),
				},
			},
		},
		{
			desc:        "group reached the max-size should be aggregated",
			gracePeriod: 1 * time.Minute,
			maxDelay:    0, // no maxdelay limit
			maxSize:     5,
			aggregateFunc: func(gname string, tasks []*Task) *Task {
				return NewTask(gname, nil, MaxRetry(len(tasks))) // use max retry to see how many tasks were aggregated
			},
			tasks: []*Task{
				NewTask("task1", nil, Group("mygroup")),
				NewTask("task2", nil, Group("mygroup")),
				NewTask("task3", nil, Group("mygroup")),
				NewTask("task4", nil, Group("mygroup")),
				NewTask("task5", nil, Group("mygroup")),
			},
			enqueueFrequency: 300 * time.Millisecond,
			waitTime:         defaultAggregationCheckInterval * 2,
			wantGroups: map[string]map[string][]base.Z{
				"default": {
					"mygroup": {},
				},
			},
			wantPending: map[string][]*base.TaskMessage{
				"default": {
					h.NewTaskMessageBuilder().SetType("mygroup").SetRetry(5).Build(),
				},
			},
		},
	}

	for _, tc := range tests {
		h.FlushDB(t, r)

		aggregator := newAggregator(aggregatorParams{
			logger:          testLogger,
			broker:          rdbClient,
			queues:          []string{"default"},
			gracePeriod:     tc.gracePeriod,
			maxDelay:        tc.maxDelay,
			maxSize:         tc.maxSize,
			groupAggregator: GroupAggregatorFunc(tc.aggregateFunc),
		})

		var wg sync.WaitGroup
		aggregator.start(&wg)

		for _, task := range tc.tasks {
			if _, err := client.Enqueue(task); err != nil {
				t.Errorf("%s: Client Enqueue failed: %v", tc.desc, err)
				aggregator.shutdown()
				wg.Wait()
				continue
			}
			time.Sleep(tc.enqueueFrequency)
		}

		time.Sleep(tc.waitTime)

		for qname, groups := range tc.wantGroups {
			for gname, want := range groups {
				gotGroup := h.GetGroupEntries(t, r, qname, gname)
				if diff := cmp.Diff(want, gotGroup, h.SortZSetEntryOpt); diff != "" {
					t.Errorf("%s: mismatch found in %q; (-want,+got)\n%s", tc.desc, base.GroupKey(qname, gname), diff)
				}
			}
		}

		for qname, want := range tc.wantPending {
			gotPending := h.GetPendingMessages(t, r, qname)
			if diff := cmp.Diff(want, gotPending, h.SortMsgOpt, h.IgnoreIDOpt); diff != "" {
				t.Errorf("%s: mismatch found in %q; (-want,+got)\n%s", tc.desc, base.PendingKey(qname), diff)
			}
		}
		aggregator.shutdown()
		wg.Wait()
	}
}

// waitFor polls the given condition until it returns true or the deadline elapses.
func waitFor(t *testing.T, deadline time.Duration, fn func() bool, msg string) {
	t.Helper()
	deadlineAt := time.Now().Add(deadline)
	for time.Now().Before(deadlineAt) {
		if fn() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func newTestAggregator(r *rdb.RDB, gracePeriod time.Duration) (*aggregator, *sync.WaitGroup) {
	aggregator := newAggregator(aggregatorParams{
		logger:      testLogger,
		broker:      r,
		queues:      []string{"default"},
		gracePeriod: gracePeriod,
		maxDelay:    0,
		maxSize:     0,
		groupAggregator: GroupAggregatorFunc(func(gname string, tasks []*Task) *Task {
			return NewTask(gname, nil, MaxRetry(len(tasks)))
		}),
	})
	var wg sync.WaitGroup
	aggregator.start(&wg)
	return aggregator, &wg
}

// TestAggregatorWithBatchRoster verifies that a roster-gated group only produces
// its aggregated task after every roster member arrived, even long after the
// grace period, while ordinary groups without a roster keep aggregating on time.
func TestAggregatorWithBatchRoster(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	client := Client{broker: rdbClient}
	ctx := context.Background()

	const gated = "gated-group"
	const plain = "plain-group"

	aggregator, wg := newTestAggregator(rdbClient, 1*time.Second)
	defer func() {
		aggregator.shutdown()
		wg.Wait()
	}()

	members := []string{"m1", "m2", "m3"}
	if err := client.ApplyGroupRoster(ctx, GroupRoster{
		Group:   gated,
		BatchID: "batch-1",
		Members: members,
	}); err != nil {
		t.Fatalf("ApplyGroupRoster failed: %v", err)
	}

	for _, id := range members[:2] {
		if _, err := client.Enqueue(NewTask("import", nil, TaskID(id), Group(gated))); err != nil {
			t.Fatalf("Enqueue %s failed: %v", id, err)
		}
	}
	// Ordinary group tasks in the same queue must not be affected by the roster.
	for _, id := range []string{"p1", "p2"} {
		if _, err := client.Enqueue(NewTask("plain", nil, TaskID(id), Group(plain))); err != nil {
			t.Fatalf("Enqueue %s failed: %v", id, err)
		}
	}

	// Well past the grace period: the gated group must still be waiting for m3
	// while the plain group gets aggregated.
	time.Sleep(3 * time.Second)
	if got := h.GetGroupEntries(t, r, "default", gated); len(got) != 2 {
		t.Errorf("gated group entries = %d, want 2 (roster not complete)", len(got))
	}
	waitFor(t, 5*time.Second, func() bool {
		return len(h.GetPendingMessages(t, r, "default")) == 1
	}, "plain group to be aggregated on grace period")
	plainPending := h.GetPendingMessages(t, r, "default")
	if len(plainPending) != 1 || plainPending[0].Retry != 2 {
		t.Errorf("plain group aggregation = %+v, want one task with Retry=2", plainPending)
	}
	if n, _ := r.Exists(ctx, base.RosterKey("default", gated)).Result(); n != 1 {
		t.Fatal("roster of the gated group must remain while it is waiting")
	}

	// Complete the roster: the aggregated task can now be produced.
	if _, err := client.Enqueue(NewTask("import", nil, TaskID("m3"), Group(gated))); err != nil {
		t.Fatalf("Enqueue m3 failed: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		return len(h.GetPendingMessages(t, r, "default")) == 2
	}, "gated group to be aggregated after roster completion")
	pending := h.GetPendingMessages(t, r, "default")
	var gatedTask *base.TaskMessage
	for _, msg := range pending {
		if msg.Type == gated {
			gatedTask = msg
		}
	}
	if gatedTask == nil {
		t.Fatalf("aggregated task for gated group not found in pending: %+v", pending)
	}
	if gatedTask.Retry != 3 {
		t.Errorf("gated aggregated task Retry = %d, want 3", gatedTask.Retry)
	}
	if got := h.GetGroupEntries(t, r, "default", gated); len(got) != 0 {
		t.Errorf("gated group not emptied after aggregation, got %d entries", len(got))
	}
	waitFor(t, 2*time.Second, func() bool {
		n, _ := r.Exists(ctx, base.RosterKey("default", gated),
			base.RosterMembersKey("default", gated)).Result()
		return n == 0
	}, "roster keys to be cleaned up after aggregation")
}

// TestAggregatorBatchRosterMismatchKeepsWaiting verifies that an unreconcilable
// group fails explicitly: no aggregated task is created and every task keeps
// waiting in the group, with the roster left intact for intervention.
func TestAggregatorBatchRosterMismatchKeepsWaiting(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	client := Client{broker: rdbClient}
	ctx := context.Background()

	const gname = "mismatch-group"

	aggregator, wg := newTestAggregator(rdbClient, 1*time.Second)
	defer func() {
		aggregator.shutdown()
		wg.Wait()
	}()

	if err := client.ApplyGroupRoster(ctx, GroupRoster{
		Group:   gname,
		BatchID: "batch-1",
		Members: []string{"m1", "m2"},
	}); err != nil {
		t.Fatalf("ApplyGroupRoster failed: %v", err)
	}
	for _, id := range []string{"m1", "m2", "extra"} {
		if _, err := client.Enqueue(NewTask("import", nil, TaskID(id), Group(gname))); err != nil {
			t.Fatalf("Enqueue %s failed: %v", id, err)
		}
	}

	// Several aggregation checks run during this window; none may aggregate.
	time.Sleep(3 * time.Second)
	if got := h.GetGroupEntries(t, r, "default", gname); len(got) != 3 {
		t.Errorf("group entries = %d, want all 3 tasks still waiting", len(got))
	}
	if got := h.GetPendingMessages(t, r, "default"); len(got) != 0 {
		t.Errorf("pending messages = %d, want 0 (failed reconciliation must not aggregate)", len(got))
	}
	if n, _ := r.Exists(ctx, base.RosterKey("default", gname)).Result(); n != 1 {
		t.Error("roster must remain after a failed reconciliation")
	}
}

// TestApplyGroupRosterClientAPI covers the public client surface including
// validation, idempotency and conflict handling.
func TestApplyGroupRosterClientAPI(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	client := Client{broker: rdbClient}
	ctx := context.Background()

	if err := client.ApplyGroupRoster(ctx, GroupRoster{BatchID: "b", Members: []string{"m1"}}); err == nil {
		t.Error("expected error for empty group, got nil")
	}
	if err := client.ApplyGroupRoster(ctx, GroupRoster{Group: "g", Members: []string{"m1"}}); err == nil {
		t.Error("expected error for empty batch id, got nil")
	}
	if err := client.ApplyGroupRoster(ctx, GroupRoster{Group: "g", BatchID: "b"}); err == nil {
		t.Error("expected error for empty members, got nil")
	}
	if err := client.ApplyGroupRoster(ctx, GroupRoster{
		Group: "g", BatchID: "b", Members: []string{"m1", "m1"},
	}); !errors.Is(err, ErrRosterDuplicateMember) {
		t.Errorf("duplicate members: got %v, want ErrRosterDuplicateMember", err)
	}

	if err := client.ApplyGroupRoster(ctx, GroupRoster{
		Group: "g", BatchID: "b", Members: []string{"m1", "m2"},
	}); err != nil {
		t.Fatalf("first ApplyGroupRoster failed: %v", err)
	}
	// Re-applying the exact same roster is idempotent.
	if err := client.ApplyGroupRoster(ctx, GroupRoster{
		Group: "g", BatchID: "b", Members: []string{"m2", "m1"},
	}); err != nil {
		t.Errorf("idempotent ApplyGroupRoster failed: %v", err)
	}
	// Same batch id with different members is a conflict.
	if err := client.ApplyGroupRoster(ctx, GroupRoster{
		Group: "g", BatchID: "b", Members: []string{"m1"},
	}); !errors.Is(err, ErrRosterBatchConflict) {
		t.Errorf("conflicting roster: got %v, want ErrRosterBatchConflict", err)
	}
	// A new batch id replaces the roster.
	if err := client.ApplyGroupRoster(ctx, GroupRoster{
		Group: "g", BatchID: "b2", Members: []string{"m1", "m3"},
	}); err != nil {
		t.Errorf("replacing ApplyGroupRoster failed: %v", err)
	}
	got, err := r.SMembers(ctx, base.RosterMembersKey("default", "g")).Result()
	if err != nil {
		t.Fatalf("SMembers failed: %v", err)
	}
	if diff := cmp.Diff(got, []string{"m1", "m3"},
		cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
		t.Errorf("roster members mismatch: (-got,+want)\n%s", diff)
	}
}
