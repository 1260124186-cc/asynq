// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	rdb "github.com/hibiken/asynq/internal/rdb"
	h "github.com/hibiken/asynq/internal/testutil"
)

// rebaseWindowsFor builds two overlapping outage windows ending in the past
// and returns them together with the expected union duration.
func rebaseWindowsFor(now time.Time) ([]OutageWindow, time.Duration) {
	w1 := OutageWindow{Start: now.Add(-25 * time.Minute), End: now.Add(-15 * time.Minute)}
	w2 := OutageWindow{Start: now.Add(-20 * time.Minute), End: now.Add(-5 * time.Minute)}
	// Union: [now-25m, now-5m) -> 20 minutes.
	return []OutageWindow{w2, w1}, 20 * time.Minute
}

func TestInspectorRebaseTaskTiming(t *testing.T) {
	redisConn := setup(t)
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()
	client := NewClient(getRedisConnOpt(t))
	defer client.Close()
	ctx := context.Background()
	h.FlushDB(t, redisConn)

	now := time.Now()
	windows, shift := rebaseWindowsFor(now)
	shiftSec := int64(shift.Seconds())

	// Scheduled task with an absolute deadline in the affected queue.
	schedInfo, err := client.Enqueue(
		NewTask("rebase:scheduled", nil),
		ProcessIn(30*time.Minute),
		Deadline(now.Add(2*time.Hour)),
		Timeout(time.Hour),
		MaxRetry(5),
		Queue("default"),
	)
	if err != nil {
		t.Fatalf("Enqueue scheduled: %v", err)
	}

	// Pending task (already eligible for immediate processing when the outage
	// hit) in the affected queue.
	r := rdb.NewRDB(redisConn)
	pendingMsg := h.NewTaskMessageWithQueue("rebase:pending", nil, base.DefaultQueueName)
	pendingMsg.Timeout = 1800
	pendingMsg.Retry = 9
	pendingMsg.Retried = 1
	pendingMsg.Deadline = now.Add(90 * time.Minute).Unix()
	if err := r.Enqueue(ctx, pendingMsg); err != nil {
		t.Fatalf("Enqueue pending: %v", err)
	}

	// Scheduled task in a queue that is NOT affected.
	otherInfo, err := client.Enqueue(
		NewTask("rebase:other", nil),
		ProcessIn(45*time.Minute),
		Deadline(now.Add(3*time.Hour)),
		Queue("critical"),
	)
	if err != nil {
		t.Fatalf("Enqueue other: %v", err)
	}

	beforeSched, err := inspector.GetTaskInfo("default", schedInfo.ID)
	if err != nil {
		t.Fatalf("GetTaskInfo before rebase: %v", err)
	}
	beforePending, err := inspector.GetTaskInfo("default", pendingMsg.ID)
	if err != nil {
		t.Fatalf("GetTaskInfo pending before rebase: %v", err)
	}
	beforeOther, err := inspector.GetTaskInfo("critical", otherInfo.ID)
	if err != nil {
		t.Fatalf("GetTaskInfo other before rebase: %v", err)
	}

	reports, err := inspector.RebaseTaskTiming([]string{"default"}, windows)
	if err != nil {
		t.Fatalf("RebaseTaskTiming: %v", err)
	}
	if len(reports) != 1 || reports[0].Queue != "default" ||
		reports[0].Shifted != 2 || reports[0].Skipped != 0 {
		t.Fatalf("reports = %+v, want one report with Shifted=2 Skipped=0", reports)
	}

	afterSched, err := inspector.GetTaskInfo("default", schedInfo.ID)
	if err != nil {
		t.Fatalf("GetTaskInfo after rebase: %v", err)
	}
	if got := afterSched.NextProcessAt.Sub(beforeSched.NextProcessAt); abs(got-shift) > time.Second {
		t.Errorf("scheduled NextProcessAt shift = %v, want %v", got, shift)
	}
	if afterSched.Deadline.Unix() != beforeSched.Deadline.Unix()+shiftSec {
		t.Errorf("scheduled Deadline = %v, want %v",
			afterSched.Deadline, time.Unix(beforeSched.Deadline.Unix()+shiftSec, 0))
	}
	if afterSched.Timeout != beforeSched.Timeout ||
		afterSched.MaxRetry != beforeSched.MaxRetry {
		t.Error("Timeout/MaxRetry changed after rebase")
	}
	if afterSched.State != TaskStateScheduled {
		t.Errorf("scheduled state = %v, want scheduled", afterSched.State)
	}

	afterPending, err := inspector.GetTaskInfo("default", pendingMsg.ID)
	if err != nil {
		t.Fatalf("GetTaskInfo pending after rebase: %v", err)
	}
	if afterPending.State != TaskStateScheduled {
		t.Errorf("pending-derived state = %v, want scheduled", afterPending.State)
	}
	if want := now.Add(shift); abs(afterPending.NextProcessAt.Sub(want)) > 2*time.Second {
		t.Errorf("pending-derived NextProcessAt = %v, want ~%v", afterPending.NextProcessAt, want)
	}
	if afterPending.Deadline.Unix() != beforePending.Deadline.Unix()+shiftSec {
		t.Errorf("pending-derived Deadline = %v, want %v",
			afterPending.Deadline, time.Unix(beforePending.Deadline.Unix()+shiftSec, 0))
	}
	if afterPending.Timeout != 1800*time.Second || afterPending.MaxRetry != 9 ||
		afterPending.Retried != 1 {
		t.Error("pending task Timeout/Retry/Retried changed after rebase")
	}

	// The other queue is fully untouched, including its query results.
	afterOther, err := inspector.GetTaskInfo("critical", otherInfo.ID)
	if err != nil {
		t.Fatalf("GetTaskInfo other after rebase: %v", err)
	}
	if !afterOther.NextProcessAt.Equal(beforeOther.NextProcessAt) ||
		!afterOther.Deadline.Equal(beforeOther.Deadline) {
		t.Error("task in unaffected queue was modified")
	}

	// Re-driving the identical window set (same windows, reversed order) must
	// not shift anything a second time.
	rerunReports, err := inspector.RebaseTaskTiming([]string{"default"},
		[]OutageWindow{windows[1], windows[0]})
	if err != nil {
		t.Fatalf("rerun RebaseTaskTiming: %v", err)
	}
	if rerunReports[0].Shifted != 0 || rerunReports[0].Skipped != 2 {
		t.Fatalf("rerun reports = %+v, want Shifted=0 Skipped=2", rerunReports)
	}
	again, err := inspector.GetTaskInfo("default", schedInfo.ID)
	if err != nil {
		t.Fatalf("GetTaskInfo after rerun: %v", err)
	}
	if !again.NextProcessAt.Equal(afterSched.NextProcessAt) ||
		!again.Deadline.Equal(afterSched.Deadline) {
		t.Error("task shifted a second time on rerun")
	}

	// A different but overlapping window must be rejected without writes.
	conflict := []OutageWindow{{Start: now.Add(-10 * time.Minute), End: now.Add(2 * time.Minute)}}
	if _, err := inspector.RebaseTaskTiming([]string{"default"}, conflict); err == nil {
		t.Fatal("overlapping window re-submission should fail")
	}
	postConflict, err := inspector.GetTaskInfo("default", schedInfo.ID)
	if err != nil {
		t.Fatalf("GetTaskInfo after conflict: %v", err)
	}
	if !postConflict.NextProcessAt.Equal(afterSched.NextProcessAt) {
		t.Error("task mutated despite window conflict")
	}

	// Invalid arguments.
	if _, err := inspector.RebaseTaskTiming(nil, windows); err == nil {
		t.Error("empty queue list should return an error")
	}
	if _, err := inspector.RebaseTaskTiming([]string{"default"}, nil); err == nil {
		t.Error("empty window list should return an error")
	}
}

func TestRebaseWorkerDeadlineAndStartConsistency(t *testing.T) {
	redisConn := setup(t)
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()
	r := rdb.NewRDB(redisConn)
	h.FlushDB(t, redisConn)

	ctx := context.Background()
	now := time.Now()

	// Timeout is long so the absolute Deadline determines the worker's
	// context deadline.
	msg := h.NewTaskMessageWithQueue("rebase:worker", nil, base.DefaultQueueName)
	msg.Timeout = 3600
	msg.Retry = 3
	origDeadline := now.Add(90 * time.Second).Unix()
	msg.Deadline = origDeadline
	if err := r.Enqueue(ctx, msg); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Two overlapping windows with a 6-minute union.
	windows := []OutageWindow{
		{Start: now.Add(-10 * time.Minute), End: now.Add(-5 * time.Minute)},
		{Start: now.Add(-7 * time.Minute), End: now.Add(-4 * time.Minute)},
	}
	const shiftSec int64 = 360
	reports, err := inspector.RebaseTaskTiming([]string{"default"}, windows)
	if err != nil {
		t.Fatalf("RebaseTaskTiming: %v", err)
	}
	if reports[0].Shifted != 1 {
		t.Fatalf("Shifted = %d, want 1", reports[0].Shifted)
	}

	info, err := inspector.GetTaskInfo("default", msg.ID)
	if err != nil {
		t.Fatalf("GetTaskInfo: %v", err)
	}
	if info.State != TaskStateScheduled {
		t.Fatalf("state = %v, want scheduled (not started before shifted time)", info.State)
	}
	if info.Deadline.Unix() != origDeadline+shiftSec {
		t.Fatalf("stored deadline = %d, want %d", info.Deadline.Unix(), origDeadline+shiftSec)
	}

	// Worker must not start the task while its shifted process time is ahead.
	var (
		mu       sync.Mutex
		started  int
		workerDL time.Time
		haveDL   = make(chan struct{}, 1)
	)
	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		mu.Lock()
		started++
		dl, ok := ctx.Deadline()
		if ok {
			workerDL = dl
		}
		mu.Unlock()
		select {
		case haveDL <- struct{}{}:
		default:
		}
		return nil
	})
	p := newProcessorForTest(t, r, handler)
	p.start(&sync.WaitGroup{})
	time.Sleep(time.Second)
	mu.Lock()
	if started != 0 {
		t.Errorf("worker started task %d times before shifted schedule", started)
	}
	mu.Unlock()

	// Release the task (simulates the forwarder firing at the shifted time).
	if err := inspector.RunTask("default", msg.ID); err != nil {
		p.shutdown()
		t.Fatalf("RunTask: %v", err)
	}
	select {
	case <-haveDL:
	case <-time.After(3 * time.Second):
		p.shutdown()
		t.Fatal("worker did not process the released task")
	}
	p.shutdown()

	// The worker's actual context deadline must match the shifted absolute
	// Deadline stored on the message (computeDeadline takes the min of
	// now+Timeout and Deadline).
	want := time.Unix(origDeadline+shiftSec, 0)
	if diff := abs(workerDL.Sub(want)); diff > 2*time.Second {
		t.Errorf("worker context deadline = %v, want ~%v (diff %v)", workerDL, want, diff)
	}
}

func abs(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
