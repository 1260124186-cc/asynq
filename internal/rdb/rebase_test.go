// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	h "github.com/hibiken/asynq/internal/testutil"
	"github.com/hibiken/asynq/internal/timeutil"
	"github.com/redis/go-redis/v9"
)

// fixedBase is a fixed point in time so tests never depend on wall clock.
var rebaseBase = time.Unix(1700000000, 0)

func rebaseWindows() []Window {
	// Two overlapping windows: [base, base+10m) and [base+5m, base+20m),
	// merging into [base, base+20m) with a union duration of 20 minutes.
	return []Window{
		{Start: rebaseBase.Add(5 * time.Minute), End: rebaseBase.Add(20 * time.Minute)},
		{Start: rebaseBase, End: rebaseBase.Add(10 * time.Minute)},
	}
}

const rebaseShiftSec int64 = 20 * 60

func newRebaseMsg(qname string, withDeadline bool) *base.TaskMessage {
	msg := h.NewTaskMessageWithQueue("rebase_task", h.JSON(map[string]interface{}{"k": qname}), qname)
	msg.Timeout = 900
	msg.Retry = 7
	msg.Retried = 2
	if withDeadline {
		msg.Deadline = rebaseBase.Add(40 * time.Minute).Unix()
	}
	return msg
}

func TestMergeWindows(t *testing.T) {
	tests := []struct {
		name        string
		windows     []Window
		wantRanges  []TimeRange
		wantShift   time.Duration
		wantErrCode errors.Code
	}{
		{
			name: "overlapping windows merge into one interval",
			windows: []Window{
				{Start: rebaseBase, End: rebaseBase.Add(10 * time.Minute)},
				{Start: rebaseBase.Add(5 * time.Minute), End: rebaseBase.Add(20 * time.Minute)},
			},
			wantRanges: []TimeRange{
				{Start: rebaseBase, End: rebaseBase.Add(20 * time.Minute)},
			},
			wantShift: 20 * time.Minute,
		},
		{
			name: "touching intervals merge",
			windows: []Window{
				{Start: rebaseBase, End: rebaseBase.Add(10 * time.Minute)},
				{Start: rebaseBase.Add(10 * time.Minute), End: rebaseBase.Add(20 * time.Minute)},
			},
			wantRanges: []TimeRange{
				{Start: rebaseBase, End: rebaseBase.Add(20 * time.Minute)},
			},
			wantShift: 20 * time.Minute,
		},
		{
			name: "disjoint intervals stay separate and durations add up",
			windows: []Window{
				{Start: rebaseBase.Add(30 * time.Minute), End: rebaseBase.Add(40 * time.Minute)},
				{Start: rebaseBase, End: rebaseBase.Add(10 * time.Minute)},
			},
			wantRanges: []TimeRange{
				{Start: rebaseBase, End: rebaseBase.Add(10 * time.Minute)},
				{Start: rebaseBase.Add(30 * time.Minute), End: rebaseBase.Add(40 * time.Minute)},
			},
			wantShift: 20 * time.Minute,
		},
		{
			name: "duplicate windows count once",
			windows: []Window{
				{Start: rebaseBase, End: rebaseBase.Add(10 * time.Minute)},
				{Start: rebaseBase, End: rebaseBase.Add(10 * time.Minute)},
			},
			wantRanges: []TimeRange{
				{Start: rebaseBase, End: rebaseBase.Add(10 * time.Minute)},
			},
			wantShift: 10 * time.Minute,
		},
		{
			name:        "empty windows rejected",
			windows:     nil,
			wantErrCode: errors.FailedPrecondition,
		},
		{
			name: "end before start rejected",
			windows: []Window{
				{Start: rebaseBase.Add(10 * time.Minute), End: rebaseBase},
			},
			wantErrCode: errors.FailedPrecondition,
		},
		{
			name: "zero time rejected",
			windows: []Window{
				{Start: time.Time{}, End: rebaseBase},
			},
			wantErrCode: errors.FailedPrecondition,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			merged, shift, fp, err := MergeWindows(tc.windows)
			if tc.wantErrCode != 0 {
				if err == nil {
					t.Fatalf("MergeWindows returned nil error, want code %v", tc.wantErrCode)
				}
				if got := errors.CanonicalCode(err); got != tc.wantErrCode {
					t.Fatalf("MergeWindows error code = %v, want %v", got, tc.wantErrCode)
				}
				return
			}
			if err != nil {
				t.Fatalf("MergeWindows returned error: %v", err)
			}
			if diff := cmp.Diff(tc.wantRanges, merged); diff != "" {
				t.Errorf("merged ranges mismatch (-want,+got):\n%s", diff)
			}
			if shift != tc.wantShift {
				t.Errorf("shift = %v, want %v", shift, tc.wantShift)
			}
			if len(fp) != 64 { // sha256 hex
				t.Errorf("fingerprint length = %d, want 64", len(fp))
			}
			// Fingerprint must be order-independent.
			reversed := make([]Window, len(tc.windows))
			for i, w := range tc.windows {
				reversed[len(tc.windows)-1-i] = w
			}
			_, _, fp2, err := MergeWindows(reversed)
			if err != nil {
				t.Fatalf("MergeWindows(reversed) error: %v", err)
			}
			if fp != fp2 {
				t.Errorf("fingerprint changed with window order: %q != %q", fp, fp2)
			}
		})
	}
}

func TestRebaseTaskTimingShiftsNotStartedTasks(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	simNow := rebaseBase.Add(30 * time.Minute)
	r.SetClock(timeutil.NewSimulatedClock(simNow))

	const q, other = "default", "other"

	// Not-started tasks in the affected queue.
	s1 := newRebaseMsg(q, true)
	r1 := newRebaseMsg(q, true)
	p1 := newRebaseMsg(q, true)
	s2 := newRebaseMsg(q, false) // no deadline configured

	// Tasks that must never be touched.
	a1 := newRebaseMsg(q, true)     // active (running)
	c1 := newRebaseMsg(q, true)     // completed
	ar1 := newRebaseMsg(q, true)    // archived
	o1 := newRebaseMsg(other, true) // another queue

	h.SeedScheduledQueue(t, r.client, []base.Z{
		{Message: s1, Score: rebaseBase.Add(7 * time.Minute).Unix()},
		{Message: s2, Score: rebaseBase.Add(30 * time.Minute).Unix()},
	}, q)
	h.SeedRetryQueue(t, r.client, []base.Z{
		{Message: r1, Score: rebaseBase.Add(15 * time.Minute).Unix()},
	}, q)
	h.SeedPendingQueue(t, r.client, []*base.TaskMessage{p1}, q)
	// pending_since should be removed when the task moves to scheduled.
	r.client.HSet(ctx, base.TaskKey(q, p1.ID), "pending_since", rebaseBase.UnixNano())
	h.SeedActiveQueue(t, r.client, []*base.TaskMessage{a1}, q)
	h.SeedCompletedQueue(t, r.client, []base.Z{
		{Message: c1, Score: rebaseBase.Add(-time.Minute).Unix()},
	}, q)
	h.SeedArchivedQueue(t, r.client, []base.Z{
		{Message: ar1, Score: rebaseBase.Add(-2 * time.Minute).Unix()},
	}, q)
	h.SeedScheduledQueue(t, r.client, []base.Z{
		{Message: o1, Score: rebaseBase.Add(7 * time.Minute).Unix()},
	}, other)

	// Snapshot the records that must remain untouched.
	untouched := map[string]string{
		base.TaskKey(q, a1.ID):     r.client.HGet(ctx, base.TaskKey(q, a1.ID), "msg").Val(),
		base.TaskKey(q, c1.ID):     r.client.HGet(ctx, base.TaskKey(q, c1.ID), "msg").Val(),
		base.TaskKey(q, ar1.ID):    r.client.HGet(ctx, base.TaskKey(q, ar1.ID), "msg").Val(),
		base.TaskKey(other, o1.ID): r.client.HGet(ctx, base.TaskKey(other, o1.ID), "msg").Val(),
	}
	o1Score := r.client.ZScore(ctx, base.ScheduledKey(other), o1.ID).Val()

	reports, err := r.RebaseTaskTiming(ctx, []string{q}, rebaseWindows())
	if err != nil {
		t.Fatalf("RebaseTaskTiming failed: %v", err)
	}
	if len(reports) != 1 || reports[0].Queue != q || reports[0].Shifted != 4 || reports[0].Skipped != 0 {
		t.Fatalf("reports = %+v, want single report with Shifted=4 Skipped=0", reports)
	}

	// Scheduled task with deadline: score and deadline both shifted, other
	// message fields (Timeout/Retry/Retried) preserved.
	checkZSetTaskShifted(t, r, q, base.TaskStateScheduled, s1,
		rebaseBase.Add(7*time.Minute).Unix(), true)
	checkZSetTaskShifted(t, r, q, base.TaskStateRetry, r1,
		rebaseBase.Add(15*time.Minute).Unix(), true)
	checkZSetTaskShifted(t, r, q, base.TaskStateScheduled, s2,
		rebaseBase.Add(30*time.Minute).Unix(), false)

	// Pending task moved into scheduled at now + union duration.
	pendingIDs := r.client.LRange(ctx, base.PendingKey(q), 0, -1).Val()
	if len(pendingIDs) != 0 {
		t.Errorf("pending list = %v, want empty after rebase", pendingIDs)
	}
	gotScore, err := r.client.ZScore(ctx, base.ScheduledKey(q), p1.ID).Result()
	if err != nil {
		t.Fatalf("pending task %s not found in scheduled zset: %v", p1.ID, err)
	}
	if want := float64(simNow.Unix() + rebaseShiftSec); gotScore != want {
		t.Errorf("shifted pending task score = %v, want %v", gotScore, want)
	}
	p1State := r.client.HGet(ctx, base.TaskKey(q, p1.ID), "state").Val()
	if p1State != "scheduled" {
		t.Errorf("pending task state = %q, want scheduled", p1State)
	}
	if exist := r.client.Exists(ctx, base.TaskKey(q, p1.ID)).Val(); exist == 0 {
		t.Fatal("task key missing")
	}
	if exists, err := r.client.HExists(ctx, base.TaskKey(q, p1.ID), "pending_since").Result(); err != nil || exists {
		t.Errorf("pending_since should be removed, HExists=(%v,%v)", exists, err)
	}
	p1Got := h.MustUnmarshal(t, r.client.HGet(ctx, base.TaskKey(q, p1.ID), "msg").Val())
	if p1Got.Deadline != p1.Deadline+rebaseShiftSec {
		t.Errorf("pending task deadline = %d, want %d", p1Got.Deadline, p1.Deadline+rebaseShiftSec)
	}
	assertMsgPreserved(t, p1, p1Got)

	// Marker bookkeeping.
	_, _, fp, _ := MergeWindows(rebaseWindows())
	marker := rebaseMarkerKey(q, fp)
	if n := r.client.Exists(ctx, marker).Val(); n != 1 {
		t.Errorf("marker %q missing", marker)
	}
	if d := r.client.HGet(ctx, marker, "duration").Val(); d != "1200" {
		t.Errorf("marker duration = %q, want 1200", d)
	}
	if n := r.client.ZCard(ctx, rebaseIndexKey(q)).Val(); n != 1 {
		t.Errorf("rebase index size = %d, want 1", n)
	}
	if n := r.client.SCard(ctx, rebaseDoneKey(q, fp)).Val(); n != 4 {
		t.Errorf("done set size = %d, want 4", n)
	}

	// Untouched tasks keep their exact encoded message and location.
	for key, want := range untouched {
		if got := r.client.HGet(ctx, key, "msg").Val(); got != want {
			t.Errorf("task key %s was modified", key)
		}
	}
	if got := r.client.ZScore(ctx, base.ScheduledKey(other), o1.ID).Val(); got != o1Score {
		t.Errorf("other-queue task score changed: %v != %v", got, o1Score)
	}
	if got := r.client.LLen(ctx, base.ActiveKey(q)).Val(); got != 1 {
		t.Errorf("active list length = %d, want 1", got)
	}
	if got := r.client.ZCard(ctx, base.CompletedKey(q)).Val(); got != 1 {
		t.Errorf("completed set size = %d, want 1", got)
	}
	if got := r.client.ZCard(ctx, base.ArchivedKey(q)).Val(); got != 1 {
		t.Errorf("archived set size = %d, want 1", got)
	}
}

func checkZSetTaskShifted(t *testing.T, r *RDB, qname string, state base.TaskState,
	orig *base.TaskMessage, oldScore int64, withDeadline bool) {
	t.Helper()
	ctx := context.Background()
	key := base.ScheduledKey(qname)
	if state == base.TaskStateRetry {
		key = base.RetryKey(qname)
	}
	gotScore, err := r.client.ZScore(ctx, key, orig.ID).Result()
	if err != nil {
		t.Fatalf("task %s missing from %s: %v", orig.ID, key, err)
	}
	if want := float64(oldScore + rebaseShiftSec); gotScore != want {
		t.Errorf("task %s score = %v, want %v", orig.ID, gotScore, want)
	}
	got := h.MustUnmarshal(t, r.client.HGet(ctx, base.TaskKey(qname, orig.ID), "msg").Val())
	if gotState := r.client.HGet(ctx, base.TaskKey(qname, orig.ID), "state").Val(); gotState != state.String() {
		t.Errorf("task %s state = %q, want %q", orig.ID, gotState, state.String())
	}
	if withDeadline {
		if got.Deadline != orig.Deadline+rebaseShiftSec {
			t.Errorf("task %s deadline = %d, want %d", orig.ID, got.Deadline, orig.Deadline+rebaseShiftSec)
		}
	} else if got.Deadline != 0 {
		t.Errorf("task %s deadline = %d, want zero preserved", orig.ID, got.Deadline)
	}
	assertMsgPreserved(t, orig, got)
}

func assertMsgPreserved(t *testing.T, want, got *base.TaskMessage) {
	t.Helper()
	if got.Timeout != want.Timeout || got.Retry != want.Retry || got.Retried != want.Retried {
		t.Errorf("task %s timeout/retry changed: got (timeout=%d retry=%d retried=%d), want (%d,%d,%d)",
			want.ID, got.Timeout, got.Retry, got.Retried, want.Timeout, want.Retry, want.Retried)
	}
	if got.Type != want.Type || got.Queue != want.Queue || got.ID != want.ID {
		t.Errorf("task identity changed: got %+v", got)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Errorf("task %s payload changed", want.ID)
	}
}

func TestRebaseTaskTimingIdempotent(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	r.SetClock(timeutil.NewSimulatedClock(rebaseBase.Add(30 * time.Minute)))
	const q = "default"

	msgs := make([]*base.TaskMessage, 3)
	entries := make([]base.Z, 3)
	for i := range msgs {
		msgs[i] = newRebaseMsg(q, true)
		entries[i] = base.Z{Message: msgs[i], Score: rebaseBase.Add(time.Duration(i+1) * time.Minute).Unix()}
	}
	h.SeedScheduledQueue(t, r.client, entries, q)

	first, err := r.RebaseTaskTiming(ctx, []string{q}, rebaseWindows())
	if err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	if first[0].Shifted != 3 || first[0].Skipped != 0 {
		t.Fatalf("first report = %+v, want Shifted=3", first)
	}
	snapshot := map[string]string{}
	for _, m := range msgs {
		snapshot[m.ID] = r.client.HGet(ctx, base.TaskKey(q, m.ID), "msg").Val()
	}

	// Re-submit the same windows in reverse order.
	windows := rebaseWindows()
	windows[0], windows[1] = windows[1], windows[0]
	second, err := r.RebaseTaskTiming(ctx, []string{q}, windows)
	if err != nil {
		t.Fatalf("second run failed: %v", err)
	}
	if second[0].Shifted != 0 || second[0].Skipped != 3 {
		t.Fatalf("second report = %+v, want Shifted=0 Skipped=3", second)
	}
	// Third run, same thing: no double shift.
	third, err := r.RebaseTaskTiming(ctx, []string{q}, rebaseWindows())
	if err != nil {
		t.Fatalf("third run failed: %v", err)
	}
	if third[0].Shifted != 0 || third[0].Skipped != 3 {
		t.Fatalf("third report = %+v, want Shifted=0 Skipped=3", third)
	}
	for _, m := range msgs {
		if got := r.client.HGet(ctx, base.TaskKey(q, m.ID), "msg").Val(); got != snapshot[m.ID] {
			t.Errorf("task %s was shifted again on rerun", m.ID)
		}
	}
}

func TestRebaseTaskTimingResumesOnlyRemaining(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	r.SetClock(timeutil.NewSimulatedClock(rebaseBase.Add(30 * time.Minute)))
	const q = "default"

	preDone := newRebaseMsg(q, true) // simulates a task handled before interruption
	remaining := make([]*base.TaskMessage, 2)
	entries := []base.Z{{Message: preDone, Score: rebaseBase.Add(2 * time.Minute).Unix()}}
	for i := range remaining {
		remaining[i] = newRebaseMsg(q, true)
		entries = append(entries, base.Z{
			Message: remaining[i],
			Score:   rebaseBase.Add(time.Duration(3+i) * time.Minute).Unix(),
		})
	}
	h.SeedScheduledQueue(t, r.client, entries, q)

	// Simulate the state left behind by an interrupted run: marker plus a
	// progress set containing the one task already shifted.
	_, _, fp, _ := MergeWindows(rebaseWindows())
	marker := rebaseMarkerKey(q, fp)
	r.client.HSet(ctx, marker,
		"intervals", canonicalIntervals(mustMerge(t, rebaseWindows())),
		"duration", rebaseShiftSec,
		"created_at", rebaseBase.Unix(),
		"updated_at", rebaseBase.Unix())
	r.client.ZAdd(ctx, rebaseIndexKey(q), redis.Z{Score: float64(rebaseBase.Unix()), Member: fp})
	r.client.SAdd(ctx, rebaseDoneKey(q, fp), preDone.ID)

	preDoneMsgBefore := r.client.HGet(ctx, base.TaskKey(q, preDone.ID), "msg").Val()
	preDoneScoreBefore := r.client.ZScore(ctx, base.ScheduledKey(q), preDone.ID).Val()

	report, err := r.RebaseTaskTiming(ctx, []string{q}, rebaseWindows())
	if err != nil {
		t.Fatalf("resume run failed: %v", err)
	}
	if report[0].Shifted != 2 || report[0].Skipped != 1 {
		t.Fatalf("report = %+v, want Shifted=2 Skipped=1", report)
	}
	// The previously handled task is not shifted a second time.
	if got := r.client.HGet(ctx, base.TaskKey(q, preDone.ID), "msg").Val(); got != preDoneMsgBefore {
		t.Error("already-handled task message changed on resume")
	}
	if got := r.client.ZScore(ctx, base.ScheduledKey(q), preDone.ID).Val(); got != preDoneScoreBefore {
		t.Error("already-handled task score changed on resume")
	}
	// Remaining tasks were shifted exactly once.
	for _, m := range remaining {
		got := h.MustUnmarshal(t, r.client.HGet(ctx, base.TaskKey(q, m.ID), "msg").Val())
		if got.Deadline != m.Deadline+rebaseShiftSec {
			t.Errorf("remaining task %s deadline = %d, want %d", m.ID, got.Deadline, m.Deadline+rebaseShiftSec)
		}
	}
	if n := r.client.SCard(ctx, rebaseDoneKey(q, fp)).Val(); n != 3 {
		t.Errorf("done set size = %d, want 3", n)
	}
}

func TestRebaseTaskTimingOverlappingWindowConflict(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	const q = "default"

	msg := newRebaseMsg(q, true)
	h.SeedScheduledQueue(t, r.client, []base.Z{
		{Message: msg, Score: rebaseBase.Add(2 * time.Minute).Unix()},
	}, q)

	if _, err := r.RebaseTaskTiming(ctx, []string{q}, rebaseWindows()); err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	// Snapshot after the first, successful shift.
	before := r.client.HGet(ctx, base.TaskKey(q, msg.ID), "msg").Val()

	// A different window overlapping the already-registered outage must be
	// rejected and must not mutate anything.
	conflicting := []Window{
		{Start: rebaseBase.Add(15 * time.Minute), End: rebaseBase.Add(25 * time.Minute)},
	}
	_, err := r.RebaseTaskTiming(ctx, []string{q}, conflicting)
	if err == nil || errors.CanonicalCode(err) != errors.FailedPrecondition {
		t.Fatalf("conflicting windows: err = %v, want FailedPrecondition", err)
	}
	if got := r.client.HGet(ctx, base.TaskKey(q, msg.ID), "msg").Val(); got != before {
		t.Error("task mutated despite conflicting windows")
	}
	if n := r.client.ZCard(ctx, rebaseIndexKey(q)).Val(); n != 1 {
		t.Errorf("index size = %d, want still 1", n)
	}

	// Disjoint windows are independent outages and are allowed.
	disjoint := []Window{
		{Start: rebaseBase.Add(40 * time.Minute), End: rebaseBase.Add(50 * time.Minute)},
	}
	if _, err := r.RebaseTaskTiming(ctx, []string{q}, disjoint); err != nil {
		t.Fatalf("disjoint windows should be allowed, got %v", err)
	}
	got := h.MustUnmarshal(t, r.client.HGet(ctx, base.TaskKey(q, msg.ID), "msg").Val())
	// 20m from the first outage, then 10m disjoint outage.
	if want := msg.Deadline + rebaseShiftSec + 10*60; got.Deadline != want {
		t.Errorf("deadline after two disjoint outages = %d, want %d", got.Deadline, want)
	}
}

func TestRebaseTaskTimingMarkerConflict(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	const q = "default"

	msg := newRebaseMsg(q, true)
	h.SeedScheduledQueue(t, r.client, []base.Z{
		{Message: msg, Score: rebaseBase.Add(2 * time.Minute).Unix()},
	}, q)
	if _, err := r.RebaseTaskTiming(ctx, []string{q}, rebaseWindows()); err != nil {
		t.Fatalf("first run failed: %v", err)
	}
	before := r.client.HGet(ctx, base.TaskKey(q, msg.ID), "msg").Val()

	_, _, fp, _ := MergeWindows(rebaseWindows())
	r.client.HSet(ctx, rebaseMarkerKey(q, fp), "duration", 999)

	_, err := r.RebaseTaskTiming(ctx, []string{q}, rebaseWindows())
	if err == nil || errors.CanonicalCode(err) != errors.FailedPrecondition {
		t.Fatalf("marker conflict: err = %v, want FailedPrecondition", err)
	}
	if got := r.client.HGet(ctx, base.TaskKey(q, msg.ID), "msg").Val(); got != before {
		t.Error("task mutated despite marker conflict")
	}
}

func TestRebaseTaskTimingTaskRecordConflicts(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, r *RDB, q string, id string)
	}{
		{
			name: "task hash missing",
			setup: func(t *testing.T, r *RDB, q, id string) {
				// Only the zset member exists; no task hash.
				r.client.ZAdd(context.Background(), base.ScheduledKey(q),
					redis.Z{Member: id, Score: float64(rebaseBase.Unix())})
			},
		},
		{
			name: "state field conflicts with container",
			setup: func(t *testing.T, r *RDB, q, id string) {
				msg := newRebaseMsg(q, true)
				msg.ID = id
				h.SeedScheduledQueue(t, r.client, []base.Z{
					{Message: msg, Score: rebaseBase.Unix()},
				}, q)
				r.client.HSet(context.Background(), base.TaskKey(q, id), "state", "active")
			},
		},
		{
			name: "message bytes undecodable",
			setup: func(t *testing.T, r *RDB, q, id string) {
				msg := newRebaseMsg(q, true)
				msg.ID = id
				h.SeedScheduledQueue(t, r.client, []base.Z{
					{Message: msg, Score: rebaseBase.Unix()},
				}, q)
				r.client.HSet(context.Background(), base.TaskKey(q, id), "msg", "not-a-protobuf")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := setup(t)
			defer r.Close()
			ctx := context.Background()
			const q = "default"

			badID := uuid.NewString()
			tc.setup(t, r, q, badID)

			// A valid sibling task proves the failed run wrote nothing.
			good := newRebaseMsg(q, true)
			h.SeedScheduledQueue(t, r.client, []base.Z{
				{Message: good, Score: rebaseBase.Add(3 * time.Minute).Unix()},
			}, q)
			goodBefore := r.client.HGet(ctx, base.TaskKey(q, good.ID), "msg").Val()
			goodScoreBefore := r.client.ZScore(ctx, base.ScheduledKey(q), good.ID).Val()

			_, err := r.RebaseTaskTiming(ctx, []string{q}, rebaseWindows())
			if err == nil || errors.CanonicalCode(err) != errors.FailedPrecondition &&
				errors.CanonicalCode(err) != errors.Internal {
				t.Fatalf("err = %v, want failure", err)
			}

			// No marker, index entry, progress set or task mutation.
			if n := r.client.ZCard(ctx, rebaseIndexKey(q)).Val(); n != 0 {
				t.Errorf("rebase index size = %d, want 0 (no writes on failure)", n)
			}
			if n := r.client.Keys(ctx, base.QueueKeyPrefix(q)+"rebase:*").Val(); len(n) != 0 {
				t.Errorf("rebase keys created on failure: %v", n)
			}
			if got := r.client.HGet(ctx, base.TaskKey(q, good.ID), "msg").Val(); got != goodBefore {
				t.Error("sibling task mutated on failed run")
			}
			if got := r.client.ZScore(ctx, base.ScheduledKey(q), good.ID).Val(); got != goodScoreBefore {
				t.Error("sibling task score mutated on failed run")
			}
		})
	}
}

func TestRebaseTaskTimingQueueNotFound(t *testing.T) {
	r := setup(t)
	defer r.Close()
	_, err := r.RebaseTaskTiming(context.Background(), []string{"missing"}, rebaseWindows())
	if err == nil || !errors.IsQueueNotFound(err) {
		t.Fatalf("err = %v, want QueueNotFoundError", err)
	}
}

func TestRebaseTaskTimingValidation(t *testing.T) {
	r := setup(t)
	defer r.Close()
	if _, err := r.RebaseTaskTiming(context.Background(), nil, rebaseWindows()); err == nil {
		t.Error("empty queue list should fail")
	}
	if _, err := r.RebaseTaskTiming(context.Background(), []string{"default"}, nil); err == nil {
		t.Error("empty windows should fail")
	}
}

func TestRebaseTaskTimingGetTaskInfoConsistency(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	simNow := rebaseBase.Add(30 * time.Minute)
	r.SetClock(timeutil.NewSimulatedClock(simNow))
	const q = "default"

	sched := newRebaseMsg(q, true)
	retry := newRebaseMsg(q, true)
	pending := newRebaseMsg(q, true)
	h.SeedScheduledQueue(t, r.client, []base.Z{
		{Message: sched, Score: rebaseBase.Add(7 * time.Minute).Unix()},
	}, q)
	h.SeedRetryQueue(t, r.client, []base.Z{
		{Message: retry, Score: rebaseBase.Add(15 * time.Minute).Unix()},
	}, q)
	h.SeedPendingQueue(t, r.client, []*base.TaskMessage{pending}, q)

	if _, err := r.RebaseTaskTiming(ctx, []string{q}, rebaseWindows()); err != nil {
		t.Fatalf("RebaseTaskTiming failed: %v", err)
	}

	info, err := r.GetTaskInfo(q, sched.ID)
	if err != nil {
		t.Fatalf("GetTaskInfo(scheduled): %v", err)
	}
	if want := time.Unix(rebaseBase.Add(7*time.Minute).Unix()+rebaseShiftSec, 0); !info.NextProcessAt.Equal(want) {
		t.Errorf("scheduled NextProcessAt = %v, want %v", info.NextProcessAt, want)
	}
	if info.Message.Deadline != sched.Deadline+rebaseShiftSec {
		t.Errorf("scheduled Deadline = %d, want %d", info.Message.Deadline, sched.Deadline+rebaseShiftSec)
	}

	info, err = r.GetTaskInfo(q, retry.ID)
	if err != nil {
		t.Fatalf("GetTaskInfo(retry): %v", err)
	}
	if want := time.Unix(rebaseBase.Add(15*time.Minute).Unix()+rebaseShiftSec, 0); !info.NextProcessAt.Equal(want) {
		t.Errorf("retry NextProcessAt = %v, want %v", info.NextProcessAt, want)
	}

	info, err = r.GetTaskInfo(q, pending.ID)
	if err != nil {
		t.Fatalf("GetTaskInfo(pending): %v", err)
	}
	if info.State != base.TaskStateScheduled {
		t.Errorf("pending-derived state = %v, want scheduled", info.State)
	}
	if want := time.Unix(simNow.Unix()+rebaseShiftSec, 0); !info.NextProcessAt.Equal(want) {
		t.Errorf("pending-derived NextProcessAt = %v, want %v", info.NextProcessAt, want)
	}
	if info.Message.Deadline != pending.Deadline+rebaseShiftSec {
		t.Errorf("pending-derived Deadline = %d, want %d", info.Message.Deadline, pending.Deadline+rebaseShiftSec)
	}
}

func TestRebaseTaskTimingBatchingAndForwarding(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	// Start before every shifted score (all scores land at base+20m or later).
	clock := timeutil.NewSimulatedClock(rebaseBase)
	r.SetClock(clock)
	const q = "default"
	const total = rebaseApplyBatchSize + 50

	entries := make([]base.Z, total)
	origDeadline := make(map[string]int64, total)
	for i := 0; i < total; i++ {
		msg := newRebaseMsg(q, true)
		entries[i] = base.Z{Message: msg, Score: rebaseBase.Add(time.Duration(i) * time.Second).Unix()}
		origDeadline[msg.ID] = msg.Deadline
	}
	h.SeedScheduledQueue(t, r.client, entries, q)

	report, err := r.RebaseTaskTiming(ctx, []string{q}, rebaseWindows())
	if err != nil {
		t.Fatalf("RebaseTaskTiming failed: %v", err)
	}
	if report[0].Shifted != total {
		t.Fatalf("Shifted = %d, want %d", report[0].Shifted, total)
	}
	for _, e := range entries {
		got := h.MustUnmarshal(t, r.client.HGet(ctx, base.TaskKey(q, e.Message.ID), "msg").Val())
		if got.Deadline != origDeadline[e.Message.ID]+rebaseShiftSec {
			t.Fatalf("task %s deadline not shifted", e.Message.ID)
		}
	}

	// Before the shifted process time, nothing is forwarded or dequeued.
	if err := r.ForwardIfReady(q); err != nil {
		t.Fatalf("ForwardIfReady: %v", err)
	}
	if n := r.client.LLen(ctx, base.PendingKey(q)).Val(); n != 0 {
		t.Fatalf("pending list size = %d, want 0 before shifted time", n)
	}
	if _, _, err := r.Dequeue(q); err == nil {
		t.Fatal("Dequeue should fail while shifted tasks are still scheduled")
	}

	// Once the clock passes every shifted score, forwarding and dequeue work,
	// and the worker receives the shifted deadline record.
	clock.SetTime(rebaseBase.Add(2 * time.Hour))
	if err := r.ForwardIfReady(q); err != nil {
		t.Fatalf("ForwardIfReady after clock advance: %v", err)
	}
	if n := r.client.LLen(ctx, base.PendingKey(q)).Val(); n != total {
		t.Fatalf("pending list size = %d, want %d", n, total)
	}
	msg, _, err := r.Dequeue(q)
	if err != nil {
		t.Fatalf("Dequeue after forwarding: %v", err)
	}
	if msg.Deadline != origDeadline[msg.ID]+rebaseShiftSec {
		t.Errorf("dequeued task deadline = %d, want shifted %d",
			msg.Deadline, origDeadline[msg.ID]+rebaseShiftSec)
	}
	if msg.Timeout != 900 || msg.Retry != 7 || msg.Retried != 2 {
		t.Errorf("dequeued task timeout/retry fields changed: %+v", msg)
	}
}

func mustMerge(t *testing.T, windows []Window) []TimeRange {
	t.Helper()
	merged, _, _, err := MergeWindows(windows)
	if err != nil {
		t.Fatalf("MergeWindows: %v", err)
	}
	return merged
}
