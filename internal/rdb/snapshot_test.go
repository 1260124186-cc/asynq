// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/snapshot"
	h "github.com/hibiken/asynq/internal/testutil"
	"github.com/redis/go-redis/v9"
)

const snapTestQueue = "snapq"

func snapMsg(id string, gkey string) *base.TaskMessage {
	return &base.TaskMessage{
		Type:         "snap_task",
		Payload:      []byte(`{"n":` + id + `}`),
		Headers:      map[string]string{"trace": "t-" + id},
		ID:           id,
		Queue:        snapTestQueue,
		Retry:        9,
		Retried:      3,
		ErrorMsg:     "previous failure",
		LastFailedAt: 1700000000,
		Timeout:      120,
		Deadline:     1800000000,
		GroupKey:     gkey,
		Retention:    7200,
	}
}

func mustRecord(t *testing.T, state snapshot.State, msg *base.TaskMessage, score, pendingSince int64) snapshot.Record {
	t.Helper()
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}
	return snapshot.Record{State: state, Score: score, PendingSince: pendingSince, Msg: encoded}
}

// dumpKeyspace returns a stable, comparable snapshot of every Redis key.
func dumpKeyspace(t *testing.T, r *RDB) map[string]string {
	t.Helper()
	ctx := context.Background()
	var cursor uint64
	out := make(map[string]string)
	for {
		keys, next, err := r.client.Scan(ctx, cursor, "*", 200).Result()
		if err != nil {
			t.Fatalf("SCAN: %v", err)
		}
		for _, key := range keys {
			typ, err := r.client.Type(ctx, key).Result()
			if err != nil {
				t.Fatalf("TYPE %q: %v", key, err)
			}
			switch typ {
			case "string":
				out[key] = "string:" + r.client.Get(ctx, key).Val()
			case "list":
				out[key] = "list:" + join(r.client.LRange(ctx, key, 0, -1).Val())
			case "set":
				vals := r.client.SMembers(ctx, key).Val()
				sort.Strings(vals)
				out[key] = "set:" + join(vals)
			case "zset":
				zs := r.client.ZRangeWithScores(ctx, key, 0, -1).Val()
				parts := make([]string, 0, len(zs))
				for _, z := range zs {
					parts = append(parts, z.Member.(string)+"="+itoa(int64(z.Score)))
				}
				sort.Strings(parts)
				out[key] = "zset:" + join(parts)
			case "hash":
				fields := r.client.HGetAll(ctx, key).Val()
				keys := make([]string, 0, len(fields))
				for k := range fields {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				parts := make([]string, 0, len(keys))
				for _, k := range keys {
					parts = append(parts, k+"="+fields[k])
				}
				out[key] = "hash:" + join(parts)
			default:
				out[key] = typ + ":<uncompared>"
			}
		}
		if next == 0 {
			return out
		}
		cursor = next
	}
}

func join(vals []string) string {
	out := ""
	for _, v := range vals {
		out += "|" + v
	}
	return out
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}

// TR-2.1/2.2/2.3: capture covers the four restorable states with full fidelity,
// excludes history states, is read-only, and rejects unknown queues.
func TestCaptureQueueSnapshot(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	now := time.Now()
	pendingMsgs := []*base.TaskMessage{snapMsg("p1", ""), snapMsg("p2", "")}
	h.SeedPendingQueue(t, r.client, pendingMsgs, snapTestQueue)
	// Give the pending tasks explicit pending_since values.
	pendingSince := []int64{now.Add(-time.Minute).UnixNano(), now.Add(-30 * time.Second).UnixNano()}
	for i, msg := range pendingMsgs {
		if err := r.client.HSet(ctx, base.TaskKey(snapTestQueue, msg.ID),
			"pending_since", pendingSince[i]).Err(); err != nil {
			t.Fatal(err)
		}
	}

	scheduledScore := now.Add(time.Hour).Unix()
	retryScore := now.Add(2 * time.Hour).Unix()
	h.SeedScheduledQueue(t, r.client, []base.Z{
		{Message: snapMsg("s1", ""), Score: scheduledScore},
		{Message: snapMsg("s2", ""), Score: scheduledScore + 60},
	}, snapTestQueue)
	h.SeedRetryQueue(t, r.client, []base.Z{
		{Message: snapMsg("r1", ""), Score: retryScore},
	}, snapTestQueue)

	g1Score := now.Unix()
	g2Score := now.Add(10 * time.Second).Unix()
	h.SeedGroup(t, r.client, []base.Z{
		{Message: snapMsg("a1", "g1"), Score: g1Score},
		{Message: snapMsg("a2", "g1"), Score: g1Score + 1},
	}, snapTestQueue, "g1")
	h.SeedGroup(t, r.client, []base.Z{
		{Message: snapMsg("a3", "g2"), Score: g2Score},
	}, snapTestQueue, "g2")

	// History / in-flight states must never be captured.
	h.SeedActiveQueue(t, r.client, []*base.TaskMessage{snapMsg("act1", "")}, snapTestQueue)
	h.SeedArchivedQueue(t, r.client, []base.Z{{Message: snapMsg("arc1", "")}}, snapTestQueue)
	h.SeedCompletedQueue(t, r.client, []base.Z{{Message: snapMsg("cmp1", "")}}, snapTestQueue)

	before := dumpKeyspace(t, r)

	records, err := r.CaptureQueueSnapshot(ctx, snapTestQueue)
	if err != nil {
		t.Fatalf("CaptureQueueSnapshot: %v", err)
	}
	if len(records) != 8 {
		t.Fatalf("captured %d records, want 8", len(records))
	}

	// Capture is read-only.
	if diff := cmp.Diff(before, dumpKeyspace(t, r)); diff != "" {
		t.Errorf("capture mutated Redis (-before,+after):\n%s", diff)
	}

	counts := map[snapshot.State]int{}
	for _, rec := range records {
		counts[rec.State]++
	}
	wantCounts := map[snapshot.State]int{
		snapshot.StatePending:     2,
		snapshot.StateScheduled:   2,
		snapshot.StateRetry:       1,
		snapshot.StateAggregating: 3,
	}
	if diff := cmp.Diff(wantCounts, counts); diff != "" {
		t.Errorf("state counts mismatch (-want,+got):\n%s", diff)
	}

	// Pending records preserve dequeue order (p1 then p2) and pending_since.
	var pendingRecs []snapshot.Record
	for _, rec := range records {
		if rec.State == snapshot.StatePending {
			pendingRecs = append(pendingRecs, rec)
		}
	}
	if len(pendingRecs) != 2 {
		t.Fatalf("got %d pending records", len(pendingRecs))
	}
	wantPendingIDs := []string{"p1", "p2"}
	for i, rec := range pendingRecs {
		msg, err := rec.TaskMessage()
		if err != nil {
			t.Fatalf("decode pending record: %v", err)
		}
		if msg.ID != wantPendingIDs[i] {
			t.Errorf("pending order[%d] = %q, want %q", i, msg.ID, wantPendingIDs[i])
		}
		if rec.PendingSince != pendingSince[i] {
			t.Errorf("pending_since[%d] = %d, want %d", i, rec.PendingSince, pendingSince[i])
		}
	}

	// Scores and raw msg bytes survive for scheduled/retry/aggregating.
	expectScore := map[string]int64{
		"s1": scheduledScore, "s2": scheduledScore + 60,
		"r1": retryScore,
		"a1": g1Score, "a2": g1Score + 1, "a3": g2Score,
	}
	for _, rec := range records {
		if rec.State == snapshot.StatePending {
			continue
		}
		msg, _ := rec.TaskMessage()
		if got := expectScore[msg.ID]; got != rec.Score {
			t.Errorf("record %q score = %d, want %d", msg.ID, rec.Score, got)
		}
		raw, err := r.client.HGet(ctx, base.TaskKey(snapTestQueue, msg.ID), "msg").Bytes()
		if err != nil {
			t.Fatalf("HGET msg %q: %v", msg.ID, err)
		}
		if diff := cmp.Diff(raw, rec.Msg); diff != "" {
			t.Errorf("record %q msg bytes differ from Redis (-redis,+record):\n%s", msg.ID, diff)
		}
	}

	// Unknown queue.
	if _, err := r.CaptureQueueSnapshot(ctx, "no-such-queue"); !errors.IsQueueNotFound(err) {
		t.Fatalf("got %v, want QueueNotFoundError", err)
	}
}

// TR-3.1/3.3/3.5: missing tasks in all four states are recreated exactly; a
// second run is a pure no-op; unrelated keys are untouched.
func TestRestoreRecordsFourStatesAndIdempotency(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	now := time.Now()
	records := []snapshot.Record{
		mustRecord(t, snapshot.StatePending, snapMsg("p1", ""), 0, now.Add(-time.Minute).UnixNano()),
		mustRecord(t, snapshot.StatePending, snapMsg("p2", ""), 0, now.Add(-30*time.Second).UnixNano()),
		mustRecord(t, snapshot.StateScheduled, snapMsg("s1", ""), now.Add(time.Hour).Unix(), 0),
		mustRecord(t, snapshot.StateRetry, snapMsg("r1", ""), now.Add(2*time.Hour).Unix(), 0),
		mustRecord(t, snapshot.StateAggregating, snapMsg("a1", "g1"), now.Unix(), 0),
		mustRecord(t, snapshot.StateAggregating, snapMsg("a2", "g1"), now.Add(5*time.Second).Unix(), 0),
		mustRecord(t, snapshot.StateAggregating, snapMsg("a3", "g2"), now.Add(10*time.Second).Unix(), 0),
	}

	// Unrelated pre-existing keys that must never change.
	h.SeedArchivedQueue(t, r.client, []base.Z{{Message: snapMsg("arc1", "")}}, snapTestQueue)
	h.SeedCompletedQueue(t, r.client, []base.Z{{Message: snapMsg("cmp1", "")}}, snapTestQueue)
	before := dumpKeyspace(t, r)

	outcomes, err := r.RestoreRecords(ctx, records)
	if err != nil {
		t.Fatalf("RestoreRecords: %v", err)
	}
	if len(outcomes) != len(records) {
		t.Fatalf("got %d outcomes, want %d", len(outcomes), len(records))
	}
	for _, o := range outcomes {
		if !o.Written {
			t.Errorf("task %q should be newly written", o.ID)
		}
	}

	// Pending: RPUSH in record order, so LRANGE yields reversed record order.
	gotPendingIDs := r.client.LRange(ctx, base.PendingKey(snapTestQueue), 0, -1).Val()
	wantPendingLRANGE := []string{"p2", "p1"}
	if diff := cmp.Diff(wantPendingLRANGE, gotPendingIDs); diff != "" {
		t.Errorf("pending list mismatch (-want,+got):\n%s", diff)
	}
	for _, msg := range []*base.TaskMessage{snapMsg("p1", ""), snapMsg("p2", "")} {
		state := r.client.HGet(ctx, base.TaskKey(snapTestQueue, msg.ID), "state").Val()
		if state != "pending" {
			t.Errorf("task %q state = %q", msg.ID, state)
		}
	}

	// Scheduled/retry scores and full messages.
	sched := h.GetScheduledEntries(t, r.client, snapTestQueue)
	if len(sched) != 1 || sched[0].Message.ID != "s1" || sched[0].Score != now.Add(time.Hour).Unix() {
		t.Errorf("scheduled entries mismatch: %+v", sched)
	}
	retry := h.GetRetryEntries(t, r.client, snapTestQueue)
	if len(retry) != 1 || retry[0].Message.ID != "r1" || retry[0].Score != now.Add(2*time.Hour).Unix() {
		t.Errorf("retry entries mismatch: %+v", retry)
	}

	// Aggregating: group zsets, scores, groups registry, task hash fields.
	g1 := h.GetGroupEntries(t, r.client, snapTestQueue, "g1")
	if len(g1) != 2 {
		t.Errorf("g1 size = %d, want 2", len(g1))
	}
	g2 := h.GetGroupEntries(t, r.client, snapTestQueue, "g2")
	if len(g2) != 1 || g2[0].Message.ID != "a3" {
		t.Errorf("g2 mismatch: %+v", g2)
	}
	groups, err := r.client.SMembers(ctx, base.AllGroups(snapTestQueue)).Result()
	if err != nil || len(groups) != 2 {
		t.Fatalf("groups set = %v, err=%v", groups, err)
	}
	for _, id := range []string{"a1", "a2", "a3"} {
		wantGroup := map[string]string{"a1": "g1", "a2": "g1", "a3": "g2"}[id]
		fields, err := r.client.HMGet(ctx, base.TaskKey(snapTestQueue, id), "state", "group").Result()
		if err != nil {
			t.Fatal(err)
		}
		if fields[0] != "aggregating" || fields[1] != wantGroup {
			t.Errorf("aggregating task %q hash = %v, want [aggregating %s]", id, fields, wantGroup)
		}
	}

	// Full message fidelity for every restored task.
	for _, rec := range records {
		msg, _ := rec.TaskMessage()
		raw, err := r.client.HGet(ctx, base.TaskKey(snapTestQueue, msg.ID), "msg").Bytes()
		if err != nil {
			t.Fatalf("HGET %q: %v", msg.ID, err)
		}
		if diff := cmp.Diff(rec.Msg, raw); diff != "" {
			t.Errorf("restored msg %q differs (-record,+redis):\n%s", msg.ID, diff)
		}
	}

	// Second run: every record is skipped as identical, no duplicates.
	outcomes2, err := r.RestoreRecords(ctx, records)
	if err != nil {
		t.Fatalf("second RestoreRecords: %v", err)
	}
	for _, o := range outcomes2 {
		if o.Written {
			t.Errorf("task %q should be skipped on re-run", o.ID)
		}
	}
	if n := r.client.LLen(ctx, base.PendingKey(snapTestQueue)).Val(); n != 2 {
		t.Errorf("pending size after re-run = %d, want 2", n)
	}
	if n := r.client.ZCard(ctx, base.ScheduledKey(snapTestQueue)).Val(); n != 1 {
		t.Errorf("scheduled size after re-run = %d, want 1", n)
	}
	if n := r.client.ZCard(ctx, base.GroupKey(snapTestQueue, "g1")).Val(); n != 2 {
		t.Errorf("g1 size after re-run = %d, want 2", n)
	}

	// Archived/completed keys remain byte-identical.
	after := dumpKeyspace(t, r)
	for _, key := range []string{base.ArchivedKey(snapTestQueue), base.CompletedKey(snapTestQueue),
		base.TaskKey(snapTestQueue, "arc1"), base.TaskKey(snapTestQueue, "cmp1")} {
		if diff := cmp.Diff(before[key], after[key]); diff != "" {
			t.Errorf("key %q changed (-before,+after):\n%s", key, diff)
		}
	}
}

// TR-3.2: identical existing task is left byte-for-byte untouched; a different
// msg triggers conflict with zero writes.
func TestRestoreRecordsKeptAndConflict(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	msg := snapMsg("keep1", "")
	rec := mustRecord(t, snapshot.StateScheduled, msg, 100, 0)
	h.SeedScheduledQueue(t, r.client, []base.Z{{Message: msg, Score: 100}}, snapTestQueue)
	before := dumpKeyspace(t, r)

	outcomes, err := r.RestoreRecords(ctx, []snapshot.Record{rec})
	if err != nil {
		t.Fatalf("identical restore: %v", err)
	}
	if len(outcomes) != 1 || outcomes[0].Written {
		t.Fatalf("outcomes = %+v, want single skipped outcome", outcomes)
	}
	if diff := cmp.Diff(before, dumpKeyspace(t, r)); diff != "" {
		t.Errorf("identical task was modified (-before,+after):\n%s", diff)
	}

	// Same ID, different payload -> conflict, zero writes.
	conflictMsg := snapMsg("keep1", "")
	conflictMsg.Payload = []byte(`{"n":"tampered"}`)
	conflictRec := mustRecord(t, snapshot.StateScheduled, conflictMsg, 100, 0)
	beforeConflict := dumpKeyspace(t, r)
	_, err = r.RestoreRecords(ctx, []snapshot.Record{conflictRec})
	if !errors.IsSnapshotTaskConflict(err) {
		t.Fatalf("got %v, want SnapshotTaskConflictError", err)
	}
	if diff := cmp.Diff(beforeConflict, dumpKeyspace(t, r)); diff != "" {
		t.Errorf("conflict restore mutated Redis (-before,+after):\n%s", diff)
	}

	// Removing the conflicting task allows the restore to succeed.
	if err := r.DeleteTask(snapTestQueue, "keep1"); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}
	if _, err := r.RestoreRecords(ctx, []snapshot.Record{conflictRec}); err != nil {
		t.Fatalf("restore after conflict removal: %v", err)
	}
	got := h.GetScheduledMessages(t, r.client, snapTestQueue)
	if len(got) != 1 || string(got[0].Payload) != string(conflictMsg.Payload) {
		t.Errorf("restored payload mismatch: %+v", got)
	}
}

// TR-3.4: progress set drives resume; clearing it stays correct.
func TestSnapshotProgress(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	snapID := uuid.NewString()
	const epoch int64 = 42
	key := base.SnapshotProgressKey(snapID, epoch)

	if err := r.MarkSnapshotRecordsDone(ctx, snapID, epoch, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	progress, err := r.ReadSnapshotProgress(ctx, snapID, epoch)
	if err != nil {
		t.Fatal(err)
	}
	if len(progress) != 2 {
		t.Fatalf("progress size = %d, want 2", len(progress))
	}
	if _, ok := progress["a"]; !ok {
		t.Error("progress missing id a")
	}
	// A different epoch has an empty progress view.
	other, err := r.ReadSnapshotProgress(ctx, snapID, epoch+1)
	if err != nil {
		t.Fatal(err)
	}
	if len(other) != 0 {
		t.Errorf("epoch isolation broken: got %d entries", len(other))
	}
	// Empty id list is a no-op.
	if err := r.MarkSnapshotRecordsDone(ctx, snapID, epoch, nil); err != nil {
		t.Errorf("empty mark: %v", err)
	}
	if err := r.ClearSnapshotProgress(ctx, snapID, epoch); err != nil {
		t.Fatal(err)
	}
	if n := r.client.Exists(ctx, key).Val(); n != 0 {
		t.Errorf("progress key still exists after clear")
	}
	// Clearing a missing key is fine.
	if err := r.ClearSnapshotProgress(ctx, snapID, epoch); err != nil {
		t.Errorf("clear missing key: %v", err)
	}
	// ClearAll removes progress from multiple epochs at once.
	if err := r.MarkSnapshotRecordsDone(ctx, snapID, 1, []string{"x"}); err != nil {
		t.Fatal(err)
	}
	if err := r.MarkSnapshotRecordsDone(ctx, snapID, 2, []string{"y"}); err != nil {
		t.Fatal(err)
	}
	if err := r.ClearAllSnapshotProgress(ctx, snapID); err != nil {
		t.Fatalf("ClearAllSnapshotProgress: %v", err)
	}
	if n := r.client.Keys(ctx, base.SnapshotProgressKeyPattern(snapID)).Val(); len(n) != 0 {
		t.Errorf("ClearAll left keys: %v", n)
	}
}

// TR-3.4 resume semantics at the record level: records whose tasks already
// exist are skipped, missing ones are written; re-running never duplicates.
func TestRestoreResumeSemantics(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	snapID := uuid.NewString()

	records := []snapshot.Record{
		mustRecord(t, snapshot.StatePending, snapMsg("p1", ""), 0, 0),
		mustRecord(t, snapshot.StateScheduled, snapMsg("s1", ""), 100, 0),
		mustRecord(t, snapshot.StateRetry, snapMsg("r1", ""), 200, 0),
		mustRecord(t, snapshot.StateAggregating, snapMsg("a1", "g1"), 300, 0),
	}
	// Simulate a previous interrupted run: first two records were applied and
	// recorded in the progress set.
	firstHalf := records[:2]
	if _, err := r.RestoreRecords(ctx, firstHalf); err != nil {
		t.Fatal(err)
	}
	doneIDs := make([]string, 0, len(firstHalf))
	for _, rec := range firstHalf {
		msg, _ := rec.TaskMessage()
		doneIDs = append(doneIDs, msg.ID)
	}
	if err := r.MarkSnapshotRecordsDone(ctx, snapID, 7, doneIDs); err != nil {
		t.Fatal(err)
	}

	// Root-level resume filters by progress; emulate the filter here.
	progress, err := r.ReadSnapshotProgress(ctx, snapID, 7)
	if err != nil {
		t.Fatal(err)
	}
	var remaining []snapshot.Record
	for _, rec := range records {
		msg, _ := rec.TaskMessage()
		if _, ok := progress[msg.ID]; !ok {
			remaining = append(remaining, rec)
		}
	}
	if len(remaining) != 2 {
		t.Fatalf("remaining = %d, want 2", len(remaining))
	}
	outcomes, err := r.RestoreRecords(ctx, remaining)
	if err != nil {
		t.Fatalf("resume restore: %v", err)
	}
	for _, o := range outcomes {
		if !o.Written {
			t.Errorf("resume task %q should be written", o.ID)
		}
	}

	// Full re-run with an empty progress set: everything is skipped, no dups.
	if err := r.ClearSnapshotProgress(ctx, snapID, 7); err != nil {
		t.Fatal(err)
	}
	outcomes, err = r.RestoreRecords(ctx, records)
	if err != nil {
		t.Fatalf("full re-run: %v", err)
	}
	for _, o := range outcomes {
		if o.Written {
			t.Errorf("full re-run task %q should be skipped", o.ID)
		}
	}
	if n := r.client.LLen(ctx, base.PendingKey(snapTestQueue)).Val(); n != 1 {
		t.Errorf("pending size = %d, want 1", n)
	}
}

// Restored pending tasks line up behind surviving tasks while keeping their
// original FIFO order: dequeue (RPOP) sequence must be survivor, then restored
// records in snapshot dequeue order.
func TestRestorePendingOrderingWithSurvivors(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	survivor := snapMsg("sv1", "")
	h.SeedPendingQueue(t, r.client, []*base.TaskMessage{survivor}, snapTestQueue)

	records := []snapshot.Record{
		mustRecord(t, snapshot.StatePending, snapMsg("p1", ""), 0, 0),
		mustRecord(t, snapshot.StatePending, snapMsg("p2", ""), 0, 0),
	}
	if _, err := r.RestoreRecords(ctx, records); err != nil {
		t.Fatalf("RestoreRecords: %v", err)
	}

	// Head -> tail: p2, p1, sv1; RPOP dequeue order: sv1, p1, p2.
	got := r.client.LRange(ctx, base.PendingKey(snapTestQueue), 0, -1).Val()
	want := []string{"p2", "p1", "sv1"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("pending list mismatch (-want,+got):\n%s", diff)
	}
	var dequeueOrder []string
	for {
		id, err := r.client.RPop(ctx, base.PendingKey(snapTestQueue)).Result()
		if err == redis.Nil {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		dequeueOrder = append(dequeueOrder, id)
	}
	wantDequeue := []string{"sv1", "p1", "p2"}
	if diff := cmp.Diff(wantDequeue, dequeueOrder); diff != "" {
		t.Errorf("dequeue order mismatch (-want,+got):\n%s", diff)
	}
}

// CurrentTaskMsgs distinguishes missing and existing tasks and returns raw bytes.
func TestCurrentTaskMsgs(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	msg := snapMsg("p1", "")
	h.SeedPendingQueue(t, r.client, []*base.TaskMessage{msg}, snapTestQueue)
	got, err := r.CurrentTaskMsgs(ctx, snapTestQueue, []string{"p1", "missing1"})
	if err != nil {
		t.Fatalf("CurrentTaskMsgs: %v", err)
	}
	if got["missing1"] != nil {
		t.Error("missing task should map to nil")
	}
	encoded, _ := base.EncodeMessage(msg)
	if diff := cmp.Diff(encoded, got["p1"]); diff != "" {
		t.Errorf("raw msg mismatch (-want,+got):\n%s", diff)
	}
}

// EnsureQueueRegistered re-adds a queue whose registration was removed.
func TestEnsureQueueRegistered(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	if err := r.client.SRem(ctx, base.AllQueues, snapTestQueue).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.EnsureQueueRegistered(ctx, snapTestQueue); err != nil {
		t.Fatalf("EnsureQueueRegistered: %v", err)
	}
	isMember, err := r.client.SIsMember(ctx, base.AllQueues, snapTestQueue).Result()
	if err != nil {
		t.Fatal(err)
	}
	if !isMember {
		t.Error("queue was not re-registered")
	}
}
