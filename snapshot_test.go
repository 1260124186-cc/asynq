// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/hibiken/asynq/internal/snapshot"
	h "github.com/hibiken/asynq/internal/testutil"
	"github.com/redis/go-redis/v9"
)

const mgrTestQueue = "mgrq"

func mgrMsg(id, gkey string) *base.TaskMessage {
	return &base.TaskMessage{
		Type:      "mgr_task",
		Payload:   []byte(`{"id":"` + id + `"}`),
		Headers:   map[string]string{"h1": "v1"},
		ID:        id,
		Queue:     mgrTestQueue,
		Retry:     5,
		Retried:   1,
		Timeout:   90,
		GroupKey:  gkey,
		Retention: 600,
	}
}

// dumpAllKeys returns a comparable dump of every key/value in the current DB.
func dumpAllKeys(t *testing.T, r redis.UniversalClient) map[string]string {
	t.Helper()
	ctx := context.Background()
	out := make(map[string]string)
	var cursor uint64
	for {
		keys, next, err := r.Scan(ctx, cursor, "*", 200).Result()
		if err != nil {
			t.Fatalf("SCAN: %v", err)
		}
		for _, key := range keys {
			typ, err := r.Type(ctx, key).Result()
			if err != nil {
				t.Fatalf("TYPE: %v", err)
			}
			switch typ {
			case "string":
				out[key] = "s:" + r.Get(ctx, key).Val()
			case "list":
				out[key] = "l:" + joinStrings(r.LRange(ctx, key, 0, -1).Val())
			case "set":
				vals := r.SMembers(ctx, key).Val()
				sort.Strings(vals)
				out[key] = "S:" + joinStrings(vals)
			case "zset":
				zs := r.ZRangeWithScores(ctx, key, 0, -1).Val()
				parts := make([]string, 0, len(zs))
				for _, z := range zs {
					parts = append(parts, z.Member.(string))
				}
				sort.Strings(parts)
				out[key] = "z:" + joinStrings(parts)
			case "hash":
				fields := r.HGetAll(ctx, key).Val()
				names := make([]string, 0, len(fields))
				for k := range fields {
					names = append(names, k)
				}
				sort.Strings(names)
				parts := make([]string, 0, len(names))
				for _, k := range names {
					parts = append(parts, k+"="+fields[k])
				}
				out[key] = "h:" + joinStrings(parts)
			default:
				out[key] = typ
			}
		}
		if next == 0 {
			return out
		}
		cursor = next
	}
}

func joinStrings(vals []string) string {
	out := ""
	for _, v := range vals {
		out += "|" + v
	}
	return out
}

func seedSnapshotFixture(t *testing.T, r redis.UniversalClient, processAt time.Time) {
	t.Helper()
	h.SeedPendingQueue(t, r,
		[]*base.TaskMessage{mgrMsg("p1", ""), mgrMsg("p2", "")}, mgrTestQueue)
	h.SeedScheduledQueue(t, r, []base.Z{
		{Message: mgrMsg("s1", ""), Score: processAt.Unix()},
	}, mgrTestQueue)
	h.SeedRetryQueue(t, r, []base.Z{
		{Message: mgrMsg("r1", ""), Score: processAt.Add(time.Hour).Unix()},
	}, mgrTestQueue)
	h.SeedGroup(t, r, []base.Z{
		{Message: mgrMsg("a1", "g1"), Score: processAt.Unix()},
		{Message: mgrMsg("a2", "g1"), Score: processAt.Unix() + 1},
	}, mgrTestQueue, "g1")
	// History / in-flight tasks that must always remain untouched.
	h.SeedActiveQueue(t, r, []*base.TaskMessage{mgrMsg("act1", "")}, mgrTestQueue)
	h.SeedArchivedQueue(t, r, []base.Z{
		{Message: mgrMsg("arc1", ""), Score: time.Now().Unix()},
	}, mgrTestQueue)
	h.SeedCompletedQueue(t, r, []base.Z{
		{Message: mgrMsg("cmp1", ""), Score: time.Now().Unix()},
	}, mgrTestQueue)
}

// TR-4.1: full create -> partial loss -> restore -> idempotent re-run flow,
// including the archived-same-content rule and untouched history.
func TestSnapshotManagerEndToEnd(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	processAt := time.Now().Add(6 * time.Hour).Truncate(time.Second)
	seedSnapshotFixture(t, r, processAt)

	mgr := NewSnapshotManager(getRedisConnOpt(t), t.TempDir())
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	info, err := mgr.CreateQueueSnapshot(ctx, mgrTestQueue)
	if err != nil {
		t.Fatalf("CreateQueueSnapshot: %v", err)
	}
	wantInfo := &QueueSnapshotInfo{
		ID:          info.ID,
		Queue:       mgrTestQueue,
		CreatedAt:   info.CreatedAt,
		Pending:     2,
		Scheduled:   1,
		Retry:       1,
		Aggregating: 2,
		Total:       6,
	}
	if diff := cmp.Diff(wantInfo, info); diff != "" {
		t.Errorf("snapshot info mismatch (-want,+got):\n%s", diff)
	}
	listed, err := mgr.ListSnapshots(ctx)
	if err != nil || len(listed) != 1 || listed[0].ID != info.ID {
		t.Fatalf("ListSnapshots = %v, err=%v", listed, err)
	}

	// Lose four tasks; archive p2 (same ID, same content -> must stay archived).
	for _, id := range []string{"p1", "s1", "r1", "a1"} {
		if err := inspector.DeleteTask(mgrTestQueue, id); err != nil {
			t.Fatalf("DeleteTask %q: %v", id, err)
		}
	}
	if err := inspector.ArchiveTask(mgrTestQueue, "p2"); err != nil {
		t.Fatalf("ArchiveTask p2: %v", err)
	}

	res, err := mgr.RestoreQueueSnapshot(ctx, info.ID)
	if err != nil {
		t.Fatalf("RestoreQueueSnapshot: %v", err)
	}
	if !res.Completed || res.Remaining != 0 || res.Restored != 4 || res.Kept != 2 {
		t.Errorf("restore result = %+v, want Restored=4 Kept=2 Completed", res)
	}

	// Restored tasks are back in their original states with original timing.
	pending, err := inspector.ListPendingTasks(mgrTestQueue)
	if err != nil {
		t.Fatalf("ListPendingTasks: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != "p1" {
		t.Errorf("pending tasks = %+v, want only p1", pending)
	}
	scheduled, err := inspector.ListScheduledTasks(mgrTestQueue)
	if err != nil {
		t.Fatalf("ListScheduledTasks: %v", err)
	}
	if len(scheduled) != 1 || scheduled[0].ID != "s1" ||
		scheduled[0].NextProcessAt.Unix() != processAt.Unix() {
		t.Errorf("scheduled task mismatch: %+v", scheduled)
	}
	retryTasks, err := inspector.ListRetryTasks(mgrTestQueue)
	if err != nil {
		t.Fatalf("ListRetryTasks: %v", err)
	}
	if len(retryTasks) != 1 || retryTasks[0].ID != "r1" ||
		retryTasks[0].NextProcessAt.Unix() != processAt.Add(time.Hour).Unix() {
		t.Errorf("retry task mismatch: %+v", retryTasks)
	}
	aggregating, err := inspector.ListAggregatingTasks(mgrTestQueue, "g1")
	if err != nil {
		t.Fatalf("ListAggregatingTasks: %v", err)
	}
	if len(aggregating) != 2 {
		t.Errorf("aggregating g1 size = %d, want 2", len(aggregating))
	}

	// Full message/option fidelity for a restored task.
	gotInfo, err := inspector.GetTaskInfo(mgrTestQueue, "r1")
	if err != nil {
		t.Fatalf("GetTaskInfo r1: %v", err)
	}
	want := mgrMsg("r1", "")
	if gotInfo.MaxRetry != want.Retry || gotInfo.Retried != want.Retried ||
		gotInfo.Timeout != 90*time.Second || gotInfo.Retention != 600*time.Second ||
		string(gotInfo.Payload) != string(want.Payload) {
		t.Errorf("restored task options not preserved: %+v", gotInfo)
	}

	// p2 remains archived (same ID + same content rule); history untouched.
	archived, err := inspector.ListArchivedTasks(mgrTestQueue)
	if err != nil {
		t.Fatalf("ListArchivedTasks: %v", err)
	}
	archivedIDs := make(map[string]bool)
	for _, ti := range archived {
		archivedIDs[ti.ID] = true
	}
	if !archivedIDs["p2"] || !archivedIDs["arc1"] {
		t.Errorf("archived set = %v, want p2 and arc1", archivedIDs)
	}
	active, err := inspector.ListActiveTasks(mgrTestQueue)
	if err != nil {
		t.Fatalf("ListActiveTasks: %v", err)
	}
	if len(active) != 1 || active[0].ID != "act1" {
		t.Errorf("active task changed: %+v", active)
	}
	completed, err := inspector.ListCompletedTasks(mgrTestQueue)
	if err != nil {
		t.Fatalf("ListCompletedTasks: %v", err)
	}
	if len(completed) != 1 || completed[0].ID != "cmp1" {
		t.Errorf("completed task changed: %+v", completed)
	}

	// Re-run is a pure no-op with no duplicates.
	res2, err := mgr.RestoreQueueSnapshot(ctx, info.ID)
	if err != nil {
		t.Fatalf("second RestoreQueueSnapshot: %v", err)
	}
	if res2.Restored != 0 || res2.Kept != 6 || !res2.Completed {
		t.Errorf("second restore = %+v, want Restored=0 Kept=6", res2)
	}
	if n := r.LLen(ctx, base.PendingKey(mgrTestQueue)).Val(); n != 1 {
		t.Errorf("pending size = %d, want 1", n)
	}
}

// TR-4.2 / AC-6: same-ID different-content conflict fails with zero writes.
func TestSnapshotManagerConflict(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	h.SeedPendingQueue(t, r, []*base.TaskMessage{mgrMsg("c1", "")}, mgrTestQueue)

	mgr := NewSnapshotManager(getRedisConnOpt(t), t.TempDir())
	info, err := mgr.CreateQueueSnapshot(ctx, mgrTestQueue)
	if err != nil {
		t.Fatal(err)
	}

	// Tamper with the live task content while keeping its ID and state.
	tampered := mgrMsg("c1", "")
	tampered.Payload = []byte(`{"id":"tampered"}`)
	encoded, err := base.EncodeMessage(tampered)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.HSet(ctx, base.TaskKey(mgrTestQueue, "c1"), "msg", encoded).Err(); err != nil {
		t.Fatal(err)
	}
	before := dumpAllKeys(t, r)

	_, err = mgr.RestoreQueueSnapshot(ctx, info.ID)
	if !errors.Is(err, ErrSnapshotTaskConflict) {
		t.Fatalf("got %v, want ErrSnapshotTaskConflict", err)
	}
	if diff := cmp.Diff(before, dumpAllKeys(t, r)); diff != "" {
		t.Errorf("Redis changed on conflict (-before,+after):\n%s", diff)
	}
}

// TR-4.2 / AC-2: an unreadable restore point fails with zero writes.
func TestSnapshotManagerCorruptFile(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	h.SeedPendingQueue(t, r, []*base.TaskMessage{mgrMsg("p1", "")}, mgrTestQueue)

	dir := t.TempDir()
	mgr := NewSnapshotManager(getRedisConnOpt(t), dir)
	info, err := mgr.CreateQueueSnapshot(ctx, mgrTestQueue)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, snapshot.FileName(info.ID))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xFF // break the last frame CRC
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	before := dumpAllKeys(t, r)
	_, err = mgr.RestoreQueueSnapshot(ctx, info.ID)
	if !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("got %v, want ErrSnapshotCorrupt", err)
	}
	if diff := cmp.Diff(before, dumpAllKeys(t, r)); diff != "" {
		t.Errorf("Redis changed on corrupt snapshot (-before,+after):\n%s", diff)
	}
}

// TR-4.3 / AC-9: ID policy, duplicate rejection, list and delete behavior.
func TestSnapshotManagerIDAndLifecycle(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	h.SeedPendingQueue(t, r, []*base.TaskMessage{mgrMsg("p1", "")}, mgrTestQueue)

	dir := t.TempDir()
	mgr := NewSnapshotManager(getRedisConnOpt(t), dir)

	first, err := mgr.CreateQueueSnapshot(ctx, mgrTestQueue, SnapshotID("release-1"))
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != "release-1" {
		t.Errorf("ID = %q, want release-1", first.ID)
	}
	if _, err := mgr.CreateQueueSnapshot(ctx, mgrTestQueue, SnapshotID("release-1")); !errors.Is(err, ErrSnapshotAlreadyExists) {
		t.Fatalf("duplicate create = %v, want ErrSnapshotAlreadyExists", err)
	}
	for _, bad := range []string{"", "..", "a/b", `a\b`} {
		if _, err := mgr.CreateQueueSnapshot(ctx, mgrTestQueue, SnapshotID(bad)); err == nil {
			t.Errorf("invalid ID %q was accepted", bad)
		}
	}
	if got, err := mgr.ListSnapshots(ctx); err != nil || len(got) != 1 || got[0].ID != "release-1" {
		t.Fatalf("ListSnapshots = %v, err=%v", got, err)
	}
	if err := mgr.DeleteSnapshot(ctx, "release-1"); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}
	if _, err := mgr.RestoreQueueSnapshot(ctx, "release-1"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("restore after delete = %v, want ErrSnapshotNotFound", err)
	}
	if err := mgr.DeleteSnapshot(ctx, "release-1"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("second delete = %v, want ErrSnapshotNotFound", err)
	}
}

// TR-4.4 / AC-8: an interrupted restore resumes and only fills the remainder.
func TestSnapshotManagerResume(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	msgs := []*base.TaskMessage{
		mgrMsg("p1", ""), mgrMsg("p2", ""), mgrMsg("p3", ""), mgrMsg("p4", ""),
	}
	h.SeedPendingQueue(t, r, msgs, mgrTestQueue)

	dir := t.TempDir()
	mgr := NewSnapshotManager(getRedisConnOpt(t), dir)
	info, err := mgr.CreateQueueSnapshot(ctx, mgrTestQueue)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"p1", "p2", "p3", "p4"} {
		if err := r.Del(ctx, base.TaskKey(mgrTestQueue, id)).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Del(ctx, base.PendingKey(mgrTestQueue)).Err(); err != nil {
		t.Fatal(err)
	}

	// Simulate a previous run that applied p1 and p2 and recorded progress.
	f, err := os.Open(filepath.Join(dir, snapshot.FileName(info.ID)))
	if err != nil {
		t.Fatal(err)
	}
	_, records, err := snapshot.Decode(f)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	broker := rdb.NewRDB(r)
	if _, err := broker.RestoreRecords(ctx, records[:2]); err != nil {
		t.Fatalf("partial RestoreRecords: %v", err)
	}
	if err := broker.MarkSnapshotRecordsDone(ctx, info.ID, info.CreatedAt.Unix(), []string{"p1", "p2"}); err != nil {
		t.Fatal(err)
	}

	res, err := mgr.RestoreQueueSnapshot(ctx, info.ID)
	if err != nil {
		t.Fatalf("resume restore: %v", err)
	}
	if res.Restored != 2 || res.Kept != 0 || !res.Completed || res.Remaining != 0 {
		t.Errorf("resume result = %+v, want Restored=2", res)
	}
	if n := r.LLen(ctx, base.PendingKey(mgrTestQueue)).Val(); n != 4 {
		t.Errorf("pending size = %d, want 4", n)
	}
	if n := r.Exists(ctx, base.SnapshotProgressKey(info.ID, info.CreatedAt.Unix())).Val(); n != 0 {
		t.Error("progress key was not cleared after completion")
	}
}

// Stale progress from an older epoch (e.g. a reused custom snapshot ID) must
// not suppress restores of the current restore point.
func TestSnapshotManagerProgressEpochIsolation(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	h.SeedPendingQueue(t, r,
		[]*base.TaskMessage{mgrMsg("p1", ""), mgrMsg("p2", "")}, mgrTestQueue)

	dir := t.TempDir()
	mgr := NewSnapshotManager(getRedisConnOpt(t), dir)
	info, err := mgr.CreateQueueSnapshot(ctx, mgrTestQueue, SnapshotID("rel-1"))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"p1", "p2"} {
		if err := r.Del(ctx, base.TaskKey(mgrTestQueue, id)).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Del(ctx, base.PendingKey(mgrTestQueue)).Err(); err != nil {
		t.Fatal(err)
	}

	// Plant stale progress under an older epoch claiming everything is done.
	staleKey := base.SnapshotProgressKey("rel-1", info.CreatedAt.Unix()-3600)
	if err := r.SAdd(ctx, staleKey, "p1", "p2").Err(); err != nil {
		t.Fatal(err)
	}
	res, err := mgr.RestoreQueueSnapshot(ctx, "rel-1")
	if err != nil {
		t.Fatalf("restore with stale progress: %v", err)
	}
	if res.Restored != 2 || !res.Completed {
		t.Errorf("result = %+v, want both tasks restored", res)
	}
	if n := r.LLen(ctx, base.PendingKey(mgrTestQueue)).Val(); n != 2 {
		t.Errorf("pending size = %d, want 2", n)
	}
	// The stale key is untouched; deleting the snapshot clears all epochs.
	if n := r.Exists(ctx, staleKey).Val(); n != 1 {
		t.Error("stale progress key should remain until snapshot deletion")
	}
	if err := mgr.DeleteSnapshot(ctx, "rel-1"); err != nil {
		t.Fatal(err)
	}
	if n := r.Keys(ctx, base.SnapshotProgressKeyPattern("rel-1")).Val(); len(n) != 0 {
		t.Errorf("DeleteSnapshot left progress keys: %v", n)
	}
}

// FR-11: restoring into a removed queue re-registers it and succeeds.
func TestSnapshotManagerQueueRemoved(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	processAt := time.Now().Add(3 * time.Hour).Truncate(time.Second)
	h.SeedScheduledQueue(t, r, []base.Z{
		{Message: mgrMsg("s1", ""), Score: processAt.Unix()},
	}, mgrTestQueue)

	mgr := NewSnapshotManager(getRedisConnOpt(t), t.TempDir())
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	info, err := mgr.CreateQueueSnapshot(ctx, mgrTestQueue)
	if err != nil {
		t.Fatal(err)
	}
	if err := inspector.DeleteQueue(mgrTestQueue, true); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}
	queues, _ := inspector.Queues()
	for _, q := range queues {
		if q == mgrTestQueue {
			t.Fatal("queue still registered after DeleteQueue")
		}
	}

	res, err := mgr.RestoreQueueSnapshot(ctx, info.ID)
	if err != nil {
		t.Fatalf("restore after queue removal: %v", err)
	}
	if res.Restored != 1 || !res.Completed {
		t.Errorf("result = %+v", res)
	}
	queues, _ = inspector.Queues()
	found := false
	for _, q := range queues {
		if q == mgrTestQueue {
			found = true
		}
	}
	if !found {
		t.Error("queue was not re-registered by restore")
	}
	scheduled, err := inspector.ListScheduledTasks(mgrTestQueue)
	if err != nil {
		t.Fatalf("ListScheduledTasks: %v", err)
	}
	if len(scheduled) != 1 || scheduled[0].ID != "s1" ||
		scheduled[0].NextProcessAt.Unix() != processAt.Unix() {
		t.Errorf("scheduled mismatch after restore: %+v", scheduled)
	}
}

// A missing restore point file is reported explicitly.
func TestSnapshotManagerMissingFile(t *testing.T) {
	r := setup(t)
	defer r.Close()
	mgr := NewSnapshotManager(getRedisConnOpt(t), t.TempDir())
	if _, err := mgr.RestoreQueueSnapshot(context.Background(), "does-not-exist"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("got %v, want ErrSnapshotNotFound", err)
	}
}
