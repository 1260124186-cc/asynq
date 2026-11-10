// Copyright 2022 Kentaro Hibino. All rights reserved.
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
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	h "github.com/hibiken/asynq/internal/testutil"
	"github.com/hibiken/asynq/internal/timeutil"
	"github.com/redis/go-redis/v9"
)

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}

func sortedStrings(ss []string) []string {
	out := append([]string(nil), ss...)
	sort.Strings(out)
	return out
}

func cmpDiffStringSet(got, want []string) string {
	return cmp.Diff(sortedStrings(got), sortedStrings(want))
}

func cmpDiffZIDs(zs []base.Z, want []string) string {
	got := make([]string, 0, len(zs))
	for _, z := range zs {
		got = append(got, z.Message.ID)
	}
	return cmp.Diff(sortedStrings(got), sortedStrings(want))
}

func cmpDiffMsgIDs(msgs []*base.TaskMessage, want []string) string {
	got := make([]string, 0, len(msgs))
	for _, m := range msgs {
		got = append(got, m.ID)
	}
	return cmp.Diff(sortedStrings(got), sortedStrings(want))
}

func containsString(ss []string, target string) bool {
	for _, s := range ss {
		if s == target {
			return true
		}
	}
	return false
}

func rosterHash(t *testing.T, r *RDB, qname, gname string) map[string]string {
	t.Helper()
	got, err := r.client.HGetAll(context.Background(), base.RosterKey(qname, gname)).Result()
	if err != nil {
		t.Fatalf("HGetAll roster key failed: %v", err)
	}
	return got
}

func rosterMembers(t *testing.T, r *RDB, qname, gname string) []string {
	t.Helper()
	got, err := r.client.SMembers(context.Background(), base.RosterMembersKey(qname, gname)).Result()
	if err != nil {
		t.Fatalf("SMembers roster members key failed: %v", err)
	}
	return got
}

func addGroupTask(t *testing.T, r *RDB, gname string, msg *base.TaskMessage) {
	t.Helper()
	if err := r.AddToGroup(context.Background(), msg, gname); err != nil {
		t.Fatalf("AddToGroup failed: %v", err)
	}
}

func TestApplyGroupRoster(t *testing.T) {
	r := setup(t)
	defer r.Close()

	const qname = "default"
	const gname = "import-batch"
	ctx := context.Background()

	t.Run("creates roster with waiting state", func(t *testing.T) {
		h.FlushDB(t, r.client)
		clock := timeutil.NewSimulatedClock(time.Now())
		r.SetClock(clock)

		members := []string{"m1", "m2", "m3"}
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", members); err != nil {
			t.Fatalf("ApplyGroupRoster failed: %v", err)
		}
		hash := rosterHash(t, r, qname, gname)
		if hash["batch_id"] != "batch-1" {
			t.Errorf("batch_id = %q, want batch-1", hash["batch_id"])
		}
		if hash["state"] != base.RosterStateWaiting {
			t.Errorf("state = %q, want %q", hash["state"], base.RosterStateWaiting)
		}
		if hash["updated_at"] != itoa(clock.Now().Unix()) {
			t.Errorf("updated_at = %q, want %d", hash["updated_at"], clock.Now().Unix())
		}
		if diff := cmpDiffStringSet(rosterMembers(t, r, qname, gname), members); diff != "" {
			t.Errorf("roster members mismatch: (-got,+want)\n%s", diff)
		}
	})

	t.Run("re-applying same batch and members is a no-op", func(t *testing.T) {
		h.FlushDB(t, r.client)
		clock := timeutil.NewSimulatedClock(time.Now())
		r.SetClock(clock)

		members := []string{"m1", "m2"}
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", members); err != nil {
			t.Fatalf("first ApplyGroupRoster failed: %v", err)
		}
		before := rosterHash(t, r, qname, gname)

		clock.AdvanceTime(1 * time.Hour)
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", append([]string{}, members...)); err != nil {
			t.Fatalf("idempotent ApplyGroupRoster failed: %v", err)
		}
		after := rosterHash(t, r, qname, gname)
		if after["updated_at"] != before["updated_at"] {
			t.Errorf("updated_at changed on no-op: before=%q after=%q", before["updated_at"], after["updated_at"])
		}
		if after["batch_id"] != "batch-1" || after["state"] != base.RosterStateWaiting {
			t.Errorf("roster mutated by no-op apply: %v", after)
		}
		if diff := cmpDiffStringSet(rosterMembers(t, r, qname, gname), members); diff != "" {
			t.Errorf("roster members mismatch after no-op: (-got,+want)\n%s", diff)
		}
	})

	t.Run("rejects duplicate members without writing anything", func(t *testing.T) {
		h.FlushDB(t, r.client)
		err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", []string{"m1", "m1"})
		if !errors.Is(err, errors.ErrRosterDuplicateMember) {
			t.Fatalf("expected ErrRosterDuplicateMember, got %v", err)
		}
		if n, _ := r.client.Exists(ctx, base.RosterKey(qname, gname)).Result(); n != 0 {
			t.Errorf("roster key was written despite duplicate members")
		}
	})

	t.Run("rejects empty member list", func(t *testing.T) {
		h.FlushDB(t, r.client)
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", nil); err == nil {
			t.Fatal("expected error for empty roster, got nil")
		}
	})

	t.Run("same batch id with different members conflicts and keeps old roster", func(t *testing.T) {
		h.FlushDB(t, r.client)
		members := []string{"m1", "m2", "m3"}
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", members); err != nil {
			t.Fatalf("first ApplyGroupRoster failed: %v", err)
		}
		err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", []string{"m1", "m2"})
		if !errors.Is(err, errors.ErrRosterBatchConflict) {
			t.Fatalf("expected ErrRosterBatchConflict, got %v", err)
		}
		if diff := cmpDiffStringSet(rosterMembers(t, r, qname, gname), members); diff != "" {
			t.Errorf("old roster mutated by conflicting apply: (-got,+want)\n%s", diff)
		}
		if hash := rosterHash(t, r, qname, gname); hash["batch_id"] != "batch-1" {
			t.Errorf("batch_id = %q, want batch-1", hash["batch_id"])
		}
	})

	t.Run("new batch id replaces roster and drops removed waiting members", func(t *testing.T) {
		h.FlushDB(t, r.client)
		msg1 := h.NewTaskMessageBuilder().SetType("t1").SetGroup(gname).Build()
		msg2 := h.NewTaskMessageBuilder().SetType("t2").SetGroup(gname).Build()
		msg3 := h.NewTaskMessageBuilder().SetType("t3").SetGroup(gname).Build()
		addGroupTask(t, r, gname, msg1)
		addGroupTask(t, r, gname, msg2)
		addGroupTask(t, r, gname, msg3)

		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1",
			[]string{msg1.ID, msg2.ID, msg3.ID}); err != nil {
			t.Fatalf("first ApplyGroupRoster failed: %v", err)
		}

		// Latest roster removes msg3 and adds not-yet-arrived msg4.
		msg4 := h.NewTaskMessageBuilder().SetType("t4").SetGroup(gname).Build()
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-2",
			[]string{msg1.ID, msg2.ID, msg4.ID}); err != nil {
			t.Fatalf("replacing ApplyGroupRoster failed: %v", err)
		}

		hash := rosterHash(t, r, qname, gname)
		if hash["batch_id"] != "batch-2" || hash["state"] != base.RosterStateWaiting {
			t.Errorf("roster not replaced: %v", hash)
		}
		if diff := cmpDiffStringSet(rosterMembers(t, r, qname, gname),
			[]string{msg1.ID, msg2.ID, msg4.ID}); diff != "" {
			t.Errorf("roster members mismatch: (-got,+want)\n%s", diff)
		}
		gotGroup := h.GetGroupEntries(t, r.client, qname, gname)
		if diff := cmpDiffZIDs(gotGroup, []string{msg1.ID, msg2.ID}); diff != "" {
			t.Errorf("removed member still in group: (-got,+want)\n%s", diff)
		}
		if n, _ := r.client.Exists(ctx, base.TaskKey(qname, msg3.ID)).Result(); n != 0 {
			t.Errorf("task key of removed member %q was not deleted", msg3.ID)
		}
	})

	t.Run("cannot replace roster while aggregation is in progress", func(t *testing.T) {
		h.FlushDB(t, r.client)
		members := []string{"m1", "m2"}
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", members); err != nil {
			t.Fatalf("ApplyGroupRoster failed: %v", err)
		}
		aggSetKey := base.AggregationSetKey(qname, gname, "live-set")
		pipe := r.client.TxPipeline()
		pipe.HSet(ctx, base.RosterKey(qname, gname),
			"state", base.RosterStateAggregating, "agg_set", aggSetKey)
		pipe.ZAdd(ctx, base.AllAggregationSets(qname),
			redis.Z{Score: float64(r.clock.Now().Add(time.Hour).Unix()), Member: aggSetKey})
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("failed to seed in-progress aggregation: %v", err)
		}
		err := r.ApplyGroupRoster(ctx, qname, gname, "batch-2", []string{"m1", "m2", "m3"})
		if !errors.Is(err, errors.ErrRosterAggregationInProgress) {
			t.Fatalf("expected ErrRosterAggregationInProgress, got %v", err)
		}
		if hash := rosterHash(t, r, qname, gname); hash["batch_id"] != "batch-1" {
			t.Errorf("roster mutated despite in-progress aggregation: %v", hash)
		}
		if diff := cmpDiffStringSet(rosterMembers(t, r, qname, gname), members); diff != "" {
			t.Errorf("roster members mutated: (-got,+want)\n%s", diff)
		}
	})

	t.Run("takes over a roster orphaned by a crash after its set was consumed", func(t *testing.T) {
		h.FlushDB(t, r.client)
		members := []string{"m1", "m2"}
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", members); err != nil {
			t.Fatalf("ApplyGroupRoster failed: %v", err)
		}
		// Simulate: aggregator consumed and deleted the set, then crashed before
		// clearing the roster. The tracked set key is absent from AllAggregationSets.
		if err := r.client.HSet(ctx, base.RosterKey(qname, gname),
			"state", base.RosterStateAggregating,
			"agg_set", base.AggregationSetKey(qname, gname, "gone-set")).Err(); err != nil {
			t.Fatalf("HSet failed: %v", err)
		}

		// Re-applying the same batch after restart must not get stuck.
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", members); err != nil {
			t.Fatalf("ApplyGroupRoster on orphaned roster failed: %v", err)
		}
		hash := rosterHash(t, r, qname, gname)
		if hash["state"] != base.RosterStateWaiting {
			t.Errorf("state = %q, want waiting", hash["state"])
		}
		if _, ok := hash["agg_set"]; ok {
			t.Errorf("agg_set field not cleared: %v", hash)
		}
		if diff := cmpDiffStringSet(rosterMembers(t, r, qname, gname), members); diff != "" {
			t.Errorf("roster members mismatch: (-got,+want)\n%s", diff)
		}

		// A newer batch can take over as well.
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-2",
			[]string{"m1", "m3"}); err != nil {
			t.Fatalf("replacing orphaned roster failed: %v", err)
		}
		if hash := rosterHash(t, r, qname, gname); hash["batch_id"] != "batch-2" ||
			hash["state"] != base.RosterStateWaiting {
			t.Errorf("roster not replaced: %v", hash)
		}
	})
}

func TestRosterAggregationCheck(t *testing.T) {
	r := setup(t)
	defer r.Close()

	const qname = "default"
	const gname = "import-batch"
	ctx := context.Background()
	// Far-future check time must not trigger a roster-gated group: the barrier
	// ignores grace period, max delay and max size.
	future := time.Now().Add(24 * time.Hour)

	check := func() (string, error) {
		return r.AggregationCheck(qname, gname, future, time.Second, time.Second, 1)
	}

	t.Run("keeps waiting when no member has arrived", func(t *testing.T) {
		h.FlushDB(t, r.client)
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", []string{"m1", "m2"}); err != nil {
			t.Fatalf("ApplyGroupRoster failed: %v", err)
		}
		setID, err := check()
		if err != nil || setID != "" {
			t.Fatalf("AggregationCheck = (%q, %v), want (\"\", nil)", setID, err)
		}
		if hash := rosterHash(t, r, qname, gname); hash["state"] != base.RosterStateWaiting {
			t.Errorf("state = %q, want waiting", hash["state"])
		}
	})

	t.Run("keeps waiting when only part of the roster arrived", func(t *testing.T) {
		h.FlushDB(t, r.client)
		msg1 := h.NewTaskMessageBuilder().SetType("t1").SetGroup(gname).Build()
		msg2 := h.NewTaskMessageBuilder().SetType("t2").SetGroup(gname).Build()
		addGroupTask(t, r, gname, msg1)
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", []string{msg1.ID, msg2.ID}); err != nil {
			t.Fatalf("ApplyGroupRoster failed: %v", err)
		}
		setID, err := check()
		if err != nil || setID != "" {
			t.Fatalf("AggregationCheck = (%q, %v), want (\"\", nil)", setID, err)
		}
		if diff := cmpDiffZIDs(h.GetGroupEntries(t, r.client, qname, gname), []string{msg1.ID}); diff != "" {
			t.Errorf("group mutated while waiting: (-got,+want)\n%s", diff)
		}
	})

	t.Run("aggregates once every roster member arrived and is verified", func(t *testing.T) {
		h.FlushDB(t, r.client)
		msg1 := h.NewTaskMessageBuilder().SetType("t1").SetGroup(gname).Build()
		msg2 := h.NewTaskMessageBuilder().SetType("t2").SetGroup(gname).Build()
		addGroupTask(t, r, gname, msg1)
		addGroupTask(t, r, gname, msg2)
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", []string{msg1.ID, msg2.ID}); err != nil {
			t.Fatalf("ApplyGroupRoster failed: %v", err)
		}

		setID, err := check()
		if err != nil || setID == "" {
			t.Fatalf("AggregationCheck = (%q, %v), want non-empty set ID", setID, err)
		}
		if entries := h.GetGroupEntries(t, r.client, qname, gname); len(entries) != 0 {
			t.Errorf("group not emptied after aggregation, got %d entries", len(entries))
		}
		msgs, _, err := r.ReadAggregationSet(qname, gname, setID)
		if err != nil {
			t.Fatalf("ReadAggregationSet failed: %v", err)
		}
		if diff := cmpDiffMsgIDs(msgs, []string{msg1.ID, msg2.ID}); diff != "" {
			t.Errorf("aggregation set members mismatch: (-got,+want)\n%s", diff)
		}
		hash := rosterHash(t, r, qname, gname)
		if hash["state"] != base.RosterStateAggregating {
			t.Errorf("state = %q, want aggregating", hash["state"])
		}
		if hash["agg_set"] != base.AggregationSetKey(qname, gname, setID) {
			t.Errorf("agg_set = %q, want %q", hash["agg_set"],
				base.AggregationSetKey(qname, gname, setID))
		}
		if groups, _ := r.client.SMembers(ctx, base.AllGroups(qname)).Result(); containsString(groups, gname) {
			t.Errorf("group %q should be removed from all groups set", gname)
		}
	})

	t.Run("fails and keeps waiting when group contains an unexpected task", func(t *testing.T) {
		h.FlushDB(t, r.client)
		msg1 := h.NewTaskMessageBuilder().SetType("t1").SetGroup(gname).Build()
		msg2 := h.NewTaskMessageBuilder().SetType("t2").SetGroup(gname).Build()
		extra := h.NewTaskMessageBuilder().SetType("extra").SetGroup(gname).Build()
		addGroupTask(t, r, gname, msg1)
		addGroupTask(t, r, gname, msg2)
		addGroupTask(t, r, gname, extra)
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", []string{msg1.ID, msg2.ID}); err != nil {
			t.Fatalf("ApplyGroupRoster failed: %v", err)
		}

		setID, err := check()
		if !errors.Is(err, errors.ErrRosterMemberMismatch) || setID != "" {
			t.Fatalf("AggregationCheck = (%q, %v), want (\"\", ErrRosterMemberMismatch)", setID, err)
		}
		// Nothing must have moved: all three tasks stay in the group, no aggregation set.
		if diff := cmpDiffZIDs(h.GetGroupEntries(t, r.client, qname, gname),
			[]string{msg1.ID, msg2.ID, extra.ID}); diff != "" {
			t.Errorf("group mutated after failed reconciliation: (-got,+want)\n%s", diff)
		}
		if n, _ := r.client.ZCard(ctx, base.AllAggregationSets(qname)).Result(); n != 0 {
			t.Errorf("aggregation set created despite failed reconciliation")
		}
		if hash := rosterHash(t, r, qname, gname); hash["state"] != base.RosterStateWaiting {
			t.Errorf("state = %q, want waiting", hash["state"])
		}
	})

	t.Run("fails and keeps waiting when a roster member task cannot be verified", func(t *testing.T) {
		h.FlushDB(t, r.client)
		msg1 := h.NewTaskMessageBuilder().SetType("t1").SetGroup(gname).Build()
		msg2 := h.NewTaskMessageBuilder().SetType("t2").SetGroup(gname).Build()
		addGroupTask(t, r, gname, msg1)
		addGroupTask(t, r, gname, msg2)
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", []string{msg1.ID, msg2.ID}); err != nil {
			t.Fatalf("ApplyGroupRoster failed: %v", err)
		}
		// Corrupt the member by deleting its task key while leaving the group zset entry.
		if err := r.client.Del(ctx, base.TaskKey(qname, msg2.ID)).Err(); err != nil {
			t.Fatalf("Del failed: %v", err)
		}

		setID, err := check()
		if !errors.Is(err, errors.ErrRosterMemberMismatch) || setID != "" {
			t.Fatalf("AggregationCheck = (%q, %v), want (\"\", ErrRosterMemberMismatch)", setID, err)
		}
		// The helper asserts task keys exist; this case corrupts one on purpose, so
		// inspect the raw zset instead. Nothing must have moved.
		ids, err := r.client.ZRange(ctx, base.GroupKey(qname, gname), 0, -1).Result()
		if err != nil {
			t.Fatalf("ZRange group failed: %v", err)
		}
		if diff := cmpDiffStringSet(ids, []string{msg1.ID, msg2.ID}); diff != "" {
			t.Errorf("group mutated after failed reconciliation: (-got,+want)\n%s", diff)
		}
		if n, _ := r.client.ZCard(ctx, base.AllAggregationSets(qname)).Result(); n != 0 {
			t.Errorf("aggregation set created despite failed reconciliation")
		}
	})

	t.Run("does not create a second aggregation set after a restart while aggregating", func(t *testing.T) {
		h.FlushDB(t, r.client)
		msg1 := h.NewTaskMessageBuilder().SetType("t1").SetGroup(gname).Build()
		msg2 := h.NewTaskMessageBuilder().SetType("t2").SetGroup(gname).Build()
		addGroupTask(t, r, gname, msg1)
		addGroupTask(t, r, gname, msg2)
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", []string{msg1.ID, msg2.ID}); err != nil {
			t.Fatalf("ApplyGroupRoster failed: %v", err)
		}
		setID, err := check()
		if err != nil || setID == "" {
			t.Fatalf("first AggregationCheck = (%q, %v), want set ID", setID, err)
		}

		// Simulate an aggregator restart: run the check again while state is "aggregating".
		setID2, err := check()
		if err != nil || setID2 != "" {
			t.Fatalf("second AggregationCheck = (%q, %v), want (\"\", nil)", setID2, err)
		}
		if n, _ := r.client.ZCard(ctx, base.AllAggregationSets(qname)).Result(); n != 1 {
			t.Errorf("aggregation set count = %d, want 1", n)
		}
	})

	t.Run("re-aggregates after stale set is reclaimed", func(t *testing.T) {
		h.FlushDB(t, r.client)
		clock := timeutil.NewSimulatedClock(time.Now())
		r.SetClock(clock)
		msg1 := h.NewTaskMessageBuilder().SetType("t1").SetGroup(gname).Build()
		msg2 := h.NewTaskMessageBuilder().SetType("t2").SetGroup(gname).Build()
		addGroupTask(t, r, gname, msg1)
		addGroupTask(t, r, gname, msg2)
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", []string{msg1.ID, msg2.ID}); err != nil {
			t.Fatalf("ApplyGroupRoster failed: %v", err)
		}
		setID, err := check()
		if err != nil || setID == "" {
			t.Fatalf("AggregationCheck = (%q, %v), want set ID", setID, err)
		}

		// Aggregator crashed before consuming the set; it becomes stale and gets reclaimed.
		clock.AdvanceTime(aggregationTimeout + time.Minute)
		if err := r.ReclaimStaleAggregationSets(qname); err != nil {
			t.Fatalf("ReclaimStaleAggregationSets failed: %v", err)
		}
		if hash := rosterHash(t, r, qname, gname); hash["state"] != base.RosterStateWaiting {
			t.Errorf("roster state after reclaim = %q, want waiting", hash["state"])
		}
		if groups, _ := r.client.SMembers(ctx, base.AllGroups(qname)).Result(); !containsString(groups, gname) {
			t.Errorf("group %q should be visible again after reclaim", gname)
		}
		if diff := cmpDiffZIDs(h.GetGroupEntries(t, r.client, qname, gname),
			[]string{msg1.ID, msg2.ID}); diff != "" {
			t.Errorf("tasks not returned to group after reclaim: (-got,+want)\n%s", diff)
		}

		// A new check must produce exactly one replacement aggregation set.
		setID2, err := r.AggregationCheck(qname, gname, clock.Now(), time.Second, time.Second, 1)
		if err != nil || setID2 == "" || setID2 == setID {
			t.Fatalf("post-reclaim AggregationCheck = (%q, %v), want a fresh set ID", setID2, err)
		}
		if n, _ := r.client.ZCard(ctx, base.AllAggregationSets(qname)).Result(); n != 1 {
			t.Errorf("aggregation set count = %d, want 1", n)
		}
		msgs, _, err := r.ReadAggregationSet(qname, gname, setID2)
		if err != nil {
			t.Fatalf("ReadAggregationSet failed: %v", err)
		}
		if diff := cmpDiffMsgIDs(msgs, []string{msg1.ID, msg2.ID}); diff != "" {
			t.Errorf("re-aggregated set members mismatch: (-got,+want)\n%s", diff)
		}
	})
}

func TestClearGroupRoster(t *testing.T) {
	r := setup(t)
	defer r.Close()

	const qname = "default"
	const gname = "import-batch"
	ctx := context.Background()

	t.Run("no-op without a roster", func(t *testing.T) {
		h.FlushDB(t, r.client)
		if err := r.ClearGroupRoster(ctx, qname, gname, "set1"); err != nil {
			t.Fatalf("ClearGroupRoster failed: %v", err)
		}
	})

	t.Run("deletes roster when aggregation set matches", func(t *testing.T) {
		h.FlushDB(t, r.client)
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", []string{"m1"}); err != nil {
			t.Fatalf("ApplyGroupRoster failed: %v", err)
		}
		aggSetKey := base.AggregationSetKey(qname, gname, "set1")
		if err := r.client.HSet(ctx, base.RosterKey(qname, gname),
			"state", base.RosterStateAggregating, "agg_set", aggSetKey).Err(); err != nil {
			t.Fatalf("HSet failed: %v", err)
		}
		if err := r.ClearGroupRoster(ctx, qname, gname, "set1"); err != nil {
			t.Fatalf("ClearGroupRoster failed: %v", err)
		}
		if n, _ := r.client.Exists(ctx,
			base.RosterKey(qname, gname), base.RosterMembersKey(qname, gname)).Result(); n != 0 {
			t.Errorf("roster keys still exist after ClearGroupRoster")
		}
	})

	t.Run("keeps roster when aggregation set differs", func(t *testing.T) {
		h.FlushDB(t, r.client)
		if err := r.ApplyGroupRoster(ctx, qname, gname, "batch-1", []string{"m1"}); err != nil {
			t.Fatalf("ApplyGroupRoster failed: %v", err)
		}
		aggSetKey := base.AggregationSetKey(qname, gname, "set1")
		if err := r.client.HSet(ctx, base.RosterKey(qname, gname),
			"state", base.RosterStateAggregating, "agg_set", aggSetKey).Err(); err != nil {
			t.Fatalf("HSet failed: %v", err)
		}
		if err := r.ClearGroupRoster(ctx, qname, gname, "other-set"); err != nil {
			t.Fatalf("ClearGroupRoster failed: %v", err)
		}
		if n, _ := r.client.Exists(ctx,
			base.RosterKey(qname, gname), base.RosterMembersKey(qname, gname)).Result(); n != 2 {
			t.Errorf("roster keys deleted despite mismatched aggregation set")
		}
	})
}
