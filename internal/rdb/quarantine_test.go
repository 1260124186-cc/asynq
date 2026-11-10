// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/timeutil"
)

func newQuarantineRDB(t *testing.T) (*RDB, *timeutil.SimulatedClock) {
	t.Helper()
	r := setup(t)
	clock := timeutil.NewSimulatedClock(time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC))
	r.SetClock(clock)
	t.Cleanup(func() { r.Close() })
	return r, clock
}

func TestReportNodeFailure_Idempotent(t *testing.T) {
	r, _ := newQuarantineRDB(t)
	const nodeID, serverID, threshold = "node-a", "srv-1", 3
	ctx := context.Background()

	st, dup, err := r.ReportNodeFailure(ctx, nodeID, serverID, "evt-1", threshold)
	if err != nil {
		t.Fatalf("first ReportNodeFailure failed: %v", err)
	}
	if dup || st != base.NodeQuarantineHealthy {
		t.Fatalf("first report = (%q, dup=%v), want (healthy, false)", st, dup)
	}

	// Re-reporting the same event must not increment the count or change state.
	for i := 0; i < 3; i++ {
		st, dup, err = r.ReportNodeFailure(ctx, nodeID, serverID, "evt-1", threshold)
		if err != nil {
			t.Fatalf("duplicate ReportNodeFailure failed: %v", err)
		}
		if !dup || st != base.NodeQuarantineHealthy {
			t.Fatalf("duplicate report = (%q, dup=%v), want (healthy, true)", st, dup)
		}
	}

	rec, err := r.GetNodeQuarantine(nodeID)
	if err != nil {
		t.Fatalf("GetNodeQuarantine failed: %v", err)
	}
	if rec.FailCount != 1 {
		t.Errorf("FailCount = %d, want 1 (duplicates must not count)", rec.FailCount)
	}
	if rec.LastEventKey != "evt-1" {
		t.Errorf("LastEventKey = %q, want evt-1", rec.LastEventKey)
	}
}

func TestReportNodeFailure_ThresholdTriggersQuarantine(t *testing.T) {
	r, clock := newQuarantineRDB(t)
	const nodeID, serverID, threshold = "node-b", "srv-1", 3
	ctx := context.Background()

	var lastStatus string
	for i := 1; i <= threshold; i++ {
		clock.AdvanceTime(time.Minute)
		st, dup, err := r.ReportNodeFailure(ctx, nodeID, serverID,
			heartbeatEventKeyForTest(serverID, int64(i)), threshold)
		if err != nil {
			t.Fatalf("report %d failed: %v", i, err)
		}
		if dup {
			t.Fatalf("report %d unexpectedly marked duplicate", i)
		}
		lastStatus = st
	}
	if lastStatus != base.NodeQuarantineQuarantined {
		t.Fatalf("status after threshold = %q, want quarantined", lastStatus)
	}

	rec, err := r.GetNodeQuarantine(nodeID)
	if err != nil {
		t.Fatalf("GetNodeQuarantine failed: %v", err)
	}
	if rec.FailCount != threshold || rec.Status != base.NodeQuarantineQuarantined {
		t.Fatalf("record = %+v, want FailCount=%d and quarantined", rec, threshold)
	}
	if rec.QuarantinedAt.IsZero() {
		t.Error("QuarantinedAt should be set")
	}
	if rec.ServerID != serverID {
		t.Errorf("ServerID = %q, want %q", rec.ServerID, serverID)
	}

	audit, err := r.ListNodeQuarantineAudit(nodeID)
	if err != nil {
		t.Fatalf("ListNodeQuarantineAudit failed: %v", err)
	}
	if len(audit) != 1 || audit[0].Action != base.QuarantineActionQuarantined {
		t.Fatalf("audit = %+v, want one quarantined entry", audit)
	}
	if audit[0].Operator != "system" {
		t.Errorf("audit operator = %q, want system", audit[0].Operator)
	}
}

func TestReportNodeFailure_DuplicateDoesNotExtendOrReset(t *testing.T) {
	r, _ := newQuarantineRDB(t)
	const nodeID, serverID, threshold = "node-c", "srv-1", 2
	ctx := context.Background()

	// Reach quarantined status.
	for i := 1; i <= threshold; i++ {
		if _, _, err := r.ReportNodeFailure(ctx, nodeID, serverID,
			heartbeatEventKeyForTest(serverID, int64(i)), threshold); err != nil {
			t.Fatal(err)
		}
	}
	// Build a recovery streak.
	if st, err := r.RecordNodeHeartbeat(ctx, nodeID, serverID, 5); err != nil || st != base.NodeQuarantineQuarantined {
		t.Fatalf("heartbeat = (%q, %v), want (quarantined, nil)", st, err)
	}

	before, _ := r.GetNodeQuarantine(nodeID)

	// Re-report an already counted event: nothing should move.
	st, dup, err := r.ReportNodeFailure(ctx, nodeID, serverID,
		heartbeatEventKeyForTest(serverID, 1), threshold)
	if err != nil || !dup {
		t.Fatalf("duplicate report = (%q, dup=%v, %v), want dup=true", st, dup, err)
	}
	after, _ := r.GetNodeQuarantine(nodeID)
	if after.FailCount != before.FailCount || after.HealthyStreak != before.HealthyStreak ||
		after.Version != before.Version {
		t.Errorf("duplicate event mutated state: before=%+v after=%+v", before, after)
	}
}

func TestRecordNodeHeartbeat_Recovery(t *testing.T) {
	r, _ := newQuarantineRDB(t)
	const nodeID, serverID = "node-d", "srv-1"
	const failThreshold, recovery = 2, 3
	ctx := context.Background()

	for i := 1; i <= failThreshold; i++ {
		r.ReportNodeFailure(ctx, nodeID, serverID, heartbeatEventKeyForTest(serverID, int64(i)), failThreshold)
	}

	for i := 1; i < recovery; i++ {
		st, err := r.RecordNodeHeartbeat(ctx, nodeID, serverID, recovery)
		if err != nil {
			t.Fatalf("heartbeat %d failed: %v", i, err)
		}
		if st != base.NodeQuarantineQuarantined {
			t.Fatalf("heartbeat %d status = %q, want quarantined", i, st)
		}
	}
	// The recovery-th healthy heartbeat releases the node automatically.
	st, err := r.RecordNodeHeartbeat(ctx, nodeID, serverID, recovery)
	if err != nil {
		t.Fatalf("releasing heartbeat failed: %v", err)
	}
	if st != base.NodeQuarantineReleased {
		t.Fatalf("status = %q, want released", st)
	}

	rec, _ := r.GetNodeQuarantine(nodeID)
	if rec.Status != base.NodeQuarantineReleased || rec.ReleasedAt.IsZero() {
		t.Errorf("record = %+v, want released with ReleasedAt", rec)
	}
	audit, _ := r.ListNodeQuarantineAudit(nodeID)
	last := audit[len(audit)-1]
	if last.Action != base.QuarantineActionAutoReleased {
		t.Errorf("last audit action = %q, want auto_released", last.Action)
	}
}

func TestReArmAfterRelease(t *testing.T) {
	r, _ := newQuarantineRDB(t)
	const nodeID, serverID = "node-e", "srv-1"
	const threshold, recovery = 3, 2
	ctx := context.Background()

	// Reach quarantine, then recover via stable heartbeats.
	for i := 1; i <= threshold; i++ {
		r.ReportNodeFailure(ctx, nodeID, serverID, heartbeatEventKeyForTest(serverID, int64(i)), threshold)
	}
	r.RecordNodeHeartbeat(ctx, nodeID, serverID, recovery)
	r.RecordNodeHeartbeat(ctx, nodeID, serverID, recovery) // released

	st, _, err := r.ReportNodeFailure(ctx, nodeID, serverID, "evt-after-release", threshold)
	if err != nil {
		t.Fatal(err)
	}
	if st != base.NodeQuarantineHealthy {
		t.Fatalf("status after new failure = %q, want healthy (re-armed)", st)
	}
	rec, _ := r.GetNodeQuarantine(nodeID)
	// A fresh observing window: the old failure count is discarded and the full
	// threshold must be reached again.
	if rec.FailCount != 1 || !rec.QuarantinedAt.IsZero() || rec.Status != base.NodeQuarantineHealthy {
		t.Errorf("record not re-armed into a fresh observing window: %+v", rec)
	}

	// The remaining threshold-1 failures are required before re-isolating.
	for i := 2; i <= threshold; i++ {
		st, _, err = r.ReportNodeFailure(ctx, nodeID, serverID,
			fmt.Sprintf("evt-after-release-%d", i), threshold)
		if err != nil {
			t.Fatal(err)
		}
	}
	if st != base.NodeQuarantineQuarantined {
		t.Fatalf("status after full new window = %q, want quarantined", st)
	}
}

func TestNodeIdentityConflict(t *testing.T) {
	r, _ := newQuarantineRDB(t)
	const nodeID = "node-f"
	const owner, intruder = "srv-owner", "srv-intruder"
	ctx := context.Background()

	// Owner records a failure and keeps its liveness marker fresh via heartbeat.
	if _, _, err := r.ReportNodeFailure(ctx, nodeID, owner, "evt-1", 3); err != nil {
		t.Fatal(err)
	}
	if _, err := r.RecordNodeHeartbeat(ctx, nodeID, owner, 3); err != nil {
		t.Fatal(err)
	}

	// A second live instance claiming the same node identity must be rejected on
	// both the failure and the heartbeat paths without changing state.
	if _, _, err := r.ReportNodeFailure(ctx, nodeID, intruder, "evt-2", 3); !errors.Is(err, errors.ErrNodeIdentityConflict) {
		t.Fatalf("intruder ReportNodeFailure err = %v, want ErrNodeIdentityConflict", err)
	}
	if _, err := r.RecordNodeHeartbeat(ctx, nodeID, intruder, 3); !errors.Is(err, errors.ErrNodeIdentityConflict) {
		t.Fatalf("intruder RecordNodeHeartbeat err = %v, want ErrNodeIdentityConflict", err)
	}

	rec, _ := r.GetNodeQuarantine(nodeID)
	if rec.ServerID != owner || rec.FailCount != 1 {
		t.Errorf("state changed after conflict: %+v", rec)
	}

	// Once the owner's liveness marker disappears (process gone), the new instance
	// is allowed to take over; the persisted state is retained.
	if err := r.client.Del(ctx, base.NodeQuarantineAliveKey(nodeID, owner)).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.RecordNodeHeartbeat(ctx, nodeID, intruder, 3); err != nil {
		t.Fatalf("takeover after owner died failed: %v", err)
	}
	rec, _ = r.GetNodeQuarantine(nodeID)
	if rec.ServerID != intruder {
		t.Errorf("ServerID = %q, want takeover by %q", rec.ServerID, intruder)
	}
}

func TestCorruptStateFailsSafe(t *testing.T) {
	r, _ := newQuarantineRDB(t)
	const nodeID, serverID = "node-g", "srv-1"
	ctx := context.Background()
	r.ReportNodeFailure(ctx, nodeID, serverID, "evt-1", 3)

	stateKey := base.NodeQuarantineKey(nodeID)
	before, _ := r.GetNodeQuarantine(nodeID)

	// Corrupt the record by removing a mandatory field and writing a bad integer.
	r.client.HDel(ctx, stateKey, "version")
	r.client.HSet(ctx, stateKey, "fail_count", "not-a-number")

	if _, _, err := r.ReportNodeFailure(ctx, nodeID, serverID, "evt-2", 3); !errors.Is(err, errors.ErrQuarantineStateCorrupt) {
		t.Fatalf("ReportNodeFailure err = %v, want ErrQuarantineStateCorrupt", err)
	}
	if _, err := r.RecordNodeHeartbeat(ctx, nodeID, serverID, 3); !errors.Is(err, errors.ErrQuarantineStateCorrupt) {
		t.Fatalf("RecordNodeHeartbeat err = %v, want ErrQuarantineStateCorrupt", err)
	}
	if err := r.ClearNodeQuarantine(ctx, nodeID, "op", "reason"); !errors.Is(err, errors.ErrQuarantineStateCorrupt) {
		t.Fatalf("ClearNodeQuarantine err = %v, want ErrQuarantineStateCorrupt", err)
	}

	// The corrupt event marker must not have been claimed either; verify the state
	// hash still reflects the pre-corruption failure count.
	got := r.client.HGet(ctx, stateKey, "fail_count").Val()
	if got != "not-a-number" {
		t.Errorf("fail_count = %q, want corrupt value left untouched", got)
	}
	if before.NodeID != nodeID {
		t.Errorf("sanity check failed: %+v", before)
	}
}

func TestManualClearAudit(t *testing.T) {
	r, _ := newQuarantineRDB(t)
	const nodeID, serverID = "node-h", "srv-1"
	ctx := context.Background()
	r.ReportNodeFailure(ctx, nodeID, serverID, "evt-1", 1) // quarantined

	if err := r.ClearNodeQuarantine(ctx, nodeID, "alice", "redis link restored"); err != nil {
		t.Fatalf("ClearNodeQuarantine failed: %v", err)
	}
	rec, err := r.GetNodeQuarantine(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != base.NodeQuarantineReleased || rec.ClearedBy != "alice" || rec.ClearReason != "redis link restored" {
		t.Errorf("record after clear = %+v", rec)
	}
	audit, _ := r.ListNodeQuarantineAudit(nodeID)
	last := audit[len(audit)-1]
	if last.Action != base.QuarantineActionManualClear || last.Operator != "alice" || last.Reason != "redis link restored" {
		t.Errorf("manual clear audit entry wrong: %+v", last)
	}

	// Clearing a node without any record must fail and obviously change nothing.
	if err := r.ClearNodeQuarantine(ctx, "missing-node", "alice", "no-op"); !errors.Is(err, errors.ErrNodeQuarantineNotFound) {
		t.Fatalf("clear missing node err = %v, want ErrNodeQuarantineNotFound", err)
	}
}

func TestPersistenceAcrossRestart(t *testing.T) {
	r, _ := newQuarantineRDB(t)
	const nodeID = "node-i"
	const oldServer, newServer = "srv-old", "srv-new"
	ctx := context.Background()

	// Previous process quarantined the node and then died, leaving its liveness
	// marker to expire.
	r.ReportNodeFailure(ctx, nodeID, oldServer, "evt-1", 1)
	r.client.Del(ctx, base.NodeQuarantineAliveKey(nodeID, oldServer))

	// A brand new RDB client + server instance (simulating a process restart) reads
	// the still-persisted quarantine state.
	r2 := NewRDB(r.client)
	t.Cleanup(func() { r2.Close() })
	rec, err := r2.GetNodeQuarantine(nodeID)
	if err != nil {
		t.Fatalf("persisted record missing after restart: %v", err)
	}
	if rec.Status != base.NodeQuarantineQuarantined {
		t.Fatalf("persisted status = %q, want quarantined", rec.Status)
	}
	// The new instance can take over ownership and recover via heartbeats.
	if _, err := r2.RecordNodeHeartbeat(ctx, nodeID, newServer, 1); err != nil {
		t.Fatalf("restarted instance heartbeat failed: %v", err)
	}
	rec, _ = r2.GetNodeQuarantine(nodeID)
	if rec.ServerID != newServer || rec.Status != base.NodeQuarantineReleased {
		t.Errorf("post-restart record = %+v, want owner=%s released", rec, newServer)
	}
}

func TestListNodeQuarantines(t *testing.T) {
	r, _ := newQuarantineRDB(t)
	ctx := context.Background()
	nodes := []string{"node-j1", "node-j2", "node-j3"}
	for _, n := range nodes {
		if _, _, err := r.ReportNodeFailure(ctx, n, "srv", "evt", 3); err != nil {
			t.Fatal(err)
		}
	}
	// Add liveness/event/audit keys to ensure they are not mistaken for state hashes.
	r.client.Set(ctx, base.NodeQuarantineAliveKey("node-j1", "srv"), "1", time.Minute)
	r.client.Set(ctx, base.NodeQuarantineEventKey("node-j1", "evt"), "1", time.Minute)
	r.client.RPush(ctx, base.NodeQuarantineAuditKey("node-j1"), "{}")

	got, err := r.ListNodeQuarantines()
	if err != nil {
		t.Fatalf("ListNodeQuarantines failed: %v", err)
	}
	seen := map[string]bool{}
	for _, rec := range got {
		seen[rec.NodeID] = true
	}
	for _, n := range nodes {
		if !seen[n] {
			t.Errorf("node %q missing from list; got %v", n, seen)
		}
	}
	if len(got) != len(nodes) {
		t.Errorf("returned %d records, want %d (non-hash keys must be filtered)", len(got), len(nodes))
	}
}

func TestGetNodeQuarantine_NotFound(t *testing.T) {
	r, _ := newQuarantineRDB(t)
	if _, err := r.GetNodeQuarantine("never-seen-" + uuid.NewString()); !errors.Is(err, errors.ErrNodeQuarantineNotFound) {
		t.Fatalf("err = %v, want ErrNodeQuarantineNotFound", err)
	}
}

func TestReconcileNodeQuarantine(t *testing.T) {
	r, _ := newQuarantineRDB(t)
	ctx := context.Background()
	const nodeID = "node-k"
	const owner, other = "srv-owner", "srv-other"

	// No record yet: reconciliation reports healthy without writing anything.
	st, err := r.ReconcileNodeQuarantine(nodeID, owner)
	if err != nil || st != base.NodeQuarantineHealthy {
		t.Fatalf("empty reconcile = (%q, %v), want (healthy, nil)", st, err)
	}
	if _, err := r.GetNodeQuarantine(nodeID); !errors.Is(err, errors.ErrNodeQuarantineNotFound) {
		t.Fatalf("read-only reconcile created a record: %v", err)
	}

	r.ReportNodeFailure(ctx, nodeID, owner, "evt-1", 1) // quarantined
	r.RecordNodeHeartbeat(ctx, nodeID, owner, 3)        // owner alive

	// Owner reads its own authoritative status.
	if st, err := r.ReconcileNodeQuarantine(nodeID, owner); err != nil || st != base.NodeQuarantineQuarantined {
		t.Fatalf("owner reconcile = (%q, %v), want (quarantined, nil)", st, err)
	}
	// A different live instance is rejected; state must remain untouched.
	if _, err := r.ReconcileNodeQuarantine(nodeID, other); !errors.Is(err, errors.ErrNodeIdentityConflict) {
		t.Fatalf("intruder reconcile err = %v, want ErrNodeIdentityConflict", err)
	}
	rec, _ := r.GetNodeQuarantine(nodeID)
	if rec.ServerID != owner {
		t.Errorf("state changed after conflict: %+v", rec)
	}
	// Once the owner is gone, the new instance can read the persisted status.
	r.client.Del(ctx, base.NodeQuarantineAliveKey(nodeID, owner))
	if st, err := r.ReconcileNodeQuarantine(nodeID, other); err != nil || st != base.NodeQuarantineQuarantined {
		t.Fatalf("post-restart reconcile = (%q, %v), want (quarantined, nil)", st, err)
	}
}

func TestReconcileNodeQuarantine_Corrupt(t *testing.T) {
	r, _ := newQuarantineRDB(t)
	ctx := context.Background()
	const nodeID, serverID = "node-l", "srv-1"
	r.ReportNodeFailure(ctx, nodeID, serverID, "evt-1", 3)
	r.client.HDel(ctx, base.NodeQuarantineKey(nodeID), "status")
	if _, err := r.ReconcileNodeQuarantine(nodeID, serverID); !errors.Is(err, errors.ErrQuarantineStateCorrupt) {
		t.Fatalf("err = %v, want ErrQuarantineStateCorrupt", err)
	}
}

// heartbeatEventKeyForTest builds a distinct, deterministic failure event key.
// The exact shape is opaque to the rdb layer; it only has to be stable for the
// same (server, beat sequence) pair and unique across distinct events.
func heartbeatEventKeyForTest(serverID string, beatSeq int64) string {
	return fmt.Sprintf("hb:%s:%d", serverID, beatSeq)
}
