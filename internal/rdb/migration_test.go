// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	h "github.com/hibiken/asynq/internal/testutil"
)

func TestListMigrationCandidates(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	const qname = "default"
	pending1 := h.NewTaskMessage("email:v1", h.JSON(map[string]interface{}{"to": "a@example.com"}))
	pending2 := h.NewTaskMessageWithQueue("other:v1", h.JSON(map[string]interface{}{}), qname)
	scheduled1 := h.NewTaskMessage("email:v1", h.JSON(map[string]interface{}{"to": "b@example.com"}))
	scheduled2 := h.NewTaskMessage("report:v1", h.JSON(map[string]interface{}{"id": 7}))
	retry1 := h.NewTaskMessage("email:v1", h.JSON(map[string]interface{}{"to": "c@example.com"}))
	retry2 := h.NewTaskMessage("report:v1", h.JSON(map[string]interface{}{"id": 9}))
	active1 := h.NewTaskMessage("email:v1", h.JSON(map[string]interface{}{"to": "active@example.com"}))
	archived1 := h.NewTaskMessage("email:v1", h.JSON(map[string]interface{}{"to": "archived@example.com"}))
	completed1 := h.NewTaskMessage("email:v1", h.JSON(map[string]interface{}{"to": "done@example.com"}))

	h.SeedPendingQueue(t, r.client, []*base.TaskMessage{pending1, pending2}, qname)
	now := time.Now().Unix()
	h.SeedScheduledQueue(t, r.client, []base.Z{
		{Message: scheduled1, Score: now + 60},
		{Message: scheduled2, Score: now + 120},
	}, qname)
	h.SeedRetryQueue(t, r.client, []base.Z{
		{Message: retry1, Score: now + 30},
		{Message: retry2, Score: now + 90},
	}, qname)
	h.SeedActiveQueue(t, r.client, []*base.TaskMessage{active1}, qname)
	h.SeedArchivedQueue(t, r.client, []base.Z{{Message: archived1, Score: now}}, qname)
	h.SeedCompletedQueue(t, r.client, []base.Z{{Message: completed1, Score: now}}, qname)

	got, err := r.ListMigrationCandidates(qname)
	if err != nil {
		t.Fatalf("ListMigrationCandidates returned error: %v", err)
	}
	if len(got) != 6 {
		t.Fatalf("ListMigrationCandidates returned %d candidates, want 6 (pending+scheduled+retry only)", len(got))
	}

	wantByID := map[string]*MigrationCandidate{
		pending1.ID:   {Message: pending1, State: base.TaskStatePending},
		pending2.ID:   {Message: pending2, State: base.TaskStatePending},
		scheduled1.ID: {Message: scheduled1, State: base.TaskStateScheduled},
		scheduled2.ID: {Message: scheduled2, State: base.TaskStateScheduled},
		retry1.ID:     {Message: retry1, State: base.TaskStateRetry},
		retry2.ID:     {Message: retry2, State: base.TaskStateRetry},
	}
	for _, c := range got {
		want, ok := wantByID[c.Message.ID]
		if !ok {
			t.Errorf("unexpected candidate id=%q state=%s", c.Message.ID, c.State)
			continue
		}
		if c.State != want.State {
			t.Errorf("candidate id=%q state=%s, want %s", c.Message.ID, c.State, want.State)
		}
		if diff := cmp.Diff(want.Message, c.Message); diff != "" {
			t.Errorf("candidate id=%q message mismatch (-want,+got):\n%s", c.Message.ID, diff)
		}
		// Raw encoded bytes must match what is actually stored.
		stored := r.client.HGet(ctx, base.TaskKey(qname, c.Message.ID), "msg").Val()
		if string(c.Encoded) != stored {
			t.Errorf("candidate id=%q encoded bytes do not match stored msg field", c.Message.ID)
		}
	}

	// Active, archived and completed tasks must never be candidates.
	for _, id := range []string{active1.ID, archived1.ID, completed1.ID} {
		for _, c := range got {
			if c.Message.ID == id {
				t.Errorf("task id=%q in an ineligible state was returned as candidate", id)
			}
		}
	}

	// Unknown queue must surface QueueNotFoundError.
	if _, err := r.ListMigrationCandidates("nonexistent-queue"); !errors.IsQueueNotFound(err) {
		t.Errorf("ListMigrationCandidates for unknown queue returned %v, want QueueNotFoundError", err)
	}
}

func TestCommitPayloadMigration(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	now := time.Now().Unix()
	pendingMsg := h.NewTaskMessage("email:v1", []byte(`{"v":1}`))
	scheduledMsg := h.NewTaskMessage("email:v1", []byte(`{"v":1}`))
	retryMsg := h.NewTaskMessage("email:v1", []byte(`{"v":1}`))
	h.SeedPendingQueue(t, r.client, []*base.TaskMessage{pendingMsg}, base.DefaultQueueName)
	// Add pending_since to verify non-msg hash fields are preserved.
	r.client.HSet(ctx, base.TaskKey(pendingMsg.Queue, pendingMsg.ID), "pending_since", now)
	h.SeedScheduledQueue(t, r.client, []base.Z{{Message: scheduledMsg, Score: now + 60}}, base.DefaultQueueName)
	h.SeedRetryQueue(t, r.client, []base.Z{{Message: retryMsg, Score: now + 30}}, base.DefaultQueueName)

	migrated := func(msg *base.TaskMessage) []byte {
		updated := *msg
		updated.Payload = []byte(`{"v":2}`)
		updated.Headers = map[string]string{base.PayloadVersionHeader: "2"}
		encoded, err := base.EncodeMessage(&updated)
		if err != nil {
			t.Fatalf("cannot encode migrated message: %v", err)
		}
		return encoded
	}

	tests := []struct {
		name  string
		id    string
		state base.TaskState
		score int64
	}{
		{"pending", pendingMsg.ID, base.TaskStatePending, 0},
		{"scheduled", scheduledMsg.ID, base.TaskStateScheduled, now + 60},
		{"retry", retryMsg.ID, base.TaskStateRetry, now + 30},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldEncoded := r.client.HGet(ctx, base.TaskKey(base.DefaultQueueName, tc.id), "msg").Val()
			status, err := r.CommitPayloadMigration(base.DefaultQueueName, tc.id, tc.state,
				[]byte(oldEncoded), migrated(&base.TaskMessage{ID: tc.id}))
			if err != nil {
				t.Fatalf("CommitPayloadMigration returned error: %v", err)
			}
			if status != PayloadMigrationCommitted {
				t.Fatalf("CommitPayloadMigration status=%d, want PayloadMigrationCommitted", status)
			}

			stored, err := base.DecodeMessage([]byte(r.client.HGet(ctx,
				base.TaskKey(base.DefaultQueueName, tc.id), "msg").Val()))
			if err != nil {
				t.Fatalf("cannot decode stored message: %v", err)
			}
			if string(stored.Payload) != `{"v":2}` {
				t.Errorf("stored payload=%q, want v2 payload", string(stored.Payload))
			}
			if got := stored.Headers[base.PayloadVersionHeader]; got != "2" {
				t.Errorf("version header=%q, want 2", got)
			}
			if stored.ID != tc.id {
				t.Errorf("task id changed: got %q, want %q", stored.ID, tc.id)
			}
			if got := r.client.HGet(ctx, base.TaskKey(base.DefaultQueueName, tc.id), "state").Val(); got != tc.state.String() {
				t.Errorf("state field=%q, want %q", got, tc.state.String())
			}
		})
	}

	// Scheduled/retry sorted-set membership and score must be untouched.
	for _, tc := range []struct {
		key   string
		id    string
		score int64
	}{
		{base.ScheduledKey(base.DefaultQueueName), scheduledMsg.ID, now + 60},
		{base.RetryKey(base.DefaultQueueName), retryMsg.ID, now + 30},
	} {
		score, err := r.client.ZScore(ctx, tc.key, tc.id).Result()
		if err != nil {
			t.Fatalf("ZSCORE %q %q: %v", tc.key, tc.id, err)
		}
		if int64(score) != tc.score {
			t.Errorf("score for %q in %q = %d, want %d", tc.id, tc.key, int64(score), tc.score)
		}
	}
	// pending_since hash field must survive.
	if got := r.client.HGet(ctx, base.TaskKey(base.DefaultQueueName, pendingMsg.ID), "pending_since").Val(); got != strconv.FormatInt(now, 10) {
		t.Errorf("pending_since=%q, want %d", got, now)
	}
}

func TestCommitPayloadMigrationGuards(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	const qname = "default"

	t.Run("task not found", func(t *testing.T) {
		msg := h.NewTaskMessage("email:v1", []byte(`{"v":1}`))
		h.SeedPendingQueue(t, r.client, []*base.TaskMessage{msg}, qname)
		encoded := h.MustMarshal(t, msg)
		status, err := r.CommitPayloadMigration(qname, "missing-id", base.TaskStatePending, []byte(encoded), []byte(encoded))
		if err != nil {
			t.Fatalf("CommitPayloadMigration returned error: %v", err)
		}
		if status != PayloadMigrationTaskNotFound {
			t.Errorf("status=%d, want PayloadMigrationTaskNotFound", status)
		}
	})

	t.Run("state changed to active", func(t *testing.T) {
		msg := h.NewTaskMessage("email:v1", []byte(`{"v":1}`))
		h.SeedPendingQueue(t, r.client, []*base.TaskMessage{msg}, qname)
		oldEncoded := r.client.HGet(ctx, base.TaskKey(qname, msg.ID), "msg").Val()
		// Simulate the task being dequeued between scan and commit.
		r.client.HSet(ctx, base.TaskKey(qname, msg.ID), "state", "active")
		status, err := r.CommitPayloadMigration(qname, msg.ID, base.TaskStatePending,
			[]byte(oldEncoded), []byte("new-encoded"))
		if err != nil {
			t.Fatalf("CommitPayloadMigration returned error: %v", err)
		}
		if status != PayloadMigrationStateChanged {
			t.Errorf("status=%d, want PayloadMigrationStateChanged", status)
		}
		if got := r.client.HGet(ctx, base.TaskKey(qname, msg.ID), "msg").Val(); got != oldEncoded {
			t.Error("msg field was modified despite state change")
		}
	})

	t.Run("message changed concurrently", func(t *testing.T) {
		msg := h.NewTaskMessage("email:v1", []byte(`{"v":1}`))
		h.SeedRetryQueue(t, r.client, []base.Z{{Message: msg, Score: time.Now().Unix()}}, qname)
		oldEncoded := r.client.HGet(ctx, base.TaskKey(qname, msg.ID), "msg").Val()

		tampered := *msg
		tampered.Payload = []byte(`{"v":1,"retried_elsewhere":true}`)
		tamperedEncoded := h.MustMarshal(t, &tampered)
		r.client.HSet(ctx, base.TaskKey(qname, msg.ID), "msg", tamperedEncoded)

		status, err := r.CommitPayloadMigration(qname, msg.ID, base.TaskStateRetry,
			[]byte(oldEncoded), []byte("new-encoded"))
		if err != nil {
			t.Fatalf("CommitPayloadMigration returned error: %v", err)
		}
		if status != PayloadMigrationMessageChanged {
			t.Errorf("status=%d, want PayloadMigrationMessageChanged", status)
		}
		if got := r.client.HGet(ctx, base.TaskKey(qname, msg.ID), "msg").Val(); got != tamperedEncoded {
			t.Error("msg field was overwritten despite concurrent modification")
		}
	})

	t.Run("ineligible state argument", func(t *testing.T) {
		msg := h.NewTaskMessage("email:v1", []byte(`{"v":1}`))
		h.SeedActiveQueue(t, r.client, []*base.TaskMessage{msg}, qname)
		encoded := h.MustMarshal(t, msg)
		_, err := r.CommitPayloadMigration(qname, msg.ID, base.TaskStateActive, []byte(encoded), []byte(encoded))
		if err == nil || errors.CanonicalCode(err) != errors.FailedPrecondition {
			t.Errorf("got %v, want FailedPrecondition error", err)
		}
	})
}

func TestCommitPayloadMigrationDequeueSeesNewPayload(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()

	const qname = "default"
	msg := h.NewTaskMessage("email:v1", []byte(`{"v":1}`))
	h.SeedPendingQueue(t, r.client, []*base.TaskMessage{msg}, qname)
	oldEncoded := r.client.HGet(ctx, base.TaskKey(qname, msg.ID), "msg").Val()

	updated := *msg
	updated.Payload = []byte(`{"v":2}`)
	updated.Headers = map[string]string{base.PayloadVersionHeader: "2"}
	newEncoded, err := base.EncodeMessage(&updated)
	if err != nil {
		t.Fatalf("EncodeMessage: %v", err)
	}
	if _, err := r.CommitPayloadMigration(qname, msg.ID, base.TaskStatePending,
		[]byte(oldEncoded), newEncoded); err != nil {
		t.Fatalf("CommitPayloadMigration: %v", err)
	}

	got, _, err := r.Dequeue(qname)
	if err != nil {
		t.Fatalf("Dequeue returned error: %v", err)
	}
	if got.ID != msg.ID {
		t.Errorf("dequeued id=%q, want %q", got.ID, msg.ID)
	}
	if string(got.Payload) != `{"v":2}` {
		t.Errorf("dequeued payload=%q, want v2 payload", string(got.Payload))
	}
	if got.Headers[base.PayloadVersionHeader] != "2" {
		t.Errorf("dequeued version header=%q, want 2", got.Headers[base.PayloadVersionHeader])
	}
}
