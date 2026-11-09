// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
	h "github.com/hibiken/asynq/internal/testutil"
)

const testMigrationTaskType = "email:send"

// v1Payload builds a recognizable v1 payload.
func v1Payload(email string) []byte {
	b, _ := json.Marshal(map[string]interface{}{"schema": 1, "email": email})
	return b
}

// v2Payload is what the test converter produces.
func v2Payload(email string) []byte {
	b, _ := json.Marshal(map[string]interface{}{"schema": 2, "email": email, "to": email})
	return b
}

// newTestConverter returns a v1->v2 converter and a call counter.
func newTestConverter(failOn string) (func([]byte) ([]byte, error), *int) {
	calls := 0
	convert := func(old []byte) ([]byte, error) {
		calls++
		var in map[string]interface{}
		if err := json.Unmarshal(old, &in); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrUnrecognizedPayload, err)
		}
		email, _ := in["email"].(string)
		if email == failOn {
			return nil, fmt.Errorf("simulated conversion failure for %q", email)
		}
		return v2Payload(email), nil
	}
	return convert, &calls
}

func emailMigration(convert func([]byte) ([]byte, error)) *PayloadMigration {
	return &PayloadMigration{
		TaskType: testMigrationTaskType,
		Version:  2,
		Convert:  convert,
	}
}

// findTask returns the message with the given id from a slice.
func findTask(t *testing.T, msgs []*base.TaskMessage, id string) *base.TaskMessage {
	t.Helper()
	for _, m := range msgs {
		if m.ID == id {
			return m
		}
	}
	t.Fatalf("task id=%q not found", id)
	return nil
}

func TestMigrateTaskPayloadEligibleStatesAndPreservation(t *testing.T) {
	r := setup(t)
	defer r.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()
	ctx := context.Background()

	const qname = "default"
	score1 := time.Now().Add(time.Hour).Unix()
	score2 := time.Now().Add(2 * time.Hour).Unix()

	pending1 := h.NewTaskMessage(testMigrationTaskType, v1Payload("a@example.com"))
	pending1.Headers = map[string]string{"x-trace": "trace-1"}
	pending1.UniqueKey = "asynq:{default}:unique:email:send:deadbeef"
	pending2 := h.NewTaskMessage(testMigrationTaskType, v1Payload("b@example.com"))
	scheduled := h.NewTaskMessage(testMigrationTaskType, v1Payload("c@example.com"))
	retry := h.NewTaskMessage(testMigrationTaskType, v1Payload("d@example.com"))
	retry.Retry = 5
	retry.Retried = 2
	retry.ErrorMsg = "boom"
	retry.LastFailedAt = time.Now().Add(-time.Minute).Unix()
	retry.Timeout = 900
	retry.Deadline = time.Now().Add(time.Hour).Unix()
	retry.Retention = 3600
	active := h.NewTaskMessage(testMigrationTaskType, v1Payload("active@example.com"))
	completed := h.NewTaskMessage(testMigrationTaskType, v1Payload("done@example.com"))
	archived := h.NewTaskMessage(testMigrationTaskType, v1Payload("archived@example.com"))
	other := h.NewTaskMessage("other:task", v1Payload("other@example.com"))

	h.SeedPendingQueue(t, r, []*base.TaskMessage{pending1, pending2, other}, qname)
	h.SeedScheduledQueue(t, r, []base.Z{{Message: scheduled, Score: score1}}, qname)
	h.SeedRetryQueue(t, r, []base.Z{{Message: retry, Score: score2}}, qname)
	h.SeedActiveQueue(t, r, []*base.TaskMessage{active}, qname)
	h.SeedCompletedQueue(t, r, []base.Z{{Message: completed, Score: time.Now().Unix()}}, qname)
	h.SeedArchivedQueue(t, r, []base.Z{{Message: archived, Score: time.Now().Unix()}}, qname)

	convert, calls := newTestConverter("")
	result, err := inspector.MigrateTaskPayload(nil, []*PayloadMigration{emailMigration(convert)})
	if err != nil {
		t.Fatalf("MigrateTaskPayload returned error: %v", err)
	}
	if result.Scanned != 4 {
		t.Errorf("Scanned=%d, want 4", result.Scanned)
	}
	if len(result.Migrated) != 4 {
		t.Fatalf("Migrated=%d, want 4", len(result.Migrated))
	}
	if len(result.Skipped) != 0 {
		t.Errorf("Skipped=%v, want empty", result.Skipped)
	}
	if *calls != 4 {
		t.Errorf("converter invoked %d times, want 4", *calls)
	}

	// Pending tasks: payload replaced, everything else preserved.
	pendingMsgs := h.GetPendingMessages(t, r, qname)
	got := findTask(t, pendingMsgs, pending1.ID)
	if string(got.Payload) != string(v2Payload("a@example.com")) {
		t.Errorf("pending1 payload=%s, want v2", got.Payload)
	}
	if got.Headers[base.PayloadVersionHeader] != "2" {
		t.Errorf("pending1 version header=%q, want 2", got.Headers[base.PayloadVersionHeader])
	}
	if got.Headers["x-trace"] != "trace-1" {
		t.Errorf("user header lost: %v", got.Headers)
	}
	if got.UniqueKey != pending1.UniqueKey || got.ID != pending1.ID || got.Retry != pending1.Retry {
		t.Errorf("execution options not preserved: got=%+v want=%+v", got, pending1)
	}
	got2 := findTask(t, pendingMsgs, pending2.ID)
	if string(got2.Payload) != string(v2Payload("b@example.com")) {
		t.Errorf("pending2 payload=%s, want v2", got2.Payload)
	}
	// Unmatched task type must be untouched.
	otherGot := findTask(t, pendingMsgs, other.ID)
	if string(otherGot.Payload) != string(v1Payload("other@example.com")) {
		t.Errorf("unmatched task payload was modified: %s", otherGot.Payload)
	}
	if _, ok := otherGot.Headers[base.PayloadVersionHeader]; ok {
		t.Errorf("version header stamped on unmatched task")
	}

	// Scheduled: score (process time) preserved, fields preserved.
	schedEntries := h.GetScheduledEntries(t, r, qname)
	if len(schedEntries) != 1 || schedEntries[0].Score != score1 {
		t.Fatalf("scheduled entries changed: %+v", schedEntries)
	}
	schedMsg := schedEntries[0].Message
	if schedMsg.ID != scheduled.ID || string(schedMsg.Payload) != string(v2Payload("c@example.com")) {
		t.Errorf("scheduled task not migrated correctly: %+v", schedMsg)
	}

	// Retry: score, retried counter, error info preserved.
	retryEntries := h.GetRetryEntries(t, r, qname)
	if len(retryEntries) != 1 || retryEntries[0].Score != score2 {
		t.Fatalf("retry entries changed: %+v", retryEntries)
	}
	retryGot := retryEntries[0].Message
	if retryGot.ID != retry.ID {
		t.Errorf("retry task id changed: %q", retryGot.ID)
	}
	if string(retryGot.Payload) != string(v2Payload("d@example.com")) {
		t.Errorf("retry payload=%s, want v2", retryGot.Payload)
	}
	if retryGot.Retried != 2 || retryGot.Retry != 5 || retryGot.ErrorMsg != "boom" ||
		retryGot.Timeout != 900 || retryGot.Deadline != retry.Deadline || retryGot.Retention != 3600 ||
		retryGot.LastFailedAt != retry.LastFailedAt {
		t.Errorf("retry execution options not preserved: got=%+v", retryGot)
	}

	// Active, completed and archived must be completely untouched.
	for _, tc := range []struct {
		name        string
		id          string
		wantPayload []byte
		msgs        []*base.TaskMessage
	}{
		{"active", active.ID, active.Payload, h.GetActiveMessages(t, r, qname)},
		{"completed", completed.ID, completed.Payload, h.GetCompletedMessages(t, r, qname)},
		{"archived", archived.ID, archived.Payload, h.GetArchivedMessages(t, r, qname)},
	} {
		m := findTask(t, tc.msgs, tc.id)
		if string(m.Payload) != string(tc.wantPayload) {
			t.Errorf("%s task payload was modified: %s", tc.name, m.Payload)
		}
		if _, ok := m.Headers[base.PayloadVersionHeader]; ok {
			t.Errorf("version header stamped on %s task", tc.name)
		}
	}

	// State fields on the task hashes must remain the original states.
	for _, tc := range []struct {
		id    string
		state string
	}{
		{pending1.ID, "pending"}, {scheduled.ID, "scheduled"}, {retry.ID, "retry"},
		{active.ID, "active"}, {completed.ID, "completed"}, {archived.ID, "archived"},
	} {
		if got := r.HGet(ctx, base.TaskKey(qname, tc.id), "state").Val(); got != tc.state {
			t.Errorf("task %s state=%q, want %q", tc.id, got, tc.state)
		}
	}
}

func TestMigrateTaskPayloadIdempotentAndVersioned(t *testing.T) {
	r := setup(t)
	defer r.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	const qname = "default"
	m1 := h.NewTaskMessage(testMigrationTaskType, v1Payload("1@example.com"))
	m2 := h.NewTaskMessage(testMigrationTaskType, v1Payload("2@example.com"))
	m3 := h.NewTaskMessage(testMigrationTaskType, v1Payload("3@example.com"))
	score := time.Now().Add(time.Hour).Unix()
	h.SeedPendingQueue(t, r, []*base.TaskMessage{m1}, qname)
	h.SeedScheduledQueue(t, r, []base.Z{{Message: m2, Score: score}}, qname)
	h.SeedRetryQueue(t, r, []base.Z{{Message: m3, Score: score}}, qname)

	convert, calls := newTestConverter("")

	first, err := inspector.MigrateTaskPayload(nil, []*PayloadMigration{emailMigration(convert)})
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if len(first.Migrated) != 3 || *calls != 3 {
		t.Fatalf("first run: migrated=%d calls=%d, want 3/3", len(first.Migrated), *calls)
	}

	// Second run must not convert anything twice.
	second, err := inspector.MigrateTaskPayload(nil, []*PayloadMigration{emailMigration(convert)})
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if second.Scanned != 3 || len(second.Migrated) != 0 || len(second.Skipped) != 3 {
		t.Fatalf("second run: scanned=%d migrated=%d skipped=%d, want 3/0/3",
			second.Scanned, len(second.Migrated), len(second.Skipped))
	}
	for _, s := range second.Skipped {
		if s.Reason != PayloadMigrationSkipAlreadyCurrent {
			t.Errorf("skip reason=%q, want already_current", s.Reason)
		}
	}
	if *calls != 3 {
		t.Errorf("converter invoked %d times after rerun, want still 3", *calls)
	}

	// A newer migration version converts the same tasks once more.
	upgradeCalls := 0
	v3Migration := &PayloadMigration{
		TaskType: testMigrationTaskType,
		Version:  3,
		Convert: func(old []byte) ([]byte, error) {
			upgradeCalls++
			var in map[string]interface{}
			if err := json.Unmarshal(old, &in); err != nil {
				return nil, err
			}
			in["schema"] = 3
			return json.Marshal(in)
		},
	}
	third, err := inspector.MigrateTaskPayload(nil, []*PayloadMigration{v3Migration})
	if err != nil {
		t.Fatalf("v3 run: %v", err)
	}
	if len(third.Migrated) != 3 || upgradeCalls != 3 {
		t.Fatalf("v3 run: migrated=%d calls=%d, want 3/3", len(third.Migrated), upgradeCalls)
	}
	for _, m := range h.GetPendingMessages(t, r, qname) {
		if m.Headers[base.PayloadVersionHeader] != "3" {
			t.Errorf("pending version header=%q, want 3", m.Headers[base.PayloadVersionHeader])
		}
	}
}

func TestMigrateTaskPayloadConversionFailureKeepsOriginalState(t *testing.T) {
	r := setup(t)
	defer r.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	const qname = "default"
	good1 := h.NewTaskMessage(testMigrationTaskType, v1Payload("good1@example.com"))
	good2 := h.NewTaskMessage(testMigrationTaskType, v1Payload("good2@example.com"))
	bad := h.NewTaskMessage(testMigrationTaskType, v1Payload("bad@example.com"))
	h.SeedPendingQueue(t, r, []*base.TaskMessage{good1, good2, bad}, qname)

	// Converter fails for the "bad" task; phase one must abort before any write.
	convert, calls := newTestConverter("bad@example.com")
	result, err := inspector.MigrateTaskPayload(nil, []*PayloadMigration{emailMigration(convert)})
	if err == nil {
		t.Fatal("MigrateTaskPayload returned nil error, want conversion failure")
	}
	if result != nil {
		t.Errorf("result=%v, want nil on failure", result)
	}
	var convErr *PayloadConversionError
	if !errors.As(err, &convErr) {
		t.Fatalf("error type=%T, want *PayloadConversionError", err)
	}
	if convErr.ID != bad.ID || convErr.Queue != qname || convErr.Type != testMigrationTaskType {
		t.Errorf("conversion error identity=%+v, want id=%s", convErr, bad.ID)
	}
	if *calls == 0 {
		t.Error("converter was never invoked")
	}

	// Every task must still carry its original v1 payload with no marker.
	for _, m := range h.GetPendingMessages(t, r, qname) {
		email := map[string]interface{}{}
		_ = json.Unmarshal(m.Payload, &email)
		if email["schema"].(float64) != 1 {
			t.Errorf("task %s payload changed after failed migration: %s", m.ID, m.Payload)
		}
		if _, ok := m.Headers[base.PayloadVersionHeader]; ok {
			t.Errorf("task %s got version header despite failed migration", m.ID)
		}
	}

	// ErrUnrecognizedPayload must be reachable via errors.Is when wrapped.
	unrecognized := &PayloadMigration{
		TaskType: testMigrationTaskType,
		Version:  2,
		Convert: func([]byte) ([]byte, error) {
			return nil, fmt.Errorf("%w: garbage", ErrUnrecognizedPayload)
		},
	}
	if _, err := inspector.MigrateTaskPayload(nil, []*PayloadMigration{unrecognized}); !errors.Is(err, ErrUnrecognizedPayload) {
		t.Errorf("got %v, want error wrapping ErrUnrecognizedPayload", err)
	}
}

func TestMigrateTaskPayloadResumeAfterInterruption(t *testing.T) {
	r := setup(t)
	defer r.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()
	ctx := context.Background()

	const qname = "default"
	var msgs []*base.TaskMessage
	for _, email := range []string{"1@example.com", "2@example.com", "3@example.com", "4@example.com"} {
		msgs = append(msgs, h.NewTaskMessage(testMigrationTaskType, v1Payload(email)))
	}
	var entries []base.Z
	for _, m := range msgs {
		entries = append(entries, base.Z{Message: m, Score: time.Now().Add(time.Hour).Unix()})
	}
	h.SeedRetryQueue(t, r, entries, qname)

	// Simulate an interrupted run: the first two tasks were committed before
	// the process died.
	for _, m := range msgs[:2] {
		var in map[string]interface{}
		_ = json.Unmarshal(m.Payload, &in)
		updated := *m
		updated.Payload = v2Payload(in["email"].(string))
		updated.Headers = map[string]string{base.PayloadVersionHeader: "2"}
		encoded := h.MustMarshal(t, &updated)
		if err := r.HSet(ctx, base.TaskKey(qname, m.ID), "msg", encoded).Err(); err != nil {
			t.Fatalf("seed interrupted state: %v", err)
		}
	}

	convert, calls := newTestConverter("")
	result, err := inspector.MigrateTaskPayload(nil, []*PayloadMigration{emailMigration(convert)})
	if err != nil {
		t.Fatalf("resume run: %v", err)
	}
	if result.Scanned != 4 || len(result.Migrated) != 2 || len(result.Skipped) != 2 {
		t.Fatalf("resume run: scanned=%d migrated=%d skipped=%d, want 4/2/2",
			result.Scanned, len(result.Migrated), len(result.Skipped))
	}
	if *calls != 2 {
		t.Errorf("converter invoked %d times on resume, want 2 (only unmigrated tasks)", *calls)
	}

	// All four tasks must now expose the v2 payload with the version marker.
	for _, m := range h.GetRetryMessages(t, r, qname) {
		var in map[string]interface{}
		if err := json.Unmarshal(m.Payload, &in); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		if in["schema"].(float64) != 2 {
			t.Errorf("task %s still on schema %v", m.ID, in["schema"])
		}
		if m.Headers[base.PayloadVersionHeader] != "2" {
			t.Errorf("task %s missing version header", m.ID)
		}
	}
}

func TestMigrateTaskPayloadQueueSelection(t *testing.T) {
	r := setup(t)
	defer r.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	q1, q2 := "queue1", "queue2"
	m1 := h.NewTaskMessageWithQueue(testMigrationTaskType, v1Payload("q1@example.com"), q1)
	m2 := h.NewTaskMessageWithQueue(testMigrationTaskType, v1Payload("q2@example.com"), q2)
	h.SeedPendingQueue(t, r, []*base.TaskMessage{m1}, q1)
	h.SeedPendingQueue(t, r, []*base.TaskMessage{m2}, q2)

	convert, _ := newTestConverter("")

	// Explicit queue list touches only that queue.
	if _, err := inspector.MigrateTaskPayload([]string{q1}, []*PayloadMigration{emailMigration(convert)}); err != nil {
		t.Fatalf("migrate q1: %v", err)
	}
	if got := findTask(t, h.GetPendingMessages(t, r, q1), m1.ID); string(got.Payload) == string(v1Payload("q1@example.com")) {
		t.Error("q1 task was not migrated")
	}
	if got := findTask(t, h.GetPendingMessages(t, r, q2), m2.ID); string(got.Payload) != string(v1Payload("q2@example.com")) {
		t.Error("q2 task was migrated although queue was not selected")
	}

	// Empty queue list expands to all known queues.
	if _, err := inspector.MigrateTaskPayload(nil, []*PayloadMigration{emailMigration(convert)}); err != nil {
		t.Fatalf("migrate all queues: %v", err)
	}
	if got := findTask(t, h.GetPendingMessages(t, r, q2), m2.ID); string(got.Payload) == string(v1Payload("q2@example.com")) {
		t.Error("q2 task was not migrated when migrating all queues")
	}

	// Duplicated queue names are harmless.
	if _, err := inspector.MigrateTaskPayload([]string{q1, q1}, []*PayloadMigration{emailMigration(convert)}); err != nil {
		t.Errorf("duplicated queue names returned error: %v", err)
	}

	// Unknown queue maps to ErrQueueNotFound.
	if _, err := inspector.MigrateTaskPayload([]string{"nope"}, []*PayloadMigration{emailMigration(convert)}); !errors.Is(err, ErrQueueNotFound) {
		t.Errorf("got %v, want ErrQueueNotFound", err)
	}
}

func TestMigrateTaskPayloadValidation(t *testing.T) {
	r := setup(t)
	defer r.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	noop := func(b []byte) ([]byte, error) { return b, nil }
	tests := []struct {
		name       string
		migrations []*PayloadMigration
	}{
		{"no migrations", nil},
		{"nil migration", []*PayloadMigration{nil}},
		{"empty task type", []*PayloadMigration{{TaskType: "", Version: 1, Convert: noop}}},
		{"zero version", []*PayloadMigration{{TaskType: "t", Version: 0, Convert: noop}}},
		{"negative version", []*PayloadMigration{{TaskType: "t", Version: -1, Convert: noop}}},
		{"nil convert", []*PayloadMigration{{TaskType: "t", Version: 1}}},
		{"duplicate task type", []*PayloadMigration{
			{TaskType: "t", Version: 1, Convert: noop},
			{TaskType: "t", Version: 2, Convert: noop},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := inspector.MigrateTaskPayload(nil, tc.migrations); err == nil {
				t.Error("MigrateTaskPayload returned nil error, want validation error")
			}
		})
	}
}

func TestMigrateTaskPayloadConsumerReceivesNewPayload(t *testing.T) {
	r := setup(t)
	defer r.Close()

	client := NewClient(getRedisConnOpt(t))
	defer client.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	taskID := "migration-e2e-task"
	if _, err := client.Enqueue(NewTask(testMigrationTaskType, v1Payload("e2e@example.com")),
		TaskID(taskID), ProcessIn(time.Hour), MaxRetry(7), Timeout(20*time.Minute)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	info, err := inspector.GetTaskInfo(base.DefaultQueueName, taskID)
	if err != nil {
		t.Fatalf("GetTaskInfo: %v", err)
	}
	if info.State != TaskStateScheduled {
		t.Fatalf("initial state=%s, want scheduled", info.State)
	}

	convert, _ := newTestConverter("")
	result, err := inspector.MigrateTaskPayload(nil, []*PayloadMigration{emailMigration(convert)})
	if err != nil {
		t.Fatalf("MigrateTaskPayload: %v", err)
	}
	if len(result.Migrated) != 1 {
		t.Fatalf("Migrated=%d, want 1", len(result.Migrated))
	}

	// Scheduled time and options survive; payload is the new schema.
	info, err = inspector.GetTaskInfo(base.DefaultQueueName, taskID)
	if err != nil {
		t.Fatalf("GetTaskInfo after migration: %v", err)
	}
	if info.State != TaskStateScheduled {
		t.Errorf("state=%s, want scheduled", info.State)
	}
	if info.MaxRetry != 7 || info.Timeout != 20*time.Minute {
		t.Errorf("execution options changed: retry=%d timeout=%s", info.MaxRetry, info.Timeout)
	}
	if info.Headers[PayloadVersionHeader] != "2" {
		t.Errorf("version header=%q, want 2", info.Headers[PayloadVersionHeader])
	}
	var in map[string]interface{}
	if err := json.Unmarshal(info.Payload, &in); err != nil || in["schema"].(float64) != 2 {
		t.Fatalf("payload not migrated: %s", info.Payload)
	}

	// Move the task to pending and dequeue it through the real broker path.
	if _, err := inspector.RunAllScheduledTasks(base.DefaultQueueName); err != nil {
		t.Fatalf("RunAllScheduledTasks: %v", err)
	}
	broker := rdb.NewRDB(r)
	defer broker.Close()
	msg, _, err := broker.Dequeue(base.DefaultQueueName)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if msg.ID != taskID {
		t.Errorf("dequeued id=%q, want %q", msg.ID, taskID)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(msg.Payload, &got); err != nil || got["schema"].(float64) != 2 {
		t.Errorf("consumer received old payload: %s", msg.Payload)
	}
	if msg.Retry != 7 || msg.Timeout != int64((20*time.Minute).Seconds()) {
		t.Errorf("consumer sees altered options: retry=%d timeout=%d", msg.Retry, msg.Timeout)
	}
}
