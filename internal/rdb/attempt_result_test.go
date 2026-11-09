// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	h "github.com/hibiken/asynq/internal/testutil"
)

// seedActiveTask enqueues a fresh task and dequeues it so that its hash key
// exists in the active state. It returns the message as seen by the worker.
func seedActiveTask(t *testing.T, r *RDB) *base.TaskMessage {
	t.Helper()
	msg := h.NewTaskMessage("attempt_result_task", nil)
	if err := r.Enqueue(context.Background(), msg); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	got, _, err := r.Dequeue(base.DefaultQueueName)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	return got
}

func mustDecodeFrame(t *testing.T, r *RDB, qname, id string, attempt int) *base.AttemptResult {
	t.Helper()
	frame, err := r.client.HGet(context.Background(),
		base.TaskKey(qname, id), base.AttemptResultFieldName(attempt)).Bytes()
	if err != nil {
		t.Fatalf("HGet attempt %d frame failed: %v", attempt, err)
	}
	rec, err := base.DecodeAttemptResult(frame)
	if err != nil {
		t.Fatalf("DecodeAttemptResult failed: %v", err)
	}
	return rec
}

func TestWriteAttemptResult(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	msg := seedActiveTask(t, r)
	taskKey := base.TaskKey(msg.Queue, msg.ID)

	// First write creates an independent recorded attempt.
	if n, err := r.WriteAttemptResult(msg.Queue, msg.ID, 1, []byte("first")); err != nil || n != len("first") {
		t.Fatalf("WriteAttemptResult attempt 1 returned n=%d err=%v", n, err)
	}
	rec := mustDecodeFrame(t, r, msg.Queue, msg.ID, 1)
	if rec.Attempt != 1 || rec.Status != base.AttemptStatusRecorded || string(rec.Data) != "first" {
		t.Errorf("attempt 1 record = %+v, want recorded/first", rec)
	}
	if got := r.client.HGet(ctx, taskKey, base.LegacyResultField).Val(); got != "first" {
		t.Errorf("legacy result mirror = %q, want first", got)
	}
	if r.client.Exists(ctx, taskKey+":nonexistent").Val() != 0 {
		t.Error("unexpected extra key created")
	}

	// Repeated report of the same attempt must not create additional records;
	// it replaces the single record in place.
	if _, err := r.WriteAttemptResult(msg.Queue, msg.ID, 1, []byte("first-dedup")); err != nil {
		t.Fatalf("idempotent rewrite failed: %v", err)
	}
	rec = mustDecodeFrame(t, r, msg.Queue, msg.ID, 1)
	if string(rec.Data) != "first-dedup" {
		t.Errorf("attempt 1 data = %q, want first-dedup", rec.Data)
	}
	lineageFields := 0
	for field := range r.client.HGetAll(ctx, taskKey).Val() {
		if _, ok := base.ParseAttemptResultFieldName(field); ok {
			lineageFields++
		}
	}
	if lineageFields != 1 {
		t.Errorf("got %d attempt fields after duplicate write, want 1", lineageFields)
	}

	// Subsequent attempts get independent records; earlier attempts are retained.
	if _, err := r.WriteAttemptResult(msg.Queue, msg.ID, 2, []byte("second")); err != nil {
		t.Fatalf("WriteAttemptResult attempt 2 failed: %v", err)
	}
	if _, err := r.WriteAttemptResult(msg.Queue, msg.ID, 3, []byte("third")); err != nil {
		t.Fatalf("WriteAttemptResult attempt 3 failed: %v", err)
	}
	records, err := r.ListAttemptResults(msg.Queue, msg.ID)
	if err != nil {
		t.Fatalf("ListAttemptResults failed: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("got %d records, want 3", len(records))
	}
	for i, rec := range records {
		if rec.Attempt != i+1 || rec.IsFinal() {
			t.Errorf("record %d = %+v, want attempt %d and not final", i, rec, i+1)
		}
	}
	if string(records[0].Data) != "first-dedup" {
		t.Errorf("earlier attempt data was overwritten: %q", records[0].Data)
	}
}

func TestWriteAttemptResultValidation(t *testing.T) {
	r := setup(t)
	defer r.Close()
	msg := seedActiveTask(t, r)

	if _, err := r.WriteAttemptResult(msg.Queue, msg.ID, 0, []byte("x")); !errors.Is(err, errors.ErrAttemptResultConflict) {
		t.Errorf("attempt 0: got %v, want ErrAttemptResultConflict", err)
	}
	if _, err := r.WriteAttemptResult(msg.Queue, "missing-task", 1, []byte("x")); !errors.IsTaskNotFound(err) {
		t.Errorf("missing task: got %v, want TaskNotFoundError", err)
	}
}

func TestMarkFinalResult(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	msg := seedActiveTask(t, r)
	taskKey := base.TaskKey(msg.Queue, msg.ID)

	// A running attempt must not be marked final before success: writes only
	// produce "recorded" records and no final marker exists.
	if _, err := r.WriteAttemptResult(msg.Queue, msg.ID, 1, []byte("a1")); err != nil {
		t.Fatal(err)
	}
	if marker := r.client.HGet(ctx, taskKey, base.AttemptResultFinalField).Val(); marker != "" {
		t.Fatalf("final marker set prematurely: %q", marker)
	}

	if err := r.MarkFinalResult(msg.Queue, msg.ID, 1); err != nil {
		t.Fatalf("MarkFinalResult failed: %v", err)
	}
	if got := r.client.HGet(ctx, taskKey, base.AttemptResultFinalField).Val(); got != "1" {
		t.Errorf("final marker = %q, want 1", got)
	}
	rec := mustDecodeFrame(t, r, msg.Queue, msg.ID, 1)
	if rec.Status != base.AttemptStatusFinal || !rec.IsFinal() {
		t.Errorf("record status = %v, want final", rec.Status)
	}
	if got := r.client.HGet(ctx, taskKey, base.LegacyResultField).Val(); got != "a1" {
		t.Errorf("legacy mirror = %q, want a1", got)
	}

	// Marking the same attempt final again is idempotent and adds no records.
	if err := r.MarkFinalResult(msg.Queue, msg.ID, 1); err != nil {
		t.Errorf("idempotent MarkFinalResult failed: %v", err)
	}
	records, _ := r.ListAttemptResults(msg.Queue, msg.ID)
	if len(records) != 1 || !records[0].IsFinal() {
		t.Errorf("records after re-mark = %+v", records)
	}

	// Adopting a different attempt after finalization conflicts; the marker and
	// the existing history must not change.
	if _, err := r.WriteAttemptResult(msg.Queue, msg.ID, 2, []byte("a2")); err == nil {
		t.Error("write after finalization should conflict")
	}
	if err := r.MarkFinalResult(msg.Queue, msg.ID, 2); !errors.Is(err, errors.ErrAttemptResultConflict) {
		t.Errorf("re-finalize attempt 2: got %v, want conflict", err)
	}
	if got := r.client.HGet(ctx, taskKey, base.AttemptResultFinalField).Val(); got != "1" {
		t.Errorf("final marker changed to %q, want 1", got)
	}
}

func TestMarkFinalResultCreatesEmptyRecord(t *testing.T) {
	r := setup(t)
	defer r.Close()
	msg := seedActiveTask(t, r)

	// A successful handler that never wrote a result still gets one record per
	// attempt, marked final with empty data.
	if err := r.MarkFinalResult(msg.Queue, msg.ID, 1); err != nil {
		t.Fatalf("MarkFinalResult failed: %v", err)
	}
	records, err := r.ListAttemptResults(msg.Queue, msg.ID)
	if err != nil {
		t.Fatalf("ListAttemptResults failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("got %d records, want 1", len(records))
	}
	want := &base.AttemptResult{Attempt: 1, Status: base.AttemptStatusFinal}
	if records[0].Attempt != want.Attempt || records[0].Status != want.Status ||
		len(records[0].Data) != 0 || !records[0].IsFinal() {
		t.Errorf("record = %+v, want empty final record for attempt 1", records[0])
	}
}

func TestWriteAttemptResultRejectsCorruptedLineage(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	msg := seedActiveTask(t, r)
	taskKey := base.TaskKey(msg.Queue, msg.ID)

	if _, err := r.WriteAttemptResult(msg.Queue, msg.ID, 1, []byte("good")); err != nil {
		t.Fatal(err)
	}
	// Simulate a corrupted record landing in the slot for attempt 2.
	r.client.HSet(ctx, taskKey, base.AttemptResultFieldName(2), "not-a-valid-frame")

	// The task must fail explicitly and nothing may be mutated.
	if _, err := r.WriteAttemptResult(msg.Queue, msg.ID, 3, []byte("third")); !errors.Is(err, errors.ErrAttemptResultCorrupted) {
		t.Errorf("write with corrupted lineage: got %v, want ErrAttemptResultCorrupted", err)
	}
	if r.client.HExists(ctx, taskKey, base.AttemptResultFieldName(3)).Val() {
		t.Error("attempt 3 record must not be written when lineage is corrupted")
	}
	if r.client.HGet(ctx, taskKey, base.AttemptResultFinalField).Val() != "" {
		t.Error("final marker must not be set when lineage is corrupted")
	}
	if err := r.MarkFinalResult(msg.Queue, msg.ID, 1); !errors.Is(err, errors.ErrAttemptResultCorrupted) {
		t.Errorf("finalize with corrupted lineage: got %v, want ErrAttemptResultCorrupted", err)
	}
	if _, err := r.ListAttemptResults(msg.Queue, msg.ID); !errors.Is(err, errors.ErrAttemptResultCorrupted) {
		t.Errorf("list with corrupted lineage: got %v, want ErrAttemptResultCorrupted", err)
	}
	// The healthy history is untouched.
	rec := mustDecodeFrame(t, r, msg.Queue, msg.ID, 1)
	if string(rec.Data) != "good" {
		t.Errorf("healthy record was modified: %q", rec.Data)
	}
}

func TestWriteAttemptResultRejectsNumberingConflict(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	msg := seedActiveTask(t, r)
	taskKey := base.TaskKey(msg.Queue, msg.ID)

	// Store a well-formed frame whose embedded attempt number disagrees with
	// its storage slot.
	conflictFrame, err := base.EncodeAttemptResult(&base.AttemptResult{
		Attempt:   99,
		Status:    base.AttemptStatusRecorded,
		Data:      []byte("conflicting"),
		WrittenAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	r.client.HSet(ctx, taskKey, base.AttemptResultFieldName(2), conflictFrame)

	if _, err := r.WriteAttemptResult(msg.Queue, msg.ID, 2, []byte("second")); !errors.Is(err, errors.ErrAttemptResultConflict) {
		t.Errorf("write into conflicting slot: got %v, want ErrAttemptResultConflict", err)
	}
	if _, err := r.ListAttemptResults(msg.Queue, msg.ID); !errors.Is(err, errors.ErrAttemptResultConflict) {
		t.Errorf("list conflicting lineage: got %v, want ErrAttemptResultConflict", err)
	}
	// Existing frame must remain untouched.
	if got := r.client.HGet(ctx, taskKey, base.AttemptResultFieldName(2)).Val(); got != string(conflictFrame) {
		t.Error("conflicting record was overwritten")
	}
}

func TestListAttemptResultsNotFound(t *testing.T) {
	r := setup(t)
	defer r.Close()
	// Register the queue so that the lookup reaches the task-key check.
	r.client.SAdd(context.Background(), base.AllQueues, base.DefaultQueueName)
	if _, err := r.ListAttemptResults(base.DefaultQueueName, "does-not-exist"); !errors.IsTaskNotFound(err) {
		t.Errorf("got %v, want TaskNotFoundError", err)
	}
}

func TestRetryBackfillsIncompleteAttempt(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	msg := seedActiveTask(t, r)

	// Case 1: the attempt crashed without writing anything; Retry (the same
	// operation the recoverer performs after a hard crash) fills the gap.
	if err := r.Retry(ctx, msg, time.Now().Add(time.Minute), "boom", true); err != nil {
		t.Fatalf("Retry failed: %v", err)
	}
	rec := mustDecodeFrame(t, r, msg.Queue, msg.ID, 1)
	if rec.Attempt != 1 || rec.Status != base.AttemptStatusIncomplete || len(rec.Data) != 0 {
		t.Errorf("backfilled record = %+v, want incomplete empty record for attempt 1", rec)
	}

	// The retried message now carries Retried=1; the next dequeue is attempt 2
	// and its result is recorded independently.
	info, err := r.GetTaskInfo(msg.Queue, msg.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info.Message.Retried != 1 {
		t.Errorf("Retried = %d, want 1", info.Message.Retried)
	}
	if _, err := r.WriteAttemptResult(msg.Queue, msg.ID, 2, []byte("second")); err != nil {
		t.Fatalf("WriteAttemptResult attempt 2 failed: %v", err)
	}
	records, err := r.ListAttemptResults(msg.Queue, msg.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Status != base.AttemptStatusIncomplete ||
		records[1].Status != base.AttemptStatusRecorded || string(records[1].Data) != "second" {
		t.Errorf("lineage after crash recovery = %+v", records)
	}

	// Case 2: when a result was reported before the failure, Retry must not
	// overwrite it with the placeholder.
	msg2 := seedActiveTask(t, r)
	if _, err := r.WriteAttemptResult(msg2.Queue, msg2.ID, 1, []byte("reported-before-failure")); err != nil {
		t.Fatal(err)
	}
	if err := r.Retry(ctx, msg2, time.Now().Add(time.Minute), "boom", true); err != nil {
		t.Fatalf("Retry failed: %v", err)
	}
	rec2 := mustDecodeFrame(t, r, msg2.Queue, msg2.ID, 1)
	if rec2.Status != base.AttemptStatusRecorded || string(rec2.Data) != "reported-before-failure" {
		t.Errorf("existing record was overwritten on retry: %+v", rec2)
	}
}

func TestArchiveBackfillsIncompleteAttempt(t *testing.T) {
	r := setup(t)
	defer r.Close()
	ctx := context.Background()
	msg := seedActiveTask(t, r)

	// Exhaust retries and archive without a reported result.
	msg.Retried = msg.Retry
	if err := r.Archive(ctx, msg, "terminal failure"); err != nil {
		t.Fatalf("Archive failed: %v", err)
	}
	rec := mustDecodeFrame(t, r, msg.Queue, msg.ID, msg.Retried+1)
	if rec.Status != base.AttemptStatusIncomplete || len(rec.Data) != 0 {
		t.Errorf("archived lineage = %+v, want incomplete placeholder", rec)
	}
	records, err := r.ListAttemptResults(msg.Queue, msg.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Errorf("got %d records, want 1", len(records))
	}
}
