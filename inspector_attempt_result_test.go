// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/rdb"
	h "github.com/hibiken/asynq/internal/testutil"
)

func TestInspectorListTaskAttemptResults(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	msg := h.NewTaskMessage("inspect_lineage_task", nil)
	if err := rdbClient.Enqueue(context.Background(), msg); err != nil {
		t.Fatal(err)
	}

	// Attempt 1 reports a result and fails; attempt 2 succeeds with adoption.
	if _, err := rdbClient.WriteAttemptResult(msg.Queue, msg.ID, 1, []byte("failed-result")); err != nil {
		t.Fatal(err)
	}
	if _, err := rdbClient.WriteAttemptResult(msg.Queue, msg.ID, 2, []byte("final-result")); err != nil {
		t.Fatal(err)
	}
	if err := rdbClient.MarkFinalResult(msg.Queue, msg.ID, 2); err != nil {
		t.Fatal(err)
	}

	records, err := inspector.ListTaskAttemptResults(msg.Queue, msg.ID)
	if err != nil {
		t.Fatalf("ListTaskAttemptResults failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("got %d records, want 2", len(records))
	}
	if records[0].Attempt != 1 || records[0].Status != AttemptStatusRecorded ||
		records[0].Final || string(records[0].Result) != "failed-result" {
		t.Errorf("record 0 = %+v", records[0])
	}
	if records[1].Attempt != 2 || records[1].Status != AttemptStatusFinal ||
		!records[1].Final || string(records[1].Result) != "final-result" {
		t.Errorf("record 1 = %+v", records[1])
	}
	if records[1].WrittenAt.IsZero() || time.Since(records[1].WrittenAt) > time.Minute {
		t.Errorf("WrittenAt not populated: %v", records[1].WrittenAt)
	}
}

func TestInspectorListTaskAttemptResultsErrors(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	// Unknown task maps to ErrTaskNotFound (queue itself must exist).
	r.SAdd(context.Background(), base.AllQueues, base.DefaultQueueName)
	if _, err := inspector.ListTaskAttemptResults(base.DefaultQueueName, "missing"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("missing task: got %v, want ErrTaskNotFound", err)
	}

	msg := h.NewTaskMessage("corrupt_lineage_task", nil)
	if err := rdbClient.Enqueue(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if _, err := rdbClient.WriteAttemptResult(msg.Queue, msg.ID, 1, []byte("good")); err != nil {
		t.Fatal(err)
	}
	// Corrupt the slot for attempt 2.
	r.HSet(context.Background(), base.TaskKey(msg.Queue, msg.ID),
		base.AttemptResultFieldName(2), "garbage-frame")

	if _, err := inspector.ListTaskAttemptResults(msg.Queue, msg.ID); !errors.Is(err, ErrAttemptResultCorrupted) {
		t.Errorf("corrupted lineage: got %v, want ErrAttemptResultCorrupted", err)
	}
}
