// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package base

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hibiken/asynq/internal/errors"
)

// Hash fields used to store the per-attempt result lineage on a task hash key
// (e.g. asynq:{<qname>}:t:<task_id>).
//
// Each execution attempt owns exactly one field named "ar:<attempt>"; writing
// the same attempt more than once replaces the field value in place instead of
// creating additional records. The "ar:final" field records the attempt number
// whose result was adopted when the task completed successfully.
const (
	// AttemptResultFieldPrefix is the prefix of per-attempt result hash fields.
	AttemptResultFieldPrefix = "ar:"

	// AttemptResultFinalField stores the attempt number of the adopted final
	// result. The field is only written once the task completes successfully.
	AttemptResultFinalField = "ar:final"

	// LegacyResultField mirrors the raw result bytes of the latest (or final)
	// attempt so that existing task-inspection read paths keep working.
	LegacyResultField = "result"

	// attemptResultFrameMagic versions the on-wire frame format.
	attemptResultFrameMagic = "AR1"

	// attemptResultFieldSep separates the header tokens.
	attemptResultFieldSep = ":"

	// attemptResultHeaderSep terminates the textual header and precedes the
	// raw (binary) result data.
	attemptResultHeaderSep = "\n"
)

// AttemptStatus describes the lifecycle state of a single attempt result record.
type AttemptStatus int

const (
	// AttemptStatusUnknown is the zero value and indicates an unrecognized state.
	AttemptStatusUnknown AttemptStatus = 0

	// AttemptStatusIncomplete indicates that the attempt ended without reporting
	// a result, e.g. the worker process crashed, its lease expired, or the handler
	// did not write a result. A placeholder record is kept so that the attempt
	// lineage has no gaps.
	AttemptStatusIncomplete AttemptStatus = 1

	// AttemptStatusRecorded indicates that the handler reported a result for the
	// attempt. The attempt itself may still have ended in failure; its result is
	// retained either way.
	AttemptStatusRecorded AttemptStatus = 2

	// AttemptStatusFinal indicates that this attempt's result was adopted as the
	// final task result. It is only set when the task completes successfully and
	// is never set while the attempt is still running.
	AttemptStatusFinal AttemptStatus = 3
)

func (s AttemptStatus) String() string {
	switch s {
	case AttemptStatusIncomplete:
		return "incomplete"
	case AttemptStatusRecorded:
		return "recorded"
	case AttemptStatusFinal:
		return "final"
	default:
		return "unknown"
	}
}

// AttemptResult is an independently retained result record for one task
// execution attempt.
type AttemptResult struct {
	// Attempt is the 1-based execution attempt number. The first dequeue of a
	// task is attempt 1 (TaskMessage.Retried is zero at that point).
	Attempt int

	// Status is the lifecycle state of this record.
	Status AttemptStatus

	// Data holds the raw result bytes reported by the handler. It is nil for
	// incomplete placeholder records.
	Data []byte

	// WrittenAt is the time the record was last written.
	WrittenAt time.Time
}

// IsFinal reports whether this record's result was adopted as the final result.
func (r *AttemptResult) IsFinal() bool {
	return r != nil && r.Status == AttemptStatusFinal
}

// AttemptResultFieldName returns the hash field name that stores the record of
// the given attempt.
func AttemptResultFieldName(attempt int) string {
	return fmt.Sprintf("%s%d", AttemptResultFieldPrefix, attempt)
}

// ParseAttemptResultFieldName returns the attempt number encoded in a hash
// field name produced by AttemptResultFieldName. It returns false if the field
// name does not belong to the per-attempt result lineage.
func ParseAttemptResultFieldName(field string) (attempt int, ok bool) {
	if !strings.HasPrefix(field, AttemptResultFieldPrefix) || field == AttemptResultFinalField {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(field, AttemptResultFieldPrefix))
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

// IsAttemptResultField reports whether the given hash field stores any part of
// the per-attempt result lineage.
func IsAttemptResultField(field string) bool {
	return strings.HasPrefix(field, AttemptResultFieldPrefix)
}

// EncodeAttemptResult serializes the record into a binary-safe frame:
//
//	AR1:<attempt>:<status>:<writtenAtUnixNano>:<dataLen>\n<data>
//
// The textual header can be inspected atomically inside Redis Lua scripts
// (e.g. to reject corrupted frames or attempt-number conflicts without
// overwriting existing history), while the result bytes after the separator are
// kept verbatim and may contain arbitrary data.
func EncodeAttemptResult(rec *AttemptResult) ([]byte, error) {
	if rec == nil {
		return nil, errors.New("cannot encode nil attempt result")
	}
	if rec.Attempt <= 0 {
		return nil, errors.E(errors.FailedPrecondition,
			fmt.Sprintf("attempt number must be positive, got %d", rec.Attempt))
	}
	if rec.Status == AttemptStatusUnknown {
		return nil, errors.E(errors.FailedPrecondition,
			fmt.Sprintf("attempt %d has an unspecified status", rec.Attempt))
	}
	header := strings.Join([]string{
		attemptResultFrameMagic,
		strconv.Itoa(rec.Attempt),
		strconv.Itoa(int(rec.Status)),
		strconv.FormatInt(rec.WrittenAt.UnixNano(), 10),
		strconv.Itoa(len(rec.Data)),
	}, attemptResultFieldSep)
	frame := make([]byte, 0, len(header)+1+len(rec.Data))
	frame = append(frame, header...)
	frame = append(frame, []byte(attemptResultHeaderSep)...)
	frame = append(frame, rec.Data...)
	return frame, nil
}

// DecodeAttemptResult parses a frame produced by EncodeAttemptResult. It
// returns errors.ErrAttemptResultCorrupted if the frame is malformed rather
// than panicking, so callers can fail the task explicitly without touching
// existing history.
func DecodeAttemptResult(frame []byte) (*AttemptResult, error) {
	sep := []byte(attemptResultHeaderSep)
	idx := strings.Index(string(frame), attemptResultHeaderSep)
	if idx < 0 {
		return nil, errors.E(errors.Internal, errors.ErrAttemptResultCorrupted)
	}
	header := string(frame[:idx])
	data := frame[idx+len(sep):]
	tokens := strings.Split(header, attemptResultFieldSep)
	if len(tokens) != 5 || tokens[0] != attemptResultFrameMagic {
		return nil, errors.E(errors.Internal, errors.ErrAttemptResultCorrupted)
	}
	attempt, err := strconv.Atoi(tokens[1])
	if err != nil || attempt <= 0 {
		return nil, errors.E(errors.Internal, errors.ErrAttemptResultCorrupted)
	}
	status, err := strconv.Atoi(tokens[2])
	if err != nil {
		return nil, errors.E(errors.Internal, errors.ErrAttemptResultCorrupted)
	}
	writtenAtNano, err := strconv.ParseInt(tokens[3], 10, 64)
	if err != nil {
		return nil, errors.E(errors.Internal, errors.ErrAttemptResultCorrupted)
	}
	dataLen, err := strconv.Atoi(tokens[4])
	if err != nil || dataLen < 0 || dataLen != len(data) {
		return nil, errors.E(errors.Internal, errors.ErrAttemptResultCorrupted)
	}
	switch AttemptStatus(status) {
	case AttemptStatusIncomplete, AttemptStatusRecorded, AttemptStatusFinal:
	default:
		return nil, errors.E(errors.Internal, errors.ErrAttemptResultCorrupted)
	}
	if len(data) == 0 {
		data = nil
	}
	return &AttemptResult{
		Attempt:   attempt,
		Status:    AttemptStatus(status),
		Data:      data,
		WrittenAt: time.Unix(0, writtenAtNano),
	}, nil
}
