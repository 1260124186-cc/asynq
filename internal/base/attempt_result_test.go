// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package base

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/hibiken/asynq/internal/errors"
)

func TestAttemptResultFrameRoundTrip(t *testing.T) {
	tests := []struct {
		desc string
		rec  *AttemptResult
	}{
		{
			desc: "recorded result with binary data",
			rec: &AttemptResult{
				Attempt:   2,
				Status:    AttemptStatusRecorded,
				Data:      []byte("line1\nline2:with-colons\x00binary"),
				WrittenAt: time.Unix(0, 1731148800123456789),
			},
		},
		{
			desc: "incomplete placeholder with empty data",
			rec: &AttemptResult{
				Attempt:   1,
				Status:    AttemptStatusIncomplete,
				WrittenAt: time.Unix(0, 42),
			},
		},
		{
			desc: "final result with empty payload",
			rec: &AttemptResult{
				Attempt:   3,
				Status:    AttemptStatusFinal,
				Data:      nil,
				WrittenAt: time.Unix(100, 0),
			},
		},
	}

	for _, tc := range tests {
		frame, err := EncodeAttemptResult(tc.rec)
		if err != nil {
			t.Fatalf("%s: EncodeAttemptResult failed: %v", tc.desc, err)
		}
		got, err := DecodeAttemptResult(frame)
		if err != nil {
			t.Fatalf("%s: DecodeAttemptResult failed: %v", tc.desc, err)
		}
		if diff := cmp.Diff(tc.rec, got, cmp.AllowUnexported()); diff != "" {
			t.Errorf("%s: (-want,+got)\n%s", tc.desc, diff)
		}
	}
}

func TestDecodeAttemptResultCorrupted(t *testing.T) {
	good := &AttemptResult{Attempt: 1, Status: AttemptStatusRecorded, Data: []byte("abc"), WrittenAt: time.Unix(0, 7)}
	frame, err := EncodeAttemptResult(good)
	if err != nil {
		t.Fatalf("EncodeAttemptResult failed: %v", err)
	}

	tests := []struct {
		desc  string
		frame []byte
	}{
		{"missing header separator", []byte("AR1:1:2:7:3")},
		{"bad magic", append([]byte("XX1:1:2:7:3\n"), frame[len(frame)-3:]...)},
		{"non positive attempt", []byte("AR1:0:2:7:0\n")},
		{"unknown status", []byte("AR1:1:9:7:0\n")},
		{"data length mismatch", append(frame, []byte("extra")...)},
		{"garbage", []byte{0xff, 0xfe, 0xfd}},
	}

	for _, tc := range tests {
		if _, err := DecodeAttemptResult(tc.frame); !errors.Is(err, errors.ErrAttemptResultCorrupted) {
			t.Errorf("%s: got %v, want ErrAttemptResultCorrupted", tc.desc, err)
		}
	}
}

func TestEncodeAttemptResultValidation(t *testing.T) {
	if _, err := EncodeAttemptResult(nil); err == nil {
		t.Error("nil record should fail encoding")
	}
	if _, err := EncodeAttemptResult(&AttemptResult{Attempt: 0, Status: AttemptStatusRecorded}); err == nil {
		t.Error("non-positive attempt should fail encoding")
	}
	if _, err := EncodeAttemptResult(&AttemptResult{Attempt: 1, Status: AttemptStatusUnknown}); err == nil {
		t.Error("unknown status should fail encoding")
	}
}

func TestAttemptResultFieldNameRoundTrip(t *testing.T) {
	for _, attempt := range []int{1, 2, 25} {
		field := AttemptResultFieldName(attempt)
		got, ok := ParseAttemptResultFieldName(field)
		if !ok || got != attempt {
			t.Errorf("attempt %d: field=%q got (%d,%v)", attempt, field, got, ok)
		}
	}
	for _, field := range []string{AttemptResultFinalField, "result", "ar:", "ar:abc", "msg"} {
		if _, ok := ParseAttemptResultFieldName(field); ok {
			t.Errorf("field %q should not parse as an attempt slot", field)
		}
	}
	if !IsAttemptResultField(AttemptResultFinalField) || !IsAttemptResultField(AttemptResultFieldName(3)) {
		t.Error("lineage fields should be recognized")
	}
	if IsAttemptResultField("result") {
		t.Error("legacy result field is not a lineage field")
	}
}

func TestAttemptStatusString(t *testing.T) {
	tests := map[AttemptStatus]string{
		AttemptStatusIncomplete: "incomplete",
		AttemptStatusRecorded:   "recorded",
		AttemptStatusFinal:      "final",
		AttemptStatusUnknown:    "unknown",
	}
	for status, want := range tests {
		if got := status.String(); got != want {
			t.Errorf("status %d: got %q, want %q", status, got, want)
		}
	}
}
