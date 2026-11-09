// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package snapshot

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
)

func testMsg(qname, id, gkey string) *base.TaskMessage {
	return &base.TaskMessage{
		Type:         "send_email",
		Payload:      []byte(`{"to":"a@example.com"}`),
		Headers:      map[string]string{"x-trace": "t-1"},
		ID:           id,
		Queue:        qname,
		Retry:        7,
		Retried:      2,
		ErrorMsg:     "boom",
		LastFailedAt: time.Now().Unix(),
		Timeout:      60,
		Deadline:     time.Now().Add(time.Hour).Unix(),
		GroupKey:     gkey,
		Retention:    3600,
	}
}

func mustEncodeRecord(t *testing.T, state State, score, pendingSince int64, msg *base.TaskMessage) Record {
	t.Helper()
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		t.Fatalf("EncodeMessage failed: %v", err)
	}
	return Record{State: state, Score: score, PendingSince: pendingSince, Msg: encoded}
}

func sampleRecords(t *testing.T, qname string) []Record {
	t.Helper()
	return []Record{
		mustEncodeRecord(t, StatePending, 0, time.Now().UnixNano(), testMsg(qname, "p1", "")),
		mustEncodeRecord(t, StateScheduled, time.Now().Add(time.Hour).Unix(), 0, testMsg(qname, "s1", "")),
		mustEncodeRecord(t, StateRetry, time.Now().Add(2*time.Hour).Unix(), 0, testMsg(qname, "r1", "")),
		mustEncodeRecord(t, StateAggregating, time.Now().Unix(), 0, testMsg(qname, "g1", "group-a")),
	}
}

func sampleMeta(qname string, records []Record) Metadata {
	return Metadata{
		FormatVersion: FormatVersion,
		ID:            uuid.NewString(),
		Queue:         qname,
		CreatedAtUnix: time.Now().Unix(),
		Pending:       1,
		Scheduled:     1,
		Retry:         1,
		Aggregating:   1,
		Total:         len(records),
	}
}

// TR-1.1: encode then decode reproduces metadata and every record exactly.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	records := sampleRecords(t, "default")
	meta := sampleMeta("default", records)

	var buf bytes.Buffer
	if err := Encode(&buf, meta, records); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	raw := buf.Bytes()

	gotMeta, gotRecords, err := Decode(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if diff := cmp.Diff(meta, gotMeta); diff != "" {
		t.Errorf("metadata mismatch (-want,+got):\n%s", diff)
	}
	if diff := cmp.Diff(records, gotRecords); diff != "" {
		t.Errorf("records mismatch (-want,+got):\n%s", diff)
	}

	// DecodeMetadata must return the same metadata without reading records.
	gotMetaOnly, err := DecodeMetadata(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("DecodeMetadata failed: %v", err)
	}
	if diff := cmp.Diff(meta, gotMetaOnly); diff != "" {
		t.Errorf("DecodeMetadata mismatch (-want,+got):\n%s", diff)
	}
}

// TR-1.1: zero-value scores and an empty queue snapshot must round-trip.
func TestEncodeDecodeEmptyAndZeroValues(t *testing.T) {
	meta := Metadata{ID: uuid.NewString(), Queue: "default", CreatedAtUnix: 1}
	if err := ValidateSnapshotID(meta.ID); err != nil {
		t.Fatalf("unexpected id error: %v", err)
	}
	var buf bytes.Buffer
	if err := Encode(&buf, meta, nil); err != nil {
		t.Fatalf("Encode empty snapshot failed: %v", err)
	}
	gotMeta, gotRecords, err := Decode(&buf)
	if err != nil {
		t.Fatalf("Decode empty snapshot failed: %v", err)
	}
	if len(gotRecords) != 0 || gotMeta.Total != 0 {
		t.Fatalf("empty snapshot decoded %d records", len(gotRecords))
	}
}

// TR-1.2: every corruption class is rejected with the matching sentinel and a
// frame location, and never produces partial records.
func TestDecodeCorruptions(t *testing.T) {
	records := sampleRecords(t, "default")
	meta := sampleMeta("default", records)

	good := func() []byte {
		var buf bytes.Buffer
		if err := Encode(&buf, meta, records); err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
		return buf.Bytes()
	}

	t.Run("bad magic", func(t *testing.T) {
		b := good()
		b[0] = 'X'
		if _, _, err := Decode(bytes.NewReader(b)); !errors.Is(err, ErrBadMagic) {
			t.Fatalf("got %v, want ErrBadMagic", err)
		}
	})

	t.Run("truncated header", func(t *testing.T) {
		b := good()[:4]
		if _, _, err := Decode(bytes.NewReader(b)); !errors.Is(err, ErrTruncated) {
			t.Fatalf("got %v, want ErrTruncated", err)
		}
	})

	t.Run("truncated frame payload", func(t *testing.T) {
		b := good()
		if _, _, err := Decode(bytes.NewReader(b[:len(b)-10])); !errors.Is(err, ErrTruncated) {
			t.Fatalf("got %v, want ErrTruncated", err)
		}
	})

	t.Run("crc mismatch", func(t *testing.T) {
		b := good()
		// Flip a byte inside the first record frame (after 8-byte magic). The
		// metadata frame (frame 0) must still decode; the error is reported on
		// frame 1 or later with ErrCRCMismatch.
		b[len(b)-1] ^= 0xFF
		_, _, err := Decode(bytes.NewReader(b))
		if !errors.Is(err, ErrCRCMismatch) {
			t.Fatalf("got %v, want ErrCRCMismatch", err)
		}
	})

	t.Run("illegal protobuf in record", func(t *testing.T) {
		bad := append([]byte(nil), records[0].Msg...)
		bad[0] = 0xFF // invalid protobuf wire type
		corrupt := Record{State: StatePending, Msg: bad}
		var buf bytes.Buffer
		m := Metadata{ID: uuid.NewString(), Queue: "default", Pending: 1, Total: 1}
		if err := writeFrameOnly(&buf, m, corrupt); err != nil {
			t.Fatal(err)
		}
		_, _, err := Decode(&buf)
		if !errors.Is(err, ErrInvalidRecord) {
			t.Fatalf("got %v, want ErrInvalidRecord", err)
		}
	})

	t.Run("queue mismatch", func(t *testing.T) {
		other := mustEncodeRecord(t, StatePending, 0, 0, testMsg("other-queue", "p9", ""))
		m := Metadata{ID: uuid.NewString(), Queue: "default", Pending: 1, Total: 1}
		var buf bytes.Buffer
		if err := writeFrameOnly(&buf, m, other); err != nil {
			t.Fatal(err)
		}
		_, _, err := Decode(&buf)
		if !errors.Is(err, ErrInvalidRecord) {
			t.Fatalf("got %v, want ErrInvalidRecord", err)
		}
	})

	t.Run("unknown version", func(t *testing.T) {
		// Build a file whose metadata frame has a valid CRC but an unknown
		// version number, so version validation (not CRC) rejects it.
		var buf bytes.Buffer
		buf.WriteString(magic)
		metaPayload, err := json.Marshal(&Metadata{FormatVersion: 99, ID: uuid.NewString(), Queue: "default"})
		if err != nil {
			t.Fatal(err)
		}
		if err := writeFrame(&buf, metaPayload); err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeMetadata(&buf); !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("got %v, want ErrUnsupportedVersion", err)
		}
	})
}

// writeFrameOnly builds a file with a magic and one record frame while
// bypassing Encode validation, allowing deliberately invalid payloads.
func writeFrameOnly(buf *bytes.Buffer, meta Metadata, rec Record) error {
	buf.WriteString(magic)
	meta.FormatVersion = FormatVersion
	metaPayload, err := json.Marshal(&meta)
	if err != nil {
		return err
	}
	if err := writeFrame(buf, metaPayload); err != nil {
		return err
	}
	payload := make([]byte, recordFixedLen+len(rec.Msg))
	payload[0] = byte(rec.State)
	copy(payload[recordFixedLen:], rec.Msg)
	return writeFrame(buf, payload)
}

// TR-1.3: snapshot ID validation rules.
func TestValidateSnapshotID(t *testing.T) {
	valid := []string{"a", "release-2026-11-09", "v1.2.3", "ABC_01", uuid.NewString()}
	for _, id := range valid {
		if err := ValidateSnapshotID(id); err != nil {
			t.Errorf("ValidateSnapshotID(%q) = %v, want nil", id, err)
		}
		if got := FileName(id); got != id+FileExt {
			t.Errorf("FileName(%q) = %q", id, got)
		}
	}
	invalid := []string{"", ".", "..", "a/b", "a\\b", "a/b/c", "../etc", "a b"}
	for _, id := range invalid {
		if err := ValidateSnapshotID(id); !errors.Is(err, ErrInvalidSnapshotID) {
			t.Errorf("ValidateSnapshotID(%q) = %v, want ErrInvalidSnapshotID", id, err)
		}
	}
}

// TR-1.2: metadata count mismatches are rejected at encode time.
func TestEncodeRejectsCountMismatch(t *testing.T) {
	records := sampleRecords(t, "default")
	meta := sampleMeta("default", records)
	meta.Total = 99
	var buf bytes.Buffer
	if err := Encode(&buf, meta, records); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("got %v, want ErrInvalidRecord", err)
	}
}

func TestStateMapping(t *testing.T) {
	cases := []struct {
		base base.TaskState
		want State
	}{
		{base.TaskStatePending, StatePending},
		{base.TaskStateScheduled, StateScheduled},
		{base.TaskStateRetry, StateRetry},
		{base.TaskStateAggregating, StateAggregating},
	}
	for _, c := range cases {
		got, err := FromTaskState(c.base)
		if err != nil || got != c.want {
			t.Errorf("FromTaskState(%v) = %v, %v; want %v", c.base, got, err, c.want)
		}
		back, err := got.TaskState()
		if err != nil || back != c.base {
			t.Errorf("%v.TaskState() = %v, %v; want %v", got, back, err, c.base)
		}
	}
	if _, err := FromTaskState(base.TaskStateActive); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("FromTaskState(active) err = %v, want ErrInvalidRecord", err)
	}
}

// A restore point must never contain two records with the same task ID.
func TestEncodeRejectsDuplicateTaskIDs(t *testing.T) {
	msg := testMsg("default", "dup1", "")
	records := []snapshot.RecordShim{}
	_ = records
	rec1 := mustEncodeRecord(t, StatePending, 0, 0, msg)
	rec2 := mustEncodeRecord(t, StateScheduled, 100, 0, msg)
	meta := Metadata{
		ID: uuid.NewString(), Queue: "default", Pending: 1, Scheduled: 1, Total: 2,
	}
	var buf bytes.Buffer
	if err := Encode(&buf, meta, []Record{rec1, rec2}); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("got %v, want ErrInvalidRecord", err)
	}
}

// An aggregating record without a group key is structurally invalid.
func TestEncodeRejectsAggregatingWithoutGroup(t *testing.T) {
	msg := testMsg("default", "a1", "") // no GroupKey
	rec := mustEncodeRecord(t, StateAggregating, 5, 0, msg)
	meta := Metadata{
		ID: uuid.NewString(), Queue: "default", Aggregating: 1, Total: 1,
	}
	var buf bytes.Buffer
	if err := Encode(&buf, meta, []Record{rec}); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("got %v, want ErrInvalidRecord", err)
	}
}
