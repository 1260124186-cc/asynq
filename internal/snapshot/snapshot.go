// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package snapshot defines the on-disk format and codec for queue restore
// points.
//
// A restore point is a single, self-describing file that captures the
// executable tasks (pending, scheduled, retry and aggregating) of a queue at
// a point in time. The file layout is:
//
//	+---------+----------------------------------+----------------------------------+
//	| magic   | metadata frame                   | record frame (one per task) ...  |
//	| 8 bytes | len(4) + payload + crc32(4)      | len(4) + payload + crc32(4)      |
//	+---------+----------------------------------+----------------------------------+
//
// Every frame is length-prefixed and protected by a CRC32 checksum so that a
// truncated or tampered file is detected while decoding, and the offending
// frame can be reported precisely.
package snapshot

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"regexp"

	"github.com/hibiken/asynq/internal/base"
)

const (
	// FormatVersion is the version of the file format understood by this package.
	FormatVersion uint8 = 1

	// magic is the 8-byte file header. The trailing digit tracks the major
	// format version; an incompatible format change must bump it.
	magic = "ASNQSNP1"

	// FileExt is the extension of restore point files.
	FileExt = ".asynq.snap"

	// maxFramePayload bounds a single frame payload to guard against absurd
	// allocations when decoding a corrupt length field.
	maxFramePayload = 256 << 20 // 256 MiB

	lenFieldSize = 4
	crcFieldSize = 4

	// recordFixedLen is the fixed-size header of a record frame payload:
	// state(1) + score(8) + pendingSince(8) + msgLen(4).
	recordFixedLen = 1 + 8 + 8 + 4
)

// Sentinel errors returned by the codec. Callers can use errors.Is to detect
// the specific failure class.
var (
	// ErrBadMagic indicates that the file header does not match the expected magic.
	ErrBadMagic = errors.New("snapshot: bad file magic")

	// ErrUnsupportedVersion indicates a metadata frame with an unknown format version.
	ErrUnsupportedVersion = errors.New("snapshot: unsupported format version")

	// ErrCRCMismatch indicates that a frame payload fails its checksum verification.
	ErrCRCMismatch = errors.New("snapshot: frame checksum mismatch")

	// ErrTruncated indicates that the file ends before a frame is fully read.
	ErrTruncated = errors.New("snapshot: truncated file")

	// ErrInvalidRecord indicates a malformed record or metadata frame.
	ErrInvalidRecord = errors.New("snapshot: invalid record")

	// ErrInvalidSnapshotID indicates a snapshot ID that is empty or unsafe to
	// use as a file name.
	ErrInvalidSnapshotID = errors.New("snapshot: invalid snapshot id")
)

// State is the on-disk task state ordinal. Only the four restorable states are
// represented.
type State uint8

const (
	StatePending State = iota + 1
	StateScheduled
	StateRetry
	StateAggregating
)

// String returns the lowercase state name used by asynq.
func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateScheduled:
		return "scheduled"
	case StateRetry:
		return "retry"
	case StateAggregating:
		return "aggregating"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(s))
	}
}

// FromTaskState maps a base.TaskState to a snapshot State. Active, archived and
// completed are not restorable and therefore rejected.
func FromTaskState(s base.TaskState) (State, error) {
	switch s {
	case base.TaskStatePending:
		return StatePending, nil
	case base.TaskStateScheduled:
		return StateScheduled, nil
	case base.TaskStateRetry:
		return StateRetry, nil
	case base.TaskStateAggregating:
		return StateAggregating, nil
	default:
		return 0, fmt.Errorf("%w: state %q is not restorable", ErrInvalidRecord, s.String())
	}
}

// TaskState maps the snapshot ordinal back to a base.TaskState.
func (s State) TaskState() (base.TaskState, error) {
	switch s {
	case StatePending:
		return base.TaskStatePending, nil
	case StateScheduled:
		return base.TaskStateScheduled, nil
	case StateRetry:
		return base.TaskStateRetry, nil
	case StateAggregating:
		return base.TaskStateAggregating, nil
	default:
		return 0, fmt.Errorf("%w: unknown state ordinal %d", ErrInvalidRecord, uint8(s))
	}
}

// Metadata is the JSON payload of the first frame of a restore point file.
type Metadata struct {
	FormatVersion uint8  `json:"format_version"`
	ID            string `json:"id"`
	Queue         string `json:"queue"`
	CreatedAtUnix int64  `json:"created_at_unix"`

	Pending     int `json:"pending"`
	Scheduled   int `json:"scheduled"`
	Retry       int `json:"retry"`
	Aggregating int `json:"aggregating"`
	Total       int `json:"total"`
}

// Record is a single task entry in a restore point.
//
// Score holds the Unix-second score of scheduled/retry zsets and the group
// zset score of aggregating tasks; it is zero for pending tasks. PendingSince
// holds the Unix-nano value of the pending task hash field and is zero for all
// other states. Msg is the exact protobuf-encoded base.TaskMessage bytes as
// stored in Redis, preserved without re-encoding.
type Record struct {
	State        State
	Score        int64
	PendingSince int64
	Msg          []byte
}

// TaskMessage decodes the embedded task message.
func (r Record) TaskMessage() (*base.TaskMessage, error) {
	return base.DecodeMessage(r.Msg)
}

var snapshotIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// ValidateSnapshotID reports whether id is safe to use as a restore point file
// name. It must be non-empty, contain no path separators and not be "." or "..".
func ValidateSnapshotID(id string) error {
	if id == "" || id == "." || id == ".." || !snapshotIDPattern.MatchString(id) {
		return fmt.Errorf("%w: %q", ErrInvalidSnapshotID, id)
	}
	return nil
}

// FileName returns the file name of the restore point with the given ID. The
// caller is expected to validate the ID first.
func FileName(id string) string {
	return id + FileExt
}

// Encode writes a complete restore point file to w. Metadata and every record
// are validated before writing so that an error never produces a partial,
// seemingly valid file (callers should still write to a temporary file and
// rename on success).
func Encode(w io.Writer, meta Metadata, records []Record) error {
	meta.FormatVersion = FormatVersion
	if err := validateMetadata(meta, records); err != nil {
		return err
	}
	if _, err := io.WriteString(w, magic); err != nil {
		return fmt.Errorf("snapshot: cannot write magic: %w", err)
	}
	metaPayload, err := json.Marshal(&meta)
	if err != nil {
		return fmt.Errorf("snapshot: cannot encode metadata: %w", err)
	}
	if err := writeFrame(w, metaPayload); err != nil {
		return fmt.Errorf("snapshot: metadata frame: %w", err)
	}
	for i, rec := range records {
		if err := validateRecord(meta, rec); err != nil {
			return fmt.Errorf("snapshot: record %d: %w", i, err)
		}
		payload := make([]byte, recordFixedLen+len(rec.Msg))
		payload[0] = byte(rec.State)
		binary.BigEndian.PutUint64(payload[1:9], uint64(rec.Score))
		binary.BigEndian.PutUint64(payload[9:17], uint64(rec.PendingSince))
		binary.BigEndian.PutUint32(payload[17:21], uint32(len(rec.Msg)))
		copy(payload[recordFixedLen:], rec.Msg)
		if err := writeFrame(w, payload); err != nil {
			return fmt.Errorf("snapshot: record frame %d: %w", i, err)
		}
	}
	return nil
}

// Decode reads a complete restore point file and validates every frame. It
// fails on the first unreadable frame, reporting the zero-based frame index
// (frame 0 is the metadata frame).
func Decode(r io.Reader) (Metadata, []Record, error) {
	meta, err := DecodeMetadata(r)
	if err != nil {
		return Metadata{}, nil, err
	}
	records := make([]Record, 0, meta.Total)
	for i := 0; i < meta.Total; i++ {
		payload, err := readFrame(r, i+1)
		if err != nil {
			return Metadata{}, nil, err
		}
		rec, err := decodeRecordPayload(payload)
		if err != nil {
			return Metadata{}, nil, fmt.Errorf("snapshot: frame %d: %w", i+1, err)
		}
		if err := validateRecord(meta, rec); err != nil {
			return Metadata{}, nil, fmt.Errorf("snapshot: frame %d: %w", i+1, err)
		}
		records = append(records, rec)
	}
	return meta, records, nil
}

// DecodeMetadata reads only the header and the metadata frame. It is used when
// listing restore points without decoding (and allocating) all task records.
func DecodeMetadata(r io.Reader) (Metadata, error) {
	header := make([]byte, len(magic))
	if _, err := io.ReadFull(r, header); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Metadata{}, fmt.Errorf("%w: missing magic header", ErrTruncated)
		}
		return Metadata{}, fmt.Errorf("snapshot: cannot read magic: %w", err)
	}
	if !bytes.Equal(header, []byte(magic)) {
		return Metadata{}, fmt.Errorf("%w: got %q", ErrBadMagic, string(header))
	}
	payload, err := readFrame(r, 0)
	if err != nil {
		return Metadata{}, err
	}
	var meta Metadata
	if err := json.Unmarshal(payload, &meta); err != nil {
		return Metadata{}, fmt.Errorf("snapshot: frame 0: %w: %v", ErrInvalidRecord, err)
	}
	if meta.FormatVersion != FormatVersion {
		return Metadata{}, fmt.Errorf("%w: got %d, want %d", ErrUnsupportedVersion, meta.FormatVersion, FormatVersion)
	}
	return meta, nil
}

// writeFrame writes len(payload), payload and its CRC32 checksum.
func writeFrame(w io.Writer, payload []byte) error {
	var buf [lenFieldSize]byte
	binary.BigEndian.PutUint32(buf[:], uint32(len(payload)))
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	var crcBuf [crcFieldSize]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc32.ChecksumIEEE(payload))
	_, err := w.Write(crcBuf[:])
	return err
}

// readFrame reads a frame written by writeFrame. frameIndex is used for error
// reporting only.
func readFrame(r io.Reader, frameIndex int) ([]byte, error) {
	var lenBuf [lenFieldSize]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("%w: frame %d length", ErrTruncated, frameIndex)
		}
		return nil, fmt.Errorf("snapshot: frame %d: cannot read length: %w", frameIndex, err)
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n > maxFramePayload {
		return nil, fmt.Errorf("%w: frame %d payload length %d exceeds limit %d",
			ErrInvalidRecord, frameIndex, n, maxFramePayload)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("%w: frame %d payload", ErrTruncated, frameIndex)
		}
		return nil, fmt.Errorf("snapshot: frame %d: cannot read payload: %w", frameIndex, err)
	}
	var crcBuf [crcFieldSize]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("%w: frame %d checksum", ErrTruncated, frameIndex)
		}
		return nil, fmt.Errorf("snapshot: frame %d: cannot read checksum: %w", frameIndex, err)
	}
	if got, want := crc32.ChecksumIEEE(payload), binary.BigEndian.Uint32(crcBuf[:]); got != want {
		return nil, fmt.Errorf("%w: frame %d", ErrCRCMismatch, frameIndex)
	}
	return payload, nil
}

func decodeRecordPayload(payload []byte) (Record, error) {
	if len(payload) < recordFixedLen {
		return Record{}, fmt.Errorf("%w: payload too short (%d bytes)", ErrInvalidRecord, len(payload))
	}
	msgLen := binary.BigEndian.Uint32(payload[17:21])
	if int(msgLen) != len(payload)-recordFixedLen {
		return Record{}, fmt.Errorf("%w: declared msg length %d does not match actual %d",
			ErrInvalidRecord, msgLen, len(payload)-recordFixedLen)
	}
	msg := make([]byte, msgLen)
	copy(msg, payload[recordFixedLen:])
	return Record{
		State:        State(payload[0]),
		Score:        int64(binary.BigEndian.Uint64(payload[1:9])),
		PendingSince: int64(binary.BigEndian.Uint64(payload[9:17])),
		Msg:          msg,
	}, nil
}

func validateMetadata(meta Metadata, records []Record) error {
	if err := ValidateSnapshotID(meta.ID); err != nil {
		return err
	}
	if meta.Queue == "" {
		return fmt.Errorf("%w: empty queue name", ErrInvalidRecord)
	}
	seenIDs := make(map[string]struct{}, len(records))
	var p, s, rt, a int
	for _, rec := range records {
		switch rec.State {
		case StatePending:
			p++
		case StateScheduled:
			s++
		case StateRetry:
			rt++
		case StateAggregating:
			a++
		default:
			return fmt.Errorf("%w: unknown state ordinal %d", ErrInvalidRecord, rec.State)
		}
		msg, err := rec.TaskMessage()
		if err != nil {
			return fmt.Errorf("%w: cannot decode task message: %v", ErrInvalidRecord, err)
		}
		if _, dup := seenIDs[msg.ID]; dup {
			return fmt.Errorf("%w: duplicate task ID %q across records", ErrInvalidRecord, msg.ID)
		}
		seenIDs[msg.ID] = struct{}{}
	}
	if p != meta.Pending || s != meta.Scheduled || rt != meta.Retry || a != meta.Aggregating ||
		len(records) != meta.Total || meta.Total != p+s+rt+a {
		return fmt.Errorf("%w: metadata counts %+v do not match %d records",
			ErrInvalidRecord, meta, len(records))
	}
	return nil
}

func validateRecord(meta Metadata, rec Record) error {
	if _, err := rec.State.TaskState(); err != nil {
		return err
	}
	if len(rec.Msg) == 0 {
		return fmt.Errorf("%w: empty task message", ErrInvalidRecord)
	}
	msg, err := rec.TaskMessage()
	if err != nil {
		return fmt.Errorf("%w: cannot decode task message: %v", ErrInvalidRecord, err)
	}
	if msg.ID == "" {
		return fmt.Errorf("%w: task message has empty ID", ErrInvalidRecord)
	}
	if msg.Queue != meta.Queue {
		return fmt.Errorf("%w: task %q belongs to queue %q, metadata queue is %q",
			ErrInvalidRecord, msg.ID, msg.Queue, meta.Queue)
	}
	if rec.State == StateAggregating && msg.GroupKey == "" {
		return fmt.Errorf("%w: aggregating task %q has empty group key", ErrInvalidRecord, msg.ID)
	}
	return nil
}
