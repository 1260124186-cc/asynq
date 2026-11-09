// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq/internal/base"
	ierrors "github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/rdb"
	"github.com/hibiken/asynq/internal/snapshot"
	"github.com/redis/go-redis/v9"
)

// Sentinel errors returned by SnapshotManager. errors.Is can be used to detect
// the failure class regardless of wrapping.
var (
	// ErrSnapshotNotFound indicates that no restore point file matches the given ID.
	ErrSnapshotNotFound = stderrors.New("snapshot not found")

	// ErrSnapshotCorrupt indicates that a restore point file or one of its records
	// cannot be read or decoded.
	ErrSnapshotCorrupt = stderrors.New("snapshot corrupt")

	// ErrSnapshotTaskConflict indicates that a live task with the same ID exists
	// but its content differs from the restore point record.
	ErrSnapshotTaskConflict = stderrors.New("snapshot task content conflict")

	// ErrSnapshotAlreadyExists indicates that a restore point with the chosen ID
	// already exists. Existing restore points are never overwritten.
	ErrSnapshotAlreadyExists = stderrors.New("snapshot already exists")
)

// snapshotRestoreBatchSize bounds the number of restore records applied in one
// pipelined round-trip.
const snapshotRestoreBatchSize = 256

// SnapshotManager creates restore point files of queues and restores missing
// tasks from them.
//
// Restore points are self-describing files stored in a local directory. The
// restore operation is conservative: it only recreates missing tasks, leaves
// identical tasks untouched, never deletes or overwrites, and fails the whole
// operation without any write when a conflict or an unreadable record is
// detected. Re-running the same restore point is idempotent and an
// interrupted restore resumes from where the previous run stopped.
type SnapshotManager struct {
	rdb              *rdb.RDB
	dir              string
	sharedConnection bool
}

// NewSnapshotManager returns a SnapshotManager that stores restore point files
// in dir and connects to Redis through the given options.
func NewSnapshotManager(r RedisConnOpt, dir string) *SnapshotManager {
	c, ok := r.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		panic(fmt.Sprintf("snapshot: unsupported RedisConnOpt type %T", r))
	}
	m := NewSnapshotManagerFromRedisClient(c, dir)
	m.sharedConnection = false
	return m
}

// NewSnapshotManagerFromRedisClient returns a SnapshotManager using an existing
// redis.UniversalClient. The caller is responsible for closing the client.
func NewSnapshotManagerFromRedisClient(c redis.UniversalClient, dir string) *SnapshotManager {
	return &SnapshotManager{
		rdb:              rdb.NewRDB(c),
		dir:              dir,
		sharedConnection: true,
	}
}

// Close closes the underlying Redis connection when the manager owns it.
func (m *SnapshotManager) Close() error {
	if m.sharedConnection {
		return fmt.Errorf("redis connection is shared so the SnapshotManager can't be closed through asynq")
	}
	return m.rdb.Close()
}

// QueueSnapshotInfo describes a stored restore point.
type QueueSnapshotInfo struct {
	// ID is the unique identifier of the restore point.
	ID string

	// Queue is the name of the captured queue.
	Queue string

	// CreatedAt is the time the restore point was created.
	CreatedAt time.Time

	// Number of tasks captured in each restorable state.
	Pending     int
	Scheduled   int
	Retry       int
	Aggregating int

	// Total is the total number of task records in the restore point.
	Total int
}

// RestoreSnapshotResult summarizes one restore execution.
type RestoreSnapshotResult struct {
	// SnapshotID is the restore point that was executed.
	SnapshotID string

	// Restored is the number of missing tasks that were (re)created in this run.
	Restored int

	// Kept is the number of records whose identical task already existed and
	// was left untouched in this run.
	Kept int

	// Remaining is the number of records not yet applied. It is zero when the
	// restore completes; a non-zero value only accompanies an error.
	Remaining int

	// Completed is true when every record has been handled.
	Completed bool
}

// SnapshotOption configures restore point creation.
type SnapshotOption interface{}

type snapshotIDOpt string

// SnapshotID overrides the auto-generated restore point ID with a custom,
// file-name-safe identifier (letters, digits, dot, underscore and hyphen).
func SnapshotID(id string) SnapshotOption {
	return snapshotIDOpt(id)
}

// CreateQueueSnapshot captures the pending, scheduled, retry and aggregating
// tasks of the given queue into a new restore point file in the configured
// directory and returns its metadata.
//
// The ID is auto-generated unless the SnapshotID option is given. Creating a
// restore point whose ID already exists fails with ErrSnapshotAlreadyExists;
// existing files are never overwritten.
func (m *SnapshotManager) CreateQueueSnapshot(ctx context.Context, queue string, opts ...SnapshotOption) (*QueueSnapshotInfo, error) {
	if err := base.ValidateQueueName(queue); err != nil {
		return nil, fmt.Errorf("asynq: %w", err)
	}
	if err := m.checkDir(); err != nil {
		return nil, err
	}
	id := uuid.NewString()
	for _, opt := range opts {
		if v, ok := opt.(snapshotIDOpt); ok {
			id = string(v)
		}
	}
	if err := snapshot.ValidateSnapshotID(id); err != nil {
		return nil, fmt.Errorf("asynq: %w", err)
	}
	target := filepath.Join(m.dir, snapshot.FileName(id))
	// Reserve the target name atomically so concurrent creations can never
	// overwrite each other.
	guard, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if os.IsExist(err) {
			return nil, fmt.Errorf("asynq: %w: id=%q", ErrSnapshotAlreadyExists, id)
		}
		return nil, fmt.Errorf("asynq: cannot create snapshot file: %w", err)
	}
	if err := guard.Close(); err != nil {
		return nil, fmt.Errorf("asynq: cannot create snapshot file: %w", err)
	}

	records, err := m.rdb.CaptureQueueSnapshot(ctx, queue)
	if err != nil {
		_ = os.Remove(target) // roll back the reservation
		return nil, wrapSnapshotError(err)
	}

	meta := snapshot.Metadata{
		ID:            id,
		Queue:         queue,
		CreatedAtUnix: time.Now().UTC().Unix(),
	}
	for _, rec := range records {
		switch rec.State {
		case snapshot.StatePending:
			meta.Pending++
		case snapshot.StateScheduled:
			meta.Scheduled++
		case snapshot.StateRetry:
			meta.Retry++
		case snapshot.StateAggregating:
			meta.Aggregating++
		}
	}
	meta.Total = len(records)

	tmp, err := os.CreateTemp(m.dir, "*.asynq.snap.tmp")
	if err != nil {
		_ = os.Remove(target)
		return nil, fmt.Errorf("asynq: cannot create temporary snapshot file: %w", err)
	}
	tmpName := tmp.Name()
	encodeErr := snapshot.Encode(tmp, meta, records)
	closeErr := tmp.Close()
	if encodeErr != nil || closeErr != nil {
		_ = os.Remove(tmpName)
		_ = os.Remove(target)
		if encodeErr != nil {
			return nil, fmt.Errorf("asynq: %w: %v", ErrSnapshotCorrupt, encodeErr)
		}
		return nil, fmt.Errorf("asynq: cannot finalize snapshot file: %w", closeErr)
	}
	// Rename atomically replaces the reserved guard file.
	if err := os.Rename(tmpName, target); err != nil {
		_ = os.Remove(tmpName)
		_ = os.Remove(target)
		return nil, fmt.Errorf("asynq: cannot finalize snapshot file: %w", err)
	}
	return metadataToInfo(meta), nil
}

// RestoreQueueSnapshot restores missing tasks from the restore point identified
// by snapshotID.
//
// The operation has two phases. Phase A is strictly read-only: it decodes the
// whole file and verifies every record against current Redis state. An
// unreadable record or a same-ID/different-content conflict fails the operation
// before any write. Phase B then recreates only the missing tasks with their
// original state, timing and options; progress is tracked in Redis so an
// interrupted run resumes and re-running a completed restore is a no-op.
func (m *SnapshotManager) RestoreQueueSnapshot(ctx context.Context, snapshotID string) (*RestoreSnapshotResult, error) {
	if err := snapshot.ValidateSnapshotID(snapshotID); err != nil {
		return nil, fmt.Errorf("asynq: %w", err)
	}
	if err := m.checkDir(); err != nil {
		return nil, err
	}
	path := filepath.Join(m.dir, snapshot.FileName(snapshotID))
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("asynq: %w: id=%q", ErrSnapshotNotFound, snapshotID)
		}
		return nil, fmt.Errorf("asynq: cannot open snapshot file: %w", err)
	}
	defer f.Close()

	meta, records, err := snapshot.Decode(f)
	if err != nil {
		return nil, fmt.Errorf("asynq: %w: %v", ErrSnapshotCorrupt, err)
	}
	if meta.ID != snapshotID {
		return nil, fmt.Errorf("asynq: %w: file id %q does not match requested id %q",
			ErrSnapshotCorrupt, meta.ID, snapshotID)
	}

	// Phase A: read-only preflight.
	ids := make([]string, len(records))
	for i, rec := range records {
		msg, decodeErr := rec.TaskMessage()
		if decodeErr != nil {
			return nil, fmt.Errorf("asynq: %w: %v", ErrSnapshotCorrupt, decodeErr)
		}
		ids[i] = msg.ID
	}
	current, err := m.rdb.CurrentTaskMsgs(ctx, meta.Queue, ids)
	if err != nil {
		return nil, wrapSnapshotError(err)
	}
	for _, rec := range records {
		msg, _ := rec.TaskMessage()
		raw, ok := current[msg.ID]
		if !ok {
			return nil, fmt.Errorf("asynq: %w: preflight data missing for task %q", ErrSnapshotCorrupt, msg.ID)
		}
		if raw == nil {
			continue // task absent -> restore candidate
		}
		if _, decodeErr := base.DecodeMessage(raw); decodeErr != nil {
			return nil, fmt.Errorf("asynq: %w: current task %q cannot be decoded: %v",
				ErrSnapshotCorrupt, msg.ID, decodeErr)
		}
		if !messagesEqual(rec.Msg, raw) {
			return nil, fmt.Errorf("asynq: %w: queue=%q task=%q",
				ErrSnapshotTaskConflict, meta.Queue, msg.ID)
		}
	}

	// Phase B: conditional, resumable writes.
	if err := m.rdb.EnsureQueueRegistered(ctx, meta.Queue); err != nil {
		return nil, wrapSnapshotError(err)
	}
	progress, err := m.rdb.ReadSnapshotProgress(ctx, snapshotID, meta.CreatedAtUnix)
	if err != nil {
		return nil, wrapSnapshotError(err)
	}

	result := &RestoreSnapshotResult{SnapshotID: snapshotID}
	var batch []snapshot.Record
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		outcomes, flushErr := m.rdb.RestoreRecords(ctx, batch)
		if flushErr != nil {
			return flushErr
		}
		doneIDs := make([]string, 0, len(outcomes))
		for _, o := range outcomes {
			doneIDs = append(doneIDs, o.ID)
			if o.Written {
				result.Restored++
			} else {
				result.Kept++
			}
		}
		if markErr := m.rdb.MarkSnapshotRecordsDone(ctx, snapshotID, meta.CreatedAtUnix, doneIDs); markErr != nil {
			return markErr
		}
		batch = batch[:0]
		return nil
	}

	for _, rec := range records {
		msg, _ := rec.TaskMessage()
		if _, done := progress[msg.ID]; done {
			continue
		}
		batch = append(batch, rec)
		if len(batch) == snapshotRestoreBatchSize {
			if err := flush(); err != nil {
				result.Remaining = len(records) - result.Restored - result.Kept - len(progress)
				return result, wrapSnapshotError(err)
			}
		}
	}
	if err := flush(); err != nil {
		result.Remaining = len(records) - result.Restored - result.Kept - len(progress)
		return result, wrapSnapshotError(err)
	}

	if err := m.rdb.ClearSnapshotProgress(ctx, snapshotID, meta.CreatedAtUnix); err != nil {
		return result, wrapSnapshotError(err)
	}
	result.Completed = true
	result.Remaining = 0
	return result, nil
}

// ListSnapshots returns the metadata of every restore point in the configured
// directory, ordered by creation time (then ID). A file that cannot be decoded
// is reported through ErrSnapshotCorrupt instead of being silently skipped.
func (m *SnapshotManager) ListSnapshots(ctx context.Context) ([]*QueueSnapshotInfo, error) {
	if err := m.checkDir(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return nil, fmt.Errorf("asynq: cannot read snapshot directory: %w", err)
	}
	var infos []*QueueSnapshotInfo
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), snapshot.FileExt) {
			continue
		}
		f, openErr := os.Open(filepath.Join(m.dir, entry.Name()))
		if openErr != nil {
			return nil, fmt.Errorf("asynq: cannot open snapshot %q: %w", entry.Name(), openErr)
		}
		meta, decodeErr := snapshot.DecodeMetadata(f)
		_ = f.Close()
		if decodeErr != nil {
			return nil, fmt.Errorf("asynq: %w: file=%q: %v", ErrSnapshotCorrupt, entry.Name(), decodeErr)
		}
		infos = append(infos, metadataToInfo(meta))
	}
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].CreatedAt.Unix() != infos[j].CreatedAt.Unix() {
			return infos[i].CreatedAt.Before(infos[j].CreatedAt)
		}
		return infos[i].ID < infos[j].ID
	})
	return infos, nil
}

// DeleteSnapshot removes the restore point file with the given ID and clears
// any residual restore progress for it. It returns ErrSnapshotNotFound if no
// such file exists.
func (m *SnapshotManager) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	if err := snapshot.ValidateSnapshotID(snapshotID); err != nil {
		return fmt.Errorf("asynq: %w", err)
	}
	if err := m.checkDir(); err != nil {
		return err
	}
	// Best-effort cleanup of progress state from any epoch; a missing key is harmless.
	if err := m.rdb.ClearAllSnapshotProgress(ctx, snapshotID); err != nil {
		return wrapSnapshotError(err)
	}
	path := filepath.Join(m.dir, snapshot.FileName(snapshotID))
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("asynq: %w: id=%q", ErrSnapshotNotFound, snapshotID)
		}
		return fmt.Errorf("asynq: cannot delete snapshot file: %w", err)
	}
	return nil
}

func (m *SnapshotManager) checkDir() error {
	info, err := os.Stat(m.dir)
	if err != nil {
		return fmt.Errorf("asynq: snapshot directory %q is inaccessible: %w", m.dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("asynq: snapshot path %q is not a directory", m.dir)
	}
	return nil
}

func metadataToInfo(meta snapshot.Metadata) *QueueSnapshotInfo {
	return &QueueSnapshotInfo{
		ID:          meta.ID,
		Queue:       meta.Queue,
		CreatedAt:   time.Unix(meta.CreatedAtUnix, 0).UTC(),
		Pending:     meta.Pending,
		Scheduled:   meta.Scheduled,
		Retry:       meta.Retry,
		Aggregating: meta.Aggregating,
		Total:       meta.Total,
	}
}

// messagesEqual reports whether the snapshot bytes and the current Redis bytes
// describe the same task. Raw byte equality is the fast path; a structural
// comparison on decoded messages guards against any non-deterministic encoding
// differences.
func messagesEqual(snapshotMsg, currentMsg []byte) bool {
	if bytes.Equal(snapshotMsg, currentMsg) {
		return true
	}
	a, errA := base.DecodeMessage(snapshotMsg)
	b, errB := base.DecodeMessage(currentMsg)
	if errA != nil || errB != nil {
		return false
	}
	return reflect.DeepEqual(a, b)
}

// wrapSnapshotError maps internal error types onto the public sentinel errors.
func wrapSnapshotError(err error) error {
	switch {
	case ierrors.IsSnapshotTaskConflict(err):
		return fmt.Errorf("asynq: %w: %v", ErrSnapshotTaskConflict, err)
	case ierrors.IsSnapshotCorrupt(err):
		return fmt.Errorf("asynq: %w: %v", ErrSnapshotCorrupt, err)
	case ierrors.IsQueueNotFound(err):
		return fmt.Errorf("asynq: %w: %v", ErrQueueNotFound, err)
	default:
		return fmt.Errorf("asynq: %w", err)
	}
}
