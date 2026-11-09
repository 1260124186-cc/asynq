// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"fmt"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/snapshot"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

// snapshotBatchSize bounds the number of commands issued in a single pipeline
// when capturing or restoring a queue.
const snapshotBatchSize = 256

// Restore script return codes shared by all restore commands.
const (
	restoreWritten      = 1  // task key was absent and has been (re)created
	restoreAlreadyEqual = 0  // task key exists and stores the identical msg
	restoreConflictCode = -1 // task key exists but stores a different msg
)

// RestoreRecordOutcome reports how a single restore point record was handled.
type RestoreRecordOutcome struct {
	// ID is the task ID.
	ID string

	// Written is true when the task was missing and has been (re)created,
	// false when an identical task already existed and was left untouched.
	Written bool
}

// CaptureQueueSnapshot reads all restorable tasks (pending, scheduled, retry and
// aggregating) of the given queue and returns their restore point records.
// It is strictly read-only: no Redis key is mutated. The raw msg bytes are
// preserved without re-encoding.
func (r *RDB) CaptureQueueSnapshot(ctx context.Context, qname string) ([]snapshot.Record, error) {
	op := errors.Op("rdb.CaptureQueueSnapshot")
	exists, err := r.queueExists(qname)
	if err != nil {
		return nil, errors.E(op, errors.Unknown, err)
	}
	if !exists {
		return nil, errors.E(op, errors.NotFound, &errors.QueueNotFoundError{Queue: qname})
	}

	var records []snapshot.Record
	pending, err := r.capturePending(ctx, op, qname)
	if err != nil {
		return nil, err
	}
	records = append(records, pending...)

	for _, c := range []struct {
		key   string
		state snapshot.State
	}{
		{base.ScheduledKey(qname), snapshot.StateScheduled},
		{base.RetryKey(qname), snapshot.StateRetry},
	} {
		zs, err := r.captureZSet(ctx, op, qname, c.key, c.state, "")
		if err != nil {
			return nil, err
		}
		records = append(records, zs...)
	}

	aggregating, err := r.captureAggregating(ctx, op, qname)
	if err != nil {
		return nil, err
	}
	records = append(records, aggregating...)
	return records, nil
}

// capturePending reads the pending list in dequeue (oldest-first) order and
// preserves each task's pending_since value.
func (r *RDB) capturePending(ctx context.Context, op errors.Op, qname string) ([]snapshot.Record, error) {
	ids, err := r.client.LRange(ctx, base.PendingKey(qname), 0, -1).Result()
	if err != nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "lrange", Err: err})
	}
	records := make([]snapshot.Record, 0, len(ids))
	for start := 0; start < len(ids); start += snapshotBatchSize {
		end := start + snapshotBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		pipe := r.client.Pipeline()
		cmds := make([]*redis.SliceCmd, 0, end-start)
		for j := start; j < end; j++ {
			// LRANGE returns newest-first because tasks are LPUSHed; reverse to
			// dequeue order so the file stores the original FIFO sequence.
			id := ids[len(ids)-1-j]
			cmds = append(cmds, pipe.HMGet(ctx, base.TaskKey(qname, id), "msg", "pending_since"))
		}
		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			return nil, errors.E(op, errors.Unknown, fmt.Errorf("redis pipeline error: %w", err))
		}
		for k, cmd := range cmds {
			id := ids[len(ids)-1-(start+k)]
			vals, err := cmd.Result()
			if err != nil {
				return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hmget", Err: err})
			}
			rawMsg, ok := vals[0].(string)
			if !ok || rawMsg == "" {
				return nil, errors.E(op, errors.Internal, &errors.SnapshotCorruptError{
					Reason: fmt.Sprintf("pending task %q has no readable msg field", id),
				})
			}
			rec, err := newSnapshotRecord(op, qname, id, snapshot.StatePending, 0, cast.ToInt64(vals[1]), []byte(rawMsg))
			if err != nil {
				return nil, err
			}
			records = append(records, rec)
		}
	}
	return records, nil
}

// captureZSet reads a scheduled/retry zset (or a group zset when gname is
// non-empty) in score order with batched msg reads.
func (r *RDB) captureZSet(ctx context.Context, op errors.Op, qname, key string, state snapshot.State, gname string) ([]snapshot.Record, error) {
	var records []snapshot.Record
	var start int64
	for {
		zs, err := r.client.ZRangeWithScores(ctx, key, start, start+snapshotBatchSize-1).Result()
		if err != nil {
			return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zrange", Err: err})
		}
		if len(zs) == 0 {
			return records, nil
		}
		pipe := r.client.Pipeline()
		cmds := make([]*redis.StringCmd, 0, len(zs))
		for _, z := range zs {
			id, ok := z.Member.(string)
			if !ok {
				return nil, errors.E(op, errors.Internal, &errors.SnapshotCorruptError{
					Reason: fmt.Sprintf("zset %q contains non-string member %v", key, z.Member),
				})
			}
			cmds = append(cmds, pipe.HGet(ctx, base.TaskKey(qname, id), "msg"))
		}
		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			return nil, errors.E(op, errors.Unknown, fmt.Errorf("redis pipeline error: %w", err))
		}
		for i, z := range zs {
			id := z.Member.(string)
			rawMsg, err := cmds[i].Result()
			if err != nil {
				return nil, errors.E(op, errors.Internal, &errors.SnapshotCorruptError{
					Reason: fmt.Sprintf("indexed task %q cannot be read: %v", id, err),
				})
			}
			rec, err := newSnapshotRecord(op, qname, id, state, int64(z.Score), 0, []byte(rawMsg))
			if err != nil {
				return nil, err
			}
			if gname != "" {
				msg, decodeErr := rec.TaskMessage()
				if decodeErr != nil {
					return nil, errors.E(op, errors.Internal, &errors.SnapshotCorruptError{
						Reason: fmt.Sprintf("aggregating task %q: %v", id, decodeErr),
					})
				}
				if msg.GroupKey != gname {
					return nil, errors.E(op, errors.Internal, &errors.SnapshotCorruptError{
						Reason: fmt.Sprintf("aggregating task %q is in group zset %q but msg group key is %q",
							id, gname, msg.GroupKey),
					})
				}
			}
			records = append(records, rec)
		}
		if len(zs) < snapshotBatchSize {
			return records, nil
		}
		start += int64(len(zs))
	}
}

// captureAggregating enumerates all group zsets of the queue.
func (r *RDB) captureAggregating(ctx context.Context, op errors.Op, qname string) ([]snapshot.Record, error) {
	groups, err := r.scanSet(ctx, op, base.AllGroups(qname))
	if err != nil {
		return nil, err
	}
	var records []snapshot.Record
	for _, gname := range groups {
		recs, err := r.captureZSet(ctx, op, qname, base.GroupKey(qname, gname), snapshot.StateAggregating, gname)
		if err != nil {
			return nil, err
		}
		records = append(records, recs...)
	}
	return records, nil
}

// scanSet lists all members of a SET using SSCAN with duplicate suppression.
func (r *RDB) scanSet(ctx context.Context, op errors.Op, key string) ([]string, error) {
	seen := make(map[string]struct{})
	var cursor uint64
	for {
		members, next, err := r.client.SScan(ctx, key, cursor, "*", snapshotBatchSize).Result()
		if err != nil {
			return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sscan", Err: err})
		}
		for _, m := range members {
			seen[m] = struct{}{}
		}
		if next == 0 {
			out := make([]string, 0, len(seen))
			for m := range seen {
				out = append(out, m)
			}
			return out, nil
		}
		cursor = next
	}
}

func newSnapshotRecord(op errors.Op, qname, taskID string, state snapshot.State,
	score, pendingSince int64, rawMsg []byte) (snapshot.Record, error) {
	msg, err := base.DecodeMessage(rawMsg)
	if err != nil {
		return snapshot.Record{}, errors.E(op, errors.Internal, &errors.SnapshotCorruptError{
			Reason: fmt.Sprintf("task %q: cannot decode message: %v", taskID, err),
		})
	}
	if msg.ID == "" {
		return snapshot.Record{}, errors.E(op, errors.Internal, &errors.SnapshotCorruptError{
			Reason: fmt.Sprintf("task in queue %q has empty ID", qname),
		})
	}
	if msg.Queue != qname {
		return snapshot.Record{}, errors.E(op, errors.Internal, &errors.SnapshotCorruptError{
			Reason: fmt.Sprintf("task %q belongs to queue %q, captured from queue %q", msg.ID, msg.Queue, qname),
		})
	}
	return snapshot.Record{
		State:        state,
		Score:        score,
		PendingSince: pendingSince,
		Msg:          rawMsg,
	}, nil
}

// CurrentTaskMsgs returns the raw msg bytes of the given tasks keyed by task ID.
// A nil value means the task hash does not currently exist (i.e. the task is
// missing and may be restored). An error is returned if an existing task hash
// has no readable msg field. It is used by the read-only restore preflight.
func (r *RDB) CurrentTaskMsgs(ctx context.Context, qname string, ids []string) (map[string][]byte, error) {
	op := errors.Op("rdb.CurrentTaskMsgs")
	out := make(map[string][]byte, len(ids))
	for start := 0; start < len(ids); start += snapshotBatchSize {
		end := start + snapshotBatchSize
		if end > len(ids) {
			end = len(ids)
		}
		pipe := r.client.Pipeline()
		cmds := make([]*redis.MapStringStringCmd, 0, end-start)
		for j := start; j < end; j++ {
			cmds = append(cmds, pipe.HGetAll(ctx, base.TaskKey(qname, ids[j])))
		}
		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			return nil, errors.E(op, errors.Unknown, fmt.Errorf("redis pipeline error: %w", err))
		}
		for k, cmd := range cmds {
			id := ids[start+k]
			fields, err := cmd.Result()
			if err != nil {
				return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hgetall", Err: err})
			}
			if len(fields) == 0 {
				out[id] = nil // task hash absent
				continue
			}
			raw, ok := fields["msg"]
			if !ok || raw == "" {
				return nil, errors.E(op, errors.Internal, &errors.SnapshotCorruptError{
					Reason: fmt.Sprintf("current task %q has no readable msg field", id),
				})
			}
			out[id] = []byte(raw)
		}
	}
	return out, nil
}

// EnsureQueueRegistered adds the queue to the global queue registry. It is used
// before restoring into a queue whose registration may have been removed.
func (r *RDB) EnsureQueueRegistered(ctx context.Context, qname string) error {
	if err := r.client.SAdd(ctx, base.AllQueues, qname).Err(); err != nil {
		return errors.E(errors.Op("rdb.EnsureQueueRegistered"), errors.Unknown,
			&errors.RedisCommandError{Command: "sadd", Err: err})
	}
	return nil
}

// restorePendingCmd conditionally recreates a pending task.
//
// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:pending
// ARGV[1] -> task message data (exact snapshot bytes)
// ARGV[2] -> task ID
// ARGV[3] -> pending_since Unix nano (0 if absent)
//
// Records are processed in snapshot dequeue (oldest-first) order. LPUSH
// prepends each restored ID at the list head, so (a) restored tasks line up
// behind current survivors, which remain at the tail and are dequeued first,
// and (b) among restored tasks the oldest snapshot ID stays closest to the
// tail, preserving the original FIFO order.
//
// Output: 1 = written, 0 = identical task already exists, -1 = content conflict.
var restorePendingCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	if redis.call("HGET", KEYS[1], "msg") == ARGV[1] then
		return 0
	end
	return -1
end
if tonumber(ARGV[3]) ~= 0 then
	redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", "pending", "pending_since", ARGV[3])
else
	redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", "pending")
end
redis.call("LPUSH", KEYS[2], ARGV[2])
return 1
`)

// restoreZSetCmd conditionally recreates a scheduled or retry task.
//
// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> scheduled or retry zset
// ARGV[1] -> task message data
// ARGV[2] -> task ID
// ARGV[3] -> zset score (process/retry Unix time)
// ARGV[4] -> state name ("scheduled" or "retry")
var restoreZSetCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	if redis.call("HGET", KEYS[1], "msg") == ARGV[1] then
		return 0
	end
	return -1
end
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", ARGV[4])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
return 1
`)

// restoreAggregatingCmd conditionally recreates an aggregating task.
//
// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:g:<group>
// KEYS[3] -> asynq:{<qname>}:groups
// ARGV[1] -> task message data
// ARGV[2] -> task ID
// ARGV[3] -> group zset score
// ARGV[4] -> group name
var restoreAggregatingCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	if redis.call("HGET", KEYS[1], "msg") == ARGV[1] then
		return 0
	end
	return -1
end
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", "aggregating", "group", ARGV[4])
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[2])
redis.call("SADD", KEYS[3], ARGV[4])
return 1
`)

// RestoreRecords applies the given records using one pipelined round-trip per
// batch. Each record is handled by an atomic conditional Lua script, so the
// batch is idempotent and safe to retry after a crash. A content conflict
// aborts the batch and returns SnapshotTaskConflictError; infrastructure
// errors return an Unknown error and leave already applied records in place
// (the progress set tracks them for resume).
func (r *RDB) RestoreRecords(ctx context.Context, records []snapshot.Record) ([]RestoreRecordOutcome, error) {
	op := errors.Op("rdb.RestoreRecords")
	if len(records) == 0 {
		return nil, nil
	}

	needsPending, needsZSet, needsAggregating := false, false, false
	for _, rec := range records {
		switch rec.State {
		case snapshot.StatePending:
			needsPending = true
		case snapshot.StateScheduled, snapshot.StateRetry:
			needsZSet = true
		case snapshot.StateAggregating:
			needsAggregating = true
		default:
			return nil, errors.E(op, errors.Internal, &errors.SnapshotCorruptError{
				Reason: fmt.Sprintf("unknown snapshot state ordinal %d", rec.State),
			})
		}
	}
	// Preload scripts so EVALSHA inside the pipeline does not fail with NOSCRIPT.
	if needsPending {
		if err := restorePendingCmd.Load(ctx, r.client).Err(); err != nil {
			return nil, errors.E(op, errors.Unknown, fmt.Errorf("failed to load restore script: %w", err))
		}
	}
	if needsZSet {
		if err := restoreZSetCmd.Load(ctx, r.client).Err(); err != nil {
			return nil, errors.E(op, errors.Unknown, fmt.Errorf("failed to load restore script: %w", err))
		}
	}
	if needsAggregating {
		if err := restoreAggregatingCmd.Load(ctx, r.client).Err(); err != nil {
			return nil, errors.E(op, errors.Unknown, fmt.Errorf("failed to load restore script: %w", err))
		}
	}

	pipe := r.client.Pipeline()
	cmds := make([]*redis.Cmd, 0, len(records))
	for _, rec := range records {
		msg, err := rec.TaskMessage()
		if err != nil {
			return nil, errors.E(op, errors.Internal, &errors.SnapshotCorruptError{
				Reason: fmt.Sprintf("cannot decode task message: %v", err),
			})
		}
		switch rec.State {
		case snapshot.StatePending:
			cmds = append(cmds, restorePendingCmd.Run(ctx, pipe,
				[]string{base.TaskKey(msg.Queue, msg.ID), base.PendingKey(msg.Queue)},
				rec.Msg, msg.ID, rec.PendingSince))
		case snapshot.StateScheduled, snapshot.StateRetry:
			stateName, _ := rec.State.TaskState()
			zsetKey := base.ScheduledKey(msg.Queue)
			if rec.State == snapshot.StateRetry {
				zsetKey = base.RetryKey(msg.Queue)
			}
			cmds = append(cmds, restoreZSetCmd.Run(ctx, pipe,
				[]string{base.TaskKey(msg.Queue, msg.ID), zsetKey},
				rec.Msg, msg.ID, rec.Score, stateName.String()))
		case snapshot.StateAggregating:
			cmds = append(cmds, restoreAggregatingCmd.Run(ctx, pipe,
				[]string{
					base.TaskKey(msg.Queue, msg.ID),
					base.GroupKey(msg.Queue, msg.GroupKey),
					base.AllGroups(msg.Queue),
				},
				rec.Msg, msg.ID, rec.Score, msg.GroupKey))
		}
	}

	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, errors.E(op, errors.Unknown, fmt.Errorf("redis pipeline error: %w", err))
	}

	outcomes := make([]RestoreRecordOutcome, 0, len(records))
	for i, cmd := range cmds {
		res, err := cmd.Result()
		if err != nil {
			return nil, errors.E(op, errors.Unknown, fmt.Errorf("restore script error: %w", err))
		}
		code, ok := res.(int64)
		if !ok {
			return nil, errors.E(op, errors.Internal, fmt.Sprintf("unexpected restore script return value: %v", res))
		}
		msg, decodeErr := records[i].TaskMessage()
		if decodeErr != nil {
			return nil, errors.E(op, errors.Internal, &errors.SnapshotCorruptError{
				Reason: fmt.Sprintf("cannot decode task message: %v", decodeErr),
			})
		}
		switch code {
		case restoreWritten:
			outcomes = append(outcomes, RestoreRecordOutcome{ID: msg.ID, Written: true})
		case restoreAlreadyEqual:
			outcomes = append(outcomes, RestoreRecordOutcome{ID: msg.ID, Written: false})
		case restoreConflictCode:
			return nil, errors.E(op, errors.FailedPrecondition,
				&errors.SnapshotTaskConflictError{Queue: msg.Queue, ID: msg.ID})
		default:
			return nil, errors.E(op, errors.Internal,
				fmt.Sprintf("unexpected restore script return code: %d", code))
		}
	}
	return outcomes, nil
}

// ReadSnapshotProgress returns the set of task IDs already handled by a
// (possibly interrupted) restore of the given snapshot. The epoch must match
// the restore point file's creation time so progress from an older file reusing
// the same snapshot ID is never consulted.
func (r *RDB) ReadSnapshotProgress(ctx context.Context, snapshotID string, epoch int64) (map[string]struct{}, error) {
	op := errors.Op("rdb.ReadSnapshotProgress")
	ids, err := r.client.SMembers(ctx, base.SnapshotProgressKey(snapshotID, epoch)).Result()
	if err != nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "smembers", Err: err})
	}
	out := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out, nil
}

// MarkSnapshotRecordsDone records the given task IDs as fully handled.
func (r *RDB) MarkSnapshotRecordsDone(ctx context.Context, snapshotID string, epoch int64, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	members := make([]interface{}, len(ids))
	for i, id := range ids {
		members[i] = id
	}
	if err := r.client.SAdd(ctx, base.SnapshotProgressKey(snapshotID, epoch), members...).Err(); err != nil {
		return errors.E(errors.Op("rdb.MarkSnapshotRecordsDone"), errors.Unknown,
			&errors.RedisCommandError{Command: "sadd", Err: err})
	}
	return nil
}

// ClearSnapshotProgress removes the progress set for the given snapshot and
// epoch after a restore completes. A missing key is not an error.
func (r *RDB) ClearSnapshotProgress(ctx context.Context, snapshotID string, epoch int64) error {
	if err := r.client.Del(ctx, base.SnapshotProgressKey(snapshotID, epoch)).Err(); err != nil {
		return errors.E(errors.Op("rdb.ClearSnapshotProgress"), errors.Unknown,
			&errors.RedisCommandError{Command: "del", Err: err})
	}
	return nil
}

// ClearAllSnapshotProgress removes every progress key (any epoch) belonging to
// the given snapshot ID. It is used when a restore point file is deleted.
func (r *RDB) ClearAllSnapshotProgress(ctx context.Context, snapshotID string) error {
	op := errors.Op("rdb.ClearAllSnapshotProgress")
	keys, err := r.client.Keys(ctx, base.SnapshotProgressKeyPattern(snapshotID)).Result()
	if err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "keys", Err: err})
	}
	if len(keys) == 0 {
		return nil
	}
	if err := r.client.Del(ctx, keys...).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "del", Err: err})
	}
	return nil
}
