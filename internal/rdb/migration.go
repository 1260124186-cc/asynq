// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"fmt"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

// MigrationCandidate is a task that has not started processing and is
// therefore eligible for a payload schema migration.
type MigrationCandidate struct {
	// Message is the decoded task message read from Redis.
	Message *base.TaskMessage

	// State is the current state of the task. It is one of
	// TaskStatePending, TaskStateScheduled or TaskStateRetry.
	State base.TaskState

	// Encoded is the raw encoded message exactly as stored in the task hash
	// "msg" field. It is used as an optimistic-concurrency guard when the
	// migrated message is committed back to Redis.
	Encoded []byte
}

// listMigrationListCmd returns every (id, msg) pair stored in a Redis list
// state key (i.e. the pending queue).
//
// KEYS[1] -> state list key (e.g. asynq:{<qname>}:pending)
// ARGV[1] -> task key prefix (asynq:{<qname>}:t:)
var listMigrationListCmd = redis.NewScript(`
local ids = redis.call("LRANGE", KEYS[1], 0, -1)
local data = {}
for _, id in ipairs(ids) do
	local msg = redis.call("HGET", ARGV[1] .. id, "msg")
	table.insert(data, id)
	table.insert(data, msg)
end
return data
`)

// listMigrationZSetCmd returns every (id, msg) pair stored in a Redis sorted
// set state key (i.e. the scheduled or retry queue).
//
// KEYS[1] -> state zset key (e.g. asynq:{<qname>}:scheduled)
// ARGV[1] -> task key prefix (asynq:{<qname>}:t:)
var listMigrationZSetCmd = redis.NewScript(`
local ids = redis.call("ZRANGE", KEYS[1], 0, -1)
local data = {}
for _, id in ipairs(ids) do
	local msg = redis.call("HGET", ARGV[1] .. id, "msg")
	table.insert(data, id)
	table.insert(data, msg)
end
return data
`)

// ListMigrationCandidates returns all tasks in the given queue that have not
// started processing yet, i.e. tasks in the pending, scheduled and retry
// states. Active, archived, completed and aggregating tasks are intentionally
// excluded.
//
// The state sets only store task IDs; the full message is read from each
// task hash together with its raw encoded bytes. The returned candidates are
// safe to inspect and convert without holding any Redis-side lock.
func (r *RDB) ListMigrationCandidates(qname string) ([]*MigrationCandidate, error) {
	var op errors.Op = "rdb.ListMigrationCandidates"
	if err := r.checkQueueExists(qname); err != nil {
		return nil, errors.E(op, errors.CanonicalCode(err), err)
	}
	var candidates []*MigrationCandidate
	type stateKey struct {
		state base.TaskState
		key   string
		zset  bool
	}
	stateKeys := []stateKey{
		{base.TaskStatePending, base.PendingKey(qname), false},
		{base.TaskStateScheduled, base.ScheduledKey(qname), true},
		{base.TaskStateRetry, base.RetryKey(qname), true},
	}
	for _, sk := range stateKeys {
		script := listMigrationListCmd
		if sk.zset {
			script = listMigrationZSetCmd
		}
		res, err := script.Run(context.Background(), r.client,
			[]string{sk.key}, base.TaskKeyPrefix(qname)).Result()
		if err != nil {
			return nil, errors.E(op, errors.Unknown, fmt.Errorf("redis eval error: %w", err))
		}
		pairs, err := cast.ToStringSliceE(res)
		if err != nil {
			return nil, errors.E(op, errors.Internal, fmt.Errorf("cast error: Lua script returned unexpected value: %v", res))
		}
		for i := 0; i+1 < len(pairs); i += 2 {
			id, encoded := pairs[i], pairs[i+1]
			msg, err := base.DecodeMessage([]byte(encoded))
			if err != nil {
				return nil, errors.E(op, errors.Internal,
					fmt.Errorf("cannot decode message for task id=%q in state %s: %v", id, sk.state, err))
			}
			candidates = append(candidates, &MigrationCandidate{
				Message: msg,
				State:   sk.state,
				Encoded: []byte(encoded),
			})
		}
	}
	return candidates, nil
}

// PayloadMigrationCommitStatus describes the outcome of a single payload
// migration commit.
type PayloadMigrationCommitStatus int

const (
	// PayloadMigrationCommitted means the migrated message was written atomically.
	PayloadMigrationCommitted PayloadMigrationCommitStatus = iota

	// PayloadMigrationTaskNotFound means the task disappeared between the
	// scan and the commit (e.g. it was deleted).
	PayloadMigrationTaskNotFound

	// PayloadMigrationStateChanged means the task left an eligible state
	// (pending, scheduled, retry) between the scan and the commit, e.g. it
	// was dequeued and became active. The task is left untouched.
	PayloadMigrationStateChanged

	// PayloadMigrationMessageChanged means the task message was modified by
	// another process between the scan and the commit. The task is left
	// untouched and a subsequent migration run will pick it up.
	PayloadMigrationMessageChanged
)

// commitPayloadMigrationCmd atomically replaces the "msg" field of a task
// hash, but only while the task is still in the expected state and the stored
// message is byte-identical to the message that was scanned and converted.
//
// The state sets (pending list, scheduled/retry sorted sets) store task IDs,
// not messages, so replacing the "msg" field preserves task identity, list
// position, sorted-set score (scheduled/retry time), retry counters and every
// other execution option.
//
// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// ARGV[1] -> expected state string (pending, scheduled or retry)
// ARGV[2] -> raw encoded message captured at scan time (optimistic guard)
// ARGV[3] -> new encoded message
//
// Output:
//
//	 1 -> message migrated
//	 0 -> task key does not exist
//	-1 -> task is no longer in the expected state
//	-2 -> stored message differs from the scanned message
var commitPayloadMigrationCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then
	return 0
end
local state, msg = unpack(redis.call("HMGET", KEYS[1], "state", "msg"))
if state ~= ARGV[1] then
	return -1
end
if msg ~= ARGV[2] then
	return -2
end
redis.call("HSET", KEYS[1], "msg", ARGV[3])
return 1
`)

// CommitPayloadMigration atomically writes the migrated message for the task
// identified by qname and id. The commit is rejected without any write if the
// task vanished, changed state or was modified since it was scanned.
func (r *RDB) CommitPayloadMigration(qname, id string, state base.TaskState, oldEncoded, newEncoded []byte) (PayloadMigrationCommitStatus, error) {
	var op errors.Op = "rdb.CommitPayloadMigration"
	switch state {
	case base.TaskStatePending, base.TaskStateScheduled, base.TaskStateRetry:
	default:
		return 0, errors.E(op, errors.FailedPrecondition,
			fmt.Sprintf("payload migration is only supported for pending, scheduled and retry tasks, got state %s", state))
	}
	res, err := commitPayloadMigrationCmd.Run(context.Background(), r.client,
		[]string{base.TaskKey(qname, id)},
		state.String(), oldEncoded, newEncoded).Result()
	if err != nil {
		return 0, errors.E(op, errors.Unknown, fmt.Errorf("redis eval error: %w", err))
	}
	code, ok := res.(int64)
	if !ok {
		return 0, errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value from Lua script: %v", res))
	}
	switch code {
	case 1:
		return PayloadMigrationCommitted, nil
	case 0:
		return PayloadMigrationTaskNotFound, nil
	case -1:
		return PayloadMigrationStateChanged, nil
	case -2:
		return PayloadMigrationMessageChanged, nil
	default:
		return 0, errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value from Lua script: %d", code))
	}
}
