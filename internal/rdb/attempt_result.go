// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/redis/go-redis/v9"
)

// validateFrameLua is inlined into every Lua script that touches attempt
// result frames. It mirrors base.DecodeAttemptResult and returns "OK" when the
// frame is well-formed and its embedded attempt number equals expectedAttempt
// (pass nil to skip the slot check), otherwise "CORRUPTED" or "CONFLICT".
const validateFrameLua = `
local function validate_attempt_frame(v, expected_attempt)
	local nl = string.find(v, "\n", 1, true)
	if not nl then
		return "CORRUPTED"
	end
	local header = string.sub(v, 1, nl - 1)
	local magic, att, status, ts, dlen = string.match(header, "^([^:]+):([^:]+):([^:]+):([^:]+):([^:]+)$")
	if magic ~= "AR1" then
		return "CORRUPTED"
	end
	local a = tonumber(att)
	local s = tonumber(status)
	local t = tonumber(ts)
	local l = tonumber(dlen)
	if not a or not s or not t or not l then
		return "CORRUPTED"
	end
	if s ~= 1 and s ~= 2 and s ~= 3 then
		return "CORRUPTED"
	end
	if l ~= (string.len(v) - nl) then
		return "CORRUPTED"
	end
	if expected_attempt ~= nil and a ~= expected_attempt then
		return "CONFLICT"
	end
	return "OK"
end
`

// scanLineageLua validates every per-attempt result field stored on KEYS[1].
// It must be called before any mutation so that a single corrupted or
// conflicting record fails the operation without touching existing history.
const scanLineageLua = `
local function validate_attempt_lineage(key, prefix, final_field)
	local all = redis.call("HGETALL", key)
	for i = 1, #all, 2 do
		local k = all[i]
		if string.sub(k, 1, string.len(prefix)) == prefix and k ~= final_field then
			local slot = tonumber(string.sub(k, string.len(prefix) + 1))
			if not slot then
				return "CORRUPTED"
			end
			local code = validate_attempt_frame(all[i + 1], slot)
			if code ~= "OK" then
				return code
			end
		end
	end
	return "OK"
end
`

// writeAttemptResultCmd records (or idempotently replaces) the result of one
// execution attempt.
//
// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// ARGV[1] -> attempt number
// ARGV[2] -> encoded attempt result frame (status: recorded)
// ARGV[3] -> raw result data (mirrored to the legacy "result" field when non-empty)
// ARGV[4] -> final marker field name (ar:final)
// ARGV[5] -> per-attempt field prefix (ar:)
// ARGV[6] -> legacy result field name (result)
//
// Output:
//
//	"OK"        on success
//	"NOT FOUND" when the task key does not exist
//	"CONFLICT"  when a final result was already adopted or a slot/frame mismatch is found
//	"CORRUPTED" when an existing frame cannot be decoded
var writeAttemptResultCmd = redis.NewScript(validateFrameLua + scanLineageLua + `
if redis.call("EXISTS", KEYS[1]) == 0 then
	return redis.error_reply("NOT FOUND")
end
local attempt = tonumber(ARGV[1])
-- A running attempt can never overwrite a sealed lineage: once a final result
-- has been adopted, further writes are rejected.
local final = redis.call("HGET", KEYS[1], ARGV[4])
if type(final) == "string" and #final > 0 then
	return redis.error_reply("CONFLICT")
end
local lineage_code = validate_attempt_lineage(KEYS[1], ARGV[5], ARGV[4])
if lineage_code ~= "OK" then
	return redis.error_reply(lineage_code)
end
local frame_code = validate_attempt_frame(ARGV[2], attempt)
if frame_code ~= "OK" then
	return redis.error_reply(frame_code)
end
redis.call("HSET", KEYS[1], ARGV[5] .. ARGV[1], ARGV[2])
if #ARGV[3] > 0 then
	redis.call("HSET", KEYS[1], ARGV[6], ARGV[3])
end
return redis.status_reply("OK")
`)

// WriteAttemptResult stores the result data reported by one execution attempt.
//
// Repeated calls for the same attempt replace that single record instead of
// creating additional records. The record is stored with the "recorded" status
// and is never marked final here. When an existing record is corrupted or its
// attempt number conflicts with its storage slot, the task fails explicitly and
// no stored history is modified.
func (r *RDB) WriteAttemptResult(qname, taskID string, attempt int, data []byte) (int, error) {
	var op errors.Op = "rdb.WriteAttemptResult"
	if attempt <= 0 {
		return 0, errors.E(op, errors.FailedPrecondition,
			fmt.Errorf("attempt number must be positive, got %d: %w", attempt, errors.ErrAttemptResultConflict))
	}
	frame, err := base.EncodeAttemptResult(&base.AttemptResult{
		Attempt:   attempt,
		Status:    base.AttemptStatusRecorded,
		Data:      data,
		WrittenAt: r.clock.Now(),
	})
	if err != nil {
		return 0, errors.E(op, errors.Internal, err)
	}
	err = writeAttemptResultCmd.Run(context.Background(), r.client,
		[]string{base.TaskKey(qname, taskID)},
		attempt,
		frame,
		data,
		base.AttemptResultFinalField,
		base.AttemptResultFieldPrefix,
		base.LegacyResultField,
	).Err()
	if err != nil {
		return 0, r.mapAttemptResultScriptError(op, qname, taskID, err)
	}
	return len(data), nil
}

// markFinalResultCmd adopts the given attempt's record as the final task result.
//
// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// ARGV[1] -> attempt number
// ARGV[2] -> adoption time in Unix nano
// ARGV[3] -> final marker field name (ar:final)
// ARGV[4] -> per-attempt field prefix (ar:)
// ARGV[5] -> legacy result field name (result)
//
// Output: same reply contract as writeAttemptResultCmd.
var markFinalResultCmd = redis.NewScript(validateFrameLua + scanLineageLua + `
if redis.call("EXISTS", KEYS[1]) == 0 then
	return redis.error_reply("NOT FOUND")
end
local attempt = tonumber(ARGV[1])
local lineage_code = validate_attempt_lineage(KEYS[1], ARGV[4], ARGV[3])
if lineage_code ~= "OK" then
	return redis.error_reply(lineage_code)
end
local field = ARGV[4] .. ARGV[1]
local existing = redis.call("HGET", KEYS[1], field)
local data = ""
local frame
if not existing then
	-- The successful handler did not report a result: keep an (empty) record so
	-- that every execution attempt has exactly one lineage entry.
	frame = "AR1:" .. ARGV[1] .. ":3:" .. ARGV[2] .. ":0\n"
else
	local frame_code = validate_attempt_frame(existing, attempt)
	if frame_code ~= "OK" then
		return redis.error_reply(frame_code)
	end
	local nl = string.find(existing, "\n", 1, true)
	local dlen = string.len(existing) - nl
	data = string.sub(existing, nl + 1)
	frame = "AR1:" .. ARGV[1] .. ":3:" .. ARGV[2] .. ":" .. dlen .. "\n" .. data
end
local current_final = redis.call("HGET", KEYS[1], ARGV[3])
if type(current_final) == "string" and #current_final > 0 and tonumber(current_final) ~= attempt then
	return redis.error_reply("CONFLICT")
end
redis.call("HSET", KEYS[1], field, frame, ARGV[3], ARGV[1])
if #data > 0 then
	redis.call("HSET", KEYS[1], ARGV[5], data)
end
return redis.status_reply("OK")
`)

// MarkFinalResult marks the given attempt's record as the adopted final result.
// It is only invoked after the task handler has succeeded, so a still-running
// attempt is never marked final. Calling it again for the same attempt is
// idempotent; adopting a different attempt after one was already finalized
// fails with ErrAttemptResultConflict without modifying history.
func (r *RDB) MarkFinalResult(qname, taskID string, attempt int) error {
	var op errors.Op = "rdb.MarkFinalResult"
	if attempt <= 0 {
		return errors.E(op, errors.FailedPrecondition,
			fmt.Errorf("attempt number must be positive, got %d: %w", attempt, errors.ErrAttemptResultConflict))
	}
	err := markFinalResultCmd.Run(context.Background(), r.client,
		[]string{base.TaskKey(qname, taskID)},
		attempt,
		r.clock.Now().UnixNano(),
		base.AttemptResultFinalField,
		base.AttemptResultFieldPrefix,
		base.LegacyResultField,
	).Err()
	if err != nil {
		return r.mapAttemptResultScriptError(op, qname, taskID, err)
	}
	return nil
}

// ListAttemptResults returns the retained per-attempt result records of a task,
// ordered by the 1-based attempt number. A corrupted frame or a mismatch
// between a record and its storage slot fails the call and leaves the stored
// lineage untouched.
func (r *RDB) ListAttemptResults(qname, id string) ([]*base.AttemptResult, error) {
	var op errors.Op = "rdb.ListAttemptResults"
	if err := r.checkQueueExists(qname); err != nil {
		return nil, errors.E(op, errors.CanonicalCode(err), err)
	}
	taskKey := base.TaskKey(qname, id)
	values, err := r.client.HGetAll(context.Background(), taskKey).Result()
	if err != nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hgetall", Err: err})
	}
	if len(values) == 0 {
		return nil, errors.E(op, errors.NotFound, &errors.TaskNotFoundError{Queue: qname, ID: id})
	}
	finalAttempt := 0
	if marker, ok := values[base.AttemptResultFinalField]; ok && len(marker) > 0 {
		n, err := strconv.Atoi(marker)
		if err != nil || n <= 0 {
			return nil, errors.E(op, errors.Internal, errors.ErrAttemptResultCorrupted)
		}
		finalAttempt = n
	}
	var records []*base.AttemptResult
	seen := make(map[int]struct{})
	for field, frameStr := range values {
		slot, ok := base.ParseAttemptResultFieldName(field)
		if !ok {
			continue
		}
		rec, err := base.DecodeAttemptResult([]byte(frameStr))
		if err != nil {
			return nil, errors.E(op, errors.Internal, errors.ErrAttemptResultCorrupted)
		}
		if rec.Attempt != slot {
			return nil, errors.E(op, errors.FailedPrecondition, errors.ErrAttemptResultConflict)
		}
		if _, dup := seen[rec.Attempt]; dup {
			return nil, errors.E(op, errors.Internal, errors.ErrAttemptResultCorrupted)
		}
		seen[rec.Attempt] = struct{}{}
		records = append(records, rec)
	}
	if finalAttempt > 0 {
		if _, ok := seen[finalAttempt]; !ok {
			return nil, errors.E(op, errors.FailedPrecondition, errors.ErrAttemptResultConflict)
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Attempt < records[j].Attempt })
	return records, nil
}

// mapAttemptResultScriptError translates the textual error replies used by the
// attempt-result Lua scripts into canonical asynq errors. Redis prefixes script
// error replies with "ERR ", so matching is done on the message suffix.
func (r *RDB) mapAttemptResultScriptError(op errors.Op, qname, taskID string, err error) error {
	msg := err.Error()
	switch {
	case strings.HasSuffix(msg, "NOT FOUND"):
		return errors.E(op, errors.NotFound, &errors.TaskNotFoundError{Queue: qname, ID: taskID})
	case strings.HasSuffix(msg, "CORRUPTED"):
		return errors.E(op, errors.Internal, errors.ErrAttemptResultCorrupted)
	case strings.HasSuffix(msg, "CONFLICT"):
		return errors.E(op, errors.FailedPrecondition, errors.ErrAttemptResultConflict)
	default:
		return errors.E(op, errors.Unknown, fmt.Errorf("redis eval error: %w", err))
	}
}
