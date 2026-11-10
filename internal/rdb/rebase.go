// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/redis/go-redis/v9"
)

// rebaseApplyBatchSize limits the number of per-task scripts packed into a
// single pipeline round-trip.
const rebaseApplyBatchSize = 200

// Window represents a time interval during which the Redis control plane was
// unavailable. It is interpreted as a half-open interval [Start, End).
type Window struct {
	Start time.Time
	End   time.Time
}

// TimeRange is a merged, non-overlapping time interval in Unix seconds.
type TimeRange struct {
	// Start and End are the inclusive start and exclusive end of the interval.
	Start time.Time
	End   time.Time
}

func (r TimeRange) unixPair() (int64, int64) {
	return r.Start.Unix(), r.End.Unix()
}

// RebaseReport summarizes a rebase run for a single queue.
type RebaseReport struct {
	// Queue is the name of the queue the report applies to.
	Queue string

	// Shifted is the number of tasks shifted during this invocation.
	Shifted int64

	// Skipped is the number of eligible tasks skipped because an earlier
	// (interrupted) invocation had already shifted them.
	Skipped int64
}

// MergeWindows normalizes the given outage windows into a list of
// non-overlapping, sorted time ranges and returns the union duration together
// with a deterministic fingerprint.
//
// The fingerprint is derived solely from the merged intervals, so the same
// windows submitted in any order (or re-submitted after an interrupted run)
// always produce the same fingerprint.
func MergeWindows(windows []Window) (merged []TimeRange, shift time.Duration, fingerprint string, err error) {
	if len(windows) == 0 {
		return nil, 0, "", errors.E(errors.FailedPrecondition, "at least one outage window is required")
	}
	type secRange struct{ start, end int64 }
	ranges := make([]secRange, 0, len(windows))
	for i, w := range windows {
		if w.End.IsZero() || w.Start.IsZero() {
			return nil, 0, "", errors.E(errors.FailedPrecondition,
				fmt.Sprintf("window at index %d has zero Start or End", i))
		}
		s, e := w.Start.Unix(), w.End.Unix()
		if e <= s {
			return nil, 0, "", errors.E(errors.FailedPrecondition,
				fmt.Sprintf("window at index %d must end after it starts: start=%d end=%d", i, s, e))
		}
		ranges = append(ranges, secRange{s, e})
	}
	// Sort by start, then by end, so ordering of the input does not matter.
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].start != ranges[j].start {
			return ranges[i].start < ranges[j].start
		}
		return ranges[i].end < ranges[j].end
	})

	var union []secRange
	var totalSec int64
	for _, rg := range ranges {
		if n := len(union); n > 0 && rg.start <= union[n-1].end {
			// Overlapping or touching: extend the current interval if needed.
			if rg.end > union[n-1].end {
				totalSec += rg.end - union[n-1].end
				union[n-1].end = rg.end
			}
			continue
		}
		union = append(union, rg)
		totalSec += rg.end - rg.start
	}

	parts := make([]string, len(union))
	merged = make([]TimeRange, len(union))
	for i, rg := range union {
		parts[i] = fmt.Sprintf("%d-%d", rg.start, rg.end)
		merged[i] = TimeRange{Start: time.Unix(rg.start, 0), End: time.Unix(rg.end, 0)}
	}
	canonical := strings.Join(parts, ",")
	sum := sha256.Sum256([]byte(canonical))
	fingerprint = hex.EncodeToString(sum[:])
	return merged, time.Duration(totalSec) * time.Second, fingerprint, nil
}

// Redis keys used by the rebase machinery. Every key stays inside the queue's
// hash-tag slot ("asynq:{<qname>}:") so that the Lua scripts remain valid in
// a Redis Cluster deployment.
func rebaseIndexKey(qname string) string { return base.QueueKeyPrefix(qname) + "rebases" }
func rebaseMarkerKey(qname, fp string) string {
	return base.QueueKeyPrefix(qname) + "rebase:" + fp
}
func rebaseDoneKey(qname, fp string) string {
	return base.QueueKeyPrefix(qname) + "rebase:" + fp + ":done"
}

// ensureRebaseMarkerCmd registers a rebase marker or verifies an existing one.
//
// KEYS[1] -> rebases index (ZSET)
// KEYS[2] -> marker hash
//
// ARGV[1] -> fingerprint
// ARGV[2] -> canonical merged intervals ("s-e,s-e")
// ARGV[3] -> union duration in seconds
// ARGV[4] -> current unix time (seconds)
// ARGV[5] -> number of new intervals, followed by start/end pairs
// ARGV[*] -> number of existing markers, followed by for each marker:
//
//	fingerprint, interval count, start/end pairs
//
// Output:
//
//	 0 -> marker created
//	 1 -> marker already exists with identical parameters (resume)
//	-1 -> marker exists with conflicting parameters
//	-2 -> another marker covers an overlapping time range
var ensureRebaseMarkerCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) == 1 then
	local c = redis.call("HGET", KEYS[2], "intervals")
	local d = redis.call("HGET", KEYS[2], "duration")
	if c == ARGV[2] and tonumber(d) == tonumber(ARGV[3]) then
		return 1
	end
	return -1
end

local idx = 5
local m = tonumber(ARGV[idx]); idx = idx + 1
local newInts = {}
for k = 1, m do
	newInts[#newInts+1] = {tonumber(ARGV[idx]), tonumber(ARGV[idx+1])}
	idx = idx + 2
end

local n = tonumber(ARGV[idx]); idx = idx + 1
for _ = 1, n do
	idx = idx + 1 -- existing fingerprint, unused
	local cnt = tonumber(ARGV[idx]); idx = idx + 1
	for _ = 1, cnt do
		local s = tonumber(ARGV[idx]); local e = tonumber(ARGV[idx+1]); idx = idx + 2
		for _, ni in ipairs(newInts) do
			if ni[1] < e and s < ni[2] then
				return -2
			end
		end
	end
end

redis.call("HSET", KEYS[2],
           "intervals", ARGV[2],
           "duration", ARGV[3],
           "created_at", ARGV[4],
           "updated_at", ARGV[4])
redis.call("ZADD", KEYS[1], ARGV[4], ARGV[1])
return 0
`)

// collectRebaseCandidatesCmd returns the not-yet-started tasks that are
// eligible for a rebase, excluding tasks already recorded in the progress set.
//
// KEYS[1] -> scheduled zset
// KEYS[2] -> retry zset
// KEYS[3] -> pending list
// KEYS[4] -> done set
//
// Output: a flat array of {state, taskID, score} triples. Score is "0" for
// pending tasks (which carry no per-task score) and the zset score otherwise.
var collectRebaseCandidatesCmd = redis.NewScript(`
local res = {}
local function addZset(key, state)
	local pairs = redis.call("ZRANGE", key, 0, -1, "WITHSCORES")
	for i = 1, #pairs, 2 do
		local id = pairs[i]
		if redis.call("SISMEMBER", KEYS[4], id) == 0 then
			res[#res+1] = state
			res[#res+1] = id
			res[#res+1] = pairs[i+1]
		end
	end
end
addZset(KEYS[1], "scheduled")
addZset(KEYS[2], "retry")
local pending = redis.call("LRANGE", KEYS[3], 0, -1)
for _, id in ipairs(pending) do
	if redis.call("SISMEMBER", KEYS[4], id) == 0 then
		res[#res+1] = "pending"
		res[#res+1] = id
		res[#res+1] = "0"
	end
end
return res
`)

// rebaseZSetTaskCmd atomically shifts a scheduled or retry task.
//
// KEYS[1] -> task hash key
// KEYS[2] -> delayed zset (scheduled or retry)
// KEYS[3] -> done set
//
// ARGV[1] -> task ID
// ARGV[2] -> re-encoded task message (deadline shifted)
// ARGV[3] -> expected current state ("scheduled" or "retry")
// ARGV[4] -> expected current zset score
// ARGV[5] -> new zset score
//
// Output:
//
//	 1 -> task shifted
//	 0 -> task already handled (idempotent no-op)
//	-1 -> task key missing
//	-2 -> state field conflicts with the expected state
//	-3 -> zset membership or score conflicts
var rebaseZSetTaskCmd = redis.NewScript(`
if redis.call("SISMEMBER", KEYS[3], ARGV[1]) == 1 then
	return 0
end
if redis.call("EXISTS", KEYS[1]) == 0 then
	return -1
end
local state = redis.call("HGET", KEYS[1], "state")
if state ~= ARGV[3] then
	return -2
end
local score = redis.call("ZSCORE", KEYS[2], ARGV[1])
if score == false or tonumber(score) ~= tonumber(ARGV[4]) then
	return -3
end
redis.call("HSET", KEYS[1], "msg", ARGV[2])
redis.call("ZADD", KEYS[2], ARGV[5], ARGV[1])
redis.call("SADD", KEYS[3], ARGV[1])
return 1
`)

// rebasePendingTaskCmd atomically shifts a pending task by moving it into the
// scheduled zset so that it is not dequeued until the shifted process time.
//
// KEYS[1] -> task hash key
// KEYS[2] -> pending list
// KEYS[3] -> scheduled zset
// KEYS[4] -> done set
//
// ARGV[1] -> task ID
// ARGV[2] -> re-encoded task message (deadline shifted)
// ARGV[3] -> new scheduled score (now + union duration)
//
// Output:
//
//	 1 -> task shifted
//	 0 -> task already handled (idempotent no-op)
//	-1 -> task key missing
//	-2 -> state field conflicts with "pending"
//	-3 -> task ID missing from the pending list
var rebasePendingTaskCmd = redis.NewScript(`
if redis.call("SISMEMBER", KEYS[4], ARGV[1]) == 1 then
	return 0
end
if redis.call("EXISTS", KEYS[1]) == 0 then
	return -1
end
local state = redis.call("HGET", KEYS[1], "state")
if state ~= "pending" then
	return -2
end
if redis.call("LREM", KEYS[2], 1, ARGV[1]) == 0 then
	return -3
end
redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "scheduled")
redis.call("HDEL", KEYS[1], "pending_since")
redis.call("ZADD", KEYS[3], ARGV[3], ARGV[1])
redis.call("SADD", KEYS[4], ARGV[1])
return 1
`)

// rebaseTask is a validated, ready-to-apply mutation for a single task.
type rebaseTask struct {
	id       string
	state    base.TaskState
	oldScore int64 // zset score for scheduled/retry; zero for pending
	newScore int64 // shifted score; pending tasks use now + shift
	encoded  []byte
}

// rebasePlan holds everything needed to apply a queue's rebase after the
// read-only validation phase has succeeded.
type rebasePlan struct {
	qname    string
	fp       string
	shiftSec int64
	tasks    []*rebaseTask
	skipped  int64
}

// RebaseTaskTiming merges the given outage windows into their union and, for
// each of the affected queues, shifts both the absolute deadline and the next
// process time of every not-yet-started task (pending, scheduled and retry
// states) by the union duration.
//
// Tasks in active, completed, archived or aggregating state and tasks in
// queues not listed in qnames are never modified. The Timeout, Retry and
// Retried fields of every task are preserved; only the Deadline field of the
// stored message is rewritten (and only when a deadline was set).
//
// The operation is idempotent: the same windows in any order resolve to the
// same fingerprint and re-running it (including after a process interruption)
// only processes the tasks that were not handled before. Conflicting windows
// or task records, or tasks that cannot be read, fail the whole operation
// before any task is mutated.
func (r *RDB) RebaseTaskTiming(ctx context.Context, qnames []string, windows []Window) ([]*RebaseReport, error) {
	var op errors.Op = "rdb.RebaseTaskTiming"
	if len(qnames) == 0 {
		return nil, errors.E(op, errors.FailedPrecondition, "at least one queue name is required")
	}
	merged, shift, fp, err := MergeWindows(windows)
	if err != nil {
		return nil, errors.E(op, errors.CanonicalCode(err), err)
	}
	shiftSec := int64(shift.Seconds())

	// Phase 1: validate every queue and build all plans before any task is
	// written, so that a conflict leaves all tasks in their original state.
	plans := make([]*rebasePlan, 0, len(qnames))
	for _, qname := range qnames {
		plan, err := r.planRebase(ctx, op, qname, fp, merged, shiftSec)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}

	// Phase 2: apply the validated plans. Per-task scripts are atomic and the
	// progress ("done") set makes each mutation idempotent, so an interrupted
	// invocation can simply be re-driven to finish the remaining tasks.
	reports := make([]*RebaseReport, 0, len(plans))
	for _, plan := range plans {
		report, err := r.applyRebase(ctx, op, plan)
		if err != nil {
			return nil, err
		}
		reports = append(reports, report)
	}
	return reports, nil
}

// planRebase collects eligible tasks and validates every task record
// read-only first, and only then registers/verifies the marker, so that a
// conflict or an unreadable record fails the run without any write.
func (r *RDB) planRebase(ctx context.Context, op errors.Op, qname, fp string,
	merged []TimeRange, shiftSec int64) (*rebasePlan, error) {
	exists, err := r.queueExists(qname)
	if err != nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sismember", Err: err})
	}
	if !exists {
		return nil, errors.E(op, errors.NotFound, &errors.QueueNotFoundError{Queue: qname})
	}

	indexKey := rebaseIndexKey(qname)
	markerKey := rebaseMarkerKey(qname, fp)
	doneKey := rebaseDoneKey(qname, fp)

	// Collect eligible task IDs from the not-started containers.
	res, err := collectRebaseCandidatesCmd.Run(ctx, r.client,
		[]string{
			base.ScheduledKey(qname),
			base.RetryKey(qname),
			base.PendingKey(qname),
			doneKey,
		}).Result()
	if err != nil {
		return nil, errors.E(op, errors.Unknown, fmt.Errorf("redis eval error: %w", err))
	}
	flat, err := toStringSlice(res)
	if err != nil {
		return nil, errors.E(op, errors.Internal, err)
	}
	if len(flat)%3 != 0 {
		return nil, errors.E(op, errors.Internal,
			fmt.Sprintf("collectRebaseCandidatesCmd returned malformed response: %v", res))
	}

	// Progress set size counts tasks an earlier run already handled.
	handled, err := r.client.SCard(ctx, doneKey).Result()
	if err != nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "scard", Err: err})
	}

	plan := &rebasePlan{qname: qname, fp: fp, shiftSec: shiftSec, skipped: handled}

	// Read all candidate records in one pipeline and validate them before any
	// mutation. A single unreadable or inconsistent task fails the whole run.
	taskKeys := make([]string, 0, len(flat)/3)
	type candidate struct {
		id       string
		state    base.TaskState
		oldScore int64
	}
	candidates := make([]*candidate, 0, len(flat)/3)
	for i := 0; i < len(flat); i += 3 {
		stateStr, id, scoreStr := flat[i], flat[i+1], flat[i+2]
		state, err := base.TaskStateFromString(stateStr)
		if err != nil {
			return nil, errors.E(op, errors.Internal, err)
		}
		score, err := parseInt64(scoreStr)
		if err != nil {
			return nil, errors.E(op, errors.Internal,
				fmt.Sprintf("cannot parse score %q for task %q: %v", scoreStr, id, err))
		}
		candidates = append(candidates, &candidate{id: id, state: state, oldScore: score})
		taskKeys = append(taskKeys, base.TaskKey(qname, id))
	}

	pipe := r.client.Pipeline()
	hmgets := make([]*redis.SliceCmd, len(taskKeys))
	for i, key := range taskKeys {
		hmgets[i] = pipe.HMGet(ctx, key, "msg", "state")
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, errors.E(op, errors.Unknown, fmt.Errorf("redis pipeline error: %w", err))
	}

	now := r.clock.Now().Unix()
	for i, c := range candidates {
		values, err := hmgets[i].Result()
		if err != nil {
			return nil, errors.E(op, errors.Unknown, err)
		}
		if len(values) != 2 || values[0] == nil || values[1] == nil {
			return nil, errors.E(op, errors.FailedPrecondition,
				fmt.Sprintf("task %q record is missing msg or state field", c.id))
		}
		encoded, ok := values[0].(string)
		if !ok || encoded == "" {
			return nil, errors.E(op, errors.FailedPrecondition,
				fmt.Sprintf("task %q message cannot be read", c.id))
		}
		storedState, ok := values[1].(string)
		if !ok {
			return nil, errors.E(op, errors.FailedPrecondition,
				fmt.Sprintf("task %q state field has unexpected type %T", c.id, values[1]))
		}
		if storedState != c.state.String() {
			return nil, errors.E(op, errors.FailedPrecondition,
				fmt.Sprintf("task %q state conflict: record is %q but container expects %q",
					c.id, storedState, c.state.String()))
		}
		msg, err := base.DecodeMessage([]byte(encoded))
		if err != nil {
			return nil, errors.E(op, errors.Internal,
				fmt.Sprintf("task %q message cannot be decoded: %v", c.id, err))
		}
		if msg.ID != c.id || msg.Queue != qname {
			return nil, errors.E(op, errors.FailedPrecondition,
				fmt.Sprintf("task %q record identity conflict: id=%q queue=%q", c.id, msg.ID, msg.Queue))
		}
		// Only the absolute deadline is rewritten; Timeout, Retry, Retried and
		// every other field stay byte-identical in semantics. A zero deadline
		// (none configured) is preserved as zero.
		if msg.Deadline != 0 {
			msg.Deadline += shiftSec
		}
		newEncoded, err := base.EncodeMessage(msg)
		if err != nil {
			return nil, errors.E(op, errors.Internal,
				fmt.Sprintf("cannot re-encode task %q: %v", c.id, err))
		}
		task := &rebaseTask{
			id:       c.id,
			state:    c.state,
			oldScore: c.oldScore,
			encoded:  newEncoded,
		}
		switch c.state {
		case base.TaskStateScheduled, base.TaskStateRetry:
			task.newScore = c.oldScore + shiftSec
		case base.TaskStatePending:
			// A pending task has no stored process time; schedule it now plus
			// the outage duration so the recovered fleet is not flooded.
			task.newScore = now + shiftSec
		default:
			return nil, errors.E(op, errors.Internal,
				fmt.Sprintf("unexpected candidate state %q for task %q", c.state.String(), c.id))
		}
		plan.tasks = append(plan.tasks, task)
	}

	// All task records have been validated without a single write. Register or
	// verify the marker now; overlap with an existing marker is rejected inside
	// the script so no double-shift can occur.
	existing, err := r.client.ZRange(ctx, indexKey, 0, -1).Result()
	if err != nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zrange", Err: err})
	}
	argv := []interface{}{
		fp,
		canonicalIntervals(merged),
		shiftSec,
		r.clock.Now().Unix(),
		len(merged),
	}
	for _, rg := range merged {
		s, e := rg.unixPair()
		argv = append(argv, s, e)
	}
	argv = append(argv, len(existing))
	for _, existingFP := range existing {
		key := rebaseMarkerKey(qname, existingFP)
		fields, err := r.client.HGetAll(ctx, key).Result()
		if err != nil {
			return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hgetall", Err: err})
		}
		ranges, err := parseCanonicalIntervals(fields["intervals"])
		if err != nil {
			return nil, errors.E(op, errors.Internal,
				fmt.Sprintf("corrupt rebase marker %q: %v", key, err))
		}
		argv = append(argv, existingFP, len(ranges))
		for _, rg := range ranges {
			argv = append(argv, rg.Start.Unix(), rg.End.Unix())
		}
	}
	code, err := r.runScriptWithErrorCode(ctx, op, ensureRebaseMarkerCmd,
		[]string{indexKey, markerKey}, argv...)
	if err != nil {
		return nil, err
	}
	switch code {
	case 0, 1:
		// marker created or an identical one already exists (resume)
	case -1:
		return nil, errors.E(op, errors.FailedPrecondition,
			fmt.Sprintf("rebase marker %q already exists with conflicting parameters", fp))
	case -2:
		return nil, errors.E(op, errors.FailedPrecondition,
			"another rebase covering an overlapping outage window is already registered")
	default:
		return nil, errors.E(op, errors.Internal,
			fmt.Sprintf("unexpected return value from ensureRebaseMarkerCmd: %d", code))
	}
	return plan, nil
}

// applyRebase executes the validated plan in atomic per-task batches, updating
// the marker's updated_at timestamp once finished.
func (r *RDB) applyRebase(ctx context.Context, op errors.Op, plan *rebasePlan) (*RebaseReport, error) {
	report := &RebaseReport{Queue: plan.qname, Skipped: plan.skipped}
	if len(plan.tasks) == 0 {
		return report, nil
	}

	// Preload scripts: script.Run over a pipeline only issues EVALSHA and does
	// not fall back to EVAL on NOSCRIPT.
	if err := rebaseZSetTaskCmd.Load(ctx, r.client).Err(); err != nil {
		return nil, errors.E(op, errors.Unknown, fmt.Errorf("failed to load rebase script: %w", err))
	}
	if err := rebasePendingTaskCmd.Load(ctx, r.client).Err(); err != nil {
		return nil, errors.E(op, errors.Unknown, fmt.Errorf("failed to load rebase script: %w", err))
	}

	doneKey := rebaseDoneKey(plan.qname, plan.fp)
	scheduledKey := base.ScheduledKey(plan.qname)
	for start := 0; start < len(plan.tasks); start += rebaseApplyBatchSize {
		end := start + rebaseApplyBatchSize
		if end > len(plan.tasks) {
			end = len(plan.tasks)
		}
		pipe := r.client.Pipeline()
		cmds := make([]*redis.Cmd, 0, end-start)
		for _, task := range plan.tasks[start:end] {
			taskKey := base.TaskKey(plan.qname, task.id)
			switch task.state {
			case base.TaskStateScheduled, base.TaskStateRetry:
				delayedKey := scheduledKey
				if task.state == base.TaskStateRetry {
					delayedKey = base.RetryKey(plan.qname)
				}
				cmd := rebaseZSetTaskCmd.Run(ctx, pipe,
					[]string{taskKey, delayedKey, doneKey},
					task.id, task.encoded, task.state.String(), task.oldScore, task.newScore)
				cmds = append(cmds, cmd)
			case base.TaskStatePending:
				cmd := rebasePendingTaskCmd.Run(ctx, pipe,
					[]string{taskKey, base.PendingKey(plan.qname), scheduledKey, doneKey},
					task.id, task.encoded, task.newScore)
				cmds = append(cmds, cmd)
			default:
				return nil, errors.E(op, errors.Internal,
					fmt.Sprintf("unexpected task state %q for task %q", task.state.String(), task.id))
			}
		}
		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			return nil, errors.E(op, errors.Unknown, fmt.Errorf("redis pipeline error: %w", err))
		}
		for i, cmd := range cmds {
			n, err := cmd.Int64()
			if err != nil {
				return nil, errors.E(op, errors.Unknown, fmt.Errorf("redis eval error: %w", err))
			}
			task := plan.tasks[start+i]
			switch n {
			case 1:
				report.Shifted++
			case 0:
				// Handled by a concurrent/re-driven invocation.
				report.Skipped++
			case -1:
				return nil, errors.E(op, errors.FailedPrecondition,
					fmt.Sprintf("task %q record disappeared during rebase", task.id))
			case -2:
				return nil, errors.E(op, errors.FailedPrecondition,
					fmt.Sprintf("task %q state changed during rebase", task.id))
			case -3:
				return nil, errors.E(op, errors.FailedPrecondition,
					fmt.Sprintf("task %q container membership or score conflict during rebase", task.id))
			default:
				return nil, errors.E(op, errors.Internal,
					fmt.Sprintf("unexpected return value from rebase script for task %q: %d", task.id, n))
			}
		}
	}

	if err := r.client.HSet(ctx, rebaseMarkerKey(plan.qname, plan.fp),
		"updated_at", r.clock.Now().Unix()).Err(); err != nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hset", Err: err})
	}
	return report, nil
}

func canonicalIntervals(ranges []TimeRange) string {
	parts := make([]string, len(ranges))
	for i, rg := range ranges {
		s, e := rg.unixPair()
		parts[i] = fmt.Sprintf("%d-%d", s, e)
	}
	return strings.Join(parts, ",")
}

func parseCanonicalIntervals(canonical string) ([]TimeRange, error) {
	if canonical == "" {
		return nil, fmt.Errorf("empty intervals")
	}
	parts := strings.Split(canonical, ",")
	ranges := make([]TimeRange, 0, len(parts))
	for _, part := range parts {
		var s, e int64
		if _, err := fmt.Sscanf(part, "%d-%d", &s, &e); err != nil {
			return nil, fmt.Errorf("invalid interval %q: %w", part, err)
		}
		ranges = append(ranges, TimeRange{Start: time.Unix(s, 0), End: time.Unix(e, 0)})
	}
	return ranges, nil
}

func toStringSlice(v interface{}) ([]string, error) {
	switch vals := v.(type) {
	case []interface{}:
		out := make([]string, len(vals))
		for i, val := range vals {
			s, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("unexpected value type %T in script response", val)
			}
			out[i] = s
		}
		return out, nil
	case []string:
		return vals, nil
	default:
		return nil, fmt.Errorf("unexpected script response type %T", v)
	}
}

func parseInt64(s string) (int64, error) {
	var n int64
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}
