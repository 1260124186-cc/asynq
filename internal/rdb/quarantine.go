// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hibiken/asynq/internal/base"
	asynqerrors "github.com/hibiken/asynq/internal/errors"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

const (
	// quarantineEventTTL bounds the growth of per-event idempotency markers. A marker
	// only needs to live long enough to de-duplicate repeated reports of the same event.
	quarantineEventTTL = 60 * time.Minute

	// quarantineAliveTTL is the TTL of a server instance's liveness marker. It must be
	// longer than the heartbeat interval so that a live instance keeps its marker fresh.
	quarantineAliveTTL = 30 * time.Second

	// quarantineAuditTTL bounds the growth of each node's audit log.
	quarantineAuditTTL = 90 * 24 * time.Hour

	// quarantineScanCount is the COUNT hint used when scanning quarantine keys.
	quarantineScanCount = 100
)

// Error tokens returned by the quarantine Lua scripts. They are mapped to the
// package level sentinel errors by mapQuarantineScriptErr.
const (
	quarantineErrConflict = "QCONFLICT"
	quarantineErrCorrupt  = "QCORRUPT"
	quarantineErrNotFound = "QNOTFOUND"
)

// reportNodeFailureCmd records a distinct failure event for a node and advances
// its quarantine state.
//
// Input:
//
//	KEYS[1] -> asynq:quarantine:{<nodeID>}            (state hash)
//	KEYS[2] -> asynq:quarantine:{<nodeID>}:evt:<event> (idempotency marker)
//	KEYS[3] -> asynq:quarantine:{<nodeID>}:audit       (audit list)
//	ARGV[1] -> node ID
//	ARGV[2] -> caller server ID
//	ARGV[3] -> event key
//	ARGV[4] -> current unix time (seconds)
//	ARGV[5] -> event marker TTL (seconds)
//	ARGV[6] -> failure threshold
//	ARGV[7] -> liveness key prefix (ends with ":alive:")
//	ARGV[8] -> audit TTL (seconds)
//
// Output:
//
//	{mutated, status} where mutated is 0 when the event was de-duplicated (no
//	state change) and 1 when the event was counted. On a conflict or corrupt state
//	the script returns an error reply without writing anything.
var reportNodeFailureCmd = redis.NewScript(`
local state_key = KEYS[1]
local event_key = KEYS[2]
local now = tonumber(ARGV[4])
local threshold = tonumber(ARGV[6])

-- Idempotency: the same failure event must never be counted twice or extend isolation.
if redis.call("EXISTS", event_key) == 1 then
	local st = "healthy"
	if redis.call("EXISTS", state_key) == 1 then
		local s = redis.call("HGET", state_key, "status")
		if s ~= false then st = s end
	end
	return {0, st}
end

local exists = redis.call("EXISTS", state_key)
local owner = ""
local status = "healthy"
if exists == 1 then
	owner = redis.call("HGET", state_key, "server_id")
	status = redis.call("HGET", state_key, "status")
	local ver = redis.call("HGET", state_key, "version")
	if owner == false or status == false or ver == false then
		return redis.error_reply("QCORRUPT")
	end
	-- Identity conflict: a different, still-live server instance owns this node.
	if owner ~= "" and owner ~= ARGV[2] then
		if redis.call("EXISTS", ARGV[7] .. owner) == 1 then
			return redis.error_reply("QCONFLICT")
		end
	end
end

-- Claim the event marker before any state mutation so concurrent duplicate reports lose.
redis.call("SET", event_key, ARGV[3], "EX", ARGV[5])

if exists == 0 then
	redis.call("HSET", state_key,
		"node_id", ARGV[1],
		"server_id", ARGV[2],
		"status", "healthy",
		"fail_count", 1,
		"healthy_streak", 0,
		"first_fail_at", now,
		"last_fail_at", now,
		"last_event_key", ARGV[3],
		"quarantined_at", 0,
		"released_at", 0,
		"cleared_by", "",
		"clear_reason", "",
		"updated_at", now,
		"version", 1)
	if threshold <= 1 then
		redis.call("HSET", state_key, "status", "quarantined", "quarantined_at", now)
		local entry = cjson.encode({at = now, node_id = ARGV[1], action = "quarantined",
			operator = "system", reason = "failure threshold reached", server_id = ARGV[2], status = "quarantined"})
		redis.call("RPUSH", KEYS[3], entry)
		redis.call("EXPIRE", KEYS[3], ARGV[8])
		return {1, "quarantined"}
	end
	return {1, "healthy"}
end

-- Existing record; this is a distinct failure event. Take over ownership if the
-- previous owner is no longer alive.
if owner ~= "" and owner ~= ARGV[2] then
	redis.call("HSET", state_key, "server_id", ARGV[2])
end

local fc
if status == "released" then
	-- Re-arm: a failure after a release starts a brand new observing window seeded
	-- with this single event. The previous window's failure count is discarded so
	-- the full threshold must be reached again.
	fc = 1
	redis.call("HSET", state_key,
		"status", "healthy",
		"fail_count", 1,
		"healthy_streak", 0,
		"first_fail_at", now,
		"last_fail_at", now,
		"last_event_key", ARGV[3],
		"quarantined_at", 0,
		"released_at", 0,
		"cleared_by", "",
		"clear_reason", "",
		"updated_at", now)
	status = "healthy"
else
	local prev = tonumber(redis.call("HGET", state_key, "fail_count"))
	if prev == nil then return redis.error_reply("QCORRUPT") end
	fc = prev + 1
	redis.call("HSET", state_key,
		"fail_count", fc,
		"last_fail_at", now,
		"last_event_key", ARGV[3],
		"healthy_streak", 0,
		"updated_at", now)
end

local ver = tonumber(redis.call("HGET", state_key, "version"))
if ver == nil then return redis.error_reply("QCORRUPT") end
redis.call("HSET", state_key, "version", ver + 1)

if fc >= threshold and status ~= "quarantined" then
	redis.call("HSET", state_key, "status", "quarantined", "quarantined_at", now)
	local entry = cjson.encode({at = now, node_id = ARGV[1], action = "quarantined",
		operator = "system", reason = "failure threshold reached", server_id = ARGV[2], status = "quarantined"})
	redis.call("RPUSH", KEYS[3], entry)
	redis.call("EXPIRE", KEYS[3], ARGV[8])
	return {1, "quarantined"}
end
return {1, status}
`)

// recordNodeHeartbeatCmd records a successful heartbeat, refreshes the caller's
// liveness marker and drives the automatic release of a quarantined node once the
// required number of consecutive healthy heartbeats is reached.
//
// Input:
//
//	KEYS[1] -> state hash
//	KEYS[2] -> asynq:quarantine:{<nodeID>}:alive:<serverID>
//	KEYS[3] -> audit list
//	ARGV[1] -> node ID
//	ARGV[2] -> caller server ID
//	ARGV[3] -> current unix time (seconds)
//	ARGV[4] -> liveness TTL (seconds)
//	ARGV[5] -> recovery threshold (consecutive healthy heartbeats)
//	ARGV[6] -> liveness key prefix
//	ARGV[7] -> audit TTL (seconds)
//
// Output: the resulting node status, or an error reply on conflict/corruption.
var recordNodeHeartbeatCmd = redis.NewScript(`
-- Refresh the caller liveness marker regardless of record presence.
redis.call("SET", KEYS[2], ARGV[2], "EX", ARGV[4])

if redis.call("EXISTS", KEYS[1]) == 0 then
	return "healthy"
end

local owner = redis.call("HGET", KEYS[1], "server_id")
local status = redis.call("HGET", KEYS[1], "status")
local ver = redis.call("HGET", KEYS[1], "version")
if owner == false or status == false or ver == false then
	return redis.error_reply("QCORRUPT")
end
if owner ~= "" and owner ~= ARGV[2] then
	if redis.call("EXISTS", ARGV[6] .. owner) == 1 then
		return redis.error_reply("QCONFLICT")
	end
	-- Previous owner is gone (process restarted); hand ownership over.
	redis.call("HSET", KEYS[1], "server_id", ARGV[2])
end

local now = tonumber(ARGV[3])
if status ~= "quarantined" then
	return status
end

local streak = tonumber(redis.call("HGET", KEYS[1], "healthy_streak"))
if streak == nil then return redis.error_reply("QCORRUPT") end
streak = streak + 1
redis.call("HSET", KEYS[1], "healthy_streak", streak, "updated_at", now)

local v = tonumber(redis.call("HGET", KEYS[1], "version"))
if v == nil then return redis.error_reply("QCORRUPT") end
redis.call("HSET", KEYS[1], "version", v + 1)

if streak >= tonumber(ARGV[5]) then
	redis.call("HSET", KEYS[1], "status", "released", "released_at", now)
	local entry = cjson.encode({at = now, node_id = ARGV[1], action = "auto_released",
		operator = "system", reason = "stable heartbeats recovered", server_id = ARGV[2], status = "released"})
	redis.call("RPUSH", KEYS[3], entry)
	redis.call("EXPIRE", KEYS[3], ARGV[7])
	return "released"
end
return "quarantined"
`)

// clearNodeQuarantineCmd manually lifts a quarantine and writes an immutable audit
// entry. It refuses to mutate anything when the record is missing or corrupt.
//
// Input:
//
//	KEYS[1] -> state hash
//	KEYS[2] -> audit list
//	ARGV[1] -> node ID
//	ARGV[2] -> operator
//	ARGV[3] -> reason
//	ARGV[4] -> current unix time (seconds)
//	ARGV[5] -> audit TTL (seconds)
var clearNodeQuarantineCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then
	return redis.error_reply("QNOTFOUND")
end
local status = redis.call("HGET", KEYS[1], "status")
local ver = redis.call("HGET", KEYS[1], "version")
if status == false or ver == false then
	return redis.error_reply("QCORRUPT")
end

local now = tonumber(ARGV[4])
local entry = cjson.encode({at = now, node_id = ARGV[1], action = "manual_clear",
	operator = ARGV[2], reason = ARGV[3], server_id = "", status = status})
-- Append the audit record before mutating the state.
redis.call("RPUSH", KEYS[2], entry)
redis.call("EXPIRE", KEYS[2], ARGV[5])

if status == "quarantined" then
	redis.call("HSET", KEYS[1], "status", "released", "released_at", now,
		"healthy_streak", 0, "cleared_by", ARGV[2], "clear_reason", ARGV[3],
		"updated_at", now)
else
	redis.call("HSET", KEYS[1], "cleared_by", ARGV[2], "clear_reason", ARGV[3],
		"updated_at", now)
end
local v = tonumber(ver)
redis.call("HSET", KEYS[1], "version", v + 1)
return redis.status_reply("OK")
`)

// reconcileNodeQuarantineCmd is the read-only, identity-checked reconciliation.
//
// Input:
//
//	KEYS[1] -> state hash
//	ARGV[1] -> node ID
//	ARGV[2] -> caller server ID
//	ARGV[3] -> liveness key prefix
//
// Output: the authoritative status, "healthy" when no record exists, or an error
// reply on conflict/corruption. The script never writes.
var reconcileNodeQuarantineCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 0 then
	return "healthy"
end
local owner = redis.call("HGET", KEYS[1], "server_id")
local status = redis.call("HGET", KEYS[1], "status")
local ver = redis.call("HGET", KEYS[1], "version")
if owner == false or status == false or ver == false then
	return redis.error_reply("QCORRUPT")
end
if owner ~= "" and owner ~= ARGV[2] then
	if redis.call("EXISTS", ARGV[3] .. owner) == 1 then
		return redis.error_reply("QCONFLICT")
	end
end
return status
`)

// ReportNodeFailure records a distinct failure event for the node and returns the
// resulting status and whether the event was de-duplicated.
func (r *RDB) ReportNodeFailure(ctx context.Context, nodeID, serverID, eventKey string, failureThreshold int) (status string, duplicated bool, err error) {
	var op asynqerrors.Op = "rdb.ReportNodeFailure"
	keys := []string{
		base.NodeQuarantineKey(nodeID),
		base.NodeQuarantineEventKey(nodeID, eventKey),
		base.NodeQuarantineAuditKey(nodeID),
	}
	args := []interface{}{
		nodeID,
		serverID,
		eventKey,
		r.clock.Now().Unix(),
		int64(quarantineEventTTL.Seconds()),
		failureThreshold,
		base.NodeQuarantineKey(nodeID) + ":alive:",
		int64(quarantineAuditTTL.Seconds()),
	}
	res, err := reportNodeFailureCmd.Run(ctx, r.client, keys, args...).Result()
	if err != nil {
		return "", false, mapQuarantineScriptErr(op, err)
	}
	parts, err := cast.ToStringSliceE(res)
	if err != nil || len(parts) != 2 {
		return "", false, asynqerrors.E(op, asynqerrors.Internal, fmt.Sprintf("unexpected return value from Lua script: %v", res))
	}
	mutated, err := strconv.Atoi(parts[0])
	if err != nil {
		return "", false, asynqerrors.E(op, asynqerrors.Internal, fmt.Sprintf("cannot parse mutation flag: %v", res))
	}
	return parts[1], mutated == 0, nil
}

// RecordNodeHeartbeat records a successful heartbeat and returns the resulting status.
func (r *RDB) RecordNodeHeartbeat(ctx context.Context, nodeID, serverID string, recoveryThreshold int) (status string, err error) {
	var op asynqerrors.Op = "rdb.RecordNodeHeartbeat"
	keys := []string{
		base.NodeQuarantineKey(nodeID),
		base.NodeQuarantineAliveKey(nodeID, serverID),
		base.NodeQuarantineAuditKey(nodeID),
	}
	args := []interface{}{
		nodeID,
		serverID,
		r.clock.Now().Unix(),
		int64(quarantineAliveTTL.Seconds()),
		recoveryThreshold,
		base.NodeQuarantineKey(nodeID) + ":alive:",
		int64(quarantineAuditTTL.Seconds()),
	}
	res, err := recordNodeHeartbeatCmd.Run(ctx, r.client, keys, args...).Result()
	if err != nil {
		return "", mapQuarantineScriptErr(op, err)
	}
	s, err := cast.ToStringE(res)
	if err != nil {
		return "", asynqerrors.E(op, asynqerrors.Internal, fmt.Sprintf("unexpected return value from Lua script: %v", res))
	}
	return s, nil
}

// ClearNodeQuarantine manually lifts the quarantine and records the action in the
// node's audit log.
func (r *RDB) ClearNodeQuarantine(ctx context.Context, nodeID, operator, reason string) error {
	var op asynqerrors.Op = "rdb.ClearNodeQuarantine"
	keys := []string{
		base.NodeQuarantineKey(nodeID),
		base.NodeQuarantineAuditKey(nodeID),
	}
	args := []interface{}{
		nodeID,
		operator,
		reason,
		r.clock.Now().Unix(),
		int64(quarantineAuditTTL.Seconds()),
	}
	if err := clearNodeQuarantineCmd.Run(ctx, r.client, keys, args...).Err(); err != nil {
		return mapQuarantineScriptErr(op, err)
	}
	return nil
}

// GetNodeQuarantine returns the persisted quarantine record for the node.
func (r *RDB) GetNodeQuarantine(nodeID string) (*base.NodeQuarantine, error) {
	var op asynqerrors.Op = "rdb.GetNodeQuarantine"
	m, err := r.client.HGetAll(context.Background(), base.NodeQuarantineKey(nodeID)).Result()
	if err != nil {
		return nil, asynqerrors.E(op, asynqerrors.Unknown, &asynqerrors.RedisCommandError{Command: "hgetall", Err: err})
	}
	if len(m) == 0 {
		return nil, asynqerrors.E(op, asynqerrors.NotFound, asynqerrors.ErrNodeQuarantineNotFound)
	}
	rec, err := parseNodeQuarantine(m)
	if err != nil {
		return nil, asynqerrors.E(op, asynqerrors.Internal, err)
	}
	return rec, nil
}

// ReconcileNodeQuarantine returns the authoritative node status for the calling
// server instance, rejecting the reconciliation on identity conflict or corruption
// without performing any write.
func (r *RDB) ReconcileNodeQuarantine(nodeID, serverID string) (status string, err error) {
	var op asynqerrors.Op = "rdb.ReconcileNodeQuarantine"
	keys := []string{base.NodeQuarantineKey(nodeID)}
	args := []interface{}{
		nodeID,
		serverID,
		base.NodeQuarantineKey(nodeID) + ":alive:",
	}
	res, err := reconcileNodeQuarantineCmd.Run(context.Background(), r.client, keys, args...).Result()
	if err != nil {
		return "", mapQuarantineScriptErr(op, err)
	}
	s, err := cast.ToStringE(res)
	if err != nil {
		return "", asynqerrors.E(op, asynqerrors.Internal, fmt.Sprintf("unexpected return value from Lua script: %v", res))
	}
	return s, nil
}

// ListNodeQuarantines returns every persisted quarantine record. State hashes are
// distinguished from event, liveness and audit keys by their redis type.
func (r *RDB) ListNodeQuarantines() ([]*base.NodeQuarantine, error) {
	var op asynqerrors.Op = "rdb.ListNodeQuarantines"
	ctx := context.Background()
	seen := make(map[string]struct{})
	var out []*base.NodeQuarantine

	collect := func(c redis.UniversalClient) error {
		var cursor uint64
		for {
			keys, next, err := c.Scan(ctx, cursor, base.NodeQuarantineScanMatch(), int64(quarantineScanCount)).Result()
			if err != nil {
				return asynqerrors.E(op, asynqerrors.Unknown, &asynqerrors.RedisCommandError{Command: "scan", Err: err})
			}
			for _, key := range keys {
				t, err := c.Type(ctx, key).Result()
				if err != nil {
					return asynqerrors.E(op, asynqerrors.Unknown, &asynqerrors.RedisCommandError{Command: "type", Err: err})
				}
				if t != "hash" {
					continue // skip event markers, liveness markers and audit lists
				}
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				m, err := c.HGetAll(ctx, key).Result()
				if err != nil {
					return asynqerrors.E(op, asynqerrors.Unknown, &asynqerrors.RedisCommandError{Command: "hgetall", Err: err})
				}
				if len(m) == 0 {
					continue
				}
				rec, err := parseNodeQuarantine(m)
				if err != nil {
					return asynqerrors.E(op, asynqerrors.Internal, err)
				}
				out = append(out, rec)
			}
			cursor = next
			if cursor == 0 {
				return nil
			}
		}
	}

	switch client := r.client.(type) {
	case *redis.ClusterClient:
		err := client.ForEachMaster(ctx, func(ctx context.Context, c *redis.Client) error {
			return collect(c)
		})
		if err != nil {
			return nil, err
		}
	default:
		if err := collect(r.client); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// quarantineAuditJSON is the on-the-wire shape of an audit entry. Time is stored
// as a unix timestamp so it can be produced directly from Lua.
type quarantineAuditJSON struct {
	At       int64  `json:"at"`
	NodeID   string `json:"node_id"`
	Action   string `json:"action"`
	Operator string `json:"operator"`
	Reason   string `json:"reason"`
	ServerID string `json:"server_id"`
	Status   string `json:"status"`
}

// ListNodeQuarantineAudit returns the audit entries for the node, oldest first.
func (r *RDB) ListNodeQuarantineAudit(nodeID string) ([]*base.QuarantineAuditEntry, error) {
	var op asynqerrors.Op = "rdb.ListNodeQuarantineAudit"
	vals, err := r.client.LRange(context.Background(), base.NodeQuarantineAuditKey(nodeID), 0, -1).Result()
	if err != nil {
		return nil, asynqerrors.E(op, asynqerrors.Unknown, &asynqerrors.RedisCommandError{Command: "lrange", Err: err})
	}
	out := make([]*base.QuarantineAuditEntry, 0, len(vals))
	for _, v := range vals {
		var raw quarantineAuditJSON
		if err := json.Unmarshal([]byte(v), &raw); err != nil {
			return nil, asynqerrors.E(op, asynqerrors.Internal, asynqerrors.ErrQuarantineStateCorrupt)
		}
		out = append(out, &base.QuarantineAuditEntry{
			At:       time.Unix(raw.At, 0),
			NodeID:   raw.NodeID,
			Action:   raw.Action,
			Operator: raw.Operator,
			Reason:   raw.Reason,
			ServerID: raw.ServerID,
			Status:   raw.Status,
		})
	}
	return out, nil
}

// mapQuarantineScriptErr translates the error replies emitted by the quarantine
// Lua scripts into the package level sentinel errors. Any other failure is treated
// as an infrastructure error.
func mapQuarantineScriptErr(op asynqerrors.Op, err error) error {
	var rerr redis.Error
	if stderrors.As(err, &rerr) {
		switch {
		case strings.Contains(rerr.Error(), quarantineErrConflict):
			return asynqerrors.E(op, asynqerrors.FailedPrecondition, asynqerrors.ErrNodeIdentityConflict)
		case strings.Contains(rerr.Error(), quarantineErrCorrupt):
			return asynqerrors.E(op, asynqerrors.Internal, asynqerrors.ErrQuarantineStateCorrupt)
		case strings.Contains(rerr.Error(), quarantineErrNotFound):
			return asynqerrors.E(op, asynqerrors.NotFound, asynqerrors.ErrNodeQuarantineNotFound)
		}
	}
	return asynqerrors.E(op, asynqerrors.Unknown, fmt.Errorf("redis eval error: %w", err))
}

// parseNodeQuarantine validates and converts a redis hash into a NodeQuarantine
// record. Any missing mandatory field or unparsable numeric value is reported as
// state corruption so that the caller fails the reconciliation.
func parseNodeQuarantine(m map[string]string) (*base.NodeQuarantine, error) {
	mustString := func(field string) (string, error) {
		v, ok := m[field]
		if !ok {
			return "", asynqerrors.ErrQuarantineStateCorrupt
		}
		return v, nil
	}
	mustInt64 := func(field string) (int64, error) {
		v, err := mustString(field)
		if err != nil {
			return 0, err
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, asynqerrors.ErrQuarantineStateCorrupt
		}
		return n, nil
	}
	optTime := func(field string) (time.Time, error) {
		n, err := mustInt64(field)
		if err != nil {
			return time.Time{}, err
		}
		if n == 0 {
			return time.Time{}, nil
		}
		return time.Unix(n, 0), nil
	}

	nodeID, err := mustString("node_id")
	if err != nil {
		return nil, err
	}
	serverID, err := mustString("server_id")
	if err != nil {
		return nil, err
	}
	status, err := mustString("status")
	if err != nil {
		return nil, err
	}
	failCount, err := mustInt64("fail_count")
	if err != nil {
		return nil, err
	}
	healthyStreak, err := mustInt64("healthy_streak")
	if err != nil {
		return nil, err
	}
	firstFailAt, err := optTime("first_fail_at")
	if err != nil {
		return nil, err
	}
	lastFailAt, err := optTime("last_fail_at")
	if err != nil {
		return nil, err
	}
	quarantinedAt, err := optTime("quarantined_at")
	if err != nil {
		return nil, err
	}
	releasedAt, err := optTime("released_at")
	if err != nil {
		return nil, err
	}
	updatedAt, err := optTime("updated_at")
	if err != nil {
		return nil, err
	}
	version, err := mustInt64("version")
	if err != nil {
		return nil, err
	}
	return &base.NodeQuarantine{
		NodeID:        nodeID,
		ServerID:      serverID,
		Status:        status,
		FailCount:     int(failCount),
		HealthyStreak: int(healthyStreak),
		FirstFailAt:   firstFailAt,
		LastFailAt:    lastFailAt,
		QuarantinedAt: quarantinedAt,
		ReleasedAt:    releasedAt,
		LastEventKey:  m["last_event_key"],
		ClearedBy:     m["cleared_by"],
		ClearReason:   m["clear_reason"],
		UpdatedAt:     updatedAt,
		Version:       version,
	}, nil
}
