// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package base defines foundational types and constants used in asynq package.
package base

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/errors"
	pb "github.com/hibiken/asynq/internal/proto"
	"github.com/hibiken/asynq/internal/timeutil"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Version of asynq library and CLI.
const Version = "0.26.0"

// DefaultQueueName is the queue name used if none are specified by user.
const DefaultQueueName = "default"

// DefaultQueue is the redis key for the default queue.
var DefaultQueue = PendingKey(DefaultQueueName)

// Global Redis keys.
const (
	AllServers    = "asynq:servers"    // ZSET
	AllWorkers    = "asynq:workers"    // ZSET
	AllSchedulers = "asynq:schedulers" // ZSET
	AllQueues     = "asynq:queues"     // SET
	CancelChannel = "asynq:cancel"     // PubSub channel
)

// TaskState denotes the state of a task.
type TaskState int

const (
	TaskStateActive TaskState = iota + 1
	TaskStatePending
	TaskStateScheduled
	TaskStateRetry
	TaskStateArchived
	TaskStateCompleted
	TaskStateAggregating // describes a state where task is waiting in a group to be aggregated
)

func (s TaskState) String() string {
	switch s {
	case TaskStateActive:
		return "active"
	case TaskStatePending:
		return "pending"
	case TaskStateScheduled:
		return "scheduled"
	case TaskStateRetry:
		return "retry"
	case TaskStateArchived:
		return "archived"
	case TaskStateCompleted:
		return "completed"
	case TaskStateAggregating:
		return "aggregating"
	}
	panic(fmt.Sprintf("internal error: unknown task state %d", s))
}

func TaskStateFromString(s string) (TaskState, error) {
	switch s {
	case "active":
		return TaskStateActive, nil
	case "pending":
		return TaskStatePending, nil
	case "scheduled":
		return TaskStateScheduled, nil
	case "retry":
		return TaskStateRetry, nil
	case "archived":
		return TaskStateArchived, nil
	case "completed":
		return TaskStateCompleted, nil
	case "aggregating":
		return TaskStateAggregating, nil
	}
	return 0, errors.E(errors.FailedPrecondition, fmt.Sprintf("%q is not supported task state", s))
}

// ValidateQueueName validates a given qname to be used as a queue name.
// Returns nil if valid, otherwise returns non-nil error.
func ValidateQueueName(qname string) error {
	if len(strings.TrimSpace(qname)) == 0 {
		return fmt.Errorf("queue name must contain one or more characters")
	}
	return nil
}

// QueueKeyPrefix returns a prefix for all keys in the given queue.
func QueueKeyPrefix(qname string) string {
	return "asynq:{" + qname + "}:"
}

// TaskKeyPrefix returns a prefix for task key.
func TaskKeyPrefix(qname string) string {
	return QueueKeyPrefix(qname) + "t:"
}

// TaskKey returns a redis key for the given task message.
func TaskKey(qname, id string) string {
	return TaskKeyPrefix(qname) + id
}

// PendingKey returns a redis key for the given queue name.
func PendingKey(qname string) string {
	return QueueKeyPrefix(qname) + "pending"
}

// ActiveKey returns a redis key for the active tasks.
func ActiveKey(qname string) string {
	return QueueKeyPrefix(qname) + "active"
}

// ScheduledKey returns a redis key for the scheduled tasks.
func ScheduledKey(qname string) string {
	return QueueKeyPrefix(qname) + "scheduled"
}

// RetryKey returns a redis key for the retry tasks.
func RetryKey(qname string) string {
	return QueueKeyPrefix(qname) + "retry"
}

// ArchivedKey returns a redis key for the archived tasks.
func ArchivedKey(qname string) string {
	return QueueKeyPrefix(qname) + "archived"
}

// LeaseKey returns a redis key for the lease.
func LeaseKey(qname string) string {
	return QueueKeyPrefix(qname) + "lease"
}

func CompletedKey(qname string) string {
	return QueueKeyPrefix(qname) + "completed"
}

// PausedKey returns a redis key to indicate that the given queue is paused.
func PausedKey(qname string) string {
	return QueueKeyPrefix(qname) + "paused"
}

// ProcessedTotalKey returns a redis key for total processed count for the given queue.
func ProcessedTotalKey(qname string) string {
	return QueueKeyPrefix(qname) + "processed"
}

// FailedTotalKey returns a redis key for total failure count for the given queue.
func FailedTotalKey(qname string) string {
	return QueueKeyPrefix(qname) + "failed"
}

// ProcessedKey returns a redis key for processed count for the given day for the queue.
func ProcessedKey(qname string, t time.Time) string {
	return QueueKeyPrefix(qname) + "processed:" + t.UTC().Format("2006-01-02")
}

// FailedKey returns a redis key for failure count for the given day for the queue.
func FailedKey(qname string, t time.Time) string {
	return QueueKeyPrefix(qname) + "failed:" + t.UTC().Format("2006-01-02")
}

// ServerInfoKey returns a redis key for process info.
func ServerInfoKey(hostname string, pid int, serverID string) string {
	return fmt.Sprintf("asynq:servers:{%s:%d:%s}", hostname, pid, serverID)
}

// WorkersKey returns a redis key for the workers given hostname, pid, and server ID.
func WorkersKey(hostname string, pid int, serverID string) string {
	return fmt.Sprintf("asynq:workers:{%s:%d:%s}", hostname, pid, serverID)
}

// Node quarantine statuses. The quarantine lifecycle is:
// healthy -> quarantined -> released (and released -> healthy on the next failure).
const (
	// NodeQuarantineHealthy means the node is allowed to pull new tasks and is
	// accumulating failure events before reaching the quarantine threshold.
	NodeQuarantineHealthy = "healthy"

	// NodeQuarantineQuarantined means the node is temporarily isolated and must
	// not pull new tasks off queues; other nodes keep consuming normally.
	NodeQuarantineQuarantined = "quarantined"

	// NodeQuarantineReleased means a previously quarantined node has been
	// released, either automatically after stable heartbeats or manually by an
	// operator. The record is retained for audit and is re-armed on the next failure.
	NodeQuarantineReleased = "released"
)

// Audit actions recorded in a node's quarantine audit log.
const (
	QuarantineActionQuarantined  = "quarantined"
	QuarantineActionAutoReleased = "auto_released"
	QuarantineActionManualClear  = "manual_clear"
)

// NodeQuarantineKey returns the redis hash key holding the quarantine record for
// the given node. The node identity is wrapped in a hash tag so that every
// per-node key (state, event markers, liveness marker and audit log) is pinned to
// the same cluster hash slot, which keeps the reconciliation Lua scripts atomic
// in a Redis Cluster deployment.
func NodeQuarantineKey(nodeID string) string {
	return "asynq:quarantine:{" + nodeID + "}"
}

// NodeQuarantineEventKey returns the redis key used as an idempotency marker for
// a single failure event reported by the given node. Re-reporting the same event
// hits this marker and is therefore not counted twice.
func NodeQuarantineEventKey(nodeID, eventKey string) string {
	return NodeQuarantineKey(nodeID) + ":evt:" + eventKey
}

// NodeQuarantineAliveKey returns the redis key marking a particular server
// instance (identified by its server ID) as live for the given node identity.
// The key carries a TTL and is refreshed on every successful heartbeat.
func NodeQuarantineAliveKey(nodeID, serverID string) string {
	return NodeQuarantineKey(nodeID) + ":alive:" + serverID
}

// NodeQuarantineAuditKey returns the redis list key holding the immutable audit
// trail (quarantine, auto-release and manual clear entries) for the given node.
func NodeQuarantineAuditKey(nodeID string) string {
	return NodeQuarantineKey(nodeID) + ":audit"
}

// NodeQuarantineScanMatch returns the SCAN match pattern covering every key owned
// by the node quarantine subsystem.
func NodeQuarantineScanMatch() string {
	return "asynq:quarantine:*"
}

// NodeQuarantine is the persisted, per-node quarantine record. It is stored in a
// redis hash and survives process restarts.
type NodeQuarantine struct {
	// NodeID is the stable identity of the worker node, shared across restarts.
	NodeID string

	// ServerID is the identity of the server instance currently owning the record.
	// It is used together with the liveness marker to detect node identity conflicts.
	ServerID string

	// Status is one of the NodeQuarantine* status constants.
	Status string

	// FailCount is the number of distinct failure events recorded for the current
	// observing/quarantine window. Re-reporting the same event does not increment it.
	FailCount int

	// HealthyStreak is the number of consecutive successful heartbeats observed
	// while the node is quarantined. It resets to zero on any new failure.
	HealthyStreak int

	// FirstFailAt is the time of the first failure event in the current window.
	FirstFailAt time.Time

	// LastFailAt is the time of the most recent distinct failure event.
	LastFailAt time.Time

	// QuarantinedAt is the time the node most recently entered the quarantined status.
	QuarantinedAt time.Time

	// ReleasedAt is the time the node most recently left the quarantined status.
	ReleasedAt time.Time

	// LastEventKey is the idempotency key of the most recently processed failure event.
	LastEventKey string

	// ClearedBy records the operator that manually cleared the quarantine, if any.
	ClearedBy string

	// ClearReason records the reason supplied with the most recent manual clear.
	ClearReason string

	// UpdatedAt is the time the record was last mutated.
	UpdatedAt time.Time

	// Version is a monotonically increasing revision bumped on every mutation; it
	// is part of the corruption checks performed during reconciliation.
	Version int64
}

// QuarantineAuditEntry is one immutable record in a node's quarantine audit trail.
type QuarantineAuditEntry struct {
	// At is the time the entry was written.
	At time.Time `json:"at"`

	// NodeID is the node identity the entry belongs to.
	NodeID string `json:"node_id"`

	// Action is one of the QuarantineAction* constants.
	Action string `json:"action"`

	// Operator is "system" for automatic transitions or the operator that
	// performed a manual clear.
	Operator string `json:"operator"`

	// Reason is a human readable explanation of the entry.
	Reason string `json:"reason"`

	// ServerID is the server instance that produced the entry, when applicable.
	ServerID string `json:"server_id"`

	// Status is the resulting node status after the action.
	Status string `json:"status"`
}

// SchedulerEntriesKey returns a redis key for the scheduler entries given scheduler ID.
func SchedulerEntriesKey(schedulerID string) string {
	return "asynq:schedulers:{" + schedulerID + "}"
}

// SchedulerHistoryKey returns a redis key for the scheduler's history for the given entry.
func SchedulerHistoryKey(entryID string) string {
	return "asynq:scheduler_history:" + entryID
}

// UniqueKey returns a redis key with the given type, payload, and queue name.
func UniqueKey(qname, tasktype string, payload []byte) string {
	if payload == nil {
		return QueueKeyPrefix(qname) + "unique:" + tasktype + ":"
	}
	checksum := md5.Sum(payload)
	return QueueKeyPrefix(qname) + "unique:" + tasktype + ":" + hex.EncodeToString(checksum[:])
}

// GroupKeyPrefix returns a prefix for group key.
func GroupKeyPrefix(qname string) string {
	return QueueKeyPrefix(qname) + "g:"
}

// GroupKey returns a redis key used to group tasks belong in the same group.
func GroupKey(qname, gkey string) string {
	return GroupKeyPrefix(qname) + gkey
}

// AggregationSetKey returns a redis key used for an aggregation set.
func AggregationSetKey(qname, gname, setID string) string {
	return GroupKey(qname, gname) + ":" + setID
}

// AllGroups return a redis key used to store all group keys used in a given queue.
func AllGroups(qname string) string {
	return QueueKeyPrefix(qname) + "groups"
}

// AllAggregationSets returns a redis key used to store all aggregation sets (set of tasks staged to be aggregated)
// in a given queue.
func AllAggregationSets(qname string) string {
	return QueueKeyPrefix(qname) + "aggregation_sets"
}

// TaskMessage is the internal representation of a task with additional metadata fields.
// Serialized data of this type gets written to redis.
type TaskMessage struct {
	// Type indicates the kind of the task to be performed.
	Type string

	// Payload holds data needed to process the task.
	Payload []byte

	// Headers holds additional metadata for the task.
	Headers map[string]string

	// ID is a unique identifier for each task.
	ID string

	// Queue is a name this message should be enqueued to.
	Queue string

	// Retry is the max number of retry for this task.
	Retry int

	// Retried is the number of times we've retried this task so far.
	Retried int

	// ErrorMsg holds the error message from the last failure.
	ErrorMsg string

	// Time of last failure in Unix time,
	// the number of seconds elapsed since January 1, 1970 UTC.
	//
	// Use zero to indicate no last failure
	LastFailedAt int64

	// Timeout specifies timeout in seconds.
	// If task processing doesn't complete within the timeout, the task will be retried
	// if retry count is remaining. Otherwise it will be moved to the archive.
	//
	// Use zero to indicate no timeout.
	Timeout int64

	// Deadline specifies the deadline for the task in Unix time,
	// the number of seconds elapsed since January 1, 1970 UTC.
	// If task processing doesn't complete before the deadline, the task will be retried
	// if retry count is remaining. Otherwise it will be moved to the archive.
	//
	// Use zero to indicate no deadline.
	Deadline int64

	// UniqueKey holds the redis key used for uniqueness lock for this task.
	//
	// Empty string indicates that no uniqueness lock was used.
	UniqueKey string

	// GroupKey holds the group key used for task aggregation.
	//
	// Empty string indicates no aggregation is used for this task.
	GroupKey string

	// Retention specifies the number of seconds the task should be retained after completion.
	Retention int64

	// CompletedAt is the time the task was processed successfully in Unix time,
	// the number of seconds elapsed since January 1, 1970 UTC.
	//
	// Use zero to indicate no value.
	CompletedAt int64
}

// EncodeMessage marshals the given task message and returns an encoded bytes.
func EncodeMessage(msg *TaskMessage) ([]byte, error) {
	if msg == nil {
		return nil, fmt.Errorf("cannot encode nil message")
	}
	return proto.Marshal(&pb.TaskMessage{
		Type:         msg.Type,
		Payload:      msg.Payload,
		Headers:      msg.Headers,
		Id:           msg.ID,
		Queue:        msg.Queue,
		Retry:        int32(msg.Retry),
		Retried:      int32(msg.Retried),
		ErrorMsg:     msg.ErrorMsg,
		LastFailedAt: msg.LastFailedAt,
		Timeout:      msg.Timeout,
		Deadline:     msg.Deadline,
		UniqueKey:    msg.UniqueKey,
		GroupKey:     msg.GroupKey,
		Retention:    msg.Retention,
		CompletedAt:  msg.CompletedAt,
	})
}

// DecodeMessage unmarshals the given bytes and returns a decoded task message.
func DecodeMessage(data []byte) (*TaskMessage, error) {
	var pbmsg pb.TaskMessage
	if err := proto.Unmarshal(data, &pbmsg); err != nil {
		return nil, err
	}
	return &TaskMessage{
		Type:         pbmsg.GetType(),
		Payload:      pbmsg.GetPayload(),
		Headers:      pbmsg.GetHeaders(),
		ID:           pbmsg.GetId(),
		Queue:        pbmsg.GetQueue(),
		Retry:        int(pbmsg.GetRetry()),
		Retried:      int(pbmsg.GetRetried()),
		ErrorMsg:     pbmsg.GetErrorMsg(),
		LastFailedAt: pbmsg.GetLastFailedAt(),
		Timeout:      pbmsg.GetTimeout(),
		Deadline:     pbmsg.GetDeadline(),
		UniqueKey:    pbmsg.GetUniqueKey(),
		GroupKey:     pbmsg.GetGroupKey(),
		Retention:    pbmsg.GetRetention(),
		CompletedAt:  pbmsg.GetCompletedAt(),
	}, nil
}

// TaskInfo describes a task message and its metadata.
type TaskInfo struct {
	Message       *TaskMessage
	State         TaskState
	NextProcessAt time.Time
	Result        []byte
}

// Z represents sorted set member.
type Z struct {
	Message *TaskMessage
	Score   int64
}

// ServerInfo holds information about a running server.
type ServerInfo struct {
	Host              string
	PID               int
	ServerID          string
	Concurrency       int
	Queues            map[string]int
	StrictPriority    bool
	Status            string
	Started           time.Time
	ActiveWorkerCount int
}

// EncodeServerInfo marshals the given ServerInfo and returns the encoded bytes.
func EncodeServerInfo(info *ServerInfo) ([]byte, error) {
	if info == nil {
		return nil, fmt.Errorf("cannot encode nil server info")
	}
	queues := make(map[string]int32, len(info.Queues))
	for q, p := range info.Queues {
		queues[q] = int32(p)
	}
	started := timestamppb.New(info.Started)

	return proto.Marshal(&pb.ServerInfo{
		Host:              info.Host,
		Pid:               int32(info.PID),
		ServerId:          info.ServerID,
		Concurrency:       int32(info.Concurrency),
		Queues:            queues,
		StrictPriority:    info.StrictPriority,
		Status:            info.Status,
		StartTime:         started,
		ActiveWorkerCount: int32(info.ActiveWorkerCount),
	})
}

// DecodeServerInfo decodes the given bytes into ServerInfo.
func DecodeServerInfo(b []byte) (*ServerInfo, error) {
	var pbmsg pb.ServerInfo
	if err := proto.Unmarshal(b, &pbmsg); err != nil {
		return nil, err
	}
	queues := make(map[string]int, len(pbmsg.GetQueues()))
	for q, p := range pbmsg.GetQueues() {
		queues[q] = int(p)
	}
	startTime := pbmsg.GetStartTime()

	return &ServerInfo{
		Host:              pbmsg.GetHost(),
		PID:               int(pbmsg.GetPid()),
		ServerID:          pbmsg.GetServerId(),
		Concurrency:       int(pbmsg.GetConcurrency()),
		Queues:            queues,
		StrictPriority:    pbmsg.GetStrictPriority(),
		Status:            pbmsg.GetStatus(),
		Started:           startTime.AsTime(),
		ActiveWorkerCount: int(pbmsg.GetActiveWorkerCount()),
	}, nil
}

// WorkerInfo holds information about a running worker.
type WorkerInfo struct {
	Host     string
	PID      int
	ServerID string
	ID       string
	Type     string
	Payload  []byte
	Queue    string
	Started  time.Time
	Deadline time.Time
}

// EncodeWorkerInfo marshals the given WorkerInfo and returns the encoded bytes.
func EncodeWorkerInfo(info *WorkerInfo) ([]byte, error) {
	if info == nil {
		return nil, fmt.Errorf("cannot encode nil worker info")
	}
	startTime := timestamppb.New(info.Started)
	deadline := timestamppb.New(info.Deadline)

	return proto.Marshal(&pb.WorkerInfo{
		Host:        info.Host,
		Pid:         int32(info.PID),
		ServerId:    info.ServerID,
		TaskId:      info.ID,
		TaskType:    info.Type,
		TaskPayload: info.Payload,
		Queue:       info.Queue,
		StartTime:   startTime,
		Deadline:    deadline,
	})
}

// DecodeWorkerInfo decodes the given bytes into WorkerInfo.
func DecodeWorkerInfo(b []byte) (*WorkerInfo, error) {
	var pbmsg pb.WorkerInfo
	if err := proto.Unmarshal(b, &pbmsg); err != nil {
		return nil, err
	}
	startTime := pbmsg.GetStartTime()
	deadline := pbmsg.GetDeadline()

	return &WorkerInfo{
		Host:     pbmsg.GetHost(),
		PID:      int(pbmsg.GetPid()),
		ServerID: pbmsg.GetServerId(),
		ID:       pbmsg.GetTaskId(),
		Type:     pbmsg.GetTaskType(),
		Payload:  pbmsg.GetTaskPayload(),
		Queue:    pbmsg.GetQueue(),
		Started:  startTime.AsTime(),
		Deadline: deadline.AsTime(),
	}, nil
}

// SchedulerEntry holds information about a periodic task registered with a scheduler.
type SchedulerEntry struct {
	// Identifier of this entry.
	ID string

	// Spec describes the schedule of this entry.
	Spec string

	// Type is the task type of the periodic task.
	Type string

	// Payload is the payload of the periodic task.
	Payload []byte

	// Opts is the options for the periodic task.
	Opts []string

	// Next shows the next time the task will be enqueued.
	Next time.Time

	// Prev shows the last time the task was enqueued.
	// Zero time if task was never enqueued.
	Prev time.Time
}

// EncodeSchedulerEntry marshals the given entry and returns an encoded bytes.
func EncodeSchedulerEntry(entry *SchedulerEntry) ([]byte, error) {
	if entry == nil {
		return nil, fmt.Errorf("cannot encode nil scheduler entry")
	}
	next := timestamppb.New(entry.Next)
	prev := timestamppb.New(entry.Prev)

	return proto.Marshal(&pb.SchedulerEntry{
		Id:              entry.ID,
		Spec:            entry.Spec,
		TaskType:        entry.Type,
		TaskPayload:     entry.Payload,
		EnqueueOptions:  entry.Opts,
		NextEnqueueTime: next,
		PrevEnqueueTime: prev,
	})
}

// DecodeSchedulerEntry unmarshals the given bytes and returns a decoded SchedulerEntry.
func DecodeSchedulerEntry(b []byte) (*SchedulerEntry, error) {
	var pbmsg pb.SchedulerEntry
	if err := proto.Unmarshal(b, &pbmsg); err != nil {
		return nil, err
	}
	next := pbmsg.GetNextEnqueueTime()
	prev := pbmsg.GetPrevEnqueueTime()

	return &SchedulerEntry{
		ID:      pbmsg.GetId(),
		Spec:    pbmsg.GetSpec(),
		Type:    pbmsg.GetTaskType(),
		Payload: pbmsg.GetTaskPayload(),
		Opts:    pbmsg.GetEnqueueOptions(),
		Next:    next.AsTime(),
		Prev:    prev.AsTime(),
	}, nil
}

// SchedulerEnqueueEvent holds information about an enqueue event by a scheduler.
type SchedulerEnqueueEvent struct {
	// ID of the task that was enqueued.
	TaskID string

	// Time the task was enqueued.
	EnqueuedAt time.Time
}

// EncodeSchedulerEnqueueEvent marshals the given event
// and returns an encoded bytes.
func EncodeSchedulerEnqueueEvent(event *SchedulerEnqueueEvent) ([]byte, error) {
	if event == nil {
		return nil, fmt.Errorf("cannot encode nil enqueue event")
	}
	enqueuedAt := timestamppb.New(event.EnqueuedAt)
	return proto.Marshal(&pb.SchedulerEnqueueEvent{
		TaskId:      event.TaskID,
		EnqueueTime: enqueuedAt,
	})
}

// DecodeSchedulerEnqueueEvent unmarshals the given bytes
// and returns a decoded SchedulerEnqueueEvent.
func DecodeSchedulerEnqueueEvent(b []byte) (*SchedulerEnqueueEvent, error) {
	var pbmsg pb.SchedulerEnqueueEvent
	if err := proto.Unmarshal(b, &pbmsg); err != nil {
		return nil, err
	}
	enqueuedAt := pbmsg.GetEnqueueTime()
	return &SchedulerEnqueueEvent{
		TaskID:     pbmsg.GetTaskId(),
		EnqueuedAt: enqueuedAt.AsTime(),
	}, nil
}

// Cancelations is a collection that holds cancel functions for all active tasks.
//
// Cancelations are safe for concurrent use by multiple goroutines.
type Cancelations struct {
	mu          sync.Mutex
	cancelFuncs map[string]context.CancelFunc
}

// NewCancelations returns a Cancelations instance.
func NewCancelations() *Cancelations {
	return &Cancelations{
		cancelFuncs: make(map[string]context.CancelFunc),
	}
}

// Add adds a new cancel func to the collection.
func (c *Cancelations) Add(id string, fn context.CancelFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelFuncs[id] = fn
}

// Delete deletes a cancel func from the collection given an id.
func (c *Cancelations) Delete(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cancelFuncs, id)
}

// Get returns a cancel func given an id.
func (c *Cancelations) Get(id string) (fn context.CancelFunc, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn, ok = c.cancelFuncs[id]
	return fn, ok
}

// Lease is a time bound lease for worker to process task.
// It provides a communication channel between lessor and lessee about lease expiration.
type Lease struct {
	once sync.Once
	ch   chan struct{}

	Clock timeutil.Clock

	mu       sync.Mutex
	expireAt time.Time // guarded by mu
}

func NewLease(expirationTime time.Time) *Lease {
	return &Lease{
		ch:       make(chan struct{}),
		expireAt: expirationTime,
		Clock:    timeutil.NewRealClock(),
	}
}

// Reset changes the lease to expire at the given time.
// It returns true if the lease is still valid and reset operation was successful, false if the lease had been expired.
func (l *Lease) Reset(expirationTime time.Time) bool {
	if !l.IsValid() {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.expireAt = expirationTime
	return true
}

// Sends a notification to lessee about expired lease
// Returns true if notification was sent, returns false if the lease is still valid and notification was not sent.
func (l *Lease) NotifyExpiration() bool {
	if l.IsValid() {
		return false
	}
	l.once.Do(l.closeCh)
	return true
}

func (l *Lease) closeCh() {
	close(l.ch)
}

// Done returns a communication channel from which the lessee can read to get notified when lessor notifies about lease expiration.
func (l *Lease) Done() <-chan struct{} {
	return l.ch
}

// Deadline returns the expiration time of the lease.
func (l *Lease) Deadline() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.expireAt
}

// IsValid returns true if the lease's expiration time is in the future or equals to the current time,
// returns false otherwise.
func (l *Lease) IsValid() bool {
	now := l.Clock.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.expireAt.After(now) || l.expireAt.Equal(now)
}

// BatchEnqueueItem pairs a task message with optional scheduling metadata for
// batch enqueue operations. If ProcessAt is zero, the task is enqueued for
// immediate processing; otherwise it is added to the scheduled set.
type BatchEnqueueItem struct {
	Msg       *TaskMessage
	ProcessAt time.Time // zero value → immediate
}

// Broker is a message broker that supports operations to manage task queues.
//
// See rdb.RDB as a reference implementation.
type Broker interface {
	Ping() error
	Close() error
	Enqueue(ctx context.Context, msg *TaskMessage) error
	EnqueueUnique(ctx context.Context, msg *TaskMessage, ttl time.Duration) error
	// BatchEnqueue enqueues multiple tasks in a single round-trip. It returns the
	// count of newly enqueued tasks; duplicate IDs are silently skipped. The error
	// is non-nil only on infrastructure failure (e.g. lost connection), in which
	// case the count is meaningless. Individual task scripts are atomic but the
	// batch as a whole is not transactional — partial success is possible.
	BatchEnqueue(ctx context.Context, items []BatchEnqueueItem) (int, error)
	Dequeue(qnames ...string) (*TaskMessage, time.Time, error)
	Done(ctx context.Context, msg *TaskMessage) error
	MarkAsComplete(ctx context.Context, msg *TaskMessage) error
	Requeue(ctx context.Context, msg *TaskMessage) error
	Schedule(ctx context.Context, msg *TaskMessage, processAt time.Time) error
	ScheduleUnique(ctx context.Context, msg *TaskMessage, processAt time.Time, ttl time.Duration) error
	Retry(ctx context.Context, msg *TaskMessage, processAt time.Time, errMsg string, isFailure bool) error
	Archive(ctx context.Context, msg *TaskMessage, errMsg string) error
	ForwardIfReady(qnames ...string) error

	// Group aggregation related methods
	AddToGroup(ctx context.Context, msg *TaskMessage, gname string) error
	AddToGroupUnique(ctx context.Context, msg *TaskMessage, groupKey string, ttl time.Duration) error
	ListGroups(qname string) ([]string, error)
	AggregationCheck(qname, gname string, t time.Time, gracePeriod, maxDelay time.Duration, maxSize int) (aggregationSetID string, err error)
	ReadAggregationSet(qname, gname, aggregationSetID string) ([]*TaskMessage, time.Time, error)
	DeleteAggregationSet(ctx context.Context, qname, gname, aggregationSetID string) error
	ReclaimStaleAggregationSets(qname string) error

	// Task retention related method
	DeleteExpiredCompletedTasks(qname string, batchSize int) error

	// Lease related methods
	ListLeaseExpired(cutoff time.Time, qnames ...string) ([]*TaskMessage, error)
	ExtendLease(qname string, ids ...string) (time.Time, error)

	// State snapshot related methods
	WriteServerState(info *ServerInfo, workers []*WorkerInfo, ttl time.Duration) error
	ClearServerState(host string, pid int, serverID string) error

	// Cancelation related methods
	CancelationPubSub() (*redis.PubSub, error) // TODO: Need to decouple from redis to support other brokers
	PublishCancelation(id string) error

	WriteResult(qname, id string, data []byte) (n int, err error)

	// Node quarantine related methods.
	//
	// All mutating operations are executed atomically and are fail-safe: on a node
	// identity conflict, corrupt persisted state or redis write failure they return
	// an error without modifying the pre-existing quarantine record.

	// ReportNodeFailure records a distinct failure event (e.g. a missed heartbeat
	// or an expired task lease) for the given node and advances its quarantine
	// state.
	//
	// Reporting is idempotent: re-reporting the same eventKey is a no-op that
	// returns duplicated=true without incrementing the failure count, resetting the
	// recovery streak or otherwise extending the isolation.
	//
	// failureThreshold is the number of distinct failure events that moves the node
	// into the quarantined status. The method returns the resulting node status.
	ReportNodeFailure(ctx context.Context, nodeID, serverID, eventKey string, failureThreshold int) (status string, duplicated bool, err error)

	// RecordNodeHeartbeat records a successful heartbeat for the given node and
	// refreshes the owner's liveness marker used for conflict detection.
	//
	// While the node is quarantined, each successful heartbeat increments a recovery
	// streak; once the streak reaches recoveryThreshold the node is automatically
	// released. A failed heartbeat must be reported through ReportNodeFailure rather
	// than this method. The method returns the resulting node status.
	RecordNodeHeartbeat(ctx context.Context, nodeID, serverID string, recoveryThreshold int) (status string, err error)

	// GetNodeQuarantine returns the persisted quarantine record for the given node.
	// It returns ErrNodeQuarantineNotFound when no record exists.
	GetNodeQuarantine(nodeID string) (*NodeQuarantine, error)

	// ReconcileNodeQuarantine performs a read-only, identity-checked reconciliation
	// for the calling server instance and returns the authoritative node status.
	//
	// It performs no writes. It returns ErrNodeIdentityConflict when the record is
	// owned by another still-live server instance, and ErrQuarantineStateCorrupt when
	// the persisted record is malformed; in both cases the caller must leave its
	// existing isolation state unchanged. When no record exists it returns the
	// healthy status.
	ReconcileNodeQuarantine(nodeID, serverID string) (status string, err error)

	// ListNodeQuarantines returns all known node quarantine records, including
	// healthy-observing and released nodes. Corrupt records are reported as an error.
	ListNodeQuarantines() ([]*NodeQuarantine, error)

	// ClearNodeQuarantine manually lifts the quarantine for the given node and
	// appends an immutable audit entry. It fails without mutating state when the
	// record is missing or corrupt.
	ClearNodeQuarantine(ctx context.Context, nodeID, operator, reason string) error

	// ListNodeQuarantineAudit returns the audit entries for the given node,
	// oldest entry first.
	ListNodeQuarantineAudit(nodeID string) ([]*QuarantineAuditEntry, error)
}
