// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"strconv"

	"github.com/hibiken/asynq/internal/base"
	ierrors "github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/rdb"
)

// PayloadVersionHeader is the reserved task header key recording the payload
// schema version a task's payload has been migrated to. The migration
// machinery owns this header; user code should not set or overwrite it.
const PayloadVersionHeader = base.PayloadVersionHeader

// ErrUnrecognizedPayload may be wrapped or returned by a
// PayloadMigration.Convert function to signal that the input payload does not
// match any schema the migration knows how to handle. Returning this error
// aborts the whole migration run before any task is modified.
var ErrUnrecognizedPayload = errors.New("asynq: unrecognized task payload")

// PayloadMigration describes a versioned payload conversion for tasks of a
// single task type.
type PayloadMigration struct {
	// TaskType is the task type this migration applies to. Only tasks whose
	// type matches are considered.
	TaskType string

	// Version is the payload schema version that Convert upgrades payloads to.
	// It must be a positive integer. A task whose recorded payload version is
	// already greater than or equal to Version is skipped, which makes repeated
	// migration runs idempotent.
	Version int

	// Convert transforms an older-schema payload into the payload for Version.
	// It must be deterministic: the same input payload must always produce the
	// same output payload, since it may be invoked more than once for a given
	// task across interrupted migration runs.
	//
	// A non-nil return error aborts the entire migration run before any task
	// is modified. Wrap ErrUnrecognizedPayload when the input payload cannot
	// be recognized.
	Convert func(oldPayload []byte) (newPayload []byte, err error)
}

// PayloadMigrationSkipReason explains why a task was not rewritten during a
// migration run.
type PayloadMigrationSkipReason string

const (
	// PayloadMigrationSkipAlreadyCurrent means the task's recorded payload
	// version is already at or above the migration's target version.
	PayloadMigrationSkipAlreadyCurrent PayloadMigrationSkipReason = "already_current"

	// PayloadMigrationSkipTaskNotFound means the task disappeared between the
	// scan and the commit (e.g. it was deleted).
	PayloadMigrationSkipTaskNotFound PayloadMigrationSkipReason = "task_not_found"

	// PayloadMigrationSkipStateChanged means the task left an eligible state
	// (pending, scheduled or retry) between the scan and the commit, e.g. it
	// was dequeued and became active.
	PayloadMigrationSkipStateChanged PayloadMigrationSkipReason = "state_changed"

	// PayloadMigrationSkipMessageChanged means the task was modified by
	// another process between the scan and the commit. A subsequent migration
	// run will pick it up.
	PayloadMigrationSkipMessageChanged PayloadMigrationSkipReason = "message_changed"
)

// PayloadMigrationTask identifies a task that a migration run examined.
type PayloadMigrationTask struct {
	// Queue is the queue the task belongs to.
	Queue string

	// ID is the task identifier.
	ID string

	// Type is the task type.
	Type string

	// State is the task state observed during the scan.
	State TaskState

	// Reason is set only for skipped tasks and explains why the task was not
	// rewritten. It is empty for migrated tasks.
	Reason PayloadMigrationSkipReason
}

// PayloadMigrationResult summarizes a migration run.
type PayloadMigrationResult struct {
	// Scanned is the number of eligible tasks (pending, scheduled or retry)
	// whose type matched a configured migration.
	Scanned int

	// Migrated lists the tasks whose payload was rewritten to the target
	// version during this run.
	Migrated []PayloadMigrationTask

	// Skipped lists the tasks that were not rewritten, together with the
	// reason. Already-current tasks are skipped on every rerun, while tasks
	// that changed during the run are skipped and reconciled by a rerun.
	Skipped []PayloadMigrationTask
}

// PayloadConversionError indicates that a task payload could not be converted.
// A migration run returns this error without modifying any task, so the
// entire queue set remains in its original state.
type PayloadConversionError struct {
	// Queue is the queue the failing task belongs to.
	Queue string

	// ID is the failing task identifier.
	ID string

	// Type is the failing task type.
	Type string

	// State is the task state observed during the scan.
	State TaskState

	// Err is the error returned by the PayloadMigration.Convert function.
	Err error
}

func (e *PayloadConversionError) Error() string {
	return fmt.Sprintf("asynq: payload migration failed for task id=%q type=%q queue=%q state=%s: %v",
		e.ID, e.Type, e.Queue, e.State, e.Err)
}

// Unwrap returns the underlying conversion error.
func (e *PayloadConversionError) Unwrap() error { return e.Err }

// payloadMigrationPlan is a single validated, converted task waiting to be
// committed.
type payloadMigrationPlan struct {
	qname      string
	state      base.TaskState
	oldEncoded []byte
	newEncoded []byte
	task       PayloadMigrationTask
}

// MigrateTaskPayload performs a versioned payload migration across the given
// queues.
//
// Only tasks that have not started processing are considered: tasks in the
// pending, scheduled and retry states whose type matches one of the supplied
// migrations. Active tasks, completed history, archived and aggregating tasks
// are never touched. When queues is empty, every queue known to the Inspector
// is migrated.
//
// The operation runs in two phases. First every candidate payload is scanned
// and converted in memory; if any payload is unrecognized or a Convert
// function returns an error, MigrateTaskPayload returns a *PayloadConversionError
// without writing anything. Only when all conversions succeed are the migrated
// messages committed, each atomically and guarded by the task state and the
// previously stored message.
//
// Task identity, list position, scheduled/retry time (sorted-set score),
// retry counters, headers other than the version header, and all other
// execution options are preserved. A task already at the target version is
// skipped, so re-running the operation never converts a payload twice; an
// interrupted run can simply be retried and will only commit the tasks that
// were not migrated yet.
func (i *Inspector) MigrateTaskPayload(queues []string, migrations []*PayloadMigration) (*PayloadMigrationResult, error) {
	specs, err := validatePayloadMigrations(migrations)
	if err != nil {
		return nil, err
	}
	qnames, err := i.resolveMigrationQueues(queues)
	if err != nil {
		return nil, err
	}

	result := &PayloadMigrationResult{}

	// Phase 1: scan every eligible task and convert all payloads in memory.
	// No write happens until all conversions succeed, so a conversion failure
	// leaves the entire queue set in its original state.
	var plans []payloadMigrationPlan
	for _, qname := range qnames {
		candidates, err := i.rdb.ListMigrationCandidates(qname)
		if err != nil {
			if ierrors.IsQueueNotFound(err) {
				return nil, fmt.Errorf("asynq: %w", ErrQueueNotFound)
			}
			return nil, fmt.Errorf("asynq: %w", err)
		}
		for _, candidate := range candidates {
			msg := candidate.Message
			spec, ok := specs[msg.Type]
			if !ok {
				continue
			}
			result.Scanned++
			task := PayloadMigrationTask{
				Queue: qname,
				ID:    msg.ID,
				Type:  msg.Type,
				State: toPublicTaskState(candidate.State),
			}
			if current := payloadSchemaVersion(msg); current >= spec.Version {
				task.Reason = PayloadMigrationSkipAlreadyCurrent
				result.Skipped = append(result.Skipped, task)
				continue
			}
			newPayload, err := spec.Convert(msg.Payload)
			if err != nil {
				return nil, &PayloadConversionError{
					Queue: qname,
					ID:    msg.ID,
					Type:  msg.Type,
					State: toPublicTaskState(candidate.State),
					Err:   err,
				}
			}
			updated := *msg
			updated.Payload = newPayload
			updated.Headers = maps.Clone(msg.Headers)
			if updated.Headers == nil {
				updated.Headers = make(map[string]string)
			}
			updated.Headers[base.PayloadVersionHeader] = strconv.Itoa(spec.Version)
			newEncoded, err := base.EncodeMessage(&updated)
			if err != nil {
				return nil, fmt.Errorf("asynq: cannot encode migrated task id=%q: %w", msg.ID, err)
			}
			plans = append(plans, payloadMigrationPlan{
				qname:      qname,
				state:      candidate.State,
				oldEncoded: candidate.Encoded,
				newEncoded: newEncoded,
				task:       task,
			})
		}
	}

	// Phase 2: commit the converted messages. Each commit is atomic and
	// rejects tasks that changed or disappeared since the scan; such tasks are
	// reported as skipped and will be handled by a subsequent run.
	for _, plan := range plans {
		status, err := i.rdb.CommitPayloadMigration(plan.qname, plan.task.ID, plan.state, plan.oldEncoded, plan.newEncoded)
		if err != nil {
			return nil, fmt.Errorf("asynq: %w", err)
		}
		switch status {
		case rdb.PayloadMigrationCommitted:
			result.Migrated = append(result.Migrated, plan.task)
		case rdb.PayloadMigrationTaskNotFound:
			plan.task.Reason = PayloadMigrationSkipTaskNotFound
			result.Skipped = append(result.Skipped, plan.task)
		case rdb.PayloadMigrationStateChanged:
			plan.task.Reason = PayloadMigrationSkipStateChanged
			result.Skipped = append(result.Skipped, plan.task)
		case rdb.PayloadMigrationMessageChanged:
			plan.task.Reason = PayloadMigrationSkipMessageChanged
			result.Skipped = append(result.Skipped, plan.task)
		default:
			return nil, fmt.Errorf("asynq: unexpected payload migration commit status %d", status)
		}
	}
	return result, nil
}

// validatePayloadMigrations validates the supplied migrations and indexes them
// by task type.
func validatePayloadMigrations(migrations []*PayloadMigration) (map[string]*PayloadMigration, error) {
	if len(migrations) == 0 {
		return nil, errors.New("asynq: at least one payload migration is required")
	}
	specs := make(map[string]*PayloadMigration, len(migrations))
	for _, m := range migrations {
		if m == nil {
			return nil, errors.New("asynq: payload migration must not be nil")
		}
		if m.TaskType == "" {
			return nil, errors.New("asynq: payload migration task type must not be empty")
		}
		if m.Version <= 0 {
			return nil, fmt.Errorf("asynq: payload migration for type %q must specify a positive target version", m.TaskType)
		}
		if m.Convert == nil {
			return nil, fmt.Errorf("asynq: payload migration for type %q must specify a Convert function", m.TaskType)
		}
		if _, exists := specs[m.TaskType]; exists {
			return nil, fmt.Errorf("asynq: duplicate payload migration for task type %q", m.TaskType)
		}
		specs[m.TaskType] = m
	}
	return specs, nil
}

// resolveMigrationQueues returns the validated, de-duplicated list of queues to
// migrate. An empty list expands to all queues known to the Inspector.
func (i *Inspector) resolveMigrationQueues(queues []string) ([]string, error) {
	if len(queues) == 0 {
		all, err := i.rdb.AllQueues()
		if err != nil {
			return nil, fmt.Errorf("asynq: %w", err)
		}
		queues = all
	}
	seen := make(map[string]struct{}, len(queues))
	qnames := make([]string, 0, len(queues))
	for _, q := range queues {
		if err := base.ValidateQueueName(q); err != nil {
			return nil, fmt.Errorf("asynq: %w", err)
		}
		if _, ok := seen[q]; ok {
			continue
		}
		seen[q] = struct{}{}
		qnames = append(qnames, q)
	}
	sort.Strings(qnames)
	return qnames, nil
}

// payloadSchemaVersion returns the payload version recorded on the task. A
// missing or malformed version header means the task has never been migrated.
func payloadSchemaVersion(msg *base.TaskMessage) int {
	if len(msg.Headers) == 0 {
		return 0
	}
	version, err := strconv.Atoi(msg.Headers[base.PayloadVersionHeader])
	if err != nil || version < 0 {
		return 0
	}
	return version
}

// toPublicTaskState converts an internal task state to the public TaskState.
// Migration candidates are always pending, scheduled or retry.
func toPublicTaskState(state base.TaskState) TaskState {
	switch state {
	case base.TaskStatePending:
		return TaskStatePending
	case base.TaskStateScheduled:
		return TaskStateScheduled
	case base.TaskStateRetry:
		return TaskStateRetry
	default:
		panic(fmt.Sprintf("asynq: unexpected task state %s for payload migration", state))
	}
}
