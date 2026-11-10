// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
)

// nodeFailureKind categorizes the failure signals observed on a worker node.
type nodeFailureKind int8

const (
	// failureHeartbeat represents an interrupted/failed heartbeat write.
	failureHeartbeat nodeFailureKind = iota

	// failureLease represents an expired task lease or a failed lease extension.
	failureLease
)

func (k nodeFailureKind) String() string {
	switch k {
	case failureHeartbeat:
		return "heartbeat"
	case failureLease:
		return "lease"
	}
	return "unknown"
}

// reportBufferSize bounds the channel buffering failure/heartbeat reports. The
// heartbeater produces at most a handful of reports per heartbeat interval, so a
// modest buffer is enough to avoid blocking the heartbeat path.
const reportBufferSize = 256

// heartbeatEventKey builds the idempotency key for a failed heartbeat. Including
// the server ID scopes the per-process beat sequence so restarts cannot collide
// with markers from a previous instance of the same node.
func heartbeatEventKey(serverID string, beatSeq int64) string {
	return fmt.Sprintf("hb:%s:%d", serverID, beatSeq)
}

// leaseEventKey builds the idempotency key for the expiration of one task lease.
// The same lease being observed as expired across consecutive heartbeats produces
// the same key and is therefore counted only once.
func leaseEventKey(serverID, taskID string, leaseDeadline time.Time) string {
	return fmt.Sprintf("lease:%s:%s:%d", serverID, taskID, leaseDeadline.Unix())
}

// leaseExtendEventKey builds the idempotency key for a failed lease extension
// during one heartbeat cycle.
func leaseExtendEventKey(serverID, qname string, beatSeq int64) string {
	return fmt.Sprintf("lease-ext:%s:%s:%d", serverID, qname, beatSeq)
}

// nodeFailure is a failure signal observed by the heartbeater.
type nodeFailure struct {
	kind     nodeFailureKind
	eventKey string
}

// quarantineReport is the message exchanged between the heartbeater and the
// quarantiner. Exactly one of the fields is meaningful.
type quarantineReport struct {
	// heartbeatOK reports a successful heartbeat.
	heartbeatOK bool

	// failure reports a distinct failure event; nil when heartbeatOK is true.
	failure *nodeFailure
}

// quarantineGate is the read-only view of the node's isolation state consumed by
// the processor before pulling new tasks.
type quarantineGate interface {
	// IsQuarantined reports whether the local node is currently isolated and must
	// not pull new tasks off queues.
	IsQuarantined() bool
}

// quarantineReporter is the write-only view used by the heartbeater to feed
// heartbeat outcomes and failure events into the quarantine subsystem.
type quarantineReporter interface {
	reportFailure(kind nodeFailureKind, eventKey string)
	recordHeartbeat()
}

// quarantiner reconciles the local worker node's quarantine state with redis.
//
// It consumes failure/heartbeat reports from the heartbeater, persists them through
// atomic broker operations and exposes a local, concurrency-safe gate that the
// processor consults before dequeuing tasks. A periodic reconciliation reloads the
// persisted state so that isolation survives process restarts and externally
// triggered releases are picked up promptly.
type quarantiner struct {
	logger *log.Logger
	broker base.Broker

	// stable node identity, shared across restarts.
	nodeID string
	// identity of this server instance, used for conflict detection.
	serverID string

	failureThreshold  int
	recoveryThreshold int
	interval          time.Duration

	// quarantined is a local cache of the persisted isolation state. It is the only
	// field read by the processor's hot path. It is only flipped after a confirmed
	// broker mutation or reconciliation, never on an error.
	quarantined atomic.Bool

	reports chan quarantineReport
	done    chan struct{}
	once    sync.Once
}

type quarantinerParams struct {
	logger            *log.Logger
	broker            base.Broker
	nodeID            string
	serverID          string
	failureThreshold  int
	recoveryThreshold int
	interval          time.Duration
}

func newQuarantiner(params quarantinerParams) *quarantiner {
	return &quarantiner{
		logger:            params.logger,
		broker:            params.broker,
		nodeID:            params.nodeID,
		serverID:          params.serverID,
		failureThreshold:  params.failureThreshold,
		recoveryThreshold: params.recoveryThreshold,
		interval:          params.interval,
		reports:           make(chan quarantineReport, reportBufferSize),
		done:              make(chan struct{}),
	}
}

// IsQuarantined implements quarantineGate.
func (q *quarantiner) IsQuarantined() bool {
	return q.quarantined.Load()
}

// reportFailure implements quarantineReporter. It never blocks the heartbeat path:
// if the report buffer is full the event is dropped with a warning. Dropped events
// are bounded because the heartbeater re-evaluates lease/heartbeat health on every
// beat, and the periodic reconciliation keeps the gate consistent with redis.
func (q *quarantiner) reportFailure(kind nodeFailureKind, eventKey string) {
	select {
	case q.reports <- quarantineReport{failure: &nodeFailure{kind: kind, eventKey: eventKey}}:
	default:
		q.logger.Warnf("Quarantine report buffer full; dropping %s failure event %q", kind, eventKey)
	}
}

// recordHeartbeat implements quarantineReporter.
func (q *quarantiner) recordHeartbeat() {
	select {
	case q.reports <- quarantineReport{heartbeatOK: true}:
	default:
		q.logger.Warn("Quarantine report buffer full; dropping heartbeat report")
	}
}

func (q *quarantiner) shutdown() {
	q.once.Do(func() {
		q.logger.Debug("Quarantiner shutting down...")
		q.done <- struct{}{}
	})
}

func (q *quarantiner) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Reconcile immediately so a node that restarts while still quarantined in
		// redis stops pulling new tasks without waiting for the first timer tick.
		q.reconcile()

		timer := time.NewTimer(q.interval)
		defer timer.Stop()
		for {
			select {
			case <-q.done:
				q.logger.Debug("Quarantiner done")
				return
			case <-timer.C:
				q.reconcile()
				timer.Reset(q.interval)
			case rep := <-q.reports:
				q.handle(rep)
			}
		}
	}()
}

// handle applies a single report through an atomic broker operation. On any error
// (identity conflict, corrupt state or write failure) the local gate is left
// unchanged so the pre-existing isolation state is preserved.
func (q *quarantiner) handle(rep quarantineReport) {
	ctx := context.Background()
	if rep.heartbeatOK {
		status, err := q.broker.RecordNodeHeartbeat(ctx, q.nodeID, q.serverID, q.recoveryThreshold)
		if err != nil {
			q.logger.Errorf("Failed to record node heartbeat for %q: %v; keeping quarantine state unchanged", q.nodeID, err)
			return
		}
		q.applyStatus(status)
		return
	}

	f := rep.failure
	status, duplicated, err := q.broker.ReportNodeFailure(ctx, q.nodeID, q.serverID, f.eventKey, q.failureThreshold)
	if err != nil {
		q.logger.Errorf("Failed to report %s failure for node %q: %v; keeping quarantine state unchanged", f.kind, q.nodeID, err)
		return
	}
	if duplicated {
		q.logger.Debugf("Ignored duplicate %s failure event %q for node %q", f.kind, f.eventKey, q.nodeID)
		return
	}
	q.logger.Warnf("Recorded %s failure event %q for node %q; status=%s", f.kind, f.eventKey, q.nodeID, status)
	q.applyStatus(status)
}

// reconcile reloads the persisted quarantine state through an identity-checked,
// read-only broker call and resynchronizes the local gate. This is the
// cross-restart persistence mechanism and also picks up manual clears performed
// through the Inspector. On identity conflict, corrupt state or read failure the
// gate is deliberately left unchanged.
func (q *quarantiner) reconcile() {
	status, err := q.broker.ReconcileNodeQuarantine(q.nodeID, q.serverID)
	if err != nil {
		q.logger.Errorf("Failed to reconcile quarantine state for node %q: %v; keeping quarantine state unchanged", q.nodeID, err)
		return
	}
	q.applyStatus(status)
}

// applyStatus updates the local gate to match the authoritative status.
func (q *quarantiner) applyStatus(status string) {
	quarantined := status == base.NodeQuarantineQuarantined
	if q.quarantined.Swap(quarantined) != quarantined {
		if quarantined {
			q.logger.Warnf("Node %q is now quarantined; it will stop pulling new tasks", q.nodeID)
		} else {
			q.logger.Infof("Node %q quarantine released; it will resume pulling tasks", q.nodeID)
		}
	}
}
