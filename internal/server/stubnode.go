package server

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/SushantPotu/raft-kv-store/internal/statemachine"
	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// =============================================================================
// TEMPORARY STUB — Workstream C internal testing only.
//
// SingleNodeStub is NOT a Raft implementation. It exists solely so the
// Client/API Protocol Layer workstream can prove its own gRPC wiring
// (kvctl -> KVService -> Node -> StateMachine) works end to end while the
// real internal/raft.Node (leader election + log replication) is being
// built in parallel by the Raft Core workstream.
//
// It does no consensus, has no peers, keeps no durable log, and Propose
// applies synchronously and unconditionally — there is exactly one
// "replica" and it always immediately commits whatever it's given.
//
// MUST be replaced by the real internal/raft.Node at Integration
// Checkpoint 1 (see docs/adr/0001-raft-core-io-separation.md for the
// Ready-loop contract the real Node follows, which kvserver.go does not
// currently need to drive because this stub short-circuits it). Do not
// grow this type into a real implementation of anything; if it starts
// needing peers, terms, or elections, that logic belongs in
// internal/raft, not here.
// =============================================================================

var _ raft.Node = (*SingleNodeStub)(nil)

// SingleNodeStub is a minimal single-node raft.Node stand-in. See the
// package-level warning above.
type SingleNodeStub struct {
	id raft.NodeID
	sm *fakeStateMachine

	mu        sync.Mutex
	nextIndex raft.LogIndex

	// pending correlates a proposed command's RequestID with the
	// synchronously-produced commandResult from fakeStateMachine.Apply, so
	// kvserver can retrieve write results (e.g. CompareAndSwap's
	// swapped/actual_value) without needing to consume a Ready loop, which
	// would be pure ceremony for a stub whose Propose never actually defers
	// anything.
	pendingMu sync.Mutex
	pending   map[string]statemachine.CommandResult
}

// NewSingleNodeStub constructs a stub bound to the given fake state
// machine. id is cosmetic (surfaced via Status()) — a single-node stub is
// always its own leader.
func newSingleNodeStub(id raft.NodeID, sm *fakeStateMachine) *SingleNodeStub {
	return &SingleNodeStub{
		id:      id,
		sm:      sm,
		pending: make(map[string]statemachine.CommandResult),
	}
}

// Propose immediately/synchronously applies data to the wired
// StateMachine and returns. There is no replication, no quorum, and no
// possibility of the proposal being lost or reordered — this is precisely
// the behavior a real multi-node Node must NOT have, which is why this
// type can never be more than a testing stand-in.
func (s *SingleNodeStub) Propose(ctx context.Context, data []byte) error {
	s.mu.Lock()
	s.nextIndex++
	entry := raft.LogEntry{
		Term:  1,
		Index: s.nextIndex,
		Type:  raft.EntryNormal,
		Data:  data,
	}
	s.mu.Unlock()

	result, err := s.sm.Apply(entry)
	if err != nil {
		return err
	}

	// Best-effort: if the payload looks like one of our commands, stash its
	// result for kvserver to pick up. Anything else (or a malformed
	// request) just doesn't get a correlated result, which is fine for
	// Put/Delete since their responses don't carry one.
	var cmd statemachine.Command
	if json.Unmarshal(data, &cmd) == nil && cmd.RequestID != "" {
		var res statemachine.CommandResult
		if json.Unmarshal(result, &res) == nil {
			s.pendingMu.Lock()
			s.pending[cmd.RequestID] = res
			s.pendingMu.Unlock()
		}
	}
	return nil
}

// takeResult retrieves and clears the commandResult recorded for
// requestID by the Propose call above. Because Propose is fully
// synchronous in this stub, the result is guaranteed to be present by the
// time Propose returns.
func (s *SingleNodeStub) takeResult(requestID string) (statemachine.CommandResult, bool) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	res, ok := s.pending[requestID]
	if ok {
		delete(s.pending, requestID)
	}
	return res, ok
}

// ProposeConfChange is not meaningful for a single, fixed-membership stub.
func (s *SingleNodeStub) ProposeConfChange(ctx context.Context, cc raft.ConfChange) error {
	return errors.New("stubnode: ProposeConfChange not supported by SingleNodeStub")
}

// ReadIndex is deliberately unimplemented: the real linearizable-read
// protocol (Workstream F) is Raft Core's responsibility and hadn't landed
// as of this stub being written. kvserver.go surfaces this error to
// clients as a gRPC Unimplemented status rather than pretending a
// single-node stub's local state is a meaningful stand-in for a
// majority-confirmed read index.
func (s *SingleNodeStub) ReadIndex(ctx context.Context, ctxToken []byte) error {
	return errors.New("stubnode: ReadIndex not implemented (Raft Core linearizable reads not yet available)")
}

// Step is a no-op: a single-node stub has no peers, so it never receives
// inbound Raft RPCs to step through.
func (s *SingleNodeStub) Step(ctx context.Context, msg raft.InboundMessage) error {
	return nil
}

// Ready never produces anything: Propose above applies synchronously
// instead of deferring work to a Ready loop, so there is nothing for a
// caller to drain here. A real Node's channel is never nil and must be
// drained continuously; this one is intentionally inert.
func (s *SingleNodeStub) Ready() <-chan raft.Ready {
	return nil
}

// Advance is a no-op; see Ready above.
func (s *SingleNodeStub) Advance() {}

// Tick is a no-op: no election or heartbeat timeouts exist here.
func (s *SingleNodeStub) Tick() {}

// Status reports this stub as its own, always-current leader.
func (s *SingleNodeStub) Status() raft.Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return raft.Status{
		ID:          s.id,
		Term:        1,
		Leader:      s.id,
		CommitIndex: s.nextIndex,
		IsLeader:    true,
	}
}
