// Package raft defines the interfaces that separate Raft's consensus logic
// from I/O concerns (disk persistence, network transport, and the
// application state machine). This separation is what lets the Storage
// Engine, Raft Core, and Client/API Protocol workstreams be built and
// tested independently against these interfaces before any of the real
// implementations exist.
package raft

import (
	"context"

	"github.com/SushantPotu/raft-kv-store/pkg/raftpb"
)

type NodeID string

type Term uint64

type LogIndex uint64

// ShardID identifies one independent Raft group in a Multi-Raft deployment.
// A single-shard deployment (Checkpoint 1) still has a ShardID — it's just
// the only one in play.
type ShardID string

type EntryType int

const (
	EntryNormal EntryType = iota
	EntryConfChange
)

// LogEntry is the in-process representation of one Raft log entry. It
// mirrors raftpb.LogEntry but is decoupled from the wire type so Raft Core
// never needs to import the gRPC-generated package directly.
type LogEntry struct {
	Term  Term
	Index LogIndex
	Type  EntryType
	Data  []byte
}

// HardState is the small subset of Raft state that must be persisted
// before responding to an RPC or acting on it (currentTerm, votedFor,
// commitIndex).
type HardState struct {
	Term      Term
	VotedFor  NodeID
	CommitIdx LogIndex
}

// ConfState is the set of nodes participating in consensus (voters).
// During a joint-consensus membership change this holds the joint
// configuration; see the membership-change design doc.
type ConfState struct {
	Voters []NodeID
}

// ConfChangeType distinguishes adding vs. removing a voter during a
// membership change (Workstream G).
type ConfChangeType int

const (
	ConfChangeAddNode ConfChangeType = iota
	ConfChangeRemoveNode
)

type ConfChange struct {
	Type   ConfChangeType
	NodeID NodeID
}

// Snapshot is a point-in-time, fully-compacted representation of the state
// machine plus the Raft metadata needed to resume replication from it.
type Snapshot struct {
	Data              []byte
	LastIncludedIndex LogIndex
	LastIncludedTerm  Term
	ConfState         ConfState
}

// Storage is durable persistence for one shard's Raft log and hard state.
//
// Owned by: Storage Engine workstream (internal/storage/raftlog).
// Consumed by: Raft Core (internal/raft), which never touches a file
// descriptor directly — every read/write of durable state goes through
// this interface, which is what makes Raft Core unit-testable purely
// in-memory via a fake.
type Storage interface {
	InitialState() (HardState, ConfState, error)
	// Entries returns log entries in [lo, hi), bounded by maxSize bytes.
	Entries(lo, hi LogIndex, maxSize uint64) ([]LogEntry, error)
	Term(i LogIndex) (Term, error)
	// FirstIndex is the index after the most recent compaction boundary —
	// entries before it only exist inside a Snapshot.
	FirstIndex() (LogIndex, error)
	LastIndex() (LogIndex, error)
	Append(entries []LogEntry) error
	SetHardState(hs HardState) error
	CreateSnapshot(index LogIndex, cs ConfState, data []byte) (Snapshot, error)
	ApplySnapshot(Snapshot) error
	Snapshot() (Snapshot, error)
}

// StateMachine applies committed log entries to the application (the KV
// engine, via the internal/statemachine adapter) and supports snapshotting
// so Raft can compact its log without losing the ability to bring a
// lagging or new replica up to date.
//
// Owned by: internal/statemachine (adapter wrapping internal/storage/engine.Engine).
// Consumed by: Raft Core, once an entry's index is <= the commit index.
type StateMachine interface {
	Apply(entry LogEntry) (result []byte, err error)
	Snapshot() ([]byte, error)
	RestoreSnapshot(data []byte) error
}

// Transport sends Raft RPCs to peers. Raft Core depends only on this
// interface — it never imports a gRPC package — so Raft Core's tests run
// against an in-memory fake with zero real network I/O.
//
// Owned by: Client/API Protocol Layer workstream (internal/transport/grpc).
// Consumed by: Raft Core.
type Transport interface {
	SendRequestVote(ctx context.Context, shard ShardID, target NodeID, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)
	SendAppendEntries(ctx context.Context, shard ShardID, target NodeID, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)
	SendInstallSnapshot(ctx context.Context, shard ShardID, target NodeID, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)
}

// Status is a read-only snapshot of a Node's current view of the world,
// used for observability (CloudWatch metrics, kvctl status, tests).
type Status struct {
	ID          NodeID
	Term        Term
	Leader      NodeID // empty if unknown
	CommitIndex LogIndex
	IsLeader    bool
}

// Ready bundles everything a Node produced during one step of progress
// that the caller (internal/shard.Manager) must act on: persist Entries
// and HardState (in that order, before doing anything else), send
// Messages to peers, and apply CommittedEntries to the StateMachine. This
// is the etcd/raft "Ready loop" shape — it keeps Node's internals fully
// synchronous and side-effect-free, which is what makes the deterministic
// simulation harness (internal/raft/simulate) possible.
type Ready struct {
	HardState       *HardState // nil if unchanged
	Entries         []LogEntry // newly appended, must be persisted before sending Messages
	CommittedEntries []LogEntry
	Messages        []Message
	Snapshot        *Snapshot
}

// Message is Node's internal representation of an outbound RPC, translated
// to a concrete raftpb request by internal/shard.Manager before handing it
// to Transport.
type Message struct {
	To      NodeID
	Shard   ShardID
	Kind    MessageKind
	Term    Term
	Payload any // one of *raftpb.RequestVoteRequest, *raftpb.AppendEntriesRequest, *raftpb.InstallSnapshotRequest
}

type MessageKind int

const (
	MsgRequestVote MessageKind = iota
	MsgAppendEntries
	MsgInstallSnapshot
)

// Node is the public API of one Raft group instance (one shard, one
// replica). internal/shard.Manager hosts N of these per process in a
// Multi-Raft deployment, keyed by ShardID.
//
// Owned by: Raft Core workstream (internal/raft).
// Consumed by: internal/shard.Manager, internal/server (via ReadIndex/Propose
// for client requests).
type Node interface {
	// Propose appends data to the log via consensus. Returns once the
	// proposal has been handed to the replication pipeline, not once it's
	// committed — callers needing the result should track it via the
	// StateMachine.Apply return value surfaced through Ready.CommittedEntries.
	Propose(ctx context.Context, data []byte) error
	ProposeConfChange(ctx context.Context, cc ConfChange) error
	// ReadIndex implements the linearizable-read protocol (Workstream F):
	// confirms current leadership via a majority heartbeat round, then
	// unblocks once safe to read at the leader's current commit index.
	ReadIndex(ctx context.Context, ctxToken []byte) error
	// Step feeds an inbound message (translated from a received RPC) into
	// the state machine.
	Step(ctx context.Context, msg InboundMessage) error
	// Ready delivers a Ready struct whenever there is new state to persist,
	// send, or apply. The caller must call Advance() after fully processing
	// one Ready before the next is produced.
	Ready() <-chan Ready
	Advance()
	// Tick drives logical time forward (election/heartbeat timeouts). The
	// caller controls the clock so tests can use a fake one via simulate.
	Tick()
	Status() Status
}

// InboundMessage is a received RPC translated into Node's internal
// vocabulary by internal/transport/grpc before calling Step.
type InboundMessage struct {
	From    NodeID
	Shard   ShardID
	Kind    MessageKind
	Payload any // one of *raftpb.RequestVoteRequest, *raftpb.AppendEntriesRequest, *raftpb.InstallSnapshotRequest
}
