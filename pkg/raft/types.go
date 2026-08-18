// Package raft defines the interfaces that separate Raft's consensus logic
// from I/O concerns (disk persistence, network transport, and the
// application state machine). This separation is what lets the Storage
// Engine, Raft Core, and Client/API Protocol workstreams be built and
// tested independently against these interfaces before any of the real
// implementations exist.
package raft

import (
	"context"
	"errors"

	"github.com/SushantPotu/raft-kv-store/pkg/raftpb"
)

// ErrNotLeader is the sentinel a Node implementation's Propose/
// ProposeConfChange must wrap (via fmt.Errorf("%w ...", ErrNotLeader, ...))
// when rejecting a call because this replica isn't the shard's current
// leader. It lives here, not in internal/raft, specifically so a caller
// like internal/server (which depends only on this package's interfaces,
// never on internal/raft directly — see this file's package doc comment)
// can distinguish "not leader, redirect the client via Status().Leader"
// from every other Propose failure with errors.Is, without needing to
// import the concrete Raft Core package.
var ErrNotLeader = errors.New("raft: not the leader")

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

// Transport delivers one outbound raft.Message to a peer.
//
// Design note (settled at Integration Checkpoint 1): this is deliberately
// NOT shaped as per-RPC-kind request/response methods (an earlier version
// was — SendRequestVote(req) (*resp, error), etc. — and that turned out to
// be a mismatch with how Node actually replies: a response to an inbound
// RequestVote or AppendEntries is just another outbound Message, queued in
// a *later* Ready() call, never a synchronous return from Step. See
// election.go/replication.go's n.send(from, kind, &raftpb.XxxResponse{...})
// calls, and proto/raftpb/raft.proto's RaftTransportService.Send doc
// comment for the full rationale). Send is fire-and-forget: its error
// return means only "the message could not be handed to the peer" (dial
// failure, etc.) — it carries no Raft-protocol information, because the
// real reply (if the message was a request) arrives later as its own,
// separate Send call in the other direction and is fed into the receiver's
// own Node via Step, exactly like every other inbound message.
//
// Owned by: Client/API Protocol Layer workstream (internal/transport/grpc).
// Consumed by: the Ready-loop driver (cmd/kvnode today; internal/shard.Manager
// once Multi-Raft sharding exists) — never by Raft Core itself, which only
// ever populates Ready.Messages and has no Transport reference of its own.
type Transport interface {
	// Send delivers msg to msg.To, in msg.Shard's namespace. msg carries its
	// own destination and shard, so callers never need to pass them
	// separately — the same Message the caller pulled out of Ready.Messages
	// is exactly what gets sent.
	Send(ctx context.Context, msg Message) error
	// SendInstallSnapshotChunk is split out from Send because a snapshot
	// transfer is chunked and, unlike every other message, its transport
	// binding is streaming rather than a single envelope — see
	// RaftTransportService.InstallSnapshot's proto doc comment.
	SendInstallSnapshotChunk(ctx context.Context, shard ShardID, target NodeID, chunk *raftpb.InstallSnapshotChunk) error
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
// that the caller (cmd/kvnode's Ready-loop driver today; internal/shard.Manager
// once Multi-Raft sharding exists) must act on: persist Entries and
// HardState (in that order, before doing anything else), send Messages to
// peers, and apply CommittedEntries to the StateMachine. This is inspired
// by etcd/raft's "Ready loop" shape but not identical to it: etcd/raft
// feeds a single long-lived channel from a background goroutine; this
// Node has no background goroutine at all — see Ready()'s own doc comment
// for the call-and-check contract that results. Either way, the effect is
// the same: Node's internals stay fully synchronous and side-effect-free,
// which is what makes the deterministic simulation harness
// (internal/raft/simulate) possible.
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
	// Payload is one of *raftpb.RequestVoteRequest, *raftpb.AppendEntriesRequest,
	// *raftpb.InstallSnapshotRequest — OR the corresponding *Response type.
	// Both requests and responses flow through this same Message/Step
	// plumbing (there is no separate response path — see
	// internal/raft/simulate.Cluster.drainReady, which relays Ready.Messages
	// straight into the destination's Step either way). Node implementations
	// disambiguate by the concrete Go type of Payload, not by MessageKind
	// alone, since MessageKind only names the RPC, not the direction.
	Payload any
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
	// If this replica isn't the shard's current leader, Propose returns an
	// error wrapping ErrNotLeader instead of attempting anything — callers
	// (e.g. internal/server.KVServer) check for that specifically with
	// errors.Is and redirect the client via Status().Leader rather than
	// surfacing a generic failure.
	Propose(ctx context.Context, data []byte) error
	ProposeConfChange(ctx context.Context, cc ConfChange) error
	// ReadIndex implements the linearizable-read protocol (Workstream F):
	// confirms current leadership via a majority heartbeat round, then
	// unblocks once safe to read at the leader's current commit index.
	ReadIndex(ctx context.Context, ctxToken []byte) error
	// Step feeds an inbound message (translated from a received RPC) into
	// the state machine.
	Step(ctx context.Context, msg InboundMessage) error
	// Ready is call-and-check, not a long-lived channel to range over: each
	// call synchronously packages up whatever accumulated since the last
	// Advance() (there is no background goroutine feeding it) into a
	// length-1 channel, already-filled if there's work or left empty
	// forever otherwise. Callers poll it with a non-blocking receive —
	// `select { case rd := <-node.Ready(): ...; default: }` — exactly as
	// internal/raft/simulate.Cluster.drainReady and cmd/kvnode's Ready-loop
	// driver both do. The caller must call Advance() after fully processing
	// one non-empty Ready before the next call to Ready() will produce one.
	Ready() <-chan Ready
	Advance()
	// Tick drives logical time forward (election/heartbeat timeouts). The
	// caller controls the clock so tests can use a fake one via simulate.
	Tick()
	Status() Status
}

// InboundMessage is a received RPC translated into Node's internal
// vocabulary by internal/transport/grpc before calling Step. As with
// Message.Payload above, this can be either a request or a response type —
// a Node receives peers' responses via Step too, not just their requests.
type InboundMessage struct {
	From    NodeID
	Shard   ShardID
	Kind    MessageKind
	Payload any
}
