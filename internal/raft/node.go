// Package raftcore implements raft.Node (pkg/raft.Node): leader election
// and log replication, built from "In Search of an Understandable
// Consensus Algorithm" (Ongaro & Ousterhout), not wrapped around an
// existing Raft library. It depends only on the pkg/raft interfaces
// (Storage, Transport, StateMachine) plus the Go standard library, and
// never touches a file descriptor, a socket, or real wall-clock time
// itself — see docs/adr/0001-raft-core-io-separation.md.
//
// The package is named raftcore rather than raft (its directory's base
// name) specifically so that code importing both this package and
// pkg/raft — which itself is (necessarily) named raft — doesn't need an
// import alias on every call site.
//
// Scope: leader election and log replication only. Snapshotting/log
// compaction, dynamic membership (joint consensus), and linearizable
// ReadIndex reads are separate, later workstreams; see snapshot.go,
// membership.go, and readindex.go for their stub entry points.
package raftcore

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
	"github.com/SushantPotu/raft-kv-store/pkg/raftpb"
)

// Default tick-based timing. All of Raft Core's notion of time is these
// logical ticks, driven exclusively by external Tick() calls (never a
// real timer) — see Node.Tick. Callers running a real clock decide how
// often to call Tick(); callers running a simulation (internal/raft/
// simulate) call it once per simulated round.
const (
	DefaultElectionTimeoutMinTicks = 150
	DefaultElectionTimeoutMaxTicks = 300
	DefaultHeartbeatIntervalTicks  = 20
)

// ErrNotLeader re-exports raft.ErrNotLeader for convenience within this
// package (and for any caller that prefers importing raftcore over
// pkg/raft for it) — it is the exact same sentinel, per raft.ErrNotLeader's
// doc comment on why the canonical definition lives in pkg/raft instead of
// here.
var ErrNotLeader = raft.ErrNotLeader

// Option configures optional Node behavior at construction time.
type Option func(*Node)

// WithElectionTimeoutTicks overrides the randomized election-timeout range
// (in logical ticks). Defaults to [150, 300], the range suggested by the
// Raft paper's own experiments (§9.3) scaled to "ticks" rather than
// milliseconds.
func WithElectionTimeoutTicks(min, max int) Option {
	return func(n *Node) {
		n.electionTimeoutMin = min
		n.electionTimeoutMax = max
	}
}

// WithHeartbeatIntervalTicks overrides how often (in logical ticks) a
// leader sends heartbeats. Must be well below the minimum election
// timeout or followers will spuriously start elections.
func WithHeartbeatIntervalTicks(ticks int) Option {
	return func(n *Node) { n.heartbeatInterval = ticks }
}

// WithRandSource overrides the source of randomness used for election
// timeout jitter. Tests that need fully reproducible multi-node runs
// should pass a distinctly seeded *rand.Rand per node (e.g. derived from a
// single test seed plus the node's index) rather than relying on the
// default, which is deterministic per NodeID but not parameterized by an
// externally chosen seed.
func WithRandSource(r *rand.Rand) Option {
	return func(n *Node) { n.rng = r }
}

// WithSnapshotThreshold overrides how many log entries (since the last
// compaction) accumulate before Node automatically snapshots and compacts
// its log (see snapshot.go's maybeSnapshotLocked). n <= 0 disables
// automatic snapshotting entirely. Defaults to DefaultSnapshotThreshold.
func WithSnapshotThreshold(n int) Option {
	return func(node *Node) { node.snapshotThreshold = n }
}

// DefaultSnapshotThreshold is the default number of applied-but-not-yet-
// compacted log entries that triggers an automatic snapshot.
const DefaultSnapshotThreshold = 1000

// Node implements raft.Node. All exported methods take n.mu, do their
// work synchronously, and return — there are no goroutines and no
// background timers anywhere in this type, per ADR 0001.
type Node struct {
	mu sync.Mutex

	id    raft.NodeID
	shard raft.ShardID
	// peers is the effective replication/voting target set: every other
	// voting member currently in play, excluding id. Under normal
	// operation this is a single static configuration; while a joint
	// membership change is in flight (see membership.go) it is the union
	// of the old and new configurations, so replication/heartbeats reach
	// everyone whose ack might be needed, while joint (the non-nil
	// jointConfig below) narrows majority/commit math back down to
	// requiring both halves independently.
	peers []raft.NodeID
	// baselinePeers is the last known-good *single* configuration: either
	// the constructor's peers argument (genesis) or whatever ConfState a
	// snapshot most recently recorded. recomputeConfigFromLogLocked (called
	// whenever entries arrive via AppendEntries) replays baselinePeers plus
	// every EntryConfChange currently in the in-memory log to rebuild
	// peers/joint from scratch, which is what makes an uncommitted conf
	// change get correctly rolled back if a leader change truncates it away.
	baselinePeers []raft.NodeID
	// joint is non-nil exactly while a C_old,new joint configuration
	// (paper §6) is in effect for this node.
	joint *jointConfig
	// confChangeIndex is the log index of the most recently observed
	// EntryConfChange entry (whichever of this node's own ProposeConfChange
	// calls, or one received via AppendEntries) currently governs the
	// config. It serves two purposes: (1) ProposeConfChange refuses to
	// start a new change while commitIndex hasn't yet reached it (Raft's
	// usual "one configuration change at a time" discipline), and (2)
	// while joint != nil, maybeAdvanceCommitLocked watches for commitIndex
	// to reach it to know when it's safe to auto-propose the final C_new
	// entry.
	confChangeIndex raft.LogIndex

	// snapshotThreshold and pendingSnapshot drive snapshot.go's
	// compaction/InstallSnapshot logic; see maybeSnapshotLocked and
	// handleInstallSnapshotLocked.
	snapshotThreshold int
	pendingSnapshot   *raft.Snapshot
	outSnapshotPosted bool

	// pendingReads holds every in-flight ReadIndex call this Node (as
	// leader) hasn't yet resolved; see readindex.go.
	pendingReads []*pendingReadIndex

	storage raft.Storage
	// transport is stored only for API-shape parity with how
	// internal/shard.Manager is expected to construct a Node; Node itself
	// never calls it. Per ADR 0001, outbound RPCs are surfaced through
	// Ready.Messages and it is the *caller's* job to translate them into
	// raftpb requests and invoke Transport — Node has no I/O of its own.
	transport raft.Transport
	sm        raft.StateMachine

	log *raftLog

	role     Role
	term     raft.Term
	votedFor raft.NodeID
	leader   raft.NodeID // "" if unknown

	commitIndex  raft.LogIndex
	appliedIndex raft.LogIndex // last index handed to the caller via Ready.CommittedEntries

	// Candidate-only state.
	votesGranted map[raft.NodeID]bool

	// Leader-only state (paper Figure 2).
	nextIndex      map[raft.NodeID]raft.LogIndex
	matchIndex     map[raft.NodeID]raft.LogIndex
	appendInflight map[raft.NodeID]raft.LogIndex // see handleAppendEntriesResponseLocked
	replicating    map[raft.NodeID]bool          // true while a peer has an unacknowledged AppendEntries in flight; see broadcastAppendEntriesLocked

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int

	electionTimeoutMin int
	electionTimeoutMax int
	heartbeatInterval  int

	rng *rand.Rand

	outbox  []raft.Message
	hsDirty bool

	// Bookkeeping for the Ready()/Advance() handshake: once Ready() hands
	// out a non-empty Ready, readyOutstanding is set and these record how
	// far the in-flight Ready's Entries/CommittedEntries went, so Advance()
	// knows exactly what to mark stable/applied — deliberately *not* just
	// "whatever the live state is now", in case (contract permitting) more
	// change happened in between.
	readyOutstanding   bool
	outEntriesUpTo     raft.LogIndex
	outCommittedUpTo   raft.LogIndex
	outHardStatePosted bool
}

var _ raft.Node = (*Node)(nil)

// NewNode constructs a Node for one shard replica. peers must list every
// other voting member (not including id); storage must already have been
// opened/recovered by the caller. Node reads storage.InitialState() and
// the persisted log once, at construction, to resume after a restart —
// after that it keeps its own in-memory view (log.go) and never reads
// Storage again, relying entirely on the caller persisting Ready.Entries/
// Ready.HardState per the Ready-loop contract.
func NewNode(id raft.NodeID, shard raft.ShardID, peers []raft.NodeID, storage raft.Storage, transport raft.Transport, sm raft.StateMachine, opts ...Option) raft.Node {
	n := &Node{
		id:                 id,
		shard:              shard,
		peers:              append([]raft.NodeID(nil), peers...),
		baselinePeers:      append([]raft.NodeID(nil), peers...),
		storage:            storage,
		transport:          transport,
		sm:                 sm,
		role:               RoleFollower,
		electionTimeoutMin: DefaultElectionTimeoutMinTicks,
		electionTimeoutMax: DefaultElectionTimeoutMaxTicks,
		heartbeatInterval:  DefaultHeartbeatIntervalTicks,
		snapshotThreshold:  DefaultSnapshotThreshold,
	}
	for _, opt := range opts {
		opt(n)
	}
	if n.rng == nil {
		n.rng = rand.New(rand.NewSource(defaultSeedFor(id)))
	}

	if hs, cs, err := storage.InitialState(); err == nil {
		n.term = hs.Term
		n.votedFor = hs.VotedFor
		n.commitIndex = hs.CommitIdx
		if len(cs.Voters) > 0 {
			// A prior snapshot recorded membership; it supersedes the
			// constructor's peers argument (which only matters as the
			// genesis config for a brand-new cluster).
			n.baselinePeers = removeSelf(cs.Voters, id)
			n.peers = append([]raft.NodeID(nil), n.baselinePeers...)
		}
	}
	n.log = newRaftLog(storage)
	// Anything at or before the log's base (0 unless storage already held
	// a snapshot at construction time — i.e. this is a restart, not a
	// brand-new node) is, by definition of what a snapshot is, already
	// reflected in the state machine: the caller is expected to have
	// restored it from durable storage of its own (see snapshot.go's
	// package doc comment on Storage/StateMachine persistence being the
	// caller's responsibility, same as for a *received* InstallSnapshot).
	// Starting appliedIndex at 0 regardless of that would make the very
	// first Ready() re-deliver entries already baked into that restored
	// state as CommittedEntries, double-applying them.
	if n.log.base > n.appliedIndex {
		n.appliedIndex = n.log.base
	}
	// Replay any EntryConfChange entries already sitting in the recovered
	// log on top of baselinePeers, so a restart resumes with the right
	// membership (including an in-flight joint config, if one was never
	// finalized before the crash).
	n.recomputeConfigFromLogLocked()
	n.resetElectionTimeoutLocked()

	return n
}

// defaultSeedFor derives a deterministic (but distinct per node) default
// PRNG seed from a NodeID, so that a Node constructed without an explicit
// WithRandSource still behaves reproducibly across runs without depending
// on wall-clock time.
func defaultSeedFor(id raft.NodeID) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	return int64(h.Sum64())
}

// send queues an outbound message for the next Ready().
func (n *Node) send(to raft.NodeID, kind raft.MessageKind, payload any) {
	n.outbox = append(n.outbox, raft.Message{
		To:      to,
		Shard:   n.shard,
		Kind:    kind,
		Term:    n.term,
		Payload: payload,
	})
}

// Propose appends data to the leader's log and immediately fans it out via
// AppendEntries; it does not wait for commit (see the raft.Node.Propose
// doc comment) — commit is observed by the caller via a later Ready's
// CommittedEntries.
func (n *Node) Propose(ctx context.Context, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != RoleLeader {
		return fmt.Errorf("%w (leader is %q)", ErrNotLeader, n.leader)
	}
	n.log.appendNew([]raft.LogEntry{{Term: n.term, Type: raft.EntryNormal, Data: data}})
	// force=false: don't send a second, overlapping request to a peer
	// that already has one outstanding — see broadcastAppendEntriesLocked.
	n.broadcastAppendEntriesLocked(false)
	n.maybeAdvanceCommitLocked() // covers the single-node-cluster case
	return nil
}

// ProposeConfChange implements dynamic membership changes via joint
// consensus (paper §6); see membership.go for the full implementation.
func (n *Node) ProposeConfChange(ctx context.Context, cc raft.ConfChange) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.proposeConfChangeLocked(cc)
}

// ReadIndex implements the linearizable-read protocol (paper §8); see
// readindex.go for the full implementation.
func (n *Node) ReadIndex(ctx context.Context, ctxToken []byte) error {
	return n.readIndex(ctx, ctxToken)
}

// Step feeds one inbound RPC (or RPC reply — see the package-level note in
// replication.go/election.go about how request/response are disambiguated
// by payload type, not by MessageKind) into the state machine.
func (n *Node) Step(ctx context.Context, msg raft.InboundMessage) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	switch msg.Kind {
	case raft.MsgRequestVote:
		switch p := msg.Payload.(type) {
		case *raftpb.RequestVoteRequest:
			n.handleRequestVoteLocked(msg.From, p)
		case *raftpb.RequestVoteResponse:
			n.handleRequestVoteResponseLocked(msg.From, p)
		default:
			return fmt.Errorf("raftcore: MsgRequestVote with unexpected payload type %T", msg.Payload)
		}
	case raft.MsgAppendEntries:
		switch p := msg.Payload.(type) {
		case *raftpb.AppendEntriesRequest:
			n.handleAppendEntriesLocked(msg.From, p)
		case *raftpb.AppendEntriesResponse:
			n.handleAppendEntriesResponseLocked(msg.From, p)
		default:
			return fmt.Errorf("raftcore: MsgAppendEntries with unexpected payload type %T", msg.Payload)
		}
	case raft.MsgInstallSnapshot:
		switch p := msg.Payload.(type) {
		case *raftpb.InstallSnapshotChunk:
			n.handleInstallSnapshotLocked(msg.From, p)
		case *raftpb.InstallSnapshotResponse:
			n.handleInstallSnapshotResponseLocked(msg.From, p)
		default:
			return fmt.Errorf("raftcore: MsgInstallSnapshot with unexpected payload type %T", msg.Payload)
		}
	default:
		return fmt.Errorf("raftcore: unknown message kind %v", msg.Kind)
	}
	return nil
}

// Tick drives logical time forward by one unit: followers/candidates count
// toward an election timeout, leaders count toward their next heartbeat.
func (n *Node) Tick() {
	n.mu.Lock()
	defer n.mu.Unlock()

	switch n.role {
	case RoleLeader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatInterval {
			n.heartbeatElapsed = 0
			// force=true: this is the periodic heartbeat, so resend to
			// every peer even if one still has a request outstanding —
			// see broadcastAppendEntriesLocked.
			n.broadcastAppendEntriesLocked(true)
		}
	default: // Follower or Candidate
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			n.startElectionLocked()
		}
	}
}

// Ready delivers a Ready struct exactly when there is new state to
// persist, send, or apply, on a channel that already has (at most) one
// value buffered — Cluster.Step (internal/raft/simulate) and any real
// caller are expected to read it with a non-blocking receive. Because
// Node's internals are single-threaded and synchronous, this is
// implemented without any background goroutine: whatever accumulated in
// outbox/log/commitIndex since the last Advance() is packaged up right
// here, on the calling goroutine.
func (n *Node) Ready() <-chan raft.Ready {
	n.mu.Lock()
	defer n.mu.Unlock()

	ch := make(chan raft.Ready, 1)
	if n.readyOutstanding {
		// Caller didn't Advance() the previous Ready yet; per the
		// interface contract that shouldn't happen, but fail safe rather
		// than hand out a second, possibly-inconsistent Ready.
		return ch
	}

	var rd raft.Ready
	haveWork := false

	if n.hsDirty {
		hs := raft.HardState{Term: n.term, VotedFor: n.votedFor, CommitIdx: n.commitIndex}
		rd.HardState = &hs
		n.outHardStatePosted = true
		haveWork = true
	}

	if newEntries := n.log.unstableEntries(); len(newEntries) > 0 {
		rd.Entries = newEntries
		n.outEntriesUpTo = newEntries[len(newEntries)-1].Index
		haveWork = true
	}

	if n.commitIndex > n.appliedIndex {
		committed := n.log.entriesFrom(n.appliedIndex + 1)
		// entriesFrom returns everything to the tail; clip to commitIndex.
		clipped := committed[:0]
		for _, e := range committed {
			if e.Index > n.commitIndex {
				break
			}
			clipped = append(clipped, e)
		}
		if len(clipped) > 0 {
			rd.CommittedEntries = clipped
			n.outCommittedUpTo = clipped[len(clipped)-1].Index
			haveWork = true
		}
	}

	if len(n.outbox) > 0 {
		rd.Messages = n.outbox
		n.outbox = nil
		haveWork = true
	}

	// A snapshot was just installed via Step (handleInstallSnapshotLocked):
	// surface it so the Ready-loop driver actually persists it
	// (Storage.ApplySnapshot) and restores the state machine
	// (StateMachine.RestoreSnapshot) — see that function's doc comment for
	// the full contract on who's responsible for what.
	if n.pendingSnapshot != nil {
		rd.Snapshot = n.pendingSnapshot
		n.outSnapshotPosted = true
		haveWork = true
	}

	if haveWork {
		n.readyOutstanding = true
		ch <- rd
	}
	return ch
}

// Advance tells Node the most recently delivered Ready has been fully
// processed (persisted, sent, applied), per the raft.Node contract.
func (n *Node) Advance() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.readyOutstanding {
		return
	}
	if n.outEntriesUpTo > n.log.stableIndex {
		n.log.stableIndex = n.outEntriesUpTo
	}
	if n.outCommittedUpTo > n.appliedIndex {
		n.appliedIndex = n.outCommittedUpTo
	}
	if n.outHardStatePosted {
		n.hsDirty = false
	}
	if n.outSnapshotPosted {
		n.pendingSnapshot = nil
		n.outSnapshotPosted = false
	}
	n.readyOutstanding = false
	n.outEntriesUpTo = 0
	n.outCommittedUpTo = 0
	n.outHardStatePosted = false

	// Once the caller has (per the Advance contract) fully processed this
	// Ready — including applying CommittedEntries to the state machine —
	// it's safe to snapshot at the new appliedIndex, since Snapshot() below
	// will reflect that application.
	n.maybeSnapshotLocked()
}

// Status returns a read-only snapshot of this Node's current view.
func (n *Node) Status() raft.Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return raft.Status{
		ID:          n.id,
		Term:        n.term,
		Leader:      n.leader,
		CommitIndex: n.commitIndex,
		IsLeader:    n.role == RoleLeader,
	}
}
