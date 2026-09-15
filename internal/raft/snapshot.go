package raftcore

import (
	"github.com/SushantPotu/raft-kv-store/pkg/raft"
	"github.com/SushantPotu/raft-kv-store/pkg/raftpb"
)

// This file implements the snapshotting/log-compaction workstream: a
// size/interval-based trigger that compacts the leader's (or any node's)
// log once it grows past snapshotThreshold applied-but-uncompacted
// entries, sending an InstallSnapshot RPC to bring a peer whose required
// entries have already been compacted away back up to date, and applying
// one received from a leader.
//
// Contract for who applies a snapshot's *data*, to avoid double-applying
// it once via Step and once via the Ready-loop driver: Node itself never
// calls Storage.ApplySnapshot or StateMachine.RestoreSnapshot for a
// snapshot it *receives* (handleInstallSnapshotLocked below) — it only
// updates its own in-memory bookkeeping (the log view, commitIndex,
// appliedIndex, membership) and stages the snapshot on rd.Snapshot for the
// next Ready(). The Ready-loop driver (cmd/kvnode's handleReady, which
// already contains exactly this "if rd.Snapshot != nil { ApplySnapshot;
// RestoreSnapshot }" logic and is not modified here) is the sole caller of
// those two methods for a received snapshot. For a snapshot Node creates
// itself (maybeSnapshotLocked below, the proactive-compaction path), Node
// *does* call Storage.CreateSnapshot directly and synchronously, the same
// way it already calls storage.InitialState() at construction — there's no
// Ready-mediated path for "please persist a snapshot I just made" the way
// there is for entries/HardState, so a direct call is the only option and
// mirrors how the rest of Storage's synchronous, non-Ready-mediated calls
// (InitialState, and log.go's use of Entries/FirstIndex/LastIndex) work.

// maybeSnapshotLocked checks whether the log has grown past
// snapshotThreshold entries since the last compaction and, if so, snapshots
// the state machine and compacts the in-memory log to match. Called at the
// end of Advance(), once the caller is guaranteed to have already applied
// every CommittedEntries up to n.appliedIndex (per the Ready/Advance
// contract), which is what makes it safe to ask the state machine for its
// current serialized state right now.
//
// Must be called with n.mu held.
func (n *Node) maybeSnapshotLocked() {
	if n.snapshotThreshold <= 0 || n.sm == nil || n.storage == nil || n.appliedIndex == 0 {
		return
	}
	first, err := n.storage.FirstIndex()
	if err != nil {
		return
	}
	compactedThrough := first - 1
	if n.appliedIndex <= compactedThrough {
		return
	}
	if uint64(n.appliedIndex-compactedThrough) < uint64(n.snapshotThreshold) {
		return
	}

	data, err := n.sm.Snapshot()
	if err != nil {
		return
	}
	cs := n.confStateLocked()
	snap, err := n.storage.CreateSnapshot(n.appliedIndex, cs, data)
	if err != nil {
		return
	}
	n.log.compactTo(snap.LastIncludedIndex, snap.LastIncludedTerm)
}

// confStateLocked returns this node's current view of the cluster's voting
// membership as a raft.ConfState, for persisting alongside a snapshot.
//
// Limitation: while a joint (C_old,new) configuration is in effect
// (n.joint != nil, see membership.go), this flattens to the union of both
// halves (exactly what n.peers already holds during the joint phase) —
// ConfState has no way to record "this was a joint config" distinctly from
// a plain one. A node that restores from a snapshot taken mid-joint-change
// therefore resumes believing it's in a single (unioned) configuration
// rather than resuming the joint phase precisely; the in-flight
// configuration change itself is not lost (any EntryConfChange entries
// after the snapshot boundary are still replayed normally by
// recomputeConfigFromLogLocked), only the "requires majority in both
// halves independently" joint-safety window across that specific snapshot
// boundary is not reconstructed. Taking a snapshot mid-configuration-change
// is an edge case rare enough in this project's scope that this is
// documented rather than fully solved.
func (n *Node) confStateLocked() raft.ConfState {
	voters := make([]raft.NodeID, 0, len(n.peers)+1)
	voters = append(voters, n.id)
	voters = append(voters, n.peers...)
	return raft.ConfState{Voters: voters}
}

// sendInstallSnapshotToLocked sends the leader's current Storage snapshot
// to peer p as a single-chunk transfer: the snapshot's data is already
// fully materialized in memory (Storage.Snapshot()), so there is no
// streaming/reassembly concern for Raft Core to handle here — Done is
// always true. A real multi-chunk transport-layer optimization (for very
// large snapshots) would live in internal/transport/grpc, not here.
//
// Must be called with n.mu held, and only while n.role == RoleLeader.
func (n *Node) sendInstallSnapshotToLocked(p raft.NodeID) {
	snap, err := n.storage.Snapshot()
	if err != nil || (snap.LastIncludedIndex == 0 && snap.Data == nil) {
		// No snapshot exists yet to send. This shouldn't normally happen —
		// termAt just reported prevIdx as compacted away, which implies a
		// snapshot exists — but fail safe rather than send a bogus,
		// zero-valued InstallSnapshot.
		return
	}
	n.replicating[p] = true
	// A snapshot transfer isn't tracked via appendInflight/matchIndex the
	// way an AppendEntries is; handleInstallSnapshotResponseLocked updates
	// matchIndex/nextIndex directly from Storage.Snapshot() once acked.
	delete(n.appendInflight, p)
	n.send(p, raft.MsgInstallSnapshot, &raftpb.InstallSnapshotChunk{
		ShardId:           string(n.shard),
		Term:              uint64(n.term),
		LeaderId:          string(n.id),
		LastIncludedIndex: uint64(snap.LastIncludedIndex),
		LastIncludedTerm:  uint64(snap.LastIncludedTerm),
		ConfState:         toPBConfState(snap.ConfState),
		Offset:            0,
		Data:              snap.Data,
		Done:              true,
	})
}

// handleInstallSnapshotLocked implements the InstallSnapshot RPC receiver
// rules (paper §7), single-chunk only (see this file's package doc
// comment). Term handling mirrors handleAppendEntriesLocked: reject (by
// replying with our own, higher term) if stale, otherwise adopt the sender
// as leader.
//
// Must be called with n.mu held.
func (n *Node) handleInstallSnapshotLocked(from raft.NodeID, req *raftpb.InstallSnapshotChunk) {
	term := raft.Term(req.Term)
	if term < n.term {
		n.send(from, raft.MsgInstallSnapshot, &raftpb.InstallSnapshotResponse{
			FollowerId: string(n.id),
			Term:       uint64(n.term),
		})
		return
	}
	if term > n.term || n.role != RoleFollower {
		n.becomeFollowerLocked(term, from)
	} else {
		n.leader = from
		n.resetElectionTimeoutLocked()
	}

	if n.pendingSnapshot != nil {
		// Already have one staged, not yet drained via Ready/Advance —
		// ignore this one (a duplicate resend, or an overlapping second
		// InstallSnapshot) rather than clobber it; the leader will resend
		// once it sees this peer make progress again.
		return
	}

	lastIdx := raft.LogIndex(req.LastIncludedIndex)
	lastTerm := raft.Term(req.LastIncludedTerm)

	// A stale or duplicate resend of a snapshot we've already caught up to
	// (or past) — nothing to do beyond acking.
	if lastIdx <= n.log.base {
		n.send(from, raft.MsgInstallSnapshot, &raftpb.InstallSnapshotResponse{
			FollowerId: string(n.id),
			Term:       uint64(n.term),
		})
		return
	}

	cs := fromPBConfState(req.ConfState)

	// Stage the snapshot for the Ready-loop driver to actually persist
	// (Storage.ApplySnapshot) and apply to the state machine
	// (StateMachine.RestoreSnapshot) — see this file's package doc comment
	// for why Node itself doesn't call those directly here.
	snap := raft.Snapshot{
		Data:              req.Data,
		LastIncludedIndex: lastIdx,
		LastIncludedTerm:  lastTerm,
		ConfState:         cs,
	}
	n.pendingSnapshot = &snap

	// Discard whatever log we had (it's either a strict prefix of, or in
	// conflict with, the snapshot) and resume from the snapshot boundary.
	n.log.resetToSnapshot(lastIdx, lastTerm)
	if lastIdx > n.commitIndex {
		n.commitIndex = lastIdx
		n.hsDirty = true
	}
	// appliedIndex is bumped now, trusting the Ready-loop driver to have
	// actually restored the state machine by the time it calls Advance()
	// (or relies on any later CommittedEntries) — the same trust relation
	// Advance() already has with the driver for ordinary entries.
	if lastIdx > n.appliedIndex {
		n.appliedIndex = lastIdx
	}

	n.recomputeConfigFromSnapshotLocked(cs)

	n.send(from, raft.MsgInstallSnapshot, &raftpb.InstallSnapshotResponse{
		FollowerId: string(n.id),
		Term:       uint64(n.term),
	})
}

// handleInstallSnapshotResponseLocked processes a follower's ack of an
// InstallSnapshot. Unlike AppendEntriesResponse there's no separate
// success/failure flag — reaching this point at all (term check already
// passed) means the follower accepted it — so this always advances
// matchIndex/nextIndex based on whatever snapshot is currently in Storage.
//
// Must be called with n.mu held.
func (n *Node) handleInstallSnapshotResponseLocked(from raft.NodeID, resp *raftpb.InstallSnapshotResponse) {
	term := raft.Term(resp.Term)
	if term > n.term {
		n.becomeFollowerLocked(term, "")
		return
	}
	if n.role != RoleLeader || term != n.term {
		return
	}
	n.replicating[from] = false
	n.satisfyPendingReadsLocked(from)

	snap, err := n.storage.Snapshot()
	if err != nil {
		return
	}
	if snap.LastIncludedIndex+1 > n.nextIndex[from] {
		n.nextIndex[from] = snap.LastIncludedIndex + 1
	}
	if snap.LastIncludedIndex > n.matchIndex[from] {
		n.matchIndex[from] = snap.LastIncludedIndex
	}
	n.maybeAdvanceCommitLocked()
	if n.nextIndex[from] <= n.log.lastIndex() {
		n.sendAppendEntriesToLocked(from)
	}
}

func toPBConfState(cs raft.ConfState) *raftpb.ConfState {
	voters := make([]string, len(cs.Voters))
	for i, v := range cs.Voters {
		voters[i] = string(v)
	}
	return &raftpb.ConfState{Voters: voters}
}

func fromPBConfState(cs *raftpb.ConfState) raft.ConfState {
	if cs == nil {
		return raft.ConfState{}
	}
	voters := make([]raft.NodeID, len(cs.Voters))
	for i, v := range cs.Voters {
		voters[i] = raft.NodeID(v)
	}
	return raft.ConfState{Voters: voters}
}
