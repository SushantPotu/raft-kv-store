package raftcore

import (
	"sort"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
	"github.com/SushantPotu/raft-kv-store/pkg/raftpb"
)

// broadcastAppendEntriesLocked sends an AppendEntries RPC to peers,
// carrying whatever entries that peer is currently behind on (possibly
// none, i.e. a heartbeat).
//
// force controls whether a peer that already has an unacknowledged
// request outstanding gets sent to again anyway:
//
//   - force=true is used for the periodic heartbeat (Tick) and the
//     initial broadcast on becoming leader. Since this only fires once
//     every heartbeatInterval ticks, treating any previous request to
//     that peer as abandoned and resending is safe in practice — a live
//     peer will have long since replied.
//   - force=false is used for a send triggered by a fresh client Propose:
//     a peer with a request already in flight is deliberately skipped
//     rather than sent a *second*, overlapping request. It will pick up
//     the new entry once its current request is acknowledged (see the
//     pipelining continuation in handleAppendEntriesResponseLocked) or on
//     the next heartbeat.
//
// This single-outstanding-request-per-peer discipline exists because
// raftpb.AppendEntriesResponse carries no field correlating it back to a
// specific request (see proto/raftpb/raft.proto): if two requests to the
// same peer were ever in flight at once, a response to the *older, smaller*
// one arriving after a *newer, larger* one was already sent would be
// indistinguishable from a response to the newer one, and
// handleAppendEntriesResponseLocked could credit matchIndex with entries
// the peer was never actually shown. Enforcing at most one outstanding
// request per peer sidesteps that ambiguity entirely instead of trying to
// resolve it after the fact.
//
// Must be called with n.mu held, and only while n.role == RoleLeader.
func (n *Node) broadcastAppendEntriesLocked(force bool) {
	if n.role != RoleLeader {
		return
	}
	for _, p := range n.peers {
		if force || !n.replicating[p] {
			n.sendAppendEntriesToLocked(p)
		}
	}
}

// sendAppendEntriesToLocked sends one AppendEntries RPC to peer p based on
// its current nextIndex, and records how far this request would bring p
// (if fully accepted) in appendInflight so the eventual response can
// update matchIndex correctly (see handleAppendEntriesResponseLocked), and
// marks p as having a request outstanding (see broadcastAppendEntriesLocked).
//
// Must be called with n.mu held, and only while n.role == RoleLeader.
func (n *Node) sendAppendEntriesToLocked(p raft.NodeID) {
	next := n.nextIndex[p]
	if next < 1 {
		next = 1
	}
	prevIdx := next - 1
	prevTerm, ok := n.log.termAt(prevIdx)
	if !ok {
		// prevIdx has been compacted away. Snapshotting isn't implemented
		// yet (see snapshot.go) so there's nothing better to do here; a
		// real InstallSnapshot path would take over at this point.
		prevTerm = 0
	}
	entries := n.log.entriesFrom(next)

	n.appendInflight[p] = prevIdx + raft.LogIndex(len(entries))
	n.replicating[p] = true
	n.send(p, raft.MsgAppendEntries, &raftpb.AppendEntriesRequest{
		ShardId:      string(n.shard),
		Term:         uint64(n.term),
		LeaderId:     string(n.id),
		PrevLogIndex: uint64(prevIdx),
		PrevLogTerm:  uint64(prevTerm),
		Entries:      toPBEntries(entries),
		LeaderCommit: uint64(n.commitIndex),
	})
}

// handleAppendEntriesLocked implements the AppendEntries RPC receiver
// rules from paper Figure 2.
//
// Must be called with n.mu held.
func (n *Node) handleAppendEntriesLocked(from raft.NodeID, req *raftpb.AppendEntriesRequest) {
	term := raft.Term(req.Term)

	// 1. Reply false if term < currentTerm.
	if term < n.term {
		n.send(from, raft.MsgAppendEntries, &raftpb.AppendEntriesResponse{
			Term:       uint64(n.term),
			Success:    false,
			LeaderHint: string(n.leader),
		})
		return
	}

	// A valid AppendEntries at term >= ours means `from` is the (or *a*)
	// legitimate leader for this term; adopt it. This also handles a
	// Candidate discovering its election was actually won by someone else
	// in the same term (paper §5.2).
	if term > n.term || n.role != RoleFollower {
		n.becomeFollowerLocked(term, from)
	} else {
		n.leader = from
		n.resetElectionTimeoutLocked()
	}

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose
	// term matches prevLogTerm.
	prevIdx := raft.LogIndex(req.PrevLogIndex)
	prevTerm := raft.Term(req.PrevLogTerm)
	if myTerm, ok := n.log.termAt(prevIdx); !ok || myTerm != prevTerm {
		conflictIdx, conflictTerm := n.log.conflictHint(prevIdx)
		n.send(from, raft.MsgAppendEntries, &raftpb.AppendEntriesResponse{
			Term:          uint64(n.term),
			Success:       false,
			ConflictIndex: uint64(conflictIdx),
			ConflictTerm:  uint64(conflictTerm),
			LeaderHint:    string(n.leader),
		})
		return
	}

	// 3 & 4. Delete conflicting tail if any, append new entries.
	newEntries := fromPBEntries(req.Entries)
	if len(newEntries) > 0 {
		n.log.truncateAndAppend(newEntries)
	}

	// 5. Advance commitIndex, bounded by what we actually now have on
	// disk (well, in our in-memory log destined for disk) locally.
	lastNew := prevIdx + raft.LogIndex(len(newEntries))
	if leaderCommit := raft.LogIndex(req.LeaderCommit); leaderCommit > n.commitIndex {
		newCommit := leaderCommit
		if lastNew < newCommit {
			newCommit = lastNew
		}
		if newCommit > n.commitIndex {
			n.commitIndex = newCommit
			n.hsDirty = true
		}
	}

	n.send(from, raft.MsgAppendEntries, &raftpb.AppendEntriesResponse{
		Term:       uint64(n.term),
		Success:    true,
		LeaderHint: string(n.leader),
	})
}

// handleAppendEntriesResponseLocked processes a follower's reply. Because
// AppendEntriesResponse (see proto/raftpb/raft.proto) doesn't echo back
// which request it's answering, this trusts appendInflight[from] (recorded
// at send time) to say how far the just-acknowledged request would bring
// that peer — which is only a safe thing to trust because
// broadcastAppendEntriesLocked enforces at most one outstanding request
// per peer at a time. The first thing this function does, unconditionally,
// is free that peer's outstanding-request slot so the next send (a fresh
// Propose, the next heartbeat, or this function's own pipelining
// continuation below) is the only one in flight.
//
// Must be called with n.mu held.
func (n *Node) handleAppendEntriesResponseLocked(from raft.NodeID, resp *raftpb.AppendEntriesResponse) {
	term := raft.Term(resp.Term)
	if term > n.term {
		n.becomeFollowerLocked(term, "")
		return
	}
	if n.role != RoleLeader || term != n.term {
		return
	}
	n.replicating[from] = false

	if resp.Success {
		if sentUpTo, ok := n.appendInflight[from]; ok && sentUpTo > n.matchIndex[from] {
			n.matchIndex[from] = sentUpTo
		}
		if n.matchIndex[from]+1 > n.nextIndex[from] {
			n.nextIndex[from] = n.matchIndex[from] + 1
		}
		n.maybeAdvanceCommitLocked()
		if n.nextIndex[from] <= n.log.lastIndex() {
			// Follower is still behind; keep pipelining without waiting
			// for the next heartbeat tick.
			n.sendAppendEntriesToLocked(from)
		}
		return
	}

	// Rejected: back off nextIndex using the conflict hints (paper §5.3 /
	// the "Students' Guide to Raft" fast-backtrack optimization), never
	// below what's already confirmed durable on that follower.
	newNext := raft.LogIndex(resp.ConflictIndex)
	if resp.ConflictTerm != 0 {
		if idx, found := n.log.lastIndexWithTerm(raft.Term(resp.ConflictTerm)); found {
			newNext = idx + 1
		}
	}
	if newNext < 1 {
		newNext = 1
	}
	if floor := n.matchIndex[from] + 1; newNext < floor {
		newNext = floor
	}
	if newNext < n.nextIndex[from] {
		n.nextIndex[from] = newNext
	}
	n.sendAppendEntriesToLocked(from)
}

// maybeAdvanceCommitLocked implements paper §5.4.2's commit rule: a leader
// may only mark index N committed once (a) a majority of matchIndex
// (including the leader's own log, which always matches itself) are >= N,
// *and* (b) the entry at N was appended during the leader's *current*
// term. Condition (b) is the specific, easy-to-get-wrong safety rule that
// prevents a leader from committing (and thus exposing to clients) an
// older-term entry via indirect replication counting alone.
//
// Must be called with n.mu held, and only while n.role == RoleLeader.
func (n *Node) maybeAdvanceCommitLocked() {
	if n.role != RoleLeader {
		return
	}
	match := make([]raft.LogIndex, 0, len(n.peers)+1)
	match = append(match, n.log.lastIndex()) // the leader always matches itself
	for _, p := range n.peers {
		match = append(match, n.matchIndex[p])
	}
	sort.Slice(match, func(i, j int) bool { return match[i] < match[j] })

	majorityCount := len(match)/2 + 1
	candidate := match[len(match)-majorityCount]
	if candidate <= n.commitIndex {
		return
	}
	if term, ok := n.log.termAt(candidate); ok && term == n.term {
		n.commitIndex = candidate
		n.hsDirty = true
	}
}

func toPBEntries(entries []raft.LogEntry) []*raftpb.LogEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]*raftpb.LogEntry, len(entries))
	for i, e := range entries {
		out[i] = &raftpb.LogEntry{
			Term:  uint64(e.Term),
			Index: uint64(e.Index),
			Type:  toPBEntryType(e.Type),
			Data:  e.Data,
		}
	}
	return out
}

func fromPBEntries(entries []*raftpb.LogEntry) []raft.LogEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]raft.LogEntry, len(entries))
	for i, e := range entries {
		out[i] = raft.LogEntry{
			Term:  raft.Term(e.Term),
			Index: raft.LogIndex(e.Index),
			Type:  fromPBEntryType(e.Type),
			Data:  e.Data,
		}
	}
	return out
}

func toPBEntryType(t raft.EntryType) raftpb.EntryType {
	if t == raft.EntryConfChange {
		return raftpb.EntryType_ENTRY_TYPE_CONF_CHANGE
	}
	return raftpb.EntryType_ENTRY_TYPE_NORMAL
}

func fromPBEntryType(t raftpb.EntryType) raft.EntryType {
	if t == raftpb.EntryType_ENTRY_TYPE_CONF_CHANGE {
		return raft.EntryConfChange
	}
	return raft.EntryNormal
}
