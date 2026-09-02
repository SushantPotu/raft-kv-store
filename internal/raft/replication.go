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
	// termAt(0) always trivially succeeds (index 0 is the "nothing before
	// the log" sentinel — see its own doc comment in log.go), which is
	// exactly wrong here for a peer whose nextIndex has never advanced
	// past 1 (e.g. one that's been unreachable since before this node ever
	// compacted anything): prevIdx==0 must still count as "compacted away"
	// once the log's base has moved past it, so check that explicitly
	// rather than relying solely on termAt's !ok.
	if prevIdx < n.log.base {
		n.sendInstallSnapshotToLocked(p)
		return
	}
	prevTerm, ok := n.log.termAt(prevIdx)
	if !ok {
		// prevIdx has been compacted away (it's before what raftLog/Storage
		// still hold) — an AppendEntries can't bring this peer up to date
		// from here at all, so send an InstallSnapshot instead (see
		// snapshot.go's sendInstallSnapshotToLocked).
		n.sendInstallSnapshotToLocked(p)
		return
	}
	entries := n.log.entriesFrom(next)

	n.appendInflight[p] = prevIdx + raft.LogIndex(len(entries))
	n.replicating[p] = true
	n.send(p, raft.MsgAppendEntries, &raftpb.AppendEntriesRequest{
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
			FollowerId: string(n.id),
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

	if n.pendingSnapshot != nil {
		// A previously received InstallSnapshot hasn't been drained via
		// Ready/Advance yet (see snapshot.go's package doc comment on the
		// Node/driver split of responsibility). Don't mutate the log any
		// further until it has: cmd/kvnode's Ready-loop driver (and this
		// package's own test harness) persists Entries *before* installing
		// Snapshot, so a single Ready bundling both a Snapshot and Entries
		// built on top of it would hand the driver a gap it can't apply.
		// Silently drop this request; the leader will retry (it never gets
		// a reply, so replicating[from] on the leader side simply stays
		// set until the next forced heartbeat).
		return
	}

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose
	// term matches prevLogTerm.
	prevIdx := raft.LogIndex(req.PrevLogIndex)
	prevTerm := raft.Term(req.PrevLogTerm)
	if myTerm, ok := n.log.termAt(prevIdx); !ok || myTerm != prevTerm {
		conflictIdx, conflictTerm := n.log.conflictHint(prevIdx)
		n.send(from, raft.MsgAppendEntries, &raftpb.AppendEntriesResponse{
			FollowerId:    string(n.id),
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
		// Recompute membership from scratch by replaying every
		// EntryConfChange currently in the (possibly just-truncated) log
		// over baselinePeers. This is what makes an uncommitted conf
		// change correctly roll back if a new leader's log overwrites it —
		// see the Node.baselinePeers doc comment in node.go.
		n.recomputeConfigFromLogLocked()
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
		FollowerId: string(n.id),
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
	// Whether accepted or rejected, a same-term reply from `from` proves it
	// still follows us as leader this term — that's exactly what a
	// pending ReadIndex call (readindex.go) needs confirmed.
	n.satisfyPendingReadsLocked(from)

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
// While a joint (C_old,new) configuration is in effect (n.joint != nil,
// see membership.go), paper §6 requires condition (a) to hold
// *independently* in both the old and the new configuration — candidate is
// the minimum of the two per-config majority match indices, so an index
// only counts as committed once both halves of the cluster have it.
//
// Must be called with n.mu held, and only while n.role == RoleLeader.
func (n *Node) maybeAdvanceCommitLocked() {
	if n.role != RoleLeader {
		return
	}
	var candidate raft.LogIndex
	if n.joint != nil {
		candidate = n.majorityMatchLocked(n.joint.oldPeers)
		if newC := n.majorityMatchLocked(n.joint.newPeers); newC < candidate {
			candidate = newC
		}
	} else {
		candidate = n.majorityMatchLocked(n.peers)
	}
	if candidate <= n.commitIndex {
		return
	}
	if term, ok := n.log.termAt(candidate); ok && term == n.term {
		n.commitIndex = candidate
		n.hsDirty = true
		n.promotePendingReadsLocked()
		if n.joint != nil && n.confChangeIndex != 0 && n.commitIndex >= n.confChangeIndex {
			// The joint entry just committed: paper §6 says it's now safe
			// to move on to the final, new-only configuration.
			n.transitionJointToFinalLocked()
		}
	}
}

// majorityMatchLocked returns the highest index a majority of {peers,
// self} have replicated, per the usual "sort matchIndex, take the
// majority-th" computation (paper §5.4.2) — factored out so
// maybeAdvanceCommitLocked can apply it independently to each half of a
// joint configuration.
func (n *Node) majorityMatchLocked(peers []raft.NodeID) raft.LogIndex {
	match := make([]raft.LogIndex, 0, len(peers)+1)
	match = append(match, n.log.lastIndex()) // the leader always matches itself
	for _, p := range peers {
		match = append(match, n.matchIndex[p])
	}
	sort.Slice(match, func(i, j int) bool { return match[i] < match[j] })
	majorityCount := len(match)/2 + 1
	return match[len(match)-majorityCount]
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
