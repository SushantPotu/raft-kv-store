package raftcore

import "github.com/SushantPotu/raft-kv-store/pkg/raft"

// Role is one of Raft's three server states (paper Figure 4).
type Role int

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

func (r Role) String() string {
	switch r {
	case RoleFollower:
		return "follower"
	case RoleCandidate:
		return "candidate"
	case RoleLeader:
		return "leader"
	default:
		return "unknown"
	}
}

// becomeFollowerLocked transitions to Follower, adopting term if it is
// higher than our current term (persisting the term/votedFor reset via
// HardState) and recording who we currently believe the leader is (empty
// if unknown, e.g. after just losing an election with no winner yet).
//
// Must be called with n.mu held.
func (n *Node) becomeFollowerLocked(term raft.Term, leader raft.NodeID) {
	wasLeader := n.role == RoleLeader
	n.role = RoleFollower
	if term > n.term {
		n.term = term
		n.votedFor = ""
		n.hsDirty = true
	}
	n.leader = leader
	n.votesGranted = nil
	if wasLeader {
		n.nextIndex = nil
		n.matchIndex = nil
		n.appendInflight = nil
		n.replicating = nil
		// Any ReadIndex calls blocked waiting on this node's (now former)
		// leadership can never be satisfied — fail them immediately rather
		// than leaving the caller blocked until ctx expires.
		n.failPendingReadsLocked(raft.ErrNotLeader)
	}
	n.resetElectionTimeoutLocked()
}

// becomeCandidateLocked transitions to Candidate: increments the term,
// votes for self, and resets the election timer (paper §5.2). The actual
// RequestVote broadcast happens in election.go's startElectionLocked,
// which calls this first.
//
// Must be called with n.mu held.
func (n *Node) becomeCandidateLocked() {
	n.role = RoleCandidate
	n.term++
	n.votedFor = n.id
	n.leader = ""
	n.votesGranted = map[raft.NodeID]bool{n.id: true}
	n.hsDirty = true
	n.resetElectionTimeoutLocked()
}

// becomeLeaderLocked transitions to Leader after winning an election:
// initializes nextIndex/matchIndex per peer (paper Figure 2) and appends a
// no-op entry in the new term. The no-op lets commitIndex advance safely
// even if no client proposal arrives — §5.4.2 forbids a leader from
// concluding an entry from an *earlier* term is committed by counting
// replicas alone, so without something to replicate in its own term a
// freshly elected leader could stall and never be able to expose earlier
// entries as committed either.
//
// Must be called with n.mu held.
func (n *Node) becomeLeaderLocked() {
	n.role = RoleLeader
	n.leader = n.id
	n.votesGranted = nil

	last := n.log.lastIndex()
	n.nextIndex = make(map[raft.NodeID]raft.LogIndex, len(n.peers))
	n.matchIndex = make(map[raft.NodeID]raft.LogIndex, len(n.peers))
	n.appendInflight = make(map[raft.NodeID]raft.LogIndex, len(n.peers))
	n.replicating = make(map[raft.NodeID]bool, len(n.peers))
	for _, p := range n.peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}

	n.heartbeatElapsed = 0
	n.log.appendNew([]raft.LogEntry{{Term: n.term, Type: raft.EntryNormal}})
	n.broadcastAppendEntriesLocked(true)
	n.maybeAdvanceCommitLocked()
}
