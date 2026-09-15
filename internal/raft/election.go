package raftcore

import (
	"github.com/SushantPotu/raft-kv-store/pkg/raft"
	"github.com/SushantPotu/raft-kv-store/pkg/raftpb"
)

// resetElectionTimeoutLocked zeroes the election-timeout counter and picks
// a new randomized deadline in [electionTimeoutMin, electionTimeoutMax]
// ticks. Randomization (paper §5.2) is what keeps split votes rare and
// self-resolving.
//
// Must be called with n.mu held.
func (n *Node) resetElectionTimeoutLocked() {
	n.electionElapsed = 0
	span := n.electionTimeoutMax - n.electionTimeoutMin
	if span <= 0 {
		n.electionTimeout = n.electionTimeoutMin
		return
	}
	n.electionTimeout = n.electionTimeoutMin + n.rng.Intn(span+1)
}

// startElectionLocked begins a new election: becomes (or re-becomes, on a
// split-vote timeout) a Candidate and broadcasts RequestVote to every
// peer, carrying our last log index/term so recipients can apply the
// up-to-date check (§5.4.1).
//
// Must be called with n.mu held.
func (n *Node) startElectionLocked() {
	n.becomeCandidateLocked()

	lastIdx := n.log.lastIndex()
	lastTerm, _ := n.log.termAt(lastIdx)
	for _, p := range n.peers {
		n.send(p, raft.MsgRequestVote, &raftpb.RequestVoteRequest{
			Term:         uint64(n.term),
			CandidateId:  string(n.id),
			LastLogIndex: uint64(lastIdx),
			LastLogTerm:  uint64(lastTerm),
		})
	}

	// Single-node "cluster": nothing to wait for, we're a trivial majority
	// of one.
	if len(n.peers) == 0 {
		n.becomeLeaderLocked()
	}
}

// handleRequestVoteLocked implements the RequestVote RPC receiver rules
// from paper Figure 2.
//
// Must be called with n.mu held.
func (n *Node) handleRequestVoteLocked(from raft.NodeID, req *raftpb.RequestVoteRequest) {
	term := raft.Term(req.Term)
	if term > n.term {
		n.becomeFollowerLocked(term, "")
	}

	grant := false
	if term == n.term {
		candidateID := raft.NodeID(req.CandidateId)
		upToDate := n.log.isUpToDate(raft.LogIndex(req.LastLogIndex), raft.Term(req.LastLogTerm))
		if (n.votedFor == "" || n.votedFor == candidateID) && upToDate {
			n.votedFor = candidateID
			n.hsDirty = true
			grant = true
			// Granting a vote means we believe this candidate might
			// legitimately become leader; reset our own timer so we don't
			// immediately turn around and contest the election it's
			// about to (maybe) win.
			n.resetElectionTimeoutLocked()
		}
	}

	n.send(from, raft.MsgRequestVote, &raftpb.RequestVoteResponse{
		VoterId:     string(n.id),
		Term:        uint64(n.term),
		VoteGranted: grant,
	})
}

// handleRequestVoteResponseLocked processes a vote reply. Stale replies
// (wrong term, or we're no longer a Candidate in that term) are ignored.
//
// Must be called with n.mu held.
func (n *Node) handleRequestVoteResponseLocked(from raft.NodeID, resp *raftpb.RequestVoteResponse) {
	term := raft.Term(resp.Term)
	if term > n.term {
		n.becomeFollowerLocked(term, "")
		return
	}
	if n.role != RoleCandidate || term != n.term {
		return
	}
	if !resp.VoteGranted {
		return
	}
	n.votesGranted[from] = true
	if n.hasMajorityLocked(n.votesGranted) {
		n.becomeLeaderLocked()
	}
}

// hasMajorityLocked reports whether votes (keyed by voter NodeID, true =
// granted) constitutes a majority. Under a normal single configuration
// that's a majority of n.peers+self; while a joint (paper §6) membership
// change is in flight, it requires an *independent* majority in both the
// old and the new configuration (quorumReached is evaluated twice) — see
// membership.go's jointConfig.
func (n *Node) hasMajorityLocked(votes map[raft.NodeID]bool) bool {
	if n.joint != nil {
		return quorumReached(votes, n.joint.oldPeers, n.id) && quorumReached(votes, n.joint.newPeers, n.id)
	}
	return quorumReached(votes, n.peers, n.id)
}

// quorumReached reports whether votes contains a majority of peers+self.
func quorumReached(votes map[raft.NodeID]bool, peers []raft.NodeID, self raft.NodeID) bool {
	total := len(peers) + 1
	count := 0
	if votes[self] {
		count++
	}
	for _, p := range peers {
		if votes[p] {
			count++
		}
	}
	return count*2 > total
}
