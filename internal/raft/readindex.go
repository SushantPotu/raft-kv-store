package raftcore

import (
	"context"
	"fmt"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// This file implements the linearizable-read (ReadIndex) protocol, paper
// §8: a leader confirms it's still leader via a majority-acknowledged
// heartbeat round *sent after the read started* (so a stale, earlier
// heartbeat ack can't wrongly satisfy it), and — the specific caveat §8
// calls out — a freshly elected leader must wait for an entry from its own
// current term to commit before it can trust its (possibly stale, inherited
// from whoever it beat in the election) commitIndex enough to serve any
// read at all.
//
// ReadIndex is called synchronously by a caller goroutine (a gRPC handler)
// that is *not* the same goroutine driving this Node's Tick/Step/Ready/
// Advance loop, so it has to block on a channel rather than poll — but all
// of the bookkeeping that satisfies it (majority-ack tracking, the
// current-term-commit gate) still only ever runs under n.mu, from
// handleAppendEntriesResponseLocked/handleInstallSnapshotResponseLocked (an
// ack arrived) and maybeAdvanceCommitLocked (commitIndex advanced), exactly
// like every other piece of Node state.

// readState is which phase of the ReadIndex protocol a pendingReadIndex is
// in.
type readState int

const (
	// readWaitingCommit: this leader hasn't yet committed an entry from its
	// own current term (the §8 caveat) — the confirmation round hasn't
	// started yet.
	readWaitingCommit readState = iota
	// readWaitingAcks: the confirmation round is underway; needAck lists
	// which peers haven't yet replied to the fresh (force=true) broadcast
	// sent when this round started.
	readWaitingAcks
)

// pendingReadIndex tracks one in-flight ReadIndex call.
type pendingReadIndex struct {
	state   readState
	needAck map[raft.NodeID]bool // peers this round still needs an ack from
	result  chan error           // buffered(1); written at most once
}

// readIndex is the real implementation behind Node.ReadIndex.
func (n *Node) readIndex(ctx context.Context, _ []byte) error {
	n.mu.Lock()
	if n.role != RoleLeader {
		err := fmt.Errorf("%w (leader is %q)", ErrNotLeader, n.leader)
		n.mu.Unlock()
		return err
	}

	pr := &pendingReadIndex{result: make(chan error, 1)}
	n.pendingReads = append(n.pendingReads, pr)
	if n.hasCommittedCurrentTermEntryLocked() {
		n.startReadRoundLocked(pr)
	} else {
		pr.state = readWaitingCommit
	}
	n.mu.Unlock()

	select {
	case err := <-pr.result:
		return err
	case <-ctx.Done():
		n.mu.Lock()
		n.removePendingReadLocked(pr)
		n.mu.Unlock()
		return ctx.Err()
	}
}

// hasCommittedCurrentTermEntryLocked implements the paper §8 caveat: a
// leader can only trust commitIndex for a linearizable read once it has
// itself committed at least one entry during its *current* term.
// maybeAdvanceCommitLocked already only ever advances commitIndex to an
// index whose entry's term equals n.term (that's condition (b) of the
// commit rule, §5.4.2), so simply checking the term of the entry
// commitIndex currently points at is sufficient: it's n.term exactly when
// this leader has committed something since being elected.
//
// Must be called with n.mu held.
func (n *Node) hasCommittedCurrentTermEntryLocked() bool {
	term, ok := n.log.termAt(n.commitIndex)
	return ok && term == n.term
}

// startReadRoundLocked begins (or restarts) the majority-confirmation
// round for pr: forces a fresh AppendEntries broadcast to every peer and
// records that every peer's *next* response is what's needed to satisfy
// pr. Forcing a fresh send (force=true) is what "a heartbeat sent after
// this ReadIndex call started" means in practice: broadcastAppendEntriesLocked
// enforces at most one outstanding request per peer, so the response that
// eventually clears replicating[p] can only be answering the request just
// sent here, not some earlier one — see broadcastAppendEntriesLocked's own
// doc comment on that invariant.
//
// Must be called with n.mu held, and only while n.role == RoleLeader.
func (n *Node) startReadRoundLocked(pr *pendingReadIndex) {
	pr.state = readWaitingAcks
	pr.needAck = make(map[raft.NodeID]bool, len(n.peers))
	for _, p := range n.peers {
		pr.needAck[p] = true
	}
	n.broadcastAppendEntriesLocked(true)
	n.tryCompleteReadLocked(pr) // handles the single-node-cluster case
}

// promotePendingReadsLocked is called whenever commitIndex advances while
// leader (from maybeAdvanceCommitLocked): any read still stuck in
// readWaitingCommit can now, per hasCommittedCurrentTermEntryLocked, start
// its confirmation round.
//
// Must be called with n.mu held, and only while n.role == RoleLeader.
func (n *Node) promotePendingReadsLocked() {
	if !n.hasCommittedCurrentTermEntryLocked() {
		return
	}
	for _, pr := range n.pendingReads {
		if pr.state == readWaitingCommit {
			n.startReadRoundLocked(pr)
		}
	}
}

// satisfyPendingReadsLocked is called whenever a same-term reply arrives
// from a peer (AppendEntriesResponse or InstallSnapshotResponse, success or
// not — either proves the peer still follows this leader in this term).
//
// Must be called with n.mu held, and only while n.role == RoleLeader.
func (n *Node) satisfyPendingReadsLocked(from raft.NodeID) {
	for _, pr := range n.pendingReads {
		if pr.state != readWaitingAcks {
			continue
		}
		if _, needed := pr.needAck[from]; needed {
			delete(pr.needAck, from)
			n.tryCompleteReadLocked(pr)
		}
	}
	n.prunePendingReadsLocked()
}

// tryCompleteReadLocked resolves pr (non-blocking send on its buffered
// result channel) once a majority of {peers, self} have acked this round.
//
// Must be called with n.mu held.
func (n *Node) tryCompleteReadLocked(pr *pendingReadIndex) {
	if pr.state != readWaitingAcks {
		return
	}
	total := len(n.peers) + 1
	needed := total/2 + 1
	acked := (len(n.peers) - len(pr.needAck)) + 1 // +1: self always "acks"
	if acked < needed {
		return
	}
	select {
	case pr.result <- nil:
	default:
	}
}

// failPendingReadsLocked immediately fails every pending ReadIndex call
// with err (used when this node stops being leader — see
// state.go's becomeFollowerLocked).
//
// Must be called with n.mu held.
func (n *Node) failPendingReadsLocked(err error) {
	for _, pr := range n.pendingReads {
		select {
		case pr.result <- err:
		default:
		}
	}
	n.pendingReads = nil
}

// removePendingReadLocked drops pr from n.pendingReads (used when the
// caller's ctx is cancelled before pr was otherwise resolved).
//
// Must be called with n.mu held.
func (n *Node) removePendingReadLocked(pr *pendingReadIndex) {
	for i, cand := range n.pendingReads {
		if cand == pr {
			n.pendingReads = append(n.pendingReads[:i], n.pendingReads[i+1:]...)
			return
		}
	}
}

// prunePendingReadsLocked drops any pendingReads entry whose result has
// already been delivered (its buffered channel is full), so the slice
// doesn't grow unboundedly across many ReadIndex calls.
//
// Must be called with n.mu held.
func (n *Node) prunePendingReadsLocked() {
	live := n.pendingReads[:0]
	for _, pr := range n.pendingReads {
		if len(pr.result) > 0 {
			continue // already resolved, just waiting for the caller to receive it
		}
		live = append(live, pr)
	}
	n.pendingReads = live
}
