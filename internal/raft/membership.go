package raftcore

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// This file implements dynamic membership changes via joint consensus
// (paper §6). A configuration change is proposed as an EntryConfChange log
// entry (like any normal entry, replicated and committed the same way) that
// encodes either:
//
//   - a "joint" transition into C_old,new (both the old and the new voter
//     sets at once), appended immediately when ProposeConfChange is
//     called; or
//   - a "final" transition into C_new alone, auto-proposed by the current
//     leader once the joint entry commits.
//
// While in the joint phase (Node.joint != nil), every majority computation
// that matters for safety — RequestVote's hasMajorityLocked (election.go)
// and the leader's commit rule maybeAdvanceCommitLocked (replication.go) —
// requires an *independent* majority in both C_old and C_new, which is
// exactly what prevents a split brain if the original leader is lost
// mid-transition: no server can be elected, and no entry can be committed,
// on the strength of just one of the two configurations.
//
// Every node (not just the leader) tracks this, because whichever node
// might need to run for election next has to solicit votes from — and
// count majorities over — the right set of peers. recomputeConfigFromLogLocked
// keeps every node's Node.peers/Node.joint in sync with whatever
// EntryConfChange entries are (still) in its own log, including rolling
// back an uncommitted one that a new leader's conflicting log truncates
// away (see Node.baselinePeers's doc comment in node.go).
//
// Known limitations (deliberately out of scope for this workstream — see
// the final report):
//   - A node removing *itself* from the cluster isn't specially handled
//     (majority math still counts it as a voter in the new configuration
//     until the final entry commits and its own Node.peers updates, at
//     which point it simply stops being told about anything further).
//   - Only one configuration change may be in flight at a time (paper
//     recommendation, also what etcd/raft's pendingConfIndex does) —
//     ProposeConfChange rejects a second one until the first's final entry
//     commits.
//   - A snapshot taken while a joint configuration is in effect flattens
//     to a single (unioned) ConfState — see snapshot.go's
//     confStateLocked doc comment.

// jointConfig holds both halves of an in-flight C_old,new configuration.
// Both slices exclude the node they're stored on (same convention as
// Node.peers).
type jointConfig struct {
	oldPeers []raft.NodeID
	newPeers []raft.NodeID
}

// confChangePhase distinguishes the two EntryConfChange payloads a
// configuration change produces.
type confChangePhase string

const (
	confPhaseJoint confChangePhase = "joint"
	confPhaseFinal confChangePhase = "final"
)

// confChangePayload is what gets JSON-encoded into an EntryConfChange
// LogEntry's Data. There's no raftpb message for this (proto/raftpb is out
// of bounds for this workstream) — Data is already an opaque []byte as far
// as the log/replication path is concerned, so any node-local encoding
// works; JSON (stdlib only) is used purely for simplicity, not wire
// compatibility with anything else.
type confChangePayload struct {
	Phase confChangePhase
	// Old/New list every voter (including whichever node the entry is
	// stored on) in each configuration; Old is empty for a "final" entry.
	Old []raft.NodeID `json:"old,omitempty"`
	New []raft.NodeID `json:"new"`
}

// proposeConfChangeLocked is Node.ProposeConfChange's real implementation.
//
// Must be called with n.mu held.
func (n *Node) proposeConfChangeLocked(cc raft.ConfChange) error {
	if n.role != RoleLeader {
		return fmt.Errorf("%w (leader is %q)", ErrNotLeader, n.leader)
	}
	if n.joint != nil || (n.confChangeIndex != 0 && n.commitIndex < n.confChangeIndex) {
		return errors.New("raftcore: a configuration change is already in progress")
	}

	oldPeers := append([]raft.NodeID(nil), n.peers...)
	newPeers := applyConfChangePeers(oldPeers, n.id, cc)

	payload := confChangePayload{
		Phase: confPhaseJoint,
		Old:   withSelf(oldPeers, n.id),
		New:   withSelf(newPeers, n.id),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("raftcore: encode joint conf change: %w", err)
	}

	n.joint = &jointConfig{oldPeers: oldPeers, newPeers: newPeers}
	n.peers = unionPeers(oldPeers, newPeers)

	entries := n.log.appendNew([]raft.LogEntry{{Term: n.term, Type: raft.EntryConfChange, Data: data}})
	n.confChangeIndex = entries[0].Index

	n.broadcastAppendEntriesLocked(false)
	n.maybeAdvanceCommitLocked()
	return nil
}

// transitionJointToFinalLocked auto-proposes the second, C_new-only entry
// once the joint entry has committed. Called from maybeAdvanceCommitLocked.
//
// Must be called with n.mu held, and only while n.role == RoleLeader and
// n.joint != nil.
func (n *Node) transitionJointToFinalLocked() {
	final := append([]raft.NodeID(nil), n.joint.newPeers...)
	payload := confChangePayload{Phase: confPhaseFinal, New: withSelf(final, n.id)}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}

	n.joint = nil
	n.peers = final

	entries := n.log.appendNew([]raft.LogEntry{{Term: n.term, Type: raft.EntryConfChange, Data: data}})
	n.confChangeIndex = entries[0].Index

	n.broadcastAppendEntriesLocked(false)
}

// recomputeConfigFromLogLocked rebuilds Node.peers/Node.joint/
// Node.confChangeIndex from scratch: start from baselinePeers (the last
// snapshotted, or genesis, single configuration) and replay every
// EntryConfChange entry currently in the in-memory log, in order. Called
// whenever new entries are merged into the log via AppendEntries
// (handleAppendEntriesLocked) — including when that merge truncated a
// conflicting tail — so a configuration change that gets overwritten by a
// new leader's log is correctly rolled back, and one that's still present
// (even if uncommitted) is correctly reinstated. Also called once at
// NewNode, to replay whatever was already durable across a restart.
//
// Must be called with n.mu held.
func (n *Node) recomputeConfigFromLogLocked() {
	peers := append([]raft.NodeID(nil), n.baselinePeers...)
	var joint *jointConfig
	var lastConfIdx raft.LogIndex

	for _, e := range n.log.entriesFrom(0) {
		if e.Type != raft.EntryConfChange {
			continue
		}
		var p confChangePayload
		if err := json.Unmarshal(e.Data, &p); err != nil {
			continue
		}
		lastConfIdx = e.Index
		switch p.Phase {
		case confPhaseJoint:
			oldP := removeSelf(p.Old, n.id)
			newP := removeSelf(p.New, n.id)
			joint = &jointConfig{oldPeers: oldP, newPeers: newP}
			peers = unionPeers(oldP, newP)
		case confPhaseFinal:
			joint = nil
			peers = removeSelf(p.New, n.id)
		}
	}

	n.peers = peers
	n.joint = joint
	n.confChangeIndex = lastConfIdx
}

// recomputeConfigFromSnapshotLocked replaces Node's membership view
// wholesale from a snapshot's ConfState (received via InstallSnapshot, or
// — in principle — a future "load from snapshot at startup" path). Always
// collapses to a single, non-joint configuration: see snapshot.go's
// confStateLocked doc comment on why a joint phase doesn't survive a
// snapshot boundary.
//
// Must be called with n.mu held.
func (n *Node) recomputeConfigFromSnapshotLocked(cs raft.ConfState) {
	if len(cs.Voters) == 0 {
		return
	}
	n.baselinePeers = removeSelf(cs.Voters, n.id)
	n.peers = append([]raft.NodeID(nil), n.baselinePeers...)
	n.joint = nil
	n.confChangeIndex = 0
}

// applyConfChangePeers returns a new peer list (excluding self) with cc
// applied to peers (also excluding self). Adding an already-present peer,
// removing an absent one, or a change targeting self are all no-ops for
// the peer list itself — see this file's package doc comment on the
// "removing self" limitation.
func applyConfChangePeers(peers []raft.NodeID, self raft.NodeID, cc raft.ConfChange) []raft.NodeID {
	switch cc.Type {
	case raft.ConfChangeAddNode:
		if cc.NodeID == self {
			return append([]raft.NodeID(nil), peers...)
		}
		for _, p := range peers {
			if p == cc.NodeID {
				return append([]raft.NodeID(nil), peers...)
			}
		}
		return append(append([]raft.NodeID(nil), peers...), cc.NodeID)
	case raft.ConfChangeRemoveNode:
		out := make([]raft.NodeID, 0, len(peers))
		for _, p := range peers {
			if p != cc.NodeID {
				out = append(out, p)
			}
		}
		return out
	default:
		return append([]raft.NodeID(nil), peers...)
	}
}

// unionPeers returns the deduplicated union of a and b, order preserved
// (a's elements first).
func unionPeers(a, b []raft.NodeID) []raft.NodeID {
	seen := make(map[raft.NodeID]bool, len(a)+len(b))
	var out []raft.NodeID
	for _, p := range a {
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	for _, p := range b {
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

// withSelf prepends self to peers, for encoding a full voter list
// (including self) into a confChangePayload.
func withSelf(peers []raft.NodeID, self raft.NodeID) []raft.NodeID {
	out := make([]raft.NodeID, 0, len(peers)+1)
	out = append(out, self)
	out = append(out, peers...)
	return out
}

// removeSelf returns voters with self filtered out, for decoding a full
// voter list (as stored in a confChangePayload or raft.ConfState) back
// into the peers-excluding-self convention Node.peers/baselinePeers use.
func removeSelf(voters []raft.NodeID, self raft.NodeID) []raft.NodeID {
	out := make([]raft.NodeID, 0, len(voters))
	for _, v := range voters {
		if v != self {
			out = append(out, v)
		}
	}
	return out
}
