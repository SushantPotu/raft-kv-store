package raftcore

// This file is a placeholder for the dynamic-membership-change workstream
// (adding/removing voters via joint consensus, paper §6), which extends
// the leader-election-and-replication core implemented in this package.
//
// Not implemented yet:
//   - Node.ProposeConfChange (node.go) currently returns an explicit "not
//     yet implemented" error.
//   - Entering/leaving the joint (C_old,new) configuration, and the
//     accompanying changes to how RequestVote majorities and the
//     AppendEntries commit rule (maybeAdvanceCommitLocked in
//     replication.go) are computed while a configuration change is in
//     flight.
//   - Persisting raft.ConfState via raft.Storage.CreateSnapshot/
//     ApplySnapshot.
