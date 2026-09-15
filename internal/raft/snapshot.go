package raftcore

// This file is a placeholder for the snapshotting/log-compaction
// workstream, which extends the leader-election-and-replication core
// implemented in this package (node.go, election.go, replication.go,
// log.go, state.go).
//
// Not implemented yet:
//   - Triggering/creating a snapshot via raft.Storage.CreateSnapshot once
//     the log grows past some threshold, and truncating raftLog's
//     in-memory tail to match.
//   - Sending InstallSnapshot RPCs (raft.MsgInstallSnapshot) to bring a
//     follower whose required log entries have already been compacted
//     away back up to date — Node.Step currently returns an explicit "not
//     yet implemented" error for raft.MsgInstallSnapshot, and
//     sendAppendEntriesToLocked (replication.go) has a TODO-equivalent
//     comment for the case where a peer's nextIndex falls before what
//     raftLog still holds.
//   - Restoring a Node's state from a raft.Snapshot on startup via
//     raft.StateMachine.RestoreSnapshot.
