package raftcore

// This file is a placeholder for the linearizable-read (ReadIndex)
// workstream (paper §8), which extends the leader-election-and-
// replication core implemented in this package.
//
// Not implemented yet:
//   - Node.ReadIndex (node.go) currently returns an explicit "not yet
//     implemented" error.
//   - The ReadIndex protocol itself: a leader confirming its leadership
//     via a majority heartbeat round before unblocking a read at its
//     current commitIndex, including the (leader still new in its term)
//     wait-for-a-current-term-entry-to-commit caveat the paper calls out
//     in §8.
