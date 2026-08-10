# ADR 0001: Separate Raft Core's logic from I/O

## Status
Accepted (Phase 0)

## Context
Raft Core (leader election, log replication, safety) needs to be testable
with randomized-seed fuzzing across many simulated cluster runs. Real disk
I/O, real network I/O, and real goroutine scheduling all introduce
nondeterminism that makes such tests flaky and hard to debug.

## Decision
Adopt the etcd/raft *shape* (not the library): `Node` is driven externally
by `Step()` and `Tick()`, and produces a `Ready` struct that the caller is
responsible for persisting (via `Storage`), sending (via `Transport`), and
applying (via `StateMachine`). `Node`'s internals never touch a file
descriptor or a socket directly.

## Consequences
- Raft Core can be tested via `internal/raft/simulate`, a fully
  in-process, deterministic fake network + fake clock, with zero real I/O.
- The Storage Engine, Client/API Protocol, and AWS Infrastructure
  workstreams can all be built in parallel against the `pkg/raft`
  interfaces before Raft Core's implementation exists, using the fakes in
  `pkg/raft/rafttest`.
- The cost: `internal/shard.Manager` (the real caller of `Node`) has to
  correctly implement the Ready-loop contract (persist before send, call
  `Advance()` after fully processing a `Ready`) — getting this wrong is a
  correctness bug that unit tests of `Node` alone won't catch, so
  Checkpoint 1's integration test explicitly exercises it end to end.
