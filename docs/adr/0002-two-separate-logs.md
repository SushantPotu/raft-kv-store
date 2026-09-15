# ADR 0002: Two separate logs, two separate layers

## Status
Accepted (Phase 0)

## Context
There are two things that could be called "the log" in this system, and
conflating them would be both a design mistake and a confusing story to
tell in an interview:
1. The Raft replication log — what consensus actually replicates.
2. A storage engine's write-ahead log — how one node durably persists its
   applied key-value state.

## Decision
Keep them fully separate:
- `internal/storage/raftlog` implements `pkg/raft.Storage` — durable
  persistence for Raft's log entries and hard state, compacted via Raft
  snapshots (ADR-backed by Workstream E).
- `internal/storage/wal` + `internal/storage/engine` implement
  `engine.Engine` — a Bitcask-style engine (in-memory index over
  append-only, CRC-checked value segments, with its own background
  compaction) local to each node's *applied* state machine.
- `internal/statemachine.Adapter` is the only thing that bridges them: it
  decodes a committed `raft.LogEntry` and calls into `engine.Engine`.

## Consequences
- The two logs can have different compaction triggers, different record
  formats, and be developed/tested by the same or different people without
  cross-contamination.
- This intentionally means writes exist in two places transiently (the
  Raft log, until compacted; the KV engine's WAL, until its own
  compaction) — that duplication is normal for Raft-backed KV stores
  (see etcd, TiKV) and is not something to "optimize away."
