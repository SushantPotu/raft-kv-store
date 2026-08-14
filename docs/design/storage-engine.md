# Storage Engine (Workstream A)

This document covers the two durable-storage layers built for this
workstream, per ADR-0002 ("two separate logs, two separate layers"):

1. `internal/storage/wal` + `internal/storage/engine` — a Bitcask-style KV
   engine backing `engine.Engine`.
2. `internal/storage/raftlog` — a segment-file-backed implementation of
   `pkg/raft.Storage`, the Raft replication log.

Both reuse a single shared primitive: `internal/storage/wal`'s CRC-framed,
length-prefixed record format (`[uint32 crc][uint32 len][payload]`, see
`frame.go`). The corruption/torn-tail detection logic exists exactly once
in the codebase; everything else (KV record layout, Raft entry layout,
rotation policy, truncation) is layered on top of it.

## KV engine: on-disk format

Each WAL segment is a flat sequence of frames. A KV record's payload is:

```
[8 bytes seq][1 byte op][4 bytes keylen][key][4 bytes vallen][value]
```

`op` is `PUT` (1) or `DEL` (2, tombstone — value always empty). Segments
rotate at a configurable byte threshold (default 64MB). The engine keeps
an in-memory index of `key -> (segment id, byte offset)` — not the values
themselves — and reads the value straight off disk on every `Get`. This is
the classic Bitcask tradeoff: RAM scales with key count and index
overhead, not with total data volume.

`Compact()` reads every live key's current value, writes them into a
fresh, isolated staging directory (so a crash mid-compaction can't corrupt
the live segments), atomically renames the staged segments into place
under fresh ids, then deletes the old segment files. `SnapshotAll`/
`RestoreAll` dump/restore the entire live keyspace as a single
gob-encoded, CRC-wrapped blob (`internal/storage/snapshot`), for Raft's
InstallSnapshot path.

## Raft log: on-disk format

`internal/storage/raftlog` keeps its own segment files (`*.rlog`, separate
directory from the KV WAL) with entry payloads:

```
[8 bytes index][8 bytes term][4 bytes entry-type][4 bytes datalen][data]
```

Unlike the KV WAL, entries are never tombstoned — a conflicting tail
(leader's `AppendEntries` overwriting a follower's divergent suffix) is
handled by **physically truncating** the segment file at the byte offset
of the first conflicting entry (`entrySegments.truncateTo`), deleting any
fully-superseded later segments outright. This mirrors
`rafttest.FakeStorage.Append`'s in-memory slice-and-splice behavior
exactly, but makes it durable: a restart never resurrects a truncated
tail (`TestStorageRestartAfterConflictingTailTruncation`).

`HardState`/`ConfState`/`Snapshot` metadata changes far less often than
entries but must never be read back torn, so instead of framing them as
WAL records they're written to a single file via write-tmp-then-rename
(atomic on both POSIX and Windows). Every `SetHardState` call fsyncs
before returning, matching Raft's "persist before acting on it"
requirement — there's no batched/deferred mode for hard state the way
there is for the KV WAL, since a lost hard-state write is a correctness
bug, not a throughput knob.

`CreateSnapshot(index, ...)` slices the in-memory entry log the same way
the fake does, then deletes every `*.rlog` segment strictly before the one
backing the new compaction boundary, actually reclaiming disk space.
`ApplySnapshot` deletes every entry segment outright (the whole log is
superseded) and starts fresh at segment 0.

One Windows-specific correctness note: entry segments are opened
**without** `O_APPEND` and written via `WriteAt` at explicitly tracked
offsets, rather than relying on OS append-mode positioning. A file handle
opened with `O_APPEND` on Windows cannot be `Truncate`d (`Access is
denied`), which would have silently broken the conflicting-tail-truncation
path on that platform. The KV WAL writer doesn't need this — it never
truncates — so it still uses plain `O_APPEND`.

## Crash recovery

`wal.ReplayFrames` (and everything built on it — `ReplaySegmentFile`,
`engine.Open`, `raftlog.Open`) distinguishes three end-of-data cases:

- **Clean EOF** — nothing more to replay, not an error.
- **Torn frame** (`ErrTornFrame`) — a header or payload was only partially
  written, the signature of a crash mid-`Append`. Replay stops, returns
  every record read so far, and reports `torn=true`.
- **Corrupt frame** (`ErrCorruptFrame`) — a full frame was read but its
  CRC doesn't match (bit rot). Currently folded into the same "stop and
  report" path as a torn tail, since the recovery action is identical.

`TestCrashRecoveryTornTail` (wal package) and
`TestOpenReplaysAcrossCrashTornSegment` (engine package) both simulate a
crash by truncating a real segment file at a byte offset that lands
strictly inside a record's frame, then assert replay recovers exactly the
records before the tear with no panic and no corrupted data.
`TestCrashRecoveryAcrossManyOffsets` sweeps a full range of truncation
points to make sure this holds at every possible tear point, not just one
random sample.

## Benchmark: fsync-per-write vs. batched fsync

`internal/storage/wal/bench_test.go` benchmarks `Writer.Append` under
three fsync policies, appending 128-byte values with short keys. Measured
on this machine (AMD Ryzen 9 7950X3D, Windows, local NVMe):

| Policy | ns/op | ops/sec (approx) |
|---|---|---|
| `SyncEveryWrite` (fsync after every record) | 1,122,473 | ~890 |
| `SyncBatch` (fsync once per 100 records) | 16,361 | ~61,000 |
| `SyncNone` (no explicit fsync; only on Close) | 3,938 | ~254,000 |

Batching 100 writes per fsync is **~69x** faster than fsyncing every
write, and gets within ~4x of the no-fsync ceiling while still bounding
data loss on a crash to at most 100 unflushed records (versus
unbounded loss under `SyncNone`). This is the same tradeoff every
production WAL (Kafka's `log.flush.interval.messages`, Postgres's
`commit_delay`/group commit, etc.) makes: fsync is the dominant cost of a
durable write, and amortizing it across a batch is the standard way to
buy back most of the throughput without giving up a bounded durability
window.

`raftlog`'s entry writer deliberately does *not* expose this knob — every
`Append`/`SetHardState` fsyncs unconditionally, because Raft's safety
proofs depend on "if I told a peer I persisted this, I actually did,"
which is a correctness requirement rather than a throughput/durability
tradeoff a caller should get to loosen.
