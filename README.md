# raft-kv-store

**A distributed key-value database with Raft consensus implemented from
scratch in Go — real leader election, real log replication, a real
crash-recoverable storage engine, and infrastructure-as-code for a
multi-AZ AWS deployment.**

This is not a wrapper around `etcd/raft`, `hashicorp/raft`, or any other
consensus library. Every piece of the replication protocol — leader
election, log replication, the commit-safety rule that keeps a cluster
from ever losing an acknowledged write — is implemented directly from
["In Search of an Understandable Consensus Algorithm"](https://raft.github.io/raft.pdf)
(Ongaro & Ousterhout, 2014), then proven correct with hundreds of
randomized-seed simulation runs, not just a handful of hand-picked test
cases.

## Why this exists

Most personal database or distributed-systems projects fall into one of
two camps: they wrap an existing consensus library and call it a day, or
they stay single-node and skip consensus entirely. This project
deliberately goes deeper, on purpose, in three places most projects stop
short of:

- **Consensus you actually built**, not glued together — leader election,
  log replication, and the paper's easy-to-get-wrong commit rule
  (§5.4.2: a leader may only count replication toward committing an entry
  if that entry was written in the leader's *current* term), all
  implemented and fuzz-tested against a deterministic network simulator
  that injects random node kills, restarts, and partitions.
- **A real storage engine underneath it**, not a database library — a
  Bitcask-style KV engine (in-memory index over CRC-framed, rotating
  write-ahead-log segments) and a separate, durable Raft log, both with
  actual crash-recovery tests that truncate a real file mid-write and
  verify replay recovers cleanly.
- **Infrastructure that reflects how this would really run** — Terraform
  for a multi-AZ AWS deployment (ECS Fargate, Cloud Map service
  discovery, DynamoDB-backed cluster metadata, least-privilege IAM), not
  a single Docker container standing in for "the cloud."

## What's implemented today

| Layer | Status | Where |
|---|---|---|
| Leader election | ✅ Done, fuzz-tested | `internal/raft` |
| Log replication (nextIndex/matchIndex, conflict backtracking) | ✅ Done, fuzz-tested | `internal/raft` |
| Commit-safety rule (§5.4.2) | ✅ Done, fuzz-tested | `internal/raft` |
| Crash-recoverable KV storage engine (WAL + compaction) | ✅ Done | `internal/storage/wal`, `internal/storage/engine` |
| Durable Raft log with conflicting-tail truncation | ✅ Done | `internal/storage/raftlog` |
| gRPC transport (peer-to-peer + client-facing) | ✅ Done | `internal/transport/grpc`, `internal/server` |
| Real single-shard node binary (`kvnode`) | ✅ Done | `cmd/kvnode` |
| `kvctl` CLI | ✅ Done | `cmd/kvctl` |
| AWS infrastructure-as-code (VPC, ECS Fargate, DynamoDB, Cloud Map, ALB, IAM, CloudWatch) | ✅ Written, `terraform validate`-clean, **not applied** | `deploy/terraform` |
| Log compaction / snapshotting as a live Raft feature | 🚧 Storage-layer primitives exist (`Storage.CreateSnapshot`, `Engine.SnapshotAll`); Raft Core doesn't trigger them yet | `internal/raft/snapshot.go` |
| Linearizable reads (ReadIndex) | 🚧 Stubbed, returns "not implemented" | `internal/raft/readindex.go` |
| Dynamic membership changes (joint consensus) | 🚧 Stubbed | `internal/raft/membership.go` |
| Multi-Raft sharding + routing | 🚧 Wire protocol has `shard_id` on every message; only one shard runs today | `internal/shard`, `internal/routing`, `internal/metaservice` (scaffolded, not implemented) |
| Real AWS deployment | ⏳ Infra is ready; not yet applied to a live account | — |
| Chaos testing / load testing with real numbers | ⏳ Planned | `internal/chaos`, `internal/loadgen` |

~8,400 lines of hand-written Go (excluding generated protobuf code), 57
tests, across 4 commits so far.

## Architecture

Today's reality is a single Raft group replicated across N `kvnode`
processes:

```
        kvctl (CLI)
           │
           ▼
   ┌───────────────┐   gRPC (peer RPCs, RaftTransportService)
   │    kvnode-1    │◄──────────────────────┐
   │  ┌──────────┐  │                        │
   │  │ raftcore │  │   ┌───────────────┐    │
   │  │  .Node   │──┼──►│    kvnode-2   │────┤
   │  └────┬─────┘  │   │  (same shape) │    │
   │       │        │   └───────────────┘    │
   │  ┌────▼─────┐  │                        │
   │  │ raftlog  │  │   ┌───────────────┐    │
   │  │ (Storage)│  │   │    kvnode-3   │────┘
   │  └──────────┘  │   │  (same shape) │
   │  ┌──────────┐  │   └───────────────┘
   │  │ engine   │  │
   │  │(KV store)│  │   client-facing gRPC (KVService)
   │  └──────────┘  │◄──────────────────────  kvctl / any client
   └───────────────┘
```

The target architecture — once Multi-Raft sharding lands — partitions the
keyspace across independent Raft groups, fronted by a stateless router:

```
client → kvrouter (stateless) → metaservice (DynamoDB shard map)
                │
                ▼
     kvnode (one Raft group per shard, replicas spread across AZs)
                │
                ▼
     storage engine (WAL + in-memory index + compaction)
```

### The two-log design

There are two genuinely different logs in this system, and keeping them
separate (rather than conflating "the log" into one thing) is a
deliberate design choice — see
[`docs/adr/0002-two-separate-logs.md`](docs/adr/0002-two-separate-logs.md):

1. **The Raft replication log** (`internal/storage/raftlog`) — what
   consensus actually replicates across nodes, compacted via Raft
   snapshots.
2. **The KV engine's write-ahead log** (`internal/storage/wal` +
   `internal/storage/engine`) — how one node durably persists its
   *applied* key-value state, with its own independent compaction.

Both reuse one shared, well-tested primitive: a CRC-framed,
length-prefixed record format with torn-tail and corruption detection
(see [`docs/design/storage-engine.md`](docs/design/storage-engine.md) for
the full on-disk format, the crash-recovery test strategy, and a
benchmark showing batched fsync is ~69x faster than fsync-per-write).

### Raft Core is pure logic, separated from I/O

`internal/raft.Node` never touches a disk, a socket, or real wall-clock
time. It's driven synchronously by external `Step()`/`Tick()` calls and
produces a `Ready` struct that the caller persists, sends, and applies —
the same shape `etcd/raft` uses, adopted specifically because it makes
the consensus logic testable via `internal/raft/simulate`, a fully
deterministic fake network with configurable drop/delay/partition
behavior and zero real I/O. See
[`docs/adr/0001-raft-core-io-separation.md`](docs/adr/0001-raft-core-io-separation.md).

This is what makes the correctness testing described below possible:
every safety property is proven against a simulated cluster, not
guessed at from a few manual runs.

## Correctness, proven

`internal/raft`'s test suite runs its consensus implementation through a
randomized-seed fuzz harness (`internal/raft/simulate`) that injects
random node kills, restarts, network partitions, and healing at random
points, then checks:

- **Election safety** — no two nodes ever claim leadership in the same
  term, across 100+ seeded runs.
- **Leader completeness** — killing a leader mid-stream triggers
  re-election, and every previously-committed entry survives on the
  remaining nodes.
- **Partition safety** — a minority partition never elects a leader; the
  majority partition keeps electing and committing.
- **A 100-seed × 4,000-round fuzz test** combining all of the above at
  once, asserting the cluster never loses a committed write and never
  splits brain.

That fuzz test caught a real bug during development: overlapping
in-flight `AppendEntries` requests to the same peer could let a stale
response wrongly advance `matchIndex`, since the wire protocol carries no
request-correlation field. Fixed by enforcing at most one outstanding
`AppendEntries` per peer — the kind of subtle, easy-to-miss bug this
testing strategy exists to catch.

Separately, the storage layer's own tests simulate a crash by truncating
a real file mid-write at every possible byte offset and verify replay
recovers exactly the records written before the tear, with no panic and
no silent corruption.

## A real integration bug, found and fixed

Building the three main pieces (storage, consensus, transport) in
parallel and then wiring them together at "Integration Checkpoint 1"
surfaced a genuine architectural bug worth calling out: the original
`Transport` interface was shaped as synchronous per-RPC-kind methods
(`SendRequestVote(req) (*resp, error)`, mirroring a typical request/response
RPC), but `Node` never actually produces a reply synchronously — a
response to an inbound vote or append request is just another outbound
message, queued for a later `Ready()` call. The fix: a single
`RaftTransportService.Send(RaftMessage) returns (Empty)` RPC carrying a
`oneof` over every request *and* response type — the same shape
`etcd/raft`'s own transport uses, for the same reason. This is documented
in the `Transport` interface's doc comment in
[`pkg/raft/types.go`](pkg/raft/types.go).

## Getting started

Requires **Go 1.25+**, **Docker**, and **[`buf`](https://buf.build)** (for
protobuf codegen). AWS deployment additionally needs the AWS CLI and
configured credentials — not required for local development.

```bash
git clone https://github.com/SushantPotu/raft-kv-store
cd raft-kv-store

go build ./...       # build all binaries
go test ./...         # run the full test suite
go test -race ./...   # same, with the race detector
```

### Run a local 3-node cluster

```bash
make docker-build
docker compose -f deploy/docker/docker-compose.yml up -d
go run ./cmd/kvctl --addr localhost:9001 put foo bar
go run ./cmd/kvctl --addr localhost:9002 get foo --consistency=stale
```

For the full manual walkthrough — bring the cluster up, confirm
replication, kill whichever node is leader, confirm a new one takes over
and keeps accepting writes, restart the killed node — see
[`docs/runbooks/local-cluster.md`](docs/runbooks/local-cluster.md).

### Regenerate protobuf code

```bash
make proto-gen   # runs `buf generate` against proto/*.proto
```

### Validate the AWS infrastructure (without deploying it)

```bash
cd deploy/terraform/envs/dev
terraform init -backend=false
terraform validate
```

## Project structure

```
raft-kv-store/
├── proto/                  # frozen wire contracts (raft.proto, kv.proto, meta.proto)
├── pkg/
│   ├── raft/               # Storage/StateMachine/Transport/Node interfaces + rafttest fakes
│   ├── raftpb/ kvpb/ metapb/  # generated protobuf/gRPC code
├── internal/
│   ├── raft/               # Raft consensus (leader election, replication) + simulate/ harness
│   ├── storage/
│   │   ├── wal/            # shared segment/CRC framing primitive
│   │   ├── engine/         # Bitcask-style KV engine
│   │   ├── raftlog/        # durable Raft replication log
│   │   └── snapshot/       # snapshot serialization helpers
│   ├── statemachine/       # bridges committed Raft entries to the KV engine
│   ├── transport/grpc/     # raft.Transport over gRPC
│   ├── server/             # client-facing KVService gRPC server
│   ├── shard/ routing/ metaservice/ discovery/  # Multi-Raft sharding (scaffolded, not yet implemented)
│   └── chaos/ loadgen/     # chaos & load testing (scaffolded, not yet implemented)
├── cmd/
│   ├── kvnode/             # the real node binary (Checkpoint 1)
│   ├── kvctl/              # CLI client
│   └── kvrouter/ metaservice/ chaosmonkey/ loadgen/  # scaffolded for later workstreams
├── deploy/
│   ├── docker/             # Dockerfiles + docker-compose.yml (3-node local cluster)
│   └── terraform/          # VPC, ECS Fargate, DynamoDB, Cloud Map, ALB, IAM, CloudWatch modules
├── docs/
│   ├── adr/                # architecture decision records
│   ├── design/             # deep-dive design docs
│   └── runbooks/           # manual operational walkthroughs
└── test/                   # integration/chaos/load test scaffolding
```

## Design docs

- [`docs/adr/0001-raft-core-io-separation.md`](docs/adr/0001-raft-core-io-separation.md) — why Raft Core has no background goroutine and no I/O of its own
- [`docs/adr/0002-two-separate-logs.md`](docs/adr/0002-two-separate-logs.md) — why the Raft log and the KV engine's WAL are independent
- [`docs/design/storage-engine.md`](docs/design/storage-engine.md) — on-disk formats, crash recovery, and the fsync-batching benchmark
- [`docs/runbooks/local-cluster.md`](docs/runbooks/local-cluster.md) — manual 3-node cluster test (put/get, kill the leader, confirm failover)

## Roadmap

1. **Snapshotting & log compaction** as a live Raft Core feature (the storage primitives already exist)
2. **Linearizable reads** via the ReadIndex protocol
3. **Dynamic membership changes** via joint consensus
4. **Multi-Raft sharding** — partition the keyspace across independent Raft groups with a stateless routing layer and a DynamoDB-backed metadata service
5. **Real AWS deployment** — apply the existing Terraform to a live account, across 3 availability zones
6. **Chaos testing** — kill random nodes/simulate AZ failure, measure real p50/p99 failover time
7. **Load testing** — real throughput/latency numbers under concurrent load

## Tech stack

Go 1.25 · gRPC · Protocol Buffers (`buf`) · Docker / Docker Compose ·
Terraform · AWS (ECS Fargate, DynamoDB, Cloud Map, ALB, IAM, CloudWatch)

## License

MIT — see [LICENSE](LICENSE).
