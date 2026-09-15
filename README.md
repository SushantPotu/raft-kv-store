# raft-kv-store

**A distributed, sharded key-value database with Raft consensus
implemented from scratch in Go — real leader election, log replication,
snapshotting, linearizable reads, dynamic membership, multi-shard
routing, a crash-recoverable storage engine, and chaos/load testing with
real measured numbers.**

This is not a wrapper around `etcd/raft`, `hashicorp/raft`, or any other
consensus library. Every piece of the replication protocol — leader
election, log replication, log compaction, linearizable ReadIndex reads,
joint-consensus membership changes, the commit-safety rule that keeps a
cluster from ever losing an acknowledged write — is implemented directly
from ["In Search of an Understandable Consensus Algorithm"](https://raft.github.io/raft.pdf)
(Ongaro & Ousterhout, 2014), then proven correct with hundreds of
randomized-seed simulation runs, not just a handful of hand-picked test
cases.

## Why this exists

Most personal database or distributed-systems projects fall into one of
two camps: they wrap an existing consensus library and call it a day, or
they stay single-node and skip consensus entirely. This project
deliberately goes deeper, on purpose, in four places most projects stop
short of:

- **Consensus you actually built**, not glued together — leader election,
  log replication, the paper's easy-to-get-wrong commit rule (§5.4.2),
  log compaction/snapshotting, linearizable ReadIndex reads (§8), and
  joint-consensus membership changes (§6) — all implemented and
  fuzz-tested against a deterministic network simulator that injects
  random node kills, restarts, and partitions.
- **A real storage engine underneath it**, not a database library — a
  Bitcask-style KV engine (in-memory index over CRC-framed, rotating
  write-ahead-log segments) and a separate, durable Raft log, both with
  actual crash-recovery tests that truncate a real file mid-write and
  verify replay recovers cleanly.
- **Real sharding and routing**, not a single Raft group pretending to be
  a database — the keyspace is partitioned across independent Raft
  groups, a DynamoDB-shaped metadata service tracks shard ownership and
  leadership with conditional-write safety, and a stateless router
  self-heals its leader cache when a shard fails over.
- **Measured behavior, not claimed behavior** — a chaos-testing tool that
  actually kills the leader of a real running cluster and times recovery,
  and a load generator that actually drives concurrent traffic and
  reports real percentile latencies. See [Chaos and load testing](#chaos-and-load-testing-real-numbers)
  below for the numbers this produced — including two real bugs it found
  and that got fixed as a result.

AWS infrastructure-as-code (Terraform for a multi-AZ ECS Fargate
deployment) exists and is `terraform validate`-clean, but actually
deploying it is a deliberate, out-of-scope decision for this project (no
AWS account backs it) — see [Infrastructure-as-code, not deployed](#infrastructure-as-code-not-deployed).

## What's implemented today

| Layer | Status | Where |
|---|---|---|
| Leader election | ✅ Done, fuzz-tested | `internal/raft` |
| Log replication (nextIndex/matchIndex, conflict backtracking) | ✅ Done, fuzz-tested | `internal/raft` |
| Commit-safety rule (§5.4.2) | ✅ Done, fuzz-tested | `internal/raft` |
| Log compaction / snapshotting | ✅ Done | `internal/raft/snapshot.go` |
| Linearizable reads (ReadIndex, §8) | ✅ Done | `internal/raft/readindex.go` |
| Dynamic membership changes (joint consensus, §6) | ✅ Done, fuzzed | `internal/raft/membership.go` |
| Crash-recoverable KV storage engine (WAL + compaction) | ✅ Done | `internal/storage/wal`, `internal/storage/engine` |
| Durable Raft log with conflicting-tail truncation | ✅ Done | `internal/storage/raftlog` |
| gRPC transport (peer-to-peer + client-facing) | ✅ Done | `internal/transport/grpc`, `internal/server` |
| Real single-shard node binary (`kvnode`) | ✅ Done | `cmd/kvnode` |
| Multi-Raft sharding (N shards per process) | ✅ Done | `internal/shard` |
| Shard-aware routing + metadata service | ✅ Done | `internal/routing`, `internal/metaservice`, `cmd/kvrouter`, `cmd/metaservice` |
| `kvctl` CLI | ✅ Done | `cmd/kvctl` |
| Real (non-Docker) multi-node test harness | ✅ Done | `internal/localcluster` |
| Chaos testing with real measured failover time | ✅ Done — see numbers below | `internal/chaos`, `cmd/chaosmonkey` |
| Load testing with real measured throughput/latency | ✅ Done — see numbers below | `internal/loadgen`, `cmd/loadgen` |
| AWS infrastructure-as-code (VPC, ECS Fargate, DynamoDB, Cloud Map, ALB, IAM, CloudWatch) | ✅ Written, `terraform validate`-clean, **intentionally not applied** | `deploy/terraform` |
| Real AWS deployment | ⛔ Out of scope by choice — no AWS account backs this project | — |

~15,100 lines of hand-written Go (excluding generated protobuf code), 107
tests, across 12 commits.

## Architecture

Every `kvnode` process can host either a single Raft group (the original
Checkpoint-1 shape) or N independent shards at once via
`internal/shard.Manager`, sharing one gRPC transport:

```
client → kvrouter (stateless) → metaservice (shard map + leader tracking)
                │
                ▼
     kvnode (one Raft group per shard, N replicas each)
      ┌──────────────────────────────┐
      │  shard.Manager                │
      │  ┌─────────┐   ┌─────────┐   │   gRPC peer traffic
      │  │ shard-1 │   │ shard-2 │───┼──────────────────────►  other kvnodes
      │  │ (Node)  │   │ (Node)  │   │
      │  └────┬────┘   └────┬────┘   │
      └───────┼─────────────┼────────┘
               ▼             ▼
        raftlog + engine (per shard, own data dir)
```

`metaservice` tracks each shard's key range, replica list, and current
leader (with a conditional-write guard against a stale leader-change
report overwriting a newer one). `kvrouter` resolves a key to its shard
and leader, caches that belief, and self-heals it — on a `leader_hint`
redirect *or* a transport-level failure reaching a now-dead leader — by
forcing a fresh lookup and retrying once.

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
- **A 20-seed fuzz test mixing random membership changes** into the same
  kill/partition/heal chaos, asserting joint-consensus safety holds even
  while the cluster's own configuration is changing mid-chaos.

Separately, the storage layer's own tests simulate a crash by truncating
a real file mid-write at every possible byte offset and verify replay
recovers exactly the records written before the tear, with no panic and
no silent corruption.

## Chaos and load testing: real numbers

Both of the numbers below came from actually running the tools against a
real cluster (real gRPC over loopback, real goroutines, real on-disk
storage — see [`internal/localcluster`](internal/localcluster), the
harness both are built on), not from estimating the algorithm's
theoretical bounds. Full methodology, raw data, and honest caveats about
what "in-process, single machine" does and doesn't measure are in
[`docs/benchmarks/`](docs/benchmarks/).

**Chaos testing** (`cmd/chaosmonkey`) kills the leader of a real 3-node
cluster and times how long until a client write succeeds again:

> Median (p50) failover of **111ms**, p99 of **137ms**, across 40 trials.

**Load testing** (`cmd/loadgen`) drives concurrent mixed read/write
traffic against a real 3-node cluster:

> Sustained **~20,000 ops/sec at 1.55ms p99 latency** (100k fixed-op run,
> 20 concurrent workers, 80/20 read/write mix, zero errors).

Running the load generator at scale surfaced two real, previously-latent
bugs in the Raft transport path — both confirmed root-caused and fixed
(see [`docs/benchmarks/load-testing.md`](docs/benchmarks/load-testing.md)
for the full story): an `InstallSnapshotChunk` was being routed through
the wrong `Transport` method and failing outright every time, and a
single-chunk snapshot transfer could exceed gRPC's default 4MiB message
cap under sustained write volume. Re-running the same workload after both
fixes produced zero occurrences of either failure.

## Real bugs, found and fixed

Three separate rounds of building this project surfaced genuine
correctness bugs — not typos, actual "this would silently do the wrong
thing in production" bugs:

1. **Transport interface/reality mismatch** (Checkpoint 1). The original
   `Transport` interface was shaped as synchronous per-RPC-kind methods
   (`SendRequestVote(req) (*resp, error)`), but `Node` never actually
   produces a reply synchronously — a response to an inbound vote or
   append request is just another outbound message, queued for a later
   `Ready()` call. Fixed with a single `RaftTransportService.Send(RaftMessage)`
   RPC carrying a `oneof` over every request *and* response type — the
   same shape `etcd/raft`'s own transport uses. Documented in
   [`pkg/raft/types.go`](pkg/raft/types.go)'s `Transport` doc comment.
2. **A silent false-success on a follower that doesn't know the leader
   yet.** `leaderHint()` returned `""` both when a node *is* the leader
   and when a follower simply doesn't know who the leader is yet (e.g.
   mid-election) — so a write landing in that window got a fake "OK"
   response for a write that was never proposed anywhere and silently
   vanished. Found by `internal/localcluster`'s very first end-to-end
   test. Fixed in `internal/server/kvserver.go` via `notLeaderResponse`,
   which now returns a real error instead of an ambiguous empty hint.
3. **`InstallSnapshotChunk` transport misdispatch** and **a gRPC message
   size cap hit by real snapshot payloads** — both found by load testing
   at scale, both fixed; see [above](#chaos-and-load-testing-real-numbers).

## Infrastructure-as-code, not deployed

`deploy/terraform` has complete, `terraform validate`-clean modules for a
real multi-AZ deployment: VPC across 3 availability zones, ECS Fargate
tasks per shard replica, a DynamoDB-backed shard map, Cloud Map service
discovery (no hardcoded peer IPs), an ALB fronting only the stateless
router (Raft nodes need direct leader connections, never a load
balancer), least-privilege IAM per component, and CloudWatch
dashboards/alarms. `deploy/docker` has Dockerfiles and both a
single-shard and multi-shard `docker-compose.yml` for local rehearsal.

None of this has been applied to a live AWS account — that's a
deliberate scope decision for this project, not an unfinished task. The
infrastructure is written the way it would actually need to look for a
real deployment; standing it up is left as the obvious next step for
whoever (a hiring team, a future me) wants to see it run for real.

## Getting started

Requires **Go 1.25+** and **[`buf`](https://buf.build)** (for protobuf
codegen). Docker is only needed for the optional Docker Compose
walkthrough below — everything else, including multi-node correctness
testing and the chaos/load tools, runs as plain Go processes with no
external dependencies.

```bash
git clone https://github.com/SushantPotu/raft-kv-store
cd raft-kv-store

go build ./...       # build all binaries
go test ./...         # run the full test suite
go test -race ./...   # same, with the race detector
```

### Run a real local cluster without Docker

`internal/localcluster` spins up a real multi-node cluster (real gRPC,
real on-disk storage, no fakes) as plain goroutines in one process — this
is what `cmd/chaosmonkey` and `cmd/loadgen` are built on:

```bash
go run ./cmd/chaosmonkey --nodes=3 --trials=40
go run ./cmd/loadgen --nodes=3 --concurrency=20 --ops=100000 --read-ratio=0.8
```

### Run a local 3-node cluster with Docker Compose

```bash
make docker-build
docker compose -f deploy/docker/docker-compose.yml up -d
go run ./cmd/kvctl --addr localhost:9001 put foo bar
go run ./cmd/kvctl --addr localhost:9002 get foo --consistency=stale
```

For the full manual walkthrough — bring the cluster up, confirm
replication, kill whichever node is leader, confirm a new one takes over
and keeps accepting writes, restart the killed node — see
[`docs/runbooks/local-cluster.md`](docs/runbooks/local-cluster.md). A
multi-shard variant is at
[`deploy/docker/docker-compose.multi-shard.yml`](deploy/docker/docker-compose.multi-shard.yml).

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
│   ├── raft/               # Raft consensus: election, replication, snapshotting,
│   │                       #   ReadIndex, joint-consensus membership + simulate/ harness
│   ├── storage/
│   │   ├── wal/            # shared segment/CRC framing primitive
│   │   ├── engine/         # Bitcask-style KV engine
│   │   ├── raftlog/        # durable Raft replication log
│   │   └── snapshot/       # snapshot serialization helpers
│   ├── statemachine/       # bridges committed Raft entries to the KV engine
│   ├── transport/grpc/     # raft.Transport over gRPC
│   ├── server/             # client-facing KVService gRPC server
│   ├── shard/              # Multi-Raft: N shards hosted per process
│   ├── routing/            # stateless shard-aware router (kvrouter's logic)
│   ├── metaservice/        # shard map + leader tracking (kvctl/kvrouter's source of truth)
│   ├── localcluster/       # real (non-Docker) multi-node cluster harness
│   ├── chaos/              # chaos-testing trial runner (cmd/chaosmonkey's logic)
│   └── loadgen/            # load-testing runner (cmd/loadgen's logic)
├── cmd/
│   ├── kvnode/             # the real node binary (single-shard or multi-shard)
│   ├── kvctl/              # CLI client
│   ├── kvrouter/           # stateless router binary
│   ├── metaservice/        # shard metadata service binary
│   ├── chaosmonkey/        # chaos-testing CLI
│   └── loadgen/            # load-testing CLI
├── deploy/
│   ├── docker/             # Dockerfiles + single-shard/multi-shard docker-compose files
│   └── terraform/          # VPC, ECS Fargate, DynamoDB, Cloud Map, ALB, IAM, CloudWatch modules
└── docs/
    ├── adr/                # architecture decision records
    ├── design/             # deep-dive design docs
    ├── runbooks/           # manual operational walkthroughs
    └── benchmarks/         # chaos/load testing methodology + real measured results
```

## Design docs

- [`docs/adr/0001-raft-core-io-separation.md`](docs/adr/0001-raft-core-io-separation.md) — why Raft Core has no background goroutine and no I/O of its own
- [`docs/adr/0002-two-separate-logs.md`](docs/adr/0002-two-separate-logs.md) — why the Raft log and the KV engine's WAL are independent
- [`docs/design/storage-engine.md`](docs/design/storage-engine.md) — on-disk formats, crash recovery, and the fsync-batching benchmark
- [`docs/runbooks/local-cluster.md`](docs/runbooks/local-cluster.md) — manual 3-node cluster test (put/get, kill the leader, confirm failover)
- [`docs/benchmarks/chaos-testing.md`](docs/benchmarks/chaos-testing.md) — chaos-testing methodology and real measured failover times
- [`docs/benchmarks/load-testing.md`](docs/benchmarks/load-testing.md) — load-testing methodology, real throughput/latency numbers, and the two bugs it found

## Roadmap

Everything in the original plan is done except real AWS deployment,
which is an explicit, deliberate scope decision (see
[Infrastructure-as-code, not deployed](#infrastructure-as-code-not-deployed))
rather than a gap. If this project were to continue, the natural next
steps would be:

1. Stand up the existing Terraform on a real AWS account across 3 AZs
2. A real chaos driver against that deployment (`ecs:StopTask`) implementing the same `chaos.Cluster` interface `internal/chaos` already defines for exactly this purpose
3. True multi-chunk snapshot streaming, removing the pragmatic 64MiB message-size ceiling in favor of an actually-unbounded keyspace

## Tech stack

Go 1.25 · gRPC · Protocol Buffers (`buf`) · Docker / Docker Compose ·
Terraform · AWS (ECS Fargate, DynamoDB, Cloud Map, ALB, IAM, CloudWatch)

## License

MIT — see [LICENSE](LICENSE).
