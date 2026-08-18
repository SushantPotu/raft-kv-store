# raft-kv-store

A distributed, sharded key-value database with Raft consensus implemented
from scratch in Go, deployed across multiple AWS availability zones.

This is not a wrapper around an existing consensus library — the Raft
algorithm (leader election, log replication, snapshotting, linearizable
reads, and dynamic membership changes) is implemented from the paper
("In Search of an Understandable Consensus Algorithm", Ongaro & Ousterhout).
On top of that sits a from-scratch storage engine (segment-based WAL +
in-memory index + background compaction), and on top of *that*, a
Multi-Raft sharding layer that partitions the keyspace across independent
Raft groups and routes client requests to the correct shard's leader.

## Why this exists

Most personal database/distributed-systems projects either wrap an existing
consensus library or stay single-node. This project intentionally goes
deeper: real consensus, real storage engine internals, real multi-AZ AWS
deployment with service discovery, and chaos-tested failover — with
measured throughput/latency/failover numbers to back it up.

## Architecture

```
client
  │
  ▼
kvrouter (stateless) ──► metaservice (DynamoDB-backed shard map)
  │
  ▼
kvnode (Raft group per shard, N replicas across AZs)
  │
  ▼
storage engine (WAL + in-memory index + compaction)
```

See `docs/design/` for detailed design docs on each layer:
- [`docs/design/raft.md`](docs/design/raft.md) — consensus implementation
- [`docs/design/storage-engine.md`](docs/design/storage-engine.md) — WAL/compaction internals
- [`docs/design/sharding.md`](docs/design/sharding.md) — Multi-Raft sharding & routing
- [`docs/design/aws-architecture.md`](docs/design/aws-architecture.md) — cloud deployment

## Status

Early scaffolding — see `docs/adr/` for architecture decisions as they're made.

## Development

```bash
make build       # build all binaries
make test        # run unit tests
make test-race   # run unit tests with the race detector
make proto-gen   # regenerate protobuf code from proto/*.proto
make compose-up  # bring up a local 3-node cluster via Docker Compose
```

Requires Go 1.22+, Docker, and `buf` (for proto codegen). AWS deployment
(`deploy/terraform/`) additionally requires the AWS CLI and configured
credentials — see `deploy/terraform/envs/dev/README.md` once that's set up.

`cmd/kvnode` (Integration Checkpoint 1) wires the storage engine, Raft
core, and client/API layers into the real 3-node cluster `make compose-up`
starts. See [`docs/runbooks/local-cluster.md`](docs/runbooks/local-cluster.md)
for the full manual test: bring the cluster up, put/get a key, kill the
leader, confirm a new one takes over and keeps accepting writes.

## License

MIT — see [LICENSE](LICENSE).
