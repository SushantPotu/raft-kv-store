# Runbook: local 3-node cluster (Docker Compose)

This is the manual smoke test for Integration Checkpoint 1: bring up the
3-node, single-shard `kvnode` cluster defined in
`deploy/docker/docker-compose.yml`, confirm it elects a leader, accepts and
replicates a write, survives killing the leader by electing a new one, and
keeps serving writes afterward. Run this yourself locally (it needs a
working Docker daemon, which this environment did not have available when
Checkpoint 1's code was written and verified by build/vet/test alone —
see `docs/adr/` / the Checkpoint 1 report for that caveat).

## Prerequisites

- Go 1.25+ (`go version`)
- Docker Desktop (or another Docker daemon) running — verify with
  `docker info`
- From the repo root: `go build ./... && go test ./...` should already pass

## 1. Build the images

```bash
cd raft-kv-store
docker compose -f deploy/docker/docker-compose.yml build
```

(`make docker-build` also works, but additionally builds `kvrouter` and
`metaservice`, which aren't needed for this test and aren't wired into
`docker-compose.yml` yet.)

## 2. Bring up the cluster

```bash
docker compose -f deploy/docker/docker-compose.yml up -d
docker compose -f deploy/docker/docker-compose.yml ps
```

Three containers should be running: `kvnode-1`, `kvnode-2`, `kvnode-3`,
with client ports mapped to the host as 9001/9002/9003 respectively (see
`docker-compose.yml`). Give it a few seconds for the initial leader
election — `cmd/kvnode`'s tick interval and election-timeout ticks (see the
doc comment at the top of `cmd/kvnode/main.go`) put the first election at
roughly 500ms-1s after all three nodes are up and heartbeating, in the
absence of any startup skew between containers; in practice, allow a couple
of seconds. You can watch it happen:

```bash
docker compose -f deploy/docker/docker-compose.yml logs -f
```

## 3. Find the leader and do a write

Any node will tell you (via `leader_hint` on a failed write) who it thinks
the leader is, but the simplest way is to just try each one:

```bash
go run ./cmd/kvctl --addr localhost:9001 put foo bar
go run ./cmd/kvctl --addr localhost:9002 put foo bar
go run ./cmd/kvctl --addr localhost:9003 put foo bar
```

Exactly one of these three should print `OK`; the other two should print
`kvctl put: not leader, try <leader-node-id>` to stderr and exit 1. Note
which container (kvnode-1/2/3) is the leader — you'll need to kill it in
step 5.

## 4. Confirm replication

Read the key back from a *follower* with a stale (non-linearizable) read —
the default consistency mode — retrying briefly since replication isn't
instantaneous:

```bash
for i in 1 2 3 4 5; do
  go run ./cmd/kvctl --addr localhost:9002 get foo --consistency=stale && break
  sleep 1
done
```

This should eventually print `bar`.

## 5. Kill the leader and confirm failover

Using whichever container you identified as leader in step 3 (this example
assumes `kvnode-1`):

```bash
docker kill kvnode-1
```

Wait a few seconds for the remaining two nodes to time out the old leader's
heartbeats and elect a new one (bounded by the same election-timeout window
noted in step 2 — worst case a bit under 1s from the last heartbeat, plus
any in-flight heartbeat it never got to send). Then try a write against
each of the two survivors:

```bash
go run ./cmd/kvctl --addr localhost:9002 put foo baz
go run ./cmd/kvctl --addr localhost:9003 put foo baz
```

One of them should now succeed with `OK` — this proves failover actually
elected a new leader and the cluster kept accepting writes with only 2 of
3 nodes up (a majority of the original 3).

## 6. Restart the killed node and confirm it doesn't break anything

```bash
docker compose -f deploy/docker/docker-compose.yml start kvnode-1
```

`kvnode-1` comes back with an empty data directory (see the "ephemeral data
directory" note in `cmd/kvnode/main.go` — `docker-compose.yml` declares no
volume, which is an intentional Checkpoint 1 tradeoff: proving leader
election/replication/failover matters more here than surviving container
recreation with data intact). It should rejoin as a follower, catch up via
normal AppendEntries replication from the current leader, and not disrupt
the now-established leader. Confirm the cluster is still healthy:

```bash
go run ./cmd/kvctl --addr localhost:9002 get foo --consistency=stale
```

## 7. Tear down

```bash
docker compose -f deploy/docker/docker-compose.yml down
```

## What to record when you run this

- Wall-clock time from `up -d` to the first successful `put` (initial
  election time).
- Wall-clock time from `docker kill` on the leader to the first successful
  `put` against a survivor (failover time).
- Which node was the initial leader, and which node became leader after
  the kill (confirming it's actually a *different* node, not just "a
  follower died and nothing happened").
