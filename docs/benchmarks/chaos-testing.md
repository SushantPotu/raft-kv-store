# Chaos testing: measured failover time (Workstream J)

This documents `internal/chaos` and `cmd/chaosmonkey`: a tool that kills
the leader of a real Raft/KV cluster and measures how long the cluster
takes to keep serving client writes afterward. The numbers below are
**actually measured** by running the tool (see "How to reproduce"), not
estimated from the algorithm's theoretical bounds.

## Environment (read this before comparing these numbers to anything else)

Everything here runs against `internal/localcluster`: a real, in-process,
multi-node Raft/KV cluster using real gRPC over `127.0.0.1`, real
goroutines, and real on-disk storage in temp directories. It is **not**
Docker, **not** separate OS processes, and **not** a multi-machine or
cloud deployment. Concretely, that means:

- No network latency, packet loss, or partition behavior — loopback
  round-trips are effectively free compared to a real network, so these
  numbers are a **lower bound**, not a prediction, for failover time
  across real machines/AZs.
- No OS-level process kill/restart cost, no container scheduler
  reconciliation delay, no cloud load balancer health-check interval —
  `Cluster.Kill` stops goroutines and closes file handles in the same Go
  process; a real orchestrator (ECS task replacement, Kubernetes pod
  eviction, etc.) adds seconds, not milliseconds, on top of Raft's own
  election time.
- All nodes run on one machine with one clock, so there's no clock skew
  between "replicas."

The one honest, meaningful thing this setup does measure well: **Raft's
own algorithmic contribution to failover time** — the leader-election
timeout window, the retry/leader-following behavior a client goes
through — decoupled from infrastructure-specific recovery cost. A "real
AWS chaos driver" (e.g. stopping an ECS task and reconnecting through a
real load balancer) would be a natural follow-up; `chaos.Cluster` is
defined as an interface specifically so such a driver could implement it
and reuse `RunTrial`/`RunTrials` unchanged (see its doc comment in
`internal/chaos/chaos.go`). Building that driver is out of scope here —
this project has no AWS account — so it isn't attempted.

## Methodology

For each trial (`chaos.RunTrial`):

1. Start a fresh `localcluster.Cluster` (default config: the same
   tuned-for-sub-second-elections defaults `cmd/kvnode` uses — 10ms
   ticks, 10-20 tick randomized election timeout, 2-tick heartbeat
   interval).
2. Call `AwaitHealthy`, which retries a `Put` until some node accepts it
   — that node is the current leader.
3. `Kill` that leader (closes its gRPC servers, storage files, and
   cancels its goroutines — see `localcluster.Cluster.Kill`'s doc comment
   for exactly what is and isn't simulated).
4. Start a wall-clock timer, then call `Put` again with no leader hint
   (the same leader-following retry protocol `kvctl`/the router use in
   production). Stop the timer the instant that `Put` succeeds.

**Failover time = the interval in step 4.** This is "time until a
client-visible write succeeds again," not "time until some raft.Node
internally flips to Leader" — the former is what an application actually
experiences, and doesn't require reaching into Raft internals to define.

Each trial uses its own freshly started cluster, so trials are
independent (no leftover term number or log state from one trial can
affect the next). A separate, one-off check (`chaos.RunRestartCatchUp`)
kills the leader, confirms a write succeeds against the survivors,
`Restart`s the killed node, and polls until it serves that write — a
correctness check that Kill+Restart works under an actual chaos run, not
just in `internal/localcluster`'s own unit test.

## Results (actually measured)

Run on 2026-09-16, local Windows dev machine, via:

```
chaosmonkey --nodes=3 --trials=40 --also-sizes=5 --extra-trials=15
```

| Cluster size | Trials | p50 | p90 | p99 | max | min |
|---|---|---|---|---|---|---|
| 3 nodes | 40 | 110.9ms | 127.3ms | 137.3ms | 137.3ms | 97.8ms |
| 5 nodes | 15 | 101.5ms | 114.1ms | 202.3ms | 202.3ms | 99.5ms |

Resume-bullet-ready phrasing for the primary (3-node) configuration:

> Built a chaos-testing harness that kills the leader of a real
> in-process Raft cluster and measures client-visible write recovery
> time across 40 trials: median (p50) failover of **111ms**, p99 of
> **137ms**, max **137ms**.

The restart+catch-up correctness check passed in the same run: the
killed node rejoined, and after `Restart` it served a write made while it
was dead within 437ms of the restart (a one-off measurement of AppendEntries
catch-up, not a percentile — see `chaos.RunRestartCatchUp`).

Raw per-trial rows for this run are in
[`chaos-results-sample.csv`](./chaos-results-sample.csv) (55 rows: 40 at
3 nodes, 15 at 5 nodes) alongside this document. `cmd/chaosmonkey` writes
a fresh CSV like this on every run via `--out` (default
`chaos-results.csv` in the working directory); it is not itself checked
in as build output; the copy here is a snapshot of one real run for
reference.

### Notes on the numbers

- All values cluster around 100-140ms, which tracks with the configured
  election timeout window (10-20 ticks * 10ms/tick = 100-200ms): a
  follower has to wait out (most of) a randomized election timeout before
  it can even start a new election, so failover time here is dominated by
  that timeout, not by network or disk I/O.
- The 5-node run's p99/max (202ms) came from a single slower trial; with
  only 15 samples one outlier moves the tail a lot. More trials would
  give a more stable tail estimate — 15 was chosen to keep the "handful of
  trials at a different size" comparison quick rather than to produce a
  tight p99 for 5 nodes specifically.
- 5 nodes did not measurably increase typical failover time relative to 3
  nodes in this run — expected, since Raft's election timeout mechanism
  doesn't scale with cluster size the way, say, a leader needing
  majority acks for replication does; it would take a much larger cluster
  or added network latency to see that show up.

## How to reproduce

```
cd raft-kv-store
go run ./cmd/chaosmonkey --nodes=3 --trials=40 --also-sizes=5 --extra-trials=15
```

Flags (`chaosmonkey --help` via any unrecognized flag) include `--nodes`,
`--trials`, `--out` (CSV path), `--also-sizes` (comma-separated extra
cluster sizes, empty to skip), `--extra-trials` (trial count for each
extra size), `--trial-timeout` (per-trial deadline), and `--skip-catchup`
(skip the restart correctness check).

## Test coverage

`internal/chaos/chaos_test.go` and `cmd/chaosmonkey/main_test.go` cover
the tool's own logic against real (small, fast-tick) clusters:

- `RunTrial` measures a plausible nonzero failover and correctly
  identifies that the new leader differs from the killed node.
- `RunTrials` runs several independent trials and numbers them correctly.
- `ComputeStats` percentile math (verified against hand-computed
  nearest-rank values).
- `WriteCSV` output format.
- `RunRestartCatchUp` end-to-end against a real cluster.
- The CLI (`Run`) end-to-end: flag parsing/validation, running real
  trials, writing CSV, printing a summary, and the catch-up check — all
  in-process, no subprocess.
