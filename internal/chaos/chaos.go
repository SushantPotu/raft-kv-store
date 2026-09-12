// Package chaos measures how long a real Raft/KV cluster takes to keep
// serving client writes after its leader is killed — the practical,
// defensible definition of "failover time" for this project (as opposed
// to instrumenting internal state to detect "a new leader was elected",
// which a client never observes directly and which would require reaching
// into raft.Node internals this package has no business touching).
//
// A trial is: start a cluster, wait for it to be healthy (a write
// succeeds somewhere), kill whichever node accepted that write, then time
// how long it takes for a write to succeed again against the surviving
// nodes. cmd/chaosmonkey drives this package to run many trials and
// report percentiles; internal/localcluster provides the real, in-process,
// real-gRPC cluster this package injects failures into (see that
// package's doc comment for exactly what "real" means here — no Docker,
// no separate OS processes, no fakes).
package chaos

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/SushantPotu/raft-kv-store/pkg/kvpb"
	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// Cluster is the minimal surface RunTrial and RunRestartCatchUp need from
// a target cluster. *localcluster.Cluster satisfies it as-is (its method
// set is a superset of this interface), which is exactly the point: this
// package depends on the interface, not the concrete type, so a future
// chaos driver that injects failures against a real deployment instead of
// an in-process one — e.g. one backed by ecs:StopTask/ecs:StartTask
// against a real AWS ECS cluster — could implement Cluster and reuse
// RunTrial/RunTrials/RunRestartCatchUp completely unchanged. Building
// that AWS driver is explicitly out of scope for this project (there is
// no AWS account backing it); this interface exists only so that door
// isn't nailed shut.
type Cluster interface {
	AwaitHealthy(ctx context.Context) (raft.NodeID, error)
	Put(ctx context.Context, key, value []byte, startHint raft.NodeID) (raft.NodeID, error)
	Get(ctx context.Context, id raft.NodeID, key []byte, consistency kvpb.Consistency) (value []byte, found bool, err error)
	Kill(id raft.NodeID) error
	Restart(id raft.NodeID) error
	NodeIDs() []raft.NodeID
	IsAlive(id raft.NodeID) bool
	Stop()
}

// Factory builds one fresh Cluster for a single trial. Trials use a fresh
// cluster each time (rather than reusing one cluster across many kills)
// so that one trial's outcome — term number, which node ends up leader,
// on-disk log state — can never leak into the next trial's timing; the
// cost is repaying cluster startup (leader election on a cold cluster)
// once per trial, which is cheap against internal/localcluster's
// sub-second-tuned defaults.
type Factory func() (Cluster, error)

// Result is one trial's outcome. Trial is 1-based and only meaningful
// relative to other Results produced by the same run (RunTrials assigns
// it; RunTrial leaves it zero for standalone callers to set themselves).
type Result struct {
	Trial      int
	Nodes      int
	KilledNode raft.NodeID
	NewLeader  raft.NodeID
	Failover   time.Duration
}

// RunTrial executes a single chaos trial against a freshly built cluster:
// wait for the cluster to elect a leader and accept a write, kill that
// leader, then measure wall-clock time until a write succeeds again. The
// cluster is always stopped before returning, including on error.
//
// nodes is recorded on the returned Result purely for reporting (grouping
// trials by cluster size); RunTrial has no way to ask a Cluster its own
// size since the interface deliberately doesn't expose it.
func RunTrial(ctx context.Context, newCluster Factory, nodes int) (Result, error) {
	c, err := newCluster()
	if err != nil {
		return Result{}, fmt.Errorf("chaos: start cluster: %w", err)
	}
	defer c.Stop()

	leader, err := c.AwaitHealthy(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("chaos: await healthy: %w", err)
	}

	if err := c.Kill(leader); err != nil {
		return Result{}, fmt.Errorf("chaos: kill %s: %w", leader, err)
	}

	// The measured interval starts the instant the leader is gone and
	// ends the instant a client-visible write next succeeds — this is
	// exactly what an application talking to the cluster would
	// experience, independent of how the surviving nodes internally
	// decide on a new leader. Put's own leader-following retry (the same
	// protocol kvctl/the router use in production) is what does the work
	// of finding whichever node just won the election; startHint="" lets
	// it start wherever it likes since we have no better guess.
	key := []byte(fmt.Sprintf("chaos-trial-%d", time.Now().UnixNano()))
	t0 := time.Now()
	newLeader, err := c.Put(ctx, key, []byte("v"), "")
	failover := time.Since(t0)
	if err != nil {
		return Result{}, fmt.Errorf("chaos: put after killing %s: %w", leader, err)
	}
	if newLeader == leader {
		return Result{}, fmt.Errorf("chaos: write succeeded against %s, which was just killed", leader)
	}

	return Result{
		Nodes:      nodes,
		KilledNode: leader,
		NewLeader:  newLeader,
		Failover:   failover,
	}, nil
}

// RunTrials runs n independent trials (each against its own freshly built
// cluster per Factory) and returns every successful Result. It stops and
// returns an error at the first failed trial rather than skipping it,
// since a failed trial (the cluster never recovered, or recovered onto
// the node that was just killed) indicates a real problem worth
// surfacing immediately rather than quietly shrinking the sample size.
func RunTrials(ctx context.Context, newCluster Factory, nodes, n int) ([]Result, error) {
	results := make([]Result, 0, n)
	for i := 0; i < n; i++ {
		res, err := RunTrial(ctx, newCluster, nodes)
		if err != nil {
			return results, fmt.Errorf("trial %d/%d: %w", i+1, n, err)
		}
		res.Trial = i + 1
		results = append(results, res)
	}
	return results, nil
}

// Stats summarizes a set of failover durations.
type Stats struct {
	Count         int
	Min, Max      time.Duration
	P50, P90, P99 time.Duration
}

// ComputeStats computes percentiles over durations using nearest-rank
// (the same definition most load-testing tools use for latency
// percentiles): P(p) is the smallest value at or above the fraction p of
// the sorted sample. It does not interpolate between ranks — with the
// trial counts this package expects (tens, not thousands), interpolation
// would suggest more precision than the sample actually supports.
func ComputeStats(durations []time.Duration) Stats {
	if len(durations) == 0 {
		return Stats{}
	}
	sorted := append([]time.Duration(nil), durations...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	rank := func(p float64) time.Duration {
		idx := int(math.Ceil(p*float64(len(sorted)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}

	return Stats{
		Count: len(sorted),
		Min:   sorted[0],
		Max:   sorted[len(sorted)-1],
		P50:   rank(0.50),
		P90:   rank(0.90),
		P99:   rank(0.99),
	}
}

// WriteCSV writes one row per Result (trial, nodes, killed node, node
// that took over, failover time in milliseconds) so raw trial data can be
// inspected or reprocessed independent of the summary Stats printed to
// stdout by cmd/chaosmonkey.
func WriteCSV(w io.Writer, results []Result) error {
	cw := csv.NewWriter(w)
	if err := cw.Write([]string{"trial", "nodes", "killed_node", "new_leader", "failover_ms"}); err != nil {
		return err
	}
	for _, r := range results {
		row := []string{
			strconv.Itoa(r.Trial),
			strconv.Itoa(r.Nodes),
			string(r.KilledNode),
			string(r.NewLeader),
			strconv.FormatFloat(float64(r.Failover.Microseconds())/1000.0, 'f', 3, 64),
		}
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}
