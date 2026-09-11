// Command chaosmonkey drives internal/chaos against a real, in-process
// localcluster.Cluster (see that package's doc comment for what "real"
// means here — real gRPC over loopback, real goroutines, real on-disk
// storage, no Docker/AWS) to produce actual measured failover-time
// numbers: kill the leader, time how long until a write next succeeds,
// repeat for many trials, report percentiles.
//
// Usage:
//
//	chaosmonkey [--nodes=3] [--trials=30] [--out=chaos-results.csv]
//	            [--also-sizes=5] [--extra-trials=10]
//
// Every trial's raw duration is written to --out as CSV; a percentile
// summary per cluster size is printed to stdout, followed by one
// restart+catch-up correctness check (see internal/chaos.RunRestartCatchUp)
// unless --skip-catchup is set.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/SushantPotu/raft-kv-store/internal/chaos"
	"github.com/SushantPotu/raft-kv-store/internal/localcluster"
)

func main() {
	os.Exit(Run(os.Args[1:], os.Stdout, os.Stderr))
}

// sizeRun is one (cluster size, trial count) pair to execute.
type sizeRun struct {
	nodes  int
	trials int
}

// Run executes chaosmonkey's CLI logic and returns a process exit code.
// It's exported, mirroring cmd/kvctl's Run, so a test can drive it
// in-process against a real (small, fast) cluster run without shelling
// out to a built binary.
func Run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("chaosmonkey", flag.ContinueOnError)
	fs.SetOutput(stderr)
	nodes := fs.Int("nodes", 3, "cluster size for the primary trial run")
	trials := fs.Int("trials", 30, "number of trials to run at --nodes size")
	out := fs.String("out", "chaos-results.csv", "path to write per-trial CSV results")
	alsoSizes := fs.String("also-sizes", "5", "comma-separated additional cluster sizes to also trial, at fewer trials each (empty to skip)")
	extraTrials := fs.Int("extra-trials", 10, "number of trials to run per --also-sizes entry")
	trialTimeout := fs.Duration("trial-timeout", 5*time.Second, "max wall-clock time allowed for a single trial before it's considered failed")
	skipCatchup := fs.Bool("skip-catchup", false, "skip the kill+restart catch-up correctness check")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *nodes < 1 {
		fmt.Fprintln(stderr, "chaosmonkey: --nodes must be >= 1")
		return 2
	}
	if *trials < 1 {
		fmt.Fprintln(stderr, "chaosmonkey: --trials must be >= 1")
		return 2
	}

	runs := []sizeRun{{nodes: *nodes, trials: *trials}}
	extraSizes, err := parseSizes(*alsoSizes)
	if err != nil {
		fmt.Fprintf(stderr, "chaosmonkey: --also-sizes: %v\n", err)
		return 2
	}
	for _, n := range extraSizes {
		runs = append(runs, sizeRun{nodes: n, trials: *extraTrials})
	}

	var all []chaos.Result
	for _, run := range runs {
		fmt.Fprintf(stdout, "running %d trials at nodes=%d...\n", run.trials, run.nodes)
		newCluster := clusterFactory(run.nodes)
		for i := 0; i < run.trials; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), *trialTimeout)
			res, err := chaos.RunTrial(ctx, newCluster, run.nodes)
			cancel()
			if err != nil {
				fmt.Fprintf(stderr, "chaosmonkey: trial %d (nodes=%d) failed: %v\n", i+1, run.nodes, err)
				return 1
			}
			res.Trial = len(all) + 1
			all = append(all, res)
		}
	}

	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintf(stderr, "chaosmonkey: create %s: %v\n", *out, err)
		return 1
	}
	writeErr := chaos.WriteCSV(f, all)
	closeErr := f.Close()
	if writeErr != nil {
		fmt.Fprintf(stderr, "chaosmonkey: write %s: %v\n", *out, writeErr)
		return 1
	}
	if closeErr != nil {
		fmt.Fprintf(stderr, "chaosmonkey: close %s: %v\n", *out, closeErr)
		return 1
	}
	fmt.Fprintf(stdout, "wrote %d trial rows to %s\n", len(all), *out)

	printSummary(stdout, all)

	if !*skipCatchup {
		fmt.Fprintln(stdout, "\nrunning restart+catch-up correctness check...")
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		cu, err := chaos.RunRestartCatchUp(ctx, clusterFactory(*nodes))
		cancel()
		if err != nil {
			fmt.Fprintf(stderr, "chaosmonkey: catch-up check: %v\n", err)
			return 1
		}
		if !cu.CaughtUp {
			fmt.Fprintf(stderr, "chaosmonkey: catch-up check FAILED: node %s never caught up (waited %s)\n", cu.KilledNode, cu.Elapsed)
			return 1
		}
		fmt.Fprintf(stdout, "catch-up check OK: killed node %s (new leader %s) caught up in %s after Restart\n", cu.KilledNode, cu.NewLeader, cu.Elapsed)
	}

	return 0
}

// clusterFactory returns a chaos.Factory that builds a fresh
// localcluster.Cluster of the given size, using localcluster's own
// tuned-for-sub-second-elections defaults (the same ones cmd/kvnode
// uses) rather than test-only fast ticks, so the numbers this tool
// reports reflect realistic timing rather than an artificially sped-up
// clock.
func clusterFactory(nodes int) chaos.Factory {
	return func() (chaos.Cluster, error) {
		return localcluster.Start(localcluster.Config{NumNodes: nodes})
	}
}

// parseSizes parses a comma-separated list of positive cluster sizes.
// An empty (or whitespace-only) input yields no sizes rather than an
// error, since --also-sizes="" is how a caller opts out of the
// additional-sizes run.
func parseSizes(s string) ([]int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	var sizes []int
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid size %q: %w", part, err)
		}
		if n < 1 {
			return nil, fmt.Errorf("invalid size %q: must be >= 1", part)
		}
		sizes = append(sizes, n)
	}
	return sizes, nil
}

// printSummary prints one percentile-summary line per distinct cluster
// size present in results, in ascending size order.
func printSummary(stdout io.Writer, results []chaos.Result) {
	bySize := make(map[int][]time.Duration)
	for _, r := range results {
		bySize[r.Nodes] = append(bySize[r.Nodes], r.Failover)
	}
	sizes := make([]int, 0, len(bySize))
	for size := range bySize {
		sizes = append(sizes, size)
	}
	sort.Ints(sizes)

	fmt.Fprintln(stdout, "\nfailover time summary:")
	for _, size := range sizes {
		st := chaos.ComputeStats(bySize[size])
		fmt.Fprintf(stdout, "  nodes=%d  n=%d  p50=%-10s p90=%-10s p99=%-10s max=%-10s min=%s\n",
			size, st.Count, st.P50, st.P90, st.P99, st.Max, st.Min)
	}
}
