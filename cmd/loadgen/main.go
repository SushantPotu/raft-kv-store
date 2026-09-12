// Command loadgen is Workstream K's load-testing tool: it starts a real,
// in-process, multi-node Raft/KV cluster via internal/localcluster (real
// gRPC over 127.0.0.1, real goroutines, real on-disk storage — see that
// package's doc comment for exactly what "real" does and doesn't mean
// here), waits for an initial leader election, then drives it with
// internal/loadgen.Runner and prints a measured throughput/latency
// report.
//
// No Docker, no AWS, no multi-machine deployment: this is a genuine
// measurement of this codebase's real gRPC/Raft/storage path under
// concurrent load, just not of a real multi-machine deployment of it. See
// docs/benchmarks/load-testing.md for the methodology writeup and the
// actual numbers this tool has produced.
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/SushantPotu/raft-kv-store/internal/loadgen"
	"github.com/SushantPotu/raft-kv-store/internal/localcluster"
	"github.com/SushantPotu/raft-kv-store/pkg/kvpb"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr *os.File) int {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	fs.SetOutput(stderr)

	nodes := fs.Int("nodes", 3, "number of nodes in the local cluster")
	concurrency := fs.Int("concurrency", 10, "number of concurrent worker goroutines")
	duration := fs.Duration("duration", 10*time.Second, "how long to drive load (0 to disable, use --ops instead)")
	ops := fs.Int64("ops", 0, "total operations to issue across all workers (0 to disable, use --duration instead)")
	readRatio := fs.Float64("read-ratio", 0.8, "fraction of operations that are reads (0..1); the rest are writes")
	valueSize := fs.Int("value-size", 64, "size in bytes of values written")
	keyspace := fs.Int("keyspace", 1000, "number of distinct keys operations are drawn from")
	consistency := fs.String("consistency", "stale", `read consistency: "stale" or "linearizable"`)
	requestTimeout := fs.Duration("request-timeout", 2*time.Second, "per-RPC timeout")
	overallTimeout := fs.Duration("timeout", 2*time.Minute, "safety timeout for the whole run (mainly relevant to --ops runs, which have no other time bound)")
	tickInterval := fs.Duration("tick-interval", 10*time.Millisecond, "cluster's raft tick interval (passed through to localcluster.Config)")
	seed := fs.Int64("seed", 0, "worker RNG seed (0 = seed from current time)")
	samplesFile := fs.String("samples-file", "", "optional path to write raw per-operation latency samples as CSV")
	killLeaderAfter := fs.Duration("chaos-kill-leader-after", 0,
		"bonus chaos scenario: if > 0, kill the cluster's initial leader this long into the run to capture the failover-visible latency spike (0 disables)")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	consistencyPB, err := parseConsistency(*consistency)
	if err != nil {
		fmt.Fprintf(stderr, "loadgen: %v\n", err)
		return 2
	}

	// --duration has a nonzero default so a bare `loadgen` with no flags
	// does something useful, but that default must not silently cut off
	// an explicit --ops run at 10s. fs.Visit reports only flags the user
	// actually set, so an --ops run (without an explicit --duration too)
	// gets an unbounded (Duration: 0) config, letting it run to
	// completion; --ops together with an explicit --duration keeps both,
	// per internal/loadgen.Config's documented "whichever hits first"
	// combination.
	explicitFlags := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { explicitFlags[f.Name] = true })
	effectiveDuration := *duration
	if *ops > 0 && !explicitFlags["duration"] {
		effectiveDuration = 0
	}

	cluster, err := localcluster.Start(localcluster.Config{
		NumNodes:     *nodes,
		TickInterval: *tickInterval,
	})
	if err != nil {
		fmt.Fprintf(stderr, "loadgen: start cluster: %v\n", err)
		return 1
	}
	defer cluster.Stop()

	healthCtx, healthCancel := context.WithTimeout(context.Background(), 10*time.Second)
	initialLeader, err := cluster.AwaitHealthy(healthCtx)
	healthCancel()
	if err != nil {
		fmt.Fprintf(stderr, "loadgen: cluster did not become healthy: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "cluster ready: %d nodes, initial leader %s\n", *nodes, initialLeader)

	targets := make([]loadgen.Target, 0, len(cluster.NodeIDs()))
	for _, id := range cluster.NodeIDs() {
		targets = append(targets, loadgen.Target{ID: string(id), Addr: cluster.ClientAddr(id)})
	}

	runner, err := loadgen.New(loadgen.Config{
		Targets:        targets,
		Concurrency:    *concurrency,
		Duration:       effectiveDuration,
		Ops:            *ops,
		ReadRatio:      *readRatio,
		ValueSize:      *valueSize,
		KeyspaceSize:   *keyspace,
		Consistency:    consistencyPB,
		RequestTimeout: *requestTimeout,
		RandSeed:       *seed,
	})
	if err != nil {
		fmt.Fprintf(stderr, "loadgen: %v\n", err)
		return 1
	}
	defer runner.Close()

	if *killLeaderAfter > 0 {
		// Bonus chaos scenario (see this command's doc comment and
		// docs/benchmarks/load-testing.md): kill the node that was leader
		// at startup partway through the run. It may or may not still be
		// leader by then, but internal/localcluster.Cluster.Kill on
		// whichever node it names is a real crash either way, and the
		// resulting write-latency spike while the survivors elect a
		// replacement is exactly what's interesting to capture here.
		go func() {
			time.Sleep(*killLeaderAfter)
			fmt.Fprintf(stdout, "chaos: killing %s at t+%s\n", initialLeader, *killLeaderAfter)
			if err := cluster.Kill(initialLeader); err != nil {
				fmt.Fprintf(stderr, "loadgen: chaos kill %s: %v\n", initialLeader, err)
			}
		}()
	}

	runCtx, runCancel := context.WithTimeout(context.Background(), *overallTimeout)
	defer runCancel()

	fmt.Fprintf(stdout, "running: concurrency=%d read-ratio=%.2f", *concurrency, *readRatio)
	switch {
	case *ops > 0 && effectiveDuration > 0:
		fmt.Fprintf(stdout, " ops=%d duration=%s (whichever comes first)\n", *ops, effectiveDuration)
	case *ops > 0:
		fmt.Fprintf(stdout, " ops=%d\n", *ops)
	default:
		fmt.Fprintf(stdout, " duration=%s\n", effectiveDuration)
	}

	report, err := runner.Run(runCtx)
	if err != nil {
		fmt.Fprintf(stderr, "loadgen: run: %v\n", err)
		return 1
	}

	fmt.Fprintln(stdout)
	fmt.Fprint(stdout, report.String())

	if *samplesFile != "" {
		if err := writeSamplesCSV(*samplesFile, report); err != nil {
			fmt.Fprintf(stderr, "loadgen: write samples file: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "\nraw latency samples written to %s\n", *samplesFile)
	}

	return 0
}

func parseConsistency(s string) (kvpb.Consistency, error) {
	switch s {
	case "stale", "":
		return kvpb.Consistency_CONSISTENCY_STALE, nil
	case "linearizable":
		return kvpb.Consistency_CONSISTENCY_LINEARIZABLE, nil
	default:
		return 0, fmt.Errorf("unknown --consistency %q (want stale|linearizable)", s)
	}
}

// writeSamplesCSV dumps every successful operation's latency (in
// microseconds, one row per operation) so a caller can compute their own
// percentiles, plot a histogram, etc. instead of only seeing the summary
// Report.String() prints.
func writeSamplesCSV(path string, report *loadgen.Report) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write([]string{"op", "latency_us"}); err != nil {
		return err
	}
	for _, d := range report.ReadLatencies {
		if err := w.Write([]string{"read", strconv.FormatInt(d.Microseconds(), 10)}); err != nil {
			return err
		}
	}
	for _, d := range report.WriteLatencies {
		if err := w.Write([]string{"write", strconv.FormatInt(d.Microseconds(), 10)}); err != nil {
			return err
		}
	}
	return w.Error()
}
