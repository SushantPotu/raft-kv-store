package chaos

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/SushantPotu/raft-kv-store/internal/localcluster"
)

// newTestCluster returns a Factory that builds a real 3-node
// localcluster.Cluster, tuned the same way localcluster's own tests are
// (fast ticks) so trials in this test file run quickly.
func newTestCluster(nodes int) Factory {
	return func() (Cluster, error) {
		return localcluster.Start(localcluster.Config{
			NumNodes:          nodes,
			TickInterval:      5 * time.Millisecond,
			ReadyPollInterval: 2 * time.Millisecond,
		})
	}
}

// TestRunTrialMeasuresRealFailover proves chaos.RunTrial actually drives a
// real localcluster.Cluster through a kill and measures a plausible,
// nonzero failover time — not just that it compiles against the Cluster
// interface.
func TestRunTrialMeasuresRealFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := RunTrial(ctx, newTestCluster(3), 3)
	if err != nil {
		t.Fatalf("RunTrial: %v", err)
	}
	if res.KilledNode == "" || res.NewLeader == "" {
		t.Fatalf("RunTrial returned incomplete result: %+v", res)
	}
	if res.NewLeader == res.KilledNode {
		t.Fatalf("new leader %s is the node that was killed", res.NewLeader)
	}
	if res.Failover <= 0 {
		t.Fatalf("Failover = %s, want > 0", res.Failover)
	}
	// A generous upper bound: this cluster's default election timeout
	// tops out in the low hundreds of ms (see localcluster.Config's
	// defaults), so a failover taking multiple seconds would indicate
	// something is actually wrong rather than just "a bit slow".
	if res.Failover > 5*time.Second {
		t.Fatalf("Failover = %s, suspiciously slow", res.Failover)
	}
}

// TestRunTrialsCollectsMultipleIndependentTrials confirms RunTrials
// assigns sequential 1-based Trial numbers and that each trial really did
// run against its own fresh cluster (distinct killed-node/new-leader
// pairs are not guaranteed, but every trial must independently succeed).
func TestRunTrialsCollectsMultipleIndependentTrials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const n = 5
	results, err := RunTrials(ctx, newTestCluster(3), 3, n)
	if err != nil {
		t.Fatalf("RunTrials: %v", err)
	}
	if len(results) != n {
		t.Fatalf("len(results) = %d, want %d", len(results), n)
	}
	for i, r := range results {
		if r.Trial != i+1 {
			t.Fatalf("results[%d].Trial = %d, want %d", i, r.Trial, i+1)
		}
		if r.Nodes != 3 {
			t.Fatalf("results[%d].Nodes = %d, want 3", i, r.Nodes)
		}
		if r.Failover <= 0 {
			t.Fatalf("results[%d].Failover = %s, want > 0", i, r.Failover)
		}
	}
}

func TestComputeStats(t *testing.T) {
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	durations := []time.Duration{ms(10), ms(20), ms(30), ms(40), ms(50), ms(60), ms(70), ms(80), ms(90), ms(100)}

	st := ComputeStats(durations)
	if st.Count != 10 {
		t.Fatalf("Count = %d, want 10", st.Count)
	}
	if st.Min != ms(10) {
		t.Fatalf("Min = %s, want 10ms", st.Min)
	}
	if st.Max != ms(100) {
		t.Fatalf("Max = %s, want 100ms", st.Max)
	}
	// Nearest-rank P50 over 10 sorted samples is index ceil(0.5*10)-1 = 4
	// -> the 5th value, 50ms.
	if st.P50 != ms(50) {
		t.Fatalf("P50 = %s, want 50ms", st.P50)
	}
	// P90 -> index ceil(0.9*10)-1 = 8 -> 9th value, 90ms.
	if st.P90 != ms(90) {
		t.Fatalf("P90 = %s, want 90ms", st.P90)
	}
	// P99 -> index ceil(0.99*10)-1 = 9 -> 10th value, 100ms (clamped to max).
	if st.P99 != ms(100) {
		t.Fatalf("P99 = %s, want 100ms", st.P99)
	}
}

func TestComputeStatsEmpty(t *testing.T) {
	if st := ComputeStats(nil); st.Count != 0 {
		t.Fatalf("ComputeStats(nil) = %+v, want zero value", st)
	}
}

func TestWriteCSV(t *testing.T) {
	results := []Result{
		{Trial: 1, Nodes: 3, KilledNode: "node-1", NewLeader: "node-2", Failover: 12500 * time.Microsecond},
		{Trial: 2, Nodes: 3, KilledNode: "node-2", NewLeader: "node-3", Failover: 8 * time.Millisecond},
	}
	var buf strings.Builder
	if err := WriteCSV(&buf, results); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}
	out := buf.String()
	wantLines := []string{
		"trial,nodes,killed_node,new_leader,failover_ms",
		"1,3,node-1,node-2,12.500",
		"2,3,node-2,node-3,8.000",
	}
	for _, want := range wantLines {
		if !strings.Contains(out, want) {
			t.Fatalf("WriteCSV output missing line %q; got:\n%s", want, out)
		}
	}
}

// TestRunRestartCatchUp is the correctness check required alongside
// timing trials: prove that Kill followed by Restart against a real
// cluster (not just localcluster's own unit test) really does let the
// restarted node catch up on writes it missed while dead.
func TestRunRestartCatchUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	res, err := RunRestartCatchUp(ctx, newTestCluster(3))
	if err != nil {
		t.Fatalf("RunRestartCatchUp: %v", err)
	}
	if !res.CaughtUp {
		t.Fatalf("restarted node %s never caught up (elapsed %s)", res.KilledNode, res.Elapsed)
	}
	t.Logf("restart+catch-up: killed=%s newLeader=%s elapsed=%s", res.KilledNode, res.NewLeader, res.Elapsed)
}
