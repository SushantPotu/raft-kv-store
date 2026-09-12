package loadgen

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// OpStats summarizes one class of operation (reads, writes, or both
// combined) over a Run: how many succeeded/failed, the resulting
// throughput, and latency percentiles across the successful ones. Failed
// operations are counted in Errors but excluded from the latency
// percentiles — a timeout or a "not leader" retry loop that never landed
// says something about availability, not about how fast a successful
// request was serviced, and mixing the two would understate the real
// latency of requests that actually succeeded.
type OpStats struct {
	Count      int64
	Errors     int64
	Throughput float64 // successful ops per second, over the Run's wall-clock duration

	Min, Mean, P50, P95, P99, Max time.Duration
}

// Report is the result of one Runner.Run call.
type Report struct {
	WallClock time.Duration

	Reads   OpStats
	Writes  OpStats
	Overall OpStats

	// ReadLatencies and WriteLatencies are every successful operation's
	// raw latency sample, in the order workers recorded them (not sorted;
	// OpStats' percentiles are computed from a sorted copy, not these
	// slices directly). Exposed so a caller like cmd/loadgen can dump raw
	// samples to a file for offline analysis instead of just the summary.
	ReadLatencies  []time.Duration
	WriteLatencies []time.Duration
}

// String renders a human-readable summary table, e.g. for printing to
// stdout from cmd/loadgen.
func (r *Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "wall clock: %s\n\n", r.WallClock.Round(time.Millisecond))
	fmt.Fprintf(&b, "%-8s%10s%10s%14s%10s%10s%10s%10s%10s%10s\n",
		"op", "count", "errors", "throughput", "min", "mean", "p50", "p95", "p99", "max")
	writeRow := func(name string, s OpStats) {
		fmt.Fprintf(&b, "%-8s%10d%10d%11.1f/s%10s%10s%10s%10s%10s%10s\n",
			name, s.Count, s.Errors, s.Throughput,
			s.Min.Round(time.Microsecond), s.Mean.Round(time.Microsecond),
			s.P50.Round(time.Microsecond), s.P95.Round(time.Microsecond), s.P99.Round(time.Microsecond),
			s.Max.Round(time.Microsecond))
	}
	writeRow("reads", r.Reads)
	writeRow("writes", r.Writes)
	writeRow("overall", r.Overall)
	return b.String()
}

// buildReport merges every worker's results (each worker owns a disjoint
// slot, so this is the only point that touches them all together) and
// computes OpStats for reads, writes, and both combined.
func buildReport(results []workerResult, wall time.Duration) *Report {
	var reads, writes []time.Duration
	var readErrs, writeErrs int64
	for _, res := range results {
		reads = append(reads, res.reads...)
		writes = append(writes, res.writes...)
		readErrs += res.readErrs
		writeErrs += res.writeErrs
	}

	overall := make([]time.Duration, 0, len(reads)+len(writes))
	overall = append(overall, reads...)
	overall = append(overall, writes...)

	return &Report{
		WallClock:      wall,
		Reads:          computeOpStats(reads, readErrs, wall),
		Writes:         computeOpStats(writes, writeErrs, wall),
		Overall:        computeOpStats(overall, readErrs+writeErrs, wall),
		ReadLatencies:  reads,
		WriteLatencies: writes,
	}
}

func computeOpStats(samples []time.Duration, errs int64, wall time.Duration) OpStats {
	s := OpStats{Count: int64(len(samples)), Errors: errs}
	if wall > 0 {
		s.Throughput = float64(len(samples)) / wall.Seconds()
	}
	if len(samples) == 0 {
		return s
	}

	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	s.Min = sorted[0]
	s.Max = sorted[len(sorted)-1]
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	s.Mean = sum / time.Duration(len(sorted))
	s.P50 = percentileSorted(sorted, 0.50)
	s.P95 = percentileSorted(sorted, 0.95)
	s.P99 = percentileSorted(sorted, 0.99)
	return s
}

// Percentile returns the p-th percentile (0 < p <= 1) of samples using the
// nearest-rank method: samples are sorted ascending and index
// ceil(p*n)-1 is returned, clamped to the slice's bounds. It's exported so
// callers/tests can compute an arbitrary percentile the same way OpStats'
// P50/P95/P99 are computed. Returns 0 for an empty input.
func Percentile(samples []time.Duration, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return percentileSorted(sorted, p)
}

// percentileSorted assumes sorted is already sorted ascending and
// non-empty.
func percentileSorted(sorted []time.Duration, p float64) time.Duration {
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
