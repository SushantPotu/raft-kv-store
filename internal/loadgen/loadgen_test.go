package loadgen

import (
	"context"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

// TestPercentileNearestRank checks Percentile against hand-computed
// expectations for a known, evenly-spaced dataset (1ms..100ms), so a
// regression in the nearest-rank formula (e.g. an off-by-one in the
// ceil(p*n)-1 index) would be caught directly rather than only showing up
// as a slightly-off number in a real Report.
func TestPercentileNearestRank(t *testing.T) {
	samples := make([]time.Duration, 100)
	for i := range samples {
		samples[i] = time.Duration(i+1) * time.Millisecond // 1ms..100ms
	}

	tests := []struct {
		p    float64
		want time.Duration
	}{
		{0.50, 50 * time.Millisecond}, // ceil(0.50*100)-1 = 49 -> samples[49] = 50ms
		{0.95, 95 * time.Millisecond}, // ceil(0.95*100)-1 = 94 -> samples[94] = 95ms
		{0.99, 99 * time.Millisecond}, // ceil(0.99*100)-1 = 98 -> samples[98] = 99ms
		{1.00, 100 * time.Millisecond},
	}
	for _, tt := range tests {
		if got := Percentile(samples, tt.p); got != tt.want {
			t.Errorf("Percentile(samples, %v) = %v, want %v", tt.p, got, tt.want)
		}
	}

	// Order-independence: a shuffled copy must give the same answer, since
	// Percentile sorts internally rather than assuming its input is sorted.
	shuffled := append([]time.Duration(nil), samples...)
	rand.New(rand.NewSource(1)).Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	if got := Percentile(shuffled, 0.99); got != 99*time.Millisecond {
		t.Errorf("Percentile(shuffled, 0.99) = %v, want %v", got, 99*time.Millisecond)
	}
}

func TestPercentileEmpty(t *testing.T) {
	if got := Percentile(nil, 0.5); got != 0 {
		t.Errorf("Percentile(nil, 0.5) = %v, want 0", got)
	}
}

// TestComputeOpStatsSingleSample guards against a single-element slice
// tripping up percentile index clamping (a common off-by-one source) or
// the mean computation.
func TestComputeOpStatsSingleSample(t *testing.T) {
	s := computeOpStats([]time.Duration{7 * time.Millisecond}, 0, time.Second)
	if s.Count != 1 || s.Min != 7*time.Millisecond || s.Max != 7*time.Millisecond || s.Mean != 7*time.Millisecond {
		t.Fatalf("computeOpStats(single) = %+v, want count=1 min=max=mean=7ms", s)
	}
	if s.P50 != 7*time.Millisecond || s.P95 != 7*time.Millisecond || s.P99 != 7*time.Millisecond {
		t.Fatalf("computeOpStats(single) percentiles = p50=%v p95=%v p99=%v, want all 7ms", s.P50, s.P95, s.P99)
	}
}

// TestComputeOpStatsEmptyDoesNotDivideByZero exercises the all-errors
// case (e.g. every write in a run hit a dead cluster): no samples, only
// errors, must not panic and must report zero-valued latencies rather
// than NaN/Inf throughput.
func TestComputeOpStatsEmptyDoesNotDivideByZero(t *testing.T) {
	s := computeOpStats(nil, 5, time.Second)
	if s.Count != 0 || s.Errors != 5 || s.Throughput != 0 {
		t.Fatalf("computeOpStats(empty) = %+v, want count=0 errors=5 throughput=0", s)
	}
	if s.Min != 0 || s.Max != 0 || s.Mean != 0 {
		t.Fatalf("computeOpStats(empty) latencies = %+v, want all zero", s)
	}
}

// TestOpBudgetLimitsTotalOps checks the shared-atomic-counter design
// itself, independent of the worker pool: many goroutines hammering
// take() concurrently must yield exactly `ops` successful takes in total,
// never more (a race here would silently over-run Config.Ops in a real
// run) and never fewer.
func TestOpBudgetLimitsTotalOps(t *testing.T) {
	const ops = 997 // deliberately not a multiple of the goroutine count
	const goroutines = 20

	b := newOpBudget(ops)
	var granted int64
	done := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		go func() {
			for b.take() {
				atomic.AddInt64(&granted, 1)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}

	if granted != ops {
		t.Fatalf("opBudget granted %d takes, want exactly %d", granted, ops)
	}
}

func TestOpBudgetUnlimitedAlwaysAllows(t *testing.T) {
	b := newOpBudget(0)
	for i := 0; i < 10_000; i++ {
		if !b.take() {
			t.Fatalf("unlimited opBudget refused take() on iteration %d", i)
		}
	}
}

// fakeOps builds trivial, instant doRead/doWrite funcs for exercising
// Runner.workerLoop's scheduling logic without any network I/O, plus
// atomic counters to assert on afterward.
type fakeOps struct {
	reads, writes int64
}

func (f *fakeOps) read(ctx context.Context, rng *rand.Rand, key []byte) error {
	atomic.AddInt64(&f.reads, 1)
	return nil
}

func (f *fakeOps) write(ctx context.Context, key, value []byte) error {
	atomic.AddInt64(&f.writes, 1)
	return nil
}

func newFakeRunner(cfg Config, ops *fakeOps) *Runner {
	cfg.setDefaults()
	return &Runner{
		cfg:     cfg,
		doRead:  ops.read,
		doWrite: ops.write,
	}
}

// TestRunRespectsOpsBudget is the core "worker pool respects a stop
// signal" case for Config.Ops: with no Duration set at all (so nothing
// but the budget could possibly stop it), Run must still return once
// exactly Ops operations have been issued, using a concurrency level that
// doesn't evenly divide Ops so an off-by-one in the budget or the loop's
// exit check would show up as a mismatch here.
func TestRunRespectsOpsBudget(t *testing.T) {
	ops := &fakeOps{}
	r := newFakeRunner(Config{Concurrency: 7, ReadRatio: 0.5, Ops: 5003}, ops)

	report, err := r.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	total := ops.reads + ops.writes
	if total != 5003 {
		t.Fatalf("issued %d total ops, want exactly 5003", total)
	}
	if report.Overall.Count != 5003 {
		t.Fatalf("report.Overall.Count = %d, want 5003", report.Overall.Count)
	}
}

// TestRunStopsOnContextCancel is the "stop signal" case for external
// cancellation: workers must actually exit promptly once ctx is
// cancelled, not spin forever, and Run itself must return in bounded time
// even with no Ops/Duration limit reached.
func TestRunStopsOnContextCancel(t *testing.T) {
	ops := &fakeOps{}
	r := newFakeRunner(Config{Concurrency: 10, ReadRatio: 0.8, Duration: time.Hour}, ops)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)

	runDone := make(chan struct{})
	go func() {
		if _, err := r.Run(ctx); err != nil {
			t.Errorf("Run: %v", err)
		}
		close(runDone)
	}()

	select {
	case <-runDone:
		// Workers stopped promptly after cancellation, as expected.
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s of context cancellation")
	}

	if ops.reads+ops.writes == 0 {
		t.Fatal("no operations were issued before cancellation; test isn't exercising anything")
	}
}

// TestRunReadRatioExtremes checks that ReadRatio=0 and ReadRatio=1 produce
// write-only and read-only traffic respectively — the boundary values a
// naive `rng.Float64() <= ReadRatio` (using <= instead of <) could get
// subtly wrong for the ReadRatio=0 case.
func TestRunReadRatioExtremes(t *testing.T) {
	writeOnly := &fakeOps{}
	r := newFakeRunner(Config{Concurrency: 2, ReadRatio: 0, Ops: 200}, writeOnly)
	if _, err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if writeOnly.reads != 0 || writeOnly.writes != 200 {
		t.Fatalf("ReadRatio=0: reads=%d writes=%d, want reads=0 writes=200", writeOnly.reads, writeOnly.writes)
	}

	readOnly := &fakeOps{}
	r = newFakeRunner(Config{Concurrency: 2, ReadRatio: 1, Ops: 200}, readOnly)
	if _, err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if readOnly.writes != 0 || readOnly.reads != 200 {
		t.Fatalf("ReadRatio=1: reads=%d writes=%d, want reads=200 writes=0", readOnly.reads, readOnly.writes)
	}
}

// TestRunRequiresDurationOrOps checks Config validation rejects a Config
// with neither limit set, rather than hanging forever (a real footgun for
// a load-testing tool specifically).
func TestRunRequiresDurationOrOps(t *testing.T) {
	r := newFakeRunner(Config{Concurrency: 1, ReadRatio: 0.5}, &fakeOps{})
	if _, err := r.Run(context.Background()); err == nil {
		t.Fatal("Run with neither Duration nor Ops set: want an error, got nil")
	}
}

func TestConfigValidateRejectsBadReadRatio(t *testing.T) {
	cfg := Config{ReadRatio: 1.5, Ops: 10}
	if err := cfg.validate(); err == nil {
		t.Fatal("validate() with ReadRatio=1.5: want an error, got nil")
	}
}
