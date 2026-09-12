// Package loadgen implements a configurable load generator (Workstream K)
// for driving a real KVService deployment with concurrent readers and
// writers and reporting genuine, measured throughput/latency numbers —
// not a theoretical estimate.
//
// It is deliberately decoupled from internal/localcluster: a Runner talks
// to a set of Target{ID, Addr} gRPC endpoints over persistent connections
// it dials once and reuses for every request (never
// internal/localcluster.Cluster.Put/Get's dial-per-call convenience
// helpers, which are fine for correctness tests but would make the
// connection setup itself the bottleneck under real concurrency). Writes
// follow leader_hint redirects using exactly the retry protocol
// internal/localcluster.Cluster.Put, internal/routing.Router, and
// cmd/kvctl already use against the same KVService contract — see
// (*Runner).put. This keeps Runner usable against any KVService
// deployment (a real multi-process cluster, not only localcluster), and
// keeps its worker-pool/percentile logic unit-testable without spinning
// up any gRPC servers at all: loadgen_test.go swaps in fake read/write
// funcs to exercise scheduling (stop-on-cancel, op-budget accounting) in
// isolation from the network.
package loadgen

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/SushantPotu/raft-kv-store/pkg/kvpb"
)

// Target is one gRPC-reachable KVService replica a Runner can send
// requests to. ID must match the identifier a write response's
// leader_hint field uses for this replica (for internal/localcluster this
// is the raft.NodeID string, e.g. "node-1") so a write retry can follow a
// hint straight to the right persistent connection instead of treating it
// as an opaque failure and round-robining blindly.
type Target struct {
	ID   string
	Addr string
}

// Config controls one Run. Concurrency, KeyspaceSize, RequestTimeout and
// ShardID have workable defaults (see setDefaults); ReadRatio, ValueSize,
// and one of Duration/Ops do not, since a zero value is meaningful for
// each of them (write-only load, empty values, "run until Ops is hit"
// respectively) and silently overriding a caller's explicit zero would be
// a worse default than just requiring it.
type Config struct {
	// Targets is the set of replicas to send requests to. Required by New;
	// unused by Run directly (a Runner built without New, e.g. in tests,
	// supplies its own doRead/doWrite instead).
	Targets []Target

	// Concurrency is the number of worker goroutines issuing requests
	// concurrently. Defaults to 1.
	Concurrency int

	// Duration bounds how long Run drives load, if > 0. Combinable with
	// Ops (whichever limit is hit first stops the run); at least one of
	// the two must be positive.
	Duration time.Duration
	// Ops caps the total number of operations issued across all workers,
	// if > 0. Enforced by a shared atomic budget (see opBudget) so the
	// total lands at exactly Ops regardless of goroutine interleaving.
	Ops int64

	// ReadRatio is the fraction of operations (in [0,1]) that are reads;
	// the rest are writes. A worker draws one float64 per operation and
	// compares it against this threshold, so the realized mix converges to
	// ReadRatio as operation count grows rather than following a fixed
	// round-robin pattern.
	ReadRatio float64

	// KeyspaceSize is the number of distinct keys operations are drawn
	// from (uniformly at random), so reads have a real chance of hitting
	// keys writers actually wrote instead of only ever missing. Defaults
	// to 1000.
	KeyspaceSize int
	// ValueSize is the size in bytes of the value written by a write
	// operation.
	ValueSize int

	// Consistency is the consistency level requested on every read.
	// Defaults to the kvpb zero value (CONSISTENCY_UNSPECIFIED, which
	// internal/server treats as stale) if left unset by the caller.
	Consistency kvpb.Consistency

	// RequestTimeout bounds each individual RPC (not the whole retry
	// loop a write may go through while following leader hints). Defaults
	// to 5s.
	RequestTimeout time.Duration

	// ShardID is stamped on every request's shard_id field. Defaults to
	// "shard-0" — internal/localcluster.Cluster's fixed, single, unnamed
	// shard (see its package doc comment: "a plain (unsharded)
	// Raft-replicated KV store from its clients' point of view"). Set
	// explicitly when pointing a Runner at a real multi-shard deployment.
	ShardID string

	// RandSeed seeds each worker's private *rand.Rand (offset by worker
	// index, so workers don't share a stream). 0 means "seed from the
	// current time" — Run is not reproducible by default, which is fine
	// for a load-measurement tool; pass a fixed seed for reproducible
	// test runs.
	RandSeed int64

	// DialOptions overrides the default insecure/loopback dial options
	// New uses to reach Targets. Mainly for tests.
	DialOptions []grpc.DialOption
}

func (c *Config) setDefaults() {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.KeyspaceSize <= 0 {
		c.KeyspaceSize = 1000
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 5 * time.Second
	}
	if c.ShardID == "" {
		c.ShardID = "shard-0"
	}
}

func (c *Config) validate() error {
	if c.ReadRatio < 0 || c.ReadRatio > 1 {
		return fmt.Errorf("loadgen: ReadRatio must be in [0,1], got %v", c.ReadRatio)
	}
	if c.ValueSize < 0 {
		return fmt.Errorf("loadgen: ValueSize must be >= 0")
	}
	if c.Duration <= 0 && c.Ops <= 0 {
		return fmt.Errorf("loadgen: Config needs Duration > 0 or Ops > 0")
	}
	return nil
}

func (c *Config) seed(worker int) int64 {
	if c.RandSeed != 0 {
		return c.RandSeed + int64(worker)
	}
	return time.Now().UnixNano() + int64(worker)
}

// workerResult accumulates one worker's latency samples and error counts.
// Each worker owns a disjoint slot in Run's results slice, so no locking
// is needed to merge them afterward.
type workerResult struct {
	reads, writes       []time.Duration
	readErrs, writeErrs int64
}

// Runner drives load against Config.Targets (via New) or, in tests,
// against fake doRead/doWrite funcs plumbed in directly.
type Runner struct {
	cfg Config

	order   []string // target IDs, stable iteration/round-robin order
	clients map[string]kvpb.KVServiceClient
	conns   []*grpc.ClientConn

	// leaderID is this Runner's shared, best-effort belief about which
	// target currently accepts writes, updated after every successful
	// write. Starting each write attempt from here (instead of always
	// starting at order[0]) means steady-state writers usually reach the
	// real leader on their first RPC instead of walking the whole
	// membership on every single call.
	leaderID atomic.Value // string

	doRead  func(ctx context.Context, rng *rand.Rand, key []byte) error
	doWrite func(ctx context.Context, key, value []byte) error
}

// New builds a Runner with persistent gRPC connections to every one of
// cfg.Targets, dialed once up front and reused for every request Run
// issues — see this package's doc comment for why that matters for a load
// generator specifically. Call Close when done with the Runner.
func New(cfg Config) (*Runner, error) {
	cfg.setDefaults()
	if len(cfg.Targets) == 0 {
		return nil, fmt.Errorf("loadgen: Config.Targets must be non-empty")
	}

	r := &Runner{
		cfg:     cfg,
		order:   make([]string, 0, len(cfg.Targets)),
		clients: make(map[string]kvpb.KVServiceClient, len(cfg.Targets)),
	}
	r.leaderID.Store("")

	dialOpts := cfg.DialOptions
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	for _, t := range cfg.Targets {
		conn, err := grpc.NewClient(t.Addr, dialOpts...)
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("loadgen: dial %s (%s): %w", t.ID, t.Addr, err)
		}
		r.conns = append(r.conns, conn)
		r.clients[t.ID] = kvpb.NewKVServiceClient(conn)
		r.order = append(r.order, t.ID)
	}
	r.doRead = r.realRead
	r.doWrite = r.realWrite
	return r, nil
}

// Close releases every persistent connection New opened.
func (r *Runner) Close() error {
	var firstErr error
	for _, c := range r.conns {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Run drives Config.Concurrency workers, each in a loop issuing one
// operation (read or write, chosen per Config.ReadRatio) at a time until
// ctx is done, Config.Duration elapses, or Config.Ops operations have been
// issued in total — whichever comes first. It blocks until every worker
// has stopped, then returns a Report built from every latency sample
// collected.
func (r *Runner) Run(ctx context.Context) (*Report, error) {
	r.cfg.setDefaults()
	if err := r.cfg.validate(); err != nil {
		return nil, err
	}

	runCtx := ctx
	if r.cfg.Duration > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, r.cfg.Duration)
		defer cancel()
	}
	budget := newOpBudget(r.cfg.Ops)

	results := make([]workerResult, r.cfg.Concurrency)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < r.cfg.Concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(r.cfg.seed(i)))
			r.workerLoop(runCtx, budget, rng, &results[i])
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	return buildReport(results, elapsed), nil
}

// workerLoop is the scheduling logic under test in loadgen_test.go: it
// depends only on ctx, budget, rng and the injected doRead/doWrite funcs,
// never on gRPC directly, so tests can verify stop-on-cancel and
// op-budget accounting with fake, instant operations.
func (r *Runner) workerLoop(ctx context.Context, budget *opBudget, rng *rand.Rand, res *workerResult) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if !budget.take() {
			return
		}

		isRead := rng.Float64() < r.cfg.ReadRatio
		key := r.randomKey(rng)

		t0 := time.Now()
		var err error
		if isRead {
			err = r.doRead(ctx, rng, key)
		} else {
			err = r.doWrite(ctx, key, r.randomValue(rng))
		}
		elapsed := time.Since(t0)

		switch {
		case isRead && err != nil:
			res.readErrs++
		case isRead:
			res.reads = append(res.reads, elapsed)
		case err != nil:
			res.writeErrs++
		default:
			res.writes = append(res.writes, elapsed)
		}
	}
}

func (r *Runner) randomKey(rng *rand.Rand) []byte {
	return []byte(fmt.Sprintf("loadgen-key-%06d", rng.Intn(r.cfg.KeyspaceSize)))
}

func (r *Runner) randomValue(rng *rand.Rand) []byte {
	buf := make([]byte, r.cfg.ValueSize)
	_, _ = rng.Read(buf) // math/rand.Rand.Read never errors
	return buf
}

// realRead reads a key from a uniformly random target — any replica is
// acceptable for a stale read, which is the entire point of exercising
// Config.Consistency's stale path under load (see this package's doc
// comment and internal/localcluster.Cluster.Get's own doc comment for why
// reads don't follow the leader the way writes do).
func (r *Runner) realRead(ctx context.Context, rng *rand.Rand, key []byte) error {
	id := r.order[rng.Intn(len(r.order))]
	cctx, cancel := context.WithTimeout(ctx, r.cfg.RequestTimeout)
	defer cancel()
	_, err := r.clients[id].Get(cctx, &kvpb.GetRequest{
		ShardId:     r.cfg.ShardID,
		Key:         key,
		Consistency: r.cfg.Consistency,
	})
	return err
}

// realWrite starts from this Runner's shared best-effort leader belief
// and follows leader_hint redirects via put, then records whichever
// target actually accepted the write as the new belief for the next
// writer that starts a request.
func (r *Runner) realWrite(ctx context.Context, key, value []byte) error {
	startID, _ := r.leaderID.Load().(string)
	acceptedID, err := r.put(ctx, key, value, startID)
	if err != nil {
		return err
	}
	r.leaderID.Store(acceptedID)
	return nil
}

// put mirrors internal/localcluster.Cluster.Put's retry protocol exactly
// (see that method's doc comment): start from startHint (or order[0] if
// unknown), retry on transport failure or a non-empty leader_hint by
// following the hint when it names a known target and round-robining
// otherwise, and back off briefly once a full lap of the membership has
// been tried without success (an election still in flight). The only
// difference from Cluster.Put is that this uses r.clients' persistent
// connections instead of dialing fresh per call.
func (r *Runner) put(ctx context.Context, key, value []byte, startID string) (string, error) {
	ids := r.order
	start := 0
	if startID != "" {
		for i, id := range ids {
			if id == startID {
				start = i
				break
			}
		}
	}

	var lastErr error
	tried := make(map[string]bool, len(ids))
	next := ids[start]
	for {
		if ctx.Err() != nil {
			if lastErr != nil {
				return "", lastErr
			}
			return "", ctx.Err()
		}
		if tried[next] {
			select {
			case <-ctx.Done():
				if lastErr != nil {
					return "", lastErr
				}
				return "", ctx.Err()
			case <-time.After(5 * time.Millisecond):
			}
			tried = make(map[string]bool, len(ids))
		}
		tried[next] = true

		cctx, cancel := context.WithTimeout(ctx, r.cfg.RequestTimeout)
		resp, err := r.clients[next].Put(cctx, &kvpb.PutRequest{ShardId: r.cfg.ShardID, Key: key, Value: value})
		cancel()
		if err != nil {
			lastErr = err
			next = roundRobinNext(ids, next)
			continue
		}
		if hint := resp.GetLeaderHint(); hint != "" {
			lastErr = fmt.Errorf("loadgen: %s: not leader, hint=%s", next, hint)
			if _, known := r.clients[hint]; known {
				next = hint
			} else {
				next = roundRobinNext(ids, next)
			}
			continue
		}
		return next, nil
	}
}

func roundRobinNext(ids []string, cur string) string {
	for i, id := range ids {
		if id == cur {
			return ids[(i+1)%len(ids)]
		}
	}
	return ids[0]
}

// opBudget caps the total number of operations Run issues when
// Config.Ops > 0. Concurrent workers share one budget through an atomic
// counter (rather than each getting Ops/Concurrency of their own) so the
// total issued lands at exactly Ops regardless of how unevenly workers
// get scheduled.
type opBudget struct {
	remaining int64 // negative once exhausted; unlimited budgets never decrement
	unlimited bool
}

func newOpBudget(ops int64) *opBudget {
	if ops <= 0 {
		return &opBudget{unlimited: true}
	}
	return &opBudget{remaining: ops}
}

// take reports whether the caller may issue one more operation. Safe for
// concurrent use by every worker.
func (b *opBudget) take() bool {
	if b.unlimited {
		return true
	}
	return atomic.AddInt64(&b.remaining, -1) >= 0
}
