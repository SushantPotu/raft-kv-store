// Package localcluster spins up a real, in-process, single-shard Raft/KV
// cluster on loopback gRPC — the same Storage/StateMachine/Node/Transport/
// KVServer wiring cmd/kvnode uses in production, minus separate OS
// processes or containers — so tools like cmd/chaosmonkey and cmd/loadgen
// can drive genuine leader-kill and load scenarios against a real cluster
// without requiring Docker or a multi-machine deployment.
//
// Each node is hosted via internal/shard.Manager (with exactly one shard),
// reusing its already-tested ticker/Ready-loop driver and gRPC dispatch
// rather than a third reimplementation of that logic (cmd/kvnode has the
// original single-shard version; internal/shard has the N-shard version
// this reuses with N=1). Every RPC — peer-to-peer Raft traffic and
// client-facing KV requests alike — travels real gRPC over 127.0.0.1, not
// an in-memory fake transport, so failure injection here (Kill) is a real
// process-equivalent failure: it stops real goroutines and closes real
// listeners, and a client's request to that address genuinely fails to
// connect, exactly as it would against a killed container or EC2 instance.
package localcluster

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raftcore "github.com/SushantPotu/raft-kv-store/internal/raft"
	"github.com/SushantPotu/raft-kv-store/internal/shard"
	"github.com/SushantPotu/raft-kv-store/internal/statemachine"
	"github.com/SushantPotu/raft-kv-store/internal/storage/engine"
	"github.com/SushantPotu/raft-kv-store/internal/storage/raftlog"
	grpctransport "github.com/SushantPotu/raft-kv-store/internal/transport/grpc"
	"github.com/SushantPotu/raft-kv-store/pkg/kvpb"
	"github.com/SushantPotu/raft-kv-store/pkg/raft"
	"github.com/SushantPotu/raft-kv-store/pkg/raftpb"
)

// shardID is the single shard every node in a Cluster hosts. Left as the
// zero value so callers building kvpb requests never need to set ShardId
// themselves — a Cluster is a plain (unsharded) Raft-replicated KV store
// from its clients' point of view, matching cmd/kvnode's single-shard
// deployment shape (Workstream H's sharding concerns are orthogonal to
// what chaos/load testing needs to exercise).
const shardID raft.ShardID = "shard-0"

// Config controls how a Cluster is built. All fields have workable
// defaults (see setDefaults) tuned the same way cmd/kvnode's are: fast
// enough for elections/heartbeats to complete in well under a second, so
// chaos trials and load runs stay quick, without being so fast that CI
// jitter causes spurious elections.
type Config struct {
	// NumNodes is the number of replicas in the cluster. Defaults to 3.
	NumNodes int
	// DataDir is the base directory each node's raft log + KV engine live
	// under (one subdirectory per node). If empty, a fresh temp directory
	// is created and removed by Stop.
	DataDir string

	ElectionTimeoutMinTicks int
	ElectionTimeoutMaxTicks int
	HeartbeatIntervalTicks  int
	TickInterval            time.Duration
	ReadyPollInterval       time.Duration
}

func (c *Config) setDefaults() {
	if c.NumNodes <= 0 {
		c.NumNodes = 3
	}
	if c.ElectionTimeoutMinTicks <= 0 {
		c.ElectionTimeoutMinTicks = 10
	}
	if c.ElectionTimeoutMaxTicks <= 0 {
		c.ElectionTimeoutMaxTicks = 20
	}
	if c.HeartbeatIntervalTicks <= 0 {
		c.HeartbeatIntervalTicks = 2
	}
	if c.TickInterval <= 0 {
		c.TickInterval = 10 * time.Millisecond
	}
	if c.ReadyPollInterval <= 0 {
		c.ReadyPollInterval = 3 * time.Millisecond
	}
}

// node is one cluster member's live handle. Everything here is rebuilt
// fresh by Restart except id, peerPort, clientPort and dataDir, which stay
// fixed for the node's lifetime so peers keep dialing the same address
// across a kill/restart cycle.
type node struct {
	id                   raft.NodeID
	peerPort, clientPort int
	dataDir              string

	mu       sync.Mutex
	alive    bool
	mgr      *shard.Manager
	raftNode raft.Node
	xport    *grpctransport.Client
	peerSrv  *grpc.Server
	kvSrv    *grpc.Server
	cancel   context.CancelFunc
	closers  []func() error // release order: reverse of append order
}

func (n *node) clientAddr() string { return fmt.Sprintf("127.0.0.1:%d", n.clientPort) }
func (n *node) peerAddr() string   { return fmt.Sprintf("127.0.0.1:%d", n.peerPort) }

// Cluster is a running local multi-node Raft/KV cluster. Use Start to
// create one and Stop to tear it down.
type Cluster struct {
	cfg       Config
	baseDir   string
	ownedDir  bool // true if Start created baseDir itself (Stop then removes it)
	peerAddrs map[raft.NodeID]string

	mu    sync.Mutex
	nodes map[raft.NodeID]*node
	order []raft.NodeID // stable iteration/round-robin order
}

// freePort returns a currently-unused TCP port on 127.0.0.1 by briefly
// binding to port 0 and immediately releasing it. There is an inherent,
// unavoidable TOCTOU race between this and the later bind that actually
// uses the port — acceptable for a local dev/test cluster orchestrator,
// not something production code would do.
func freePort() (int, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer lis.Close()
	return lis.Addr().(*net.TCPAddr).Port, nil
}

// Start builds and starts a Cluster per cfg, waiting for nothing beyond
// each node's servers accepting connections — callers that need a leader
// elected before proceeding should use Cluster.AwaitHealthy.
func Start(cfg Config) (*Cluster, error) {
	cfg.setDefaults()

	baseDir := cfg.DataDir
	ownedDir := false
	if baseDir == "" {
		d, err := os.MkdirTemp("", "raft-localcluster-")
		if err != nil {
			return nil, fmt.Errorf("localcluster: create temp dir: %w", err)
		}
		baseDir = d
		ownedDir = true
	}

	ids := make([]raft.NodeID, cfg.NumNodes)
	for i := range ids {
		ids[i] = raft.NodeID(fmt.Sprintf("node-%d", i+1))
	}

	peerAddrs := make(map[raft.NodeID]string, len(ids))
	peerPorts := make(map[raft.NodeID]int, len(ids))
	clientPorts := make(map[raft.NodeID]int, len(ids))
	for _, id := range ids {
		pp, err := freePort()
		if err != nil {
			return nil, fmt.Errorf("localcluster: allocate peer port for %s: %w", id, err)
		}
		cp, err := freePort()
		if err != nil {
			return nil, fmt.Errorf("localcluster: allocate client port for %s: %w", id, err)
		}
		peerPorts[id] = pp
		clientPorts[id] = cp
		peerAddrs[id] = fmt.Sprintf("127.0.0.1:%d", pp)
	}

	c := &Cluster{
		cfg:       cfg,
		baseDir:   baseDir,
		ownedDir:  ownedDir,
		peerAddrs: peerAddrs,
		nodes:     make(map[raft.NodeID]*node, len(ids)),
		order:     ids,
	}

	for _, id := range ids {
		n, err := c.buildNode(id, peerPorts[id], clientPorts[id])
		if err != nil {
			c.Stop()
			return nil, fmt.Errorf("localcluster: start %s: %w", id, err)
		}
		c.nodes[id] = n
	}
	return c, nil
}

// buildNode constructs and starts node id fresh: opens its (possibly
// pre-existing, on a Restart) on-disk storage, wires up a raft.Node hosted
// by a single-shard shard.Manager, and starts its peer/client gRPC
// servers on the fixed ports assigned to this node for the Cluster's
// lifetime.
func (c *Cluster) buildNode(id raft.NodeID, peerPort, clientPort int) (*node, error) {
	nodeDir := filepath.Join(c.baseDir, string(id))

	xport := grpctransport.NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	var peerIDs []raft.NodeID
	for peerID, addr := range c.peerAddrs {
		if peerID == id {
			continue
		}
		xport.AddPeer(peerID, addr)
		peerIDs = append(peerIDs, peerID)
	}

	registry := grpctransport.NewNodeRegistry()
	mgr := shard.NewManager(id, xport, registry,
		shard.WithTickInterval(c.cfg.TickInterval),
		shard.WithReadyPollInterval(c.cfg.ReadyPollInterval),
	)

	logStorage, err := raftlog.Open(filepath.Join(nodeDir, "raftlog"), raftlog.Options{})
	if err != nil {
		xport.Close()
		return nil, fmt.Errorf("open raft log: %w", err)
	}
	kvEngine, err := engine.Open(filepath.Join(nodeDir, "engine"), engine.Options{})
	if err != nil {
		logStorage.Close()
		xport.Close()
		return nil, fmt.Errorf("open kv engine: %w", err)
	}
	adapter := statemachine.NewAdapter(kvEngine)

	raftNode := raftcore.NewNode(id, shardID, peerIDs, logStorage, xport, adapter,
		raftcore.WithElectionTimeoutTicks(c.cfg.ElectionTimeoutMinTicks, c.cfg.ElectionTimeoutMaxTicks),
		raftcore.WithHeartbeatIntervalTicks(c.cfg.HeartbeatIntervalTicks),
	)
	mgr.AddShard(shard.Shard{ID: shardID, Node: raftNode, Storage: logStorage, SM: adapter, KV: adapter})

	peerLis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", peerPort))
	if err != nil {
		kvEngine.Close()
		logStorage.Close()
		xport.Close()
		return nil, fmt.Errorf("listen peer addr: %w", err)
	}
	peerSrv := grpc.NewServer(
		grpc.MaxRecvMsgSize(grpctransport.MaxMessageSize),
		grpc.MaxSendMsgSize(grpctransport.MaxMessageSize),
	)
	raftpb.RegisterRaftTransportServiceServer(peerSrv, grpctransport.NewServer(registry))
	go func() { _ = peerSrv.Serve(peerLis) }()

	clientLis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", clientPort))
	if err != nil {
		peerSrv.Stop()
		kvEngine.Close()
		logStorage.Close()
		xport.Close()
		return nil, fmt.Errorf("listen client addr: %w", err)
	}
	kvSrv := grpc.NewServer()
	kvpb.RegisterKVServiceServer(kvSrv, mgr.KVServiceServer())
	go func() { _ = kvSrv.Serve(clientLis) }()

	ctx, cancel := context.WithCancel(context.Background())
	mgr.Start(ctx)

	return &node{
		id:         id,
		peerPort:   peerPort,
		clientPort: clientPort,
		dataDir:    nodeDir,
		alive:      true,
		mgr:        mgr,
		raftNode:   raftNode,
		xport:      xport,
		peerSrv:    peerSrv,
		kvSrv:      kvSrv,
		cancel:     cancel,
		closers:    []func() error{kvEngine.Close, logStorage.Close},
	}, nil
}

// stop tears n down as if its process had just been killed: cancels the
// Manager's driver goroutines, stops both gRPC servers immediately (no
// drain — a crash doesn't wait for in-flight RPCs), closes the outbound
// transport, and closes the underlying storage files. Closing storage
// here isn't a simplification of "real" crash behavior, it's exactly what
// a real crash does too: the OS reclaims every file descriptor on process
// exit whether the exit was graceful or not. Doing it explicitly is what
// makes Restart able to reopen the same on-disk files afterward (notably
// on Windows, where another handle can't reopen a file whose only holder
// hasn't released it).
func (n *node) stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.alive {
		return
	}
	n.alive = false
	n.cancel()
	n.mgr.Wait()
	n.peerSrv.Stop()
	n.kvSrv.Stop()
	n.xport.Close()
	for i := len(n.closers) - 1; i >= 0; i-- {
		_ = n.closers[i]() // best-effort; a crash doesn't check for close errors either
	}
}

// NodeIDs returns every node in the cluster, in a stable order.
func (c *Cluster) NodeIDs() []raft.NodeID {
	out := make([]raft.NodeID, len(c.order))
	copy(out, c.order)
	return out
}

// ClientAddr returns id's client-facing KVService address.
func (c *Cluster) ClientAddr(id raft.NodeID) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[id]
	if !ok {
		return ""
	}
	return n.clientAddr()
}

// IsAlive reports whether id is currently running (false after Kill,
// until a subsequent Restart).
func (c *Cluster) IsAlive(id raft.NodeID) bool {
	c.mu.Lock()
	n, ok := c.nodes[id]
	c.mu.Unlock()
	if !ok {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.alive
}

// Kill stops id as if its process had crashed: see (*node).stop's doc
// comment for exactly what that does and doesn't simulate. The node's
// on-disk state is preserved for a later Restart. Kill on an
// already-dead node is a no-op.
func (c *Cluster) Kill(id raft.NodeID) error {
	c.mu.Lock()
	n, ok := c.nodes[id]
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("localcluster: unknown node %q", id)
	}
	n.stop()
	return nil
}

// Restart brings a previously-Kill'd node back: it reopens the same
// on-disk raft log and KV engine (so it recovers exactly like a real
// restarted process would, replaying its own durable log) and rebinds
// the same peer/client ports so the rest of the cluster's static peer
// list still finds it.
func (c *Cluster) Restart(id raft.NodeID) error {
	c.mu.Lock()
	old, ok := c.nodes[id]
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("localcluster: unknown node %q", id)
	}
	old.stop() // idempotent if already dead

	n, err := c.buildNode(id, old.peerPort, old.clientPort)
	if err != nil {
		return fmt.Errorf("localcluster: restart %s: %w", id, err)
	}
	c.mu.Lock()
	c.nodes[id] = n
	c.mu.Unlock()
	return nil
}

// Stop tears down every node and, if Start created the data directory
// itself, removes it.
func (c *Cluster) Stop() {
	c.mu.Lock()
	nodes := make([]*node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.mu.Unlock()

	for _, n := range nodes {
		n.stop()
	}
	if c.ownedDir {
		_ = os.RemoveAll(c.baseDir)
	}
}

// dialClient dials id's client-facing address fresh (no connection
// caching/pooling — this package trades a little per-call dial overhead
// for the simplicity of never having to invalidate a cached connection to
// a node that was just killed and restarted on the same address).
func (c *Cluster) dialClient(id raft.NodeID) (kvpb.KVServiceClient, func(), error) {
	addr := c.ClientAddr(id)
	if addr == "" {
		return nil, nil, fmt.Errorf("localcluster: unknown node %q", id)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("localcluster: dial %s (%s): %w", id, addr, err)
	}
	return kvpb.NewKVServiceClient(conn), func() { conn.Close() }, nil
}

// Put attempts to write key/value to the cluster, starting from startHint
// (or the first node in NodeIDs order if startHint is "" or unknown) and
// following each response's leader_hint — exactly the retry protocol
// cmd/kvctl and internal/routing.Router already use against this same
// KVService contract. Returns the NodeID that actually accepted the
// write, so a caller (e.g. chaos testing) can learn who the current
// leader is without a separate status RPC. Gives up once ctx is done or
// every known node has been tried without success.
func (c *Cluster) Put(ctx context.Context, key, value []byte, startHint raft.NodeID) (raft.NodeID, error) {
	ids := c.order
	start := 0
	for i, id := range ids {
		if id == startHint {
			start = i
			break
		}
	}

	var lastErr error
	tried := make(map[raft.NodeID]bool, len(ids))
	next := ids[start]
	for {
		if ctx.Err() != nil {
			if lastErr != nil {
				return "", lastErr
			}
			return "", ctx.Err()
		}
		if tried[next] {
			// Went all the way around without success (or a hint pointed
			// back at a node we already ruled out) — nothing left to try
			// this pass; wait briefly for the cluster to make progress
			// (e.g. an election still in flight) and start over.
			select {
			case <-ctx.Done():
				if lastErr != nil {
					return "", lastErr
				}
				return "", ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
			tried = make(map[raft.NodeID]bool, len(ids))
		}
		tried[next] = true

		cli, closeFn, err := c.dialClient(next)
		if err != nil {
			lastErr = err
			next = roundRobinNext(ids, next)
			continue
		}
		resp, err := cli.Put(ctx, &kvpb.PutRequest{ShardId: string(shardID), Key: key, Value: value})
		closeFn()
		if err != nil {
			lastErr = err
			next = roundRobinNext(ids, next)
			continue
		}
		if hint := resp.GetLeaderHint(); hint != "" {
			lastErr = fmt.Errorf("localcluster: %s: not leader, hint=%s", next, hint)
			hinted := raft.NodeID(hint)
			if _, known := c.nodes[hinted]; known {
				next = hinted
			} else {
				next = roundRobinNext(ids, next)
			}
			continue
		}
		return next, nil
	}
}

// Get reads key from id directly (no leader-following: a Get is meant to
// be servable by any replica at kvpb.Consistency_CONSISTENCY_STALE, and
// this package leaves linearizable-read testing to internal/raft's own
// ReadIndex tests rather than duplicating it here).
func (c *Cluster) Get(ctx context.Context, id raft.NodeID, key []byte, consistency kvpb.Consistency) (value []byte, found bool, err error) {
	cli, closeFn, err := c.dialClient(id)
	if err != nil {
		return nil, false, err
	}
	defer closeFn()
	resp, err := cli.Get(ctx, &kvpb.GetRequest{ShardId: string(shardID), Key: key, Consistency: consistency})
	if err != nil {
		return nil, false, err
	}
	return resp.GetValue(), resp.GetFound(), nil
}

func roundRobinNext(ids []raft.NodeID, cur raft.NodeID) raft.NodeID {
	for i, id := range ids {
		if id == cur {
			return ids[(i+1)%len(ids)]
		}
	}
	return ids[0]
}

// Status returns id's current raft.Status (role, term, leader as this
// node currently sees it). Useful for diagnostics and for chaos/load
// tooling that wants to identify the current leader without a Put probe.
func (c *Cluster) Status(id raft.NodeID) (raft.Status, error) {
	c.mu.Lock()
	n, ok := c.nodes[id]
	c.mu.Unlock()
	if !ok {
		return raft.Status{}, fmt.Errorf("localcluster: unknown node %q", id)
	}
	return n.raftNode.Status(), nil
}

// AwaitHealthy blocks until a Put succeeds (i.e. some node accepts
// leadership of a write) or ctx is done, returning the NodeID that
// accepted it. Useful right after Start (or Restart of the whole
// cluster) before running a chaos/load scenario against it.
func (c *Cluster) AwaitHealthy(ctx context.Context) (raft.NodeID, error) {
	return c.Put(ctx, []byte("__localcluster_health__"), []byte("ok"), "")
}
