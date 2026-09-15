// Command kvnode is Integration Checkpoint 1's real binary: it wires the
// three independently-built workstreams — Storage Engine
// (internal/storage/raftlog, internal/storage/engine), Raft Core
// (internal/raft), and Client/API Protocol Layer (internal/transport/grpc,
// internal/server) — into one process that runs a single Raft-replicated
// shard and serves both the peer-facing RaftTransportService and the
// client-facing KVService.
//
// Configuration is entirely via environment variables (matching
// deploy/docker/docker-compose.yml):
//
//	NODE_ID    this node's raft.NodeID (must match one entry in PEERS)
//	SHARD_ID   this node's raft.ShardID (single shard for Checkpoint 1)
//	PEERS      "id1=host1:port1,id2=host2:port2,..." — includes this node's
//	           own entry (used to find our own bind address) plus every peer
//	           (dialed for the peer-RPC gRPC service)
//	DATA_DIR   optional, defaults to /data
//
// Peer-RPC (RaftTransportService) listens on :9000; client-facing
// (KVService) listens on :9001, matching Dockerfile.kvnode's EXPOSE and
// docker-compose.yml's port mappings.
//
// # Multi-Raft sharding (Round 2)
//
// This same binary also supports hosting multiple shards in one process
// (internal/shard.Manager) — see multishard.go. It is opt-in, selected by
// setting SHARD_IDS (comma-separated) instead of the single-shard SHARD_ID
// var, specifically so the existing single-shard docker-compose.yml /
// Checkpoint-1 setup keeps working completely unmodified: main() below
// dispatches on which env var is present before touching anything else,
// and the single-shard code path in this file (run, runReadyLoop,
// handleReady, config, parsePeers) is untouched by that addition. See
// multishard.go's doc comment for the additional env vars
// (SHARD_IDS/ADVERTISE_ADDR/METASERVICE_ADDR/AVAILABILITY_ZONE) the
// multi-shard path reads, and
// deploy/docker/docker-compose.multi-shard.yml for a worked example.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raftcore "github.com/SushantPotu/raft-kv-store/internal/raft"
	"github.com/SushantPotu/raft-kv-store/internal/server"
	"github.com/SushantPotu/raft-kv-store/internal/statemachine"
	"github.com/SushantPotu/raft-kv-store/internal/storage/engine"
	"github.com/SushantPotu/raft-kv-store/internal/storage/raftlog"
	grpctransport "github.com/SushantPotu/raft-kv-store/internal/transport/grpc"
	"github.com/SushantPotu/raft-kv-store/pkg/kvpb"
	"github.com/SushantPotu/raft-kv-store/pkg/raft"
	"github.com/SushantPotu/raft-kv-store/pkg/raftpb"
)

const (
	peerListenAddr   = ":9000" // RaftTransportService, matches Dockerfile EXPOSE 9000
	clientListenAddr = ":9001" // KVService, matches Dockerfile EXPOSE 9001 / compose port mapping

	// tickInterval and the *Ticks constants below together determine
	// wall-clock election/heartbeat timing. internal/raft's own defaults
	// (150-300 ticks to elect, 20 ticks per heartbeat) are sized for a
	// logical-tick simulation, not real wall-clock seconds — used verbatim
	// with any real tickInterval they'd make either a snappy demo cluster
	// glacial (tens of seconds to elect at 100ms/tick) or a real deployment
	// trigger-happy (sub-second at 1ms/tick). For Checkpoint 1's Docker
	// Compose smoke test we want elections to visibly happen in a few
	// hundred ms to ~1s, and heartbeats comfortably (5x) below the minimum
	// election timeout so a healthy leader is never second-guessed by a
	// slow follower. tickInterval=50ms with min/max election timeouts of
	// 10/20 ticks (500ms/1000ms) and a heartbeat every 2 ticks (100ms)
	// achieves that.
	tickInterval           = 50 * time.Millisecond
	electionTimeoutMinTick = 10
	electionTimeoutMaxTick = 20
	heartbeatIntervalTick  = 2

	// readyPollInterval is how often the Ready-loop driver polls
	// Node.Ready() for new work. Node.Ready() (see internal/raft/node.go)
	// returns a *new* channel on every call that either already has a
	// value buffered or will never receive one — it is not something a
	// caller can block-range over like a classic Go channel, per the
	// non-blocking `select { case rd := <-n.Ready(): ... default: }` usage
	// internal/raft/simulate.Cluster.drainReady already establishes as the
	// intended pattern. A real driver therefore has to poll it; 5ms keeps
	// end-to-end propose-to-apply latency low without meaningfully
	// affecting CPU usage.
	readyPollInterval = 5 * time.Millisecond
)

func main() {
	// SHARD_IDS opts into the multi-shard path (internal/shard.Manager,
	// multishard.go); its absence keeps the original single-shard path
	// below completely unmodified, so existing single-shard deployments
	// (docker-compose.yml, Checkpoint 1) need no changes at all.
	if os.Getenv("SHARD_IDS") != "" {
		if err := runMultiShard(); err != nil {
			log.Fatalf("kvnode: %v", err)
		}
		return
	}
	if err := run(); err != nil {
		log.Fatalf("kvnode: %v", err)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log.Printf("kvnode: starting id=%s shard=%s peers=%v dataDir=%s", cfg.id, cfg.shard, cfg.peerIDs(), cfg.dataDir)

	// --- Storage Engine workstream: durable Raft log + KV engine. ---
	//
	// dataDir is used as-is (no volume declared in docker-compose.yml
	// today) so a container recreation starts with an empty log/engine.
	// That's an acceptable tradeoff for Checkpoint 1 — the point of this
	// test is proving leader election/replication/failover work, not
	// surviving container recreation. Revisit with a named volume once
	// long-term persistence across restarts needs testing.
	logStorage, err := raftlog.Open(cfg.dataDir+"/raftlog", raftlog.Options{})
	if err != nil {
		return fmt.Errorf("open raft log storage: %w", err)
	}
	defer logStorage.Close()

	kvEngine, err := engine.Open(cfg.dataDir+"/engine", engine.Options{})
	if err != nil {
		return fmt.Errorf("open kv engine: %w", err)
	}
	defer kvEngine.Close()

	adapter := statemachine.NewAdapter(kvEngine)

	// --- Client/API Protocol Layer workstream: gRPC transport. ---
	transportClient := grpctransport.NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer transportClient.Close()
	for _, p := range cfg.peers {
		if p.id == cfg.id {
			continue
		}
		transportClient.AddPeer(p.id, p.addr)
	}

	// --- Raft Core workstream: the actual consensus Node. ---
	node := raftcore.NewNode(
		cfg.id,
		cfg.shard,
		cfg.peerIDs(),
		logStorage,
		transportClient,
		adapter,
		raftcore.WithElectionTimeoutTicks(electionTimeoutMinTick, electionTimeoutMaxTick),
		raftcore.WithHeartbeatIntervalTicks(heartbeatIntervalTick),
	)

	registry := grpctransport.NewNodeRegistry()
	registry.Register(cfg.shard, node)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Peer-facing RaftTransportService.
	peerSrv := grpc.NewServer()
	raftpb.RegisterRaftTransportServiceServer(peerSrv, grpctransport.NewServer(registry))
	peerLis, err := net.Listen("tcp", peerListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", peerListenAddr, err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("kvnode: RaftTransportService listening on %s", peerListenAddr)
		if err := peerSrv.Serve(peerLis); err != nil {
			log.Printf("kvnode: peer gRPC server stopped: %v", err)
		}
	}()

	// Client-facing KVService.
	kvSrv := grpc.NewServer()
	kvpb.RegisterKVServiceServer(kvSrv, server.NewKVServer(node, adapter))
	clientLis, err := net.Listen("tcp", clientListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", clientListenAddr, err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("kvnode: KVService listening on %s", clientListenAddr)
		if err := kvSrv.Serve(clientLis); err != nil {
			log.Printf("kvnode: client gRPC server stopped: %v", err)
		}
	}()

	// Ticker goroutine: drives logical time forward for election/heartbeat
	// timeouts (see the tickInterval doc comment above for the timing
	// rationale).
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(tickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				node.Tick()
			}
		}
	}()

	// Ready-loop driver: the single-shard stand-in for the future
	// internal/shard.Manager. See readyPollInterval's doc comment for why
	// this polls rather than blocking on node.Ready() directly.
	wg.Add(1)
	go func() {
		defer wg.Done()
		runReadyLoop(ctx, node, logStorage, adapter, transportClient)
	}()

	// Block until SIGINT/SIGTERM, then shut down reasonably gracefully.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	log.Printf("kvnode: shutting down")
	cancel()
	kvSrv.GracefulStop()
	peerSrv.GracefulStop()
	wg.Wait()
	return nil
}

// runReadyLoop is the Ready-loop driver: for as long as ctx is alive, it
// polls node.Ready(), and for each non-empty Ready persists HardState/
// Entries (in that order, per the Ready doc comment), fires outbound
// Messages at their destinations asynchronously (so one slow/unreachable
// peer never blocks the loop), applies CommittedEntries to the state
// machine, and installs any Snapshot — then calls node.Advance().
func runReadyLoop(ctx context.Context, node raft.Node, storage raft.Storage, adapter *statemachine.Adapter, transport raft.Transport) {
	poll := time.NewTicker(readyPollInterval)
	defer poll.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
		}

		select {
		case rd := <-node.Ready():
			handleReady(ctx, rd, storage, adapter, transport)
			node.Advance()
		default:
		}
	}
}

func handleReady(ctx context.Context, rd raft.Ready, storage raft.Storage, adapter *statemachine.Adapter, transport raft.Transport) {
	if rd.HardState != nil {
		if err := storage.SetHardState(*rd.HardState); err != nil {
			log.Printf("kvnode: SetHardState: %v", err)
		}
	}
	if len(rd.Entries) > 0 {
		if err := storage.Append(rd.Entries); err != nil {
			log.Printf("kvnode: Append: %v", err)
		}
	}

	// Fire each outbound message in its own goroutine: a dial failure or a
	// slow/unreachable peer must never block the Ready loop from making
	// progress on the next round.
	for _, msg := range rd.Messages {
		msg := msg
		go func() {
			sendCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := transport.Send(sendCtx, msg); err != nil {
				log.Printf("kvnode: Send to %s failed: %v", msg.To, err)
			}
		}()
	}

	for _, entry := range rd.CommittedEntries {
		if entry.Type == raft.EntryConfChange {
			// Membership changes aren't implemented yet (Checkpoint 1 is a
			// static 3-node cluster) — skip rather than fail.
			continue
		}
		if len(entry.Data) == 0 {
			// A no-op entry (e.g. a new leader's empty first-term entry, if
			// Raft Core ever appends one) has nothing to apply.
			continue
		}
		if _, err := adapter.Apply(entry); err != nil {
			log.Printf("kvnode: Apply entry %d: %v", entry.Index, err)
		}
	}

	if rd.Snapshot != nil {
		if err := storage.ApplySnapshot(*rd.Snapshot); err != nil {
			log.Printf("kvnode: ApplySnapshot: %v", err)
		} else if err := adapter.RestoreSnapshot(rd.Snapshot.Data); err != nil {
			log.Printf("kvnode: RestoreSnapshot: %v", err)
		}
	}

	_ = ctx // reserved for future use (e.g. plumbing cancellation into Apply)
}

// peer is one parsed entry from PEERS.
type peer struct {
	id   raft.NodeID
	addr string
}

type config struct {
	id      raft.NodeID
	shard   raft.ShardID
	dataDir string
	peers   []peer // every entry in PEERS, including this node's own
}

// peerIDs returns every peer's NodeID except this node's own — the shape
// internal/raft.NewNode expects for its peers argument.
func (c config) peerIDs() []raft.NodeID {
	var ids []raft.NodeID
	for _, p := range c.peers {
		if p.id != c.id {
			ids = append(ids, p.id)
		}
	}
	return ids
}

func loadConfig() (config, error) {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		return config{}, fmt.Errorf("NODE_ID is required")
	}
	shardID := os.Getenv("SHARD_ID")
	if shardID == "" {
		return config{}, fmt.Errorf("SHARD_ID is required")
	}
	peersEnv := os.Getenv("PEERS")
	if peersEnv == "" {
		return config{}, fmt.Errorf("PEERS is required")
	}
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "/data"
	}

	peers, err := parsePeers(peersEnv)
	if err != nil {
		return config{}, err
	}

	found := false
	for _, p := range peers {
		if p.id == raft.NodeID(nodeID) {
			found = true
			break
		}
	}
	if !found {
		return config{}, fmt.Errorf("NODE_ID %q not present in PEERS %q", nodeID, peersEnv)
	}

	return config{
		id:      raft.NodeID(nodeID),
		shard:   raft.ShardID(shardID),
		dataDir: dataDir,
		peers:   peers,
	}, nil
}

// parsePeers parses PEERS' "id1=host1:port1,id2=host2:port2,..." format.
func parsePeers(s string) ([]peer, error) {
	var out []peer
	for _, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("malformed PEERS entry %q (want id=host:port)", entry)
		}
		out = append(out, peer{id: raft.NodeID(parts[0]), addr: parts[1]})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("PEERS contained no entries")
	}
	return out, nil
}
