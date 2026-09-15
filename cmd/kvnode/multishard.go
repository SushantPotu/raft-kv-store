// Multi-Raft sharding support for cmd/kvnode (Round 2 — see main.go's
// package doc comment for why this is opt-in via SHARD_IDS rather than a
// change to the existing single-shard path).
//
// Additional environment variables (only read when SHARD_IDS is set):
//
//	SHARD_IDS           comma-separated list of raft.ShardIDs this process
//	                    hosts (e.g. "shard-1,shard-2"). Every shard is
//	                    replicated across the same PEERS node set — a
//	                    typical Multi-Raft layout where the same physical
//	                    nodes host every shard's replicas, just as
//	                    deploy/docker/docker-compose.multi-shard.yml's
//	                    3-nodes-x-2-shards topology does.
//	PEERS               same "id1=host1:port1,..." format as the
//	                    single-shard path — one shared peer set for every
//	                    hosted shard's raft.Node.
//	DATA_DIR            optional, defaults to /data. Each shard gets its
//	                    own subdirectory (DATA_DIR/<shard_id>/raftlog,
//	                    DATA_DIR/<shard_id>/engine) — one Storage/engine
//	                    pair per shard, never shared.
//	METASERVICE_ADDR    optional. If set, this node registers itself as a
//	                    replica for every hosted shard at startup and
//	                    reports every leadership transition (see
//	                    internal/shard.Manager). If unset, the node hosts
//	                    and drives its shards exactly the same but never
//	                    talks to metaservice at all — useful for a
//	                    metaservice-less local rehearsal of just the
//	                    Multi-Raft hosting piece.
//	ADVERTISE_ADDR      optional, defaults to "<NODE_ID>:9001" (matching
//	                    clientListenAddr and docker-compose's convention of
//	                    NODE_ID doubling as the container's DNS name). The
//	                    address this node reports to metaservice as where
//	                    its KVService can be reached (kvrouter dials this).
//	AVAILABILITY_ZONE   optional, defaults to "".
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
	"github.com/SushantPotu/raft-kv-store/internal/shard"
	"github.com/SushantPotu/raft-kv-store/internal/statemachine"
	"github.com/SushantPotu/raft-kv-store/internal/storage/engine"
	"github.com/SushantPotu/raft-kv-store/internal/storage/raftlog"
	grpctransport "github.com/SushantPotu/raft-kv-store/internal/transport/grpc"
	"github.com/SushantPotu/raft-kv-store/pkg/kvpb"
	"github.com/SushantPotu/raft-kv-store/pkg/metapb"
	"github.com/SushantPotu/raft-kv-store/pkg/raft"
	"github.com/SushantPotu/raft-kv-store/pkg/raftpb"
)

type multiShardConfig struct {
	id       raft.NodeID
	shardIDs []raft.ShardID
	dataDir  string
	peers    []peer

	metaAddr      string
	advertiseAddr string
	az            string
}

func (c multiShardConfig) peerIDs() []raft.NodeID {
	var ids []raft.NodeID
	for _, p := range c.peers {
		if p.id != c.id {
			ids = append(ids, p.id)
		}
	}
	return ids
}

func loadMultiShardConfig() (multiShardConfig, error) {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		return multiShardConfig{}, fmt.Errorf("NODE_ID is required")
	}
	shardIDsEnv := os.Getenv("SHARD_IDS")
	if shardIDsEnv == "" {
		return multiShardConfig{}, fmt.Errorf("SHARD_IDS is required")
	}
	peersEnv := os.Getenv("PEERS")
	if peersEnv == "" {
		return multiShardConfig{}, fmt.Errorf("PEERS is required")
	}
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "/data"
	}

	peers, err := parsePeers(peersEnv)
	if err != nil {
		return multiShardConfig{}, err
	}
	found := false
	for _, p := range peers {
		if p.id == raft.NodeID(nodeID) {
			found = true
			break
		}
	}
	if !found {
		return multiShardConfig{}, fmt.Errorf("NODE_ID %q not present in PEERS %q", nodeID, peersEnv)
	}

	var shardIDs []raft.ShardID
	for _, s := range strings.Split(shardIDsEnv, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		shardIDs = append(shardIDs, raft.ShardID(s))
	}
	if len(shardIDs) == 0 {
		return multiShardConfig{}, fmt.Errorf("SHARD_IDS contained no entries")
	}

	advertiseAddr := os.Getenv("ADVERTISE_ADDR")
	if advertiseAddr == "" {
		advertiseAddr = nodeID + clientListenAddr
	}

	return multiShardConfig{
		id:            raft.NodeID(nodeID),
		shardIDs:      shardIDs,
		dataDir:       dataDir,
		peers:         peers,
		metaAddr:      os.Getenv("METASERVICE_ADDR"),
		advertiseAddr: advertiseAddr,
		az:            os.Getenv("AVAILABILITY_ZONE"),
	}, nil
}

// runMultiShard is the N-shard analog of run(): it builds one
// raftlog.Storage + engine.Engine + statemachine.Adapter + raft.Node per
// hosted shard (in its own data subdirectory), hands them all to a shared
// internal/shard.Manager, and serves both gRPC services exactly like the
// single-shard path — just backed by Manager's shard_id-keyed dispatch
// (grpctransport.Server/NodeRegistry for peer RPCs, Manager.KVServiceServer
// for client RPCs) instead of a single Node.
func runMultiShard() error {
	cfg, err := loadMultiShardConfig()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log.Printf("kvnode: starting (multi-shard) id=%s shards=%v peers=%v dataDir=%s", cfg.id, cfg.shardIDs, cfg.peerIDs(), cfg.dataDir)

	transportClient := grpctransport.NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer transportClient.Close()
	for _, p := range cfg.peers {
		if p.id == cfg.id {
			continue
		}
		transportClient.AddPeer(p.id, p.addr)
	}

	var metaClient metapb.MetadataServiceClient
	if cfg.metaAddr != "" {
		conn, err := grpc.NewClient(cfg.metaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial metaservice %s: %w", cfg.metaAddr, err)
		}
		defer conn.Close()
		metaClient = metapb.NewMetadataServiceClient(conn)
	}

	registry := grpctransport.NewNodeRegistry()
	var mgrOpts []shard.Option
	if metaClient != nil {
		mgrOpts = append(mgrOpts, shard.WithMetadataClient(metaClient))
	}
	mgr := shard.NewManager(cfg.id, transportClient, registry, mgrOpts...)

	// One raftlog.Storage + engine.Engine pair per shard, in its own data
	// subdirectory — see this file's doc comment. Track them for a clean
	// shutdown.
	var closers []func() error
	closeAll := func() {
		for i := len(closers) - 1; i >= 0; i-- {
			if err := closers[i](); err != nil {
				log.Printf("kvnode: close: %v", err)
			}
		}
	}
	defer closeAll()

	for _, shardID := range cfg.shardIDs {
		shardDir := cfg.dataDir + "/" + string(shardID)

		logStorage, err := raftlog.Open(shardDir+"/raftlog", raftlog.Options{})
		if err != nil {
			return fmt.Errorf("open raft log storage for shard %q: %w", shardID, err)
		}
		closers = append(closers, logStorage.Close)

		kvEngine, err := engine.Open(shardDir+"/engine", engine.Options{})
		if err != nil {
			return fmt.Errorf("open kv engine for shard %q: %w", shardID, err)
		}
		closers = append(closers, kvEngine.Close)

		adapter := statemachine.NewAdapter(kvEngine)

		node := raftcore.NewNode(
			cfg.id,
			shardID,
			cfg.peerIDs(),
			logStorage,
			transportClient,
			adapter,
			raftcore.WithElectionTimeoutTicks(electionTimeoutMinTick, electionTimeoutMaxTick),
			raftcore.WithHeartbeatIntervalTicks(heartbeatIntervalTick),
		)

		mgr.AddShard(shard.Shard{
			ID:      shardID,
			Node:    node,
			Storage: logStorage,
			SM:      adapter,
			KV:      adapter,
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Peer-facing RaftTransportService: one shared server dispatching by
	// shard_id via registry, exactly like the single-shard path.
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

	// Client-facing KVService: Manager.KVServiceServer() dispatches each
	// request to the shard named by its shard_id field.
	kvSrv := grpc.NewServer()
	kvpb.RegisterKVServiceServer(kvSrv, mgr.KVServiceServer())
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

	// Shared ticker + Ready-loop driver across every hosted shard.
	mgr.Start(ctx)

	if metaClient != nil {
		regCtx, regCancel := context.WithTimeout(ctx, 10*time.Second)
		err := mgr.RegisterReplicas(regCtx, cfg.advertiseAddr, cfg.az)
		regCancel()
		if err != nil {
			// Don't fail startup over a transient metaservice hiccup
			// (compose start-order isn't guaranteed) — the leader-change
			// reporting loop keeps trying to reach metaservice as
			// elections happen, so the shard map self-heals once
			// metaservice is reachable, but replica addresses registered
			// here specifically won't appear until this succeeds. Log
			// loudly rather than silently limping along forever.
			log.Printf("kvnode: RegisterReplicas failed (will not retry): %v", err)
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	log.Printf("kvnode: shutting down")
	cancel()
	kvSrv.GracefulStop()
	peerSrv.GracefulStop()
	wg.Wait()
	mgr.Wait()
	return nil
}
