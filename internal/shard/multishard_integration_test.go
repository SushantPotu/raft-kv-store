package shard_test

// This is the closest thing this workstream has to Checkpoint 2's "local
// rehearsal" without Docker: it stands up two real 3-replica Raft groups
// (shard-1, shard-2), each hosted across three real internal/shard.Manager
// processes-in-a-goroutine (so a single physical "kvnode" hosts a replica
// of every shard, exactly like deploy/docker/docker-compose.multi-shard.yml's
// topology), a real internal/metaservice.Service, and a real
// internal/routing.Router — all talking real gRPC over 127.0.0.1, not
// bufconn or in-process fakes. It proves:
//
//  1. Both shards elect a leader and that gets reported to metaservice.
//  2. The router resolves a key in either shard's range to the right
//     shard and forwards to its current leader, for both shards.
//  3. Killing a shard's leader outright (not a graceful stop — Stop(),
//     simulating a crashed process) causes the remaining two replicas to
//     elect a new leader, metaservice's record to update, and the
//     router — whose cache still points at the now-dead leader — to
//     self-heal (via the transport-error-triggered forced refresh path in
//     internal/routing.Router) and keep serving that shard correctly,
//     while the other shard (unaffected in leadership, though it also
//     lost a replica since one physical node hosts both) keeps working
//     too.

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raftcore "github.com/SushantPotu/raft-kv-store/internal/raft"
	"github.com/SushantPotu/raft-kv-store/internal/metaservice"
	"github.com/SushantPotu/raft-kv-store/internal/routing"
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

func listenLocal(t *testing.T) (net.Listener, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return lis, lis.Addr().String()
}

// testNode is one simulated physical kvnode process, hosting a replica of
// every shard in shardIDs via its own internal/shard.Manager.
type testNode struct {
	id         raft.NodeID
	clientAddr string

	mgr       *shard.Manager
	transport *grpctransport.Client
	peerSrv   *grpc.Server
	clientSrv *grpc.Server
	cancel    context.CancelFunc
	closers   []func() error

	stopped bool
}

// stop simulates this node's process being killed: it cancels the
// Manager's context (stopping its ticker/Ready-loop goroutines), stops
// both gRPC servers immediately (not gracefully — a crashed process
// doesn't drain in-flight RPCs), and closes every shard's storage/engine.
// Idempotent.
func (tn *testNode) stop() {
	if tn.stopped {
		return
	}
	tn.stopped = true
	tn.cancel()
	tn.mgr.Wait()
	tn.peerSrv.Stop()
	tn.clientSrv.Stop()
	tn.transport.Close()
	for i := len(tn.closers) - 1; i >= 0; i-- {
		if err := tn.closers[i](); err != nil {
			// Best-effort cleanup only; not a test failure.
			_ = err
		}
	}
}

func newTestNode(t *testing.T, id raft.NodeID, peerAddrs map[raft.NodeID]string, clientLis net.Listener, clientAddr string, peerLis net.Listener, shardIDs []raft.ShardID, metaClient metapb.MetadataServiceClient, parentCtx context.Context) *testNode {
	t.Helper()

	transport := grpctransport.NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	for peerID, addr := range peerAddrs {
		if peerID == id {
			continue
		}
		transport.AddPeer(peerID, addr)
	}

	registry := grpctransport.NewNodeRegistry()
	mgr := shard.NewManager(id, transport, registry,
		shard.WithMetadataClient(metaClient),
		// Fast ticks/polling so elections and propose->apply latency stay
		// well under the test's wait timeouts.
		shard.WithTickInterval(15*time.Millisecond),
		shard.WithReadyPollInterval(3*time.Millisecond),
	)

	tn := &testNode{id: id, clientAddr: clientAddr, mgr: mgr, transport: transport}

	dataDir := t.TempDir()
	var peerIDs []raft.NodeID
	for peerID := range peerAddrs {
		if peerID != id {
			peerIDs = append(peerIDs, peerID)
		}
	}

	for _, sid := range shardIDs {
		logStorage, err := raftlog.Open(filepath.Join(dataDir, string(sid), "raftlog"), raftlog.Options{})
		if err != nil {
			t.Fatalf("open raft log storage (node=%s shard=%s): %v", id, sid, err)
		}
		tn.closers = append(tn.closers, logStorage.Close)

		kvEngine, err := engine.Open(filepath.Join(dataDir, string(sid), "engine"), engine.Options{})
		if err != nil {
			t.Fatalf("open kv engine (node=%s shard=%s): %v", id, sid, err)
		}
		tn.closers = append(tn.closers, kvEngine.Close)

		adapter := statemachine.NewAdapter(kvEngine)

		node := raftcore.NewNode(id, sid, peerIDs, logStorage, transport, adapter,
			raftcore.WithElectionTimeoutTicks(6, 10),
			raftcore.WithHeartbeatIntervalTicks(1),
		)

		mgr.AddShard(shard.Shard{ID: sid, Node: node, Storage: logStorage, SM: adapter, KV: adapter})
	}

	peerSrv := grpc.NewServer()
	raftpb.RegisterRaftTransportServiceServer(peerSrv, grpctransport.NewServer(registry))
	go func() { _ = peerSrv.Serve(peerLis) }()
	tn.peerSrv = peerSrv

	clientSrv := grpc.NewServer()
	kvpb.RegisterKVServiceServer(clientSrv, mgr.KVServiceServer())
	go func() { _ = clientSrv.Serve(clientLis) }()
	tn.clientSrv = clientSrv

	ctx, cancel := context.WithCancel(parentCtx)
	tn.cancel = cancel
	mgr.Start(ctx)

	if err := mgr.RegisterReplicas(context.Background(), clientAddr, ""); err != nil {
		t.Fatalf("RegisterReplicas(%s): %v", id, err)
	}

	return tn
}

// waitForLeader polls metaservice until shardID has a recorded leader at
// a term strictly greater than afterTerm, or fails the test after a
// generous timeout.
func waitForLeader(t *testing.T, metaClient metapb.MetadataServiceClient, shardID string, afterTerm uint64) *metapb.ShardDescriptor {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		resp, err := metaClient.ListShards(context.Background(), &metapb.ListShardsRequest{})
		if err == nil {
			for _, d := range resp.GetShards() {
				if d.GetShardId() == shardID && d.GetCurrentLeaderNodeId() != "" && d.GetCurrentLeaderTerm() > afterTerm {
					return d
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for shard %q to have a leader (term > %d)", shardID, afterTerm)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitForValue polls router.Get(key) until it returns want or a generous
// timeout elapses. A Put only guarantees the proposal was handed to the
// replication pipeline (see raft.Node.Propose's doc comment), not that
// it's committed and applied yet, and a plain Get is a stale, local-state
// read (kvpb.Consistency's doc comment) — so a Get issued immediately
// after a Put can legitimately race ahead of that write's apply. That's a
// Raft Core / read-consistency concern this test isn't exercising; it
// just needs to observe the write eventually, so it polls instead of
// asserting on the very next read.
func waitForValue(t *testing.T, router *routing.Router, key []byte, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last *kvpb.GetResponse
	for time.Now().Before(deadline) {
		resp, err := router.Get(context.Background(), &kvpb.GetRequest{Key: key})
		if err == nil && resp.GetFound() && string(resp.GetValue()) == want {
			return
		}
		if err == nil {
			last = resp
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("Get(%q) never returned %q, last response=%+v", key, want, last)
}

func TestTwoShardsThreeReplicasRoutingAndLeaderFailover(t *testing.T) {
	nodeIDs := []raft.NodeID{"node-a", "node-b", "node-c"}
	shardIDs := []raft.ShardID{"shard-1", "shard-2"}

	peerListeners := map[raft.NodeID]net.Listener{}
	peerAddrs := map[raft.NodeID]string{}
	clientListeners := map[raft.NodeID]net.Listener{}
	clientAddrs := map[raft.NodeID]string{}
	for _, id := range nodeIDs {
		pl, paddr := listenLocal(t)
		peerListeners[id], peerAddrs[id] = pl, paddr
		cl, caddr := listenLocal(t)
		clientListeners[id], clientAddrs[id] = cl, caddr
	}

	// --- metaservice, seeded with both shards' key ranges. ---
	metaStore := metaservice.NewMemStore()
	seedCtx := context.Background()
	if err := metaStore.Put(seedCtx, &metapb.ShardDescriptor{ShardId: "shard-1", KeyRangeEnd: []byte("m")}); err != nil {
		t.Fatalf("seed shard-1: %v", err)
	}
	if err := metaStore.Put(seedCtx, &metapb.ShardDescriptor{ShardId: "shard-2", KeyRangeStart: []byte("m")}); err != nil {
		t.Fatalf("seed shard-2: %v", err)
	}
	metaLis, metaAddr := listenLocal(t)
	metaSrv := grpc.NewServer()
	metapb.RegisterMetadataServiceServer(metaSrv, metaservice.NewService(metaStore))
	go func() { _ = metaSrv.Serve(metaLis) }()
	t.Cleanup(metaSrv.Stop)

	metaConn, err := grpc.NewClient(metaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial metaservice: %v", err)
	}
	t.Cleanup(func() { metaConn.Close() })
	metaClient := metapb.NewMetadataServiceClient(metaConn)

	// --- three physical nodes, each hosting a replica of both shards. ---
	parentCtx, parentCancel := context.WithCancel(context.Background())
	t.Cleanup(parentCancel)

	nodes := make(map[raft.NodeID]*testNode)
	for _, id := range nodeIDs {
		nodes[id] = newTestNode(t, id, peerAddrs, clientListeners[id], clientAddrs[id], peerListeners[id], shardIDs, metaClient, parentCtx)
	}
	t.Cleanup(func() {
		for _, tn := range nodes {
			tn.stop()
		}
	})

	// --- both shards must elect a leader and report it. ---
	shard1Desc := waitForLeader(t, metaClient, "shard-1", 0)
	shard2Desc := waitForLeader(t, metaClient, "shard-2", 0)
	t.Logf("shard-1 leader=%s term=%d; shard-2 leader=%s term=%d",
		shard1Desc.GetCurrentLeaderNodeId(), shard1Desc.GetCurrentLeaderTerm(),
		shard2Desc.GetCurrentLeaderNodeId(), shard2Desc.GetCurrentLeaderTerm())

	// --- router, talking real gRPC to every replica. ---
	dial := routing.NewGRPCDialer(grpc.WithTransportCredentials(insecure.NewCredentials()))
	router := routing.NewRouter(metaClient, dial)

	// "alpha" < "m" => shard-1.
	if _, err := router.Put(context.Background(), &kvpb.PutRequest{Key: []byte("alpha"), Value: []byte("v-alpha")}); err != nil {
		t.Fatalf("Put(alpha): %v", err)
	}
	waitForValue(t, router, []byte("alpha"), "v-alpha")

	// "zulu" >= "m" => shard-2.
	if _, err := router.Put(context.Background(), &kvpb.PutRequest{Key: []byte("zulu"), Value: []byte("v-zulu")}); err != nil {
		t.Fatalf("Put(zulu): %v", err)
	}
	waitForValue(t, router, []byte("zulu"), "v-zulu")

	// --- kill shard-1's leader outright, as if the process crashed. ---
	killedID := raft.NodeID(shard1Desc.GetCurrentLeaderNodeId())
	t.Logf("killing node %s (shard-1's leader)", killedID)
	nodes[killedID].stop()

	// Remaining two replicas (still a majority of 3) must elect a new
	// leader for shard-1, at a strictly higher term.
	newShard1Desc := waitForLeader(t, metaClient, "shard-1", shard1Desc.GetCurrentLeaderTerm())
	if newShard1Desc.GetCurrentLeaderNodeId() == string(killedID) {
		t.Fatalf("shard-1 still recorded with the killed node as leader")
	}
	t.Logf("shard-1 new leader=%s term=%d", newShard1Desc.GetCurrentLeaderNodeId(), newShard1Desc.GetCurrentLeaderTerm())

	// The router's cache still points at the now-dead old leader for
	// shard-1 — it must self-heal (transport error -> forced metaservice
	// refresh -> retry) and keep serving reads AND writes correctly.
	waitForValue(t, router, []byte("alpha"), "v-alpha")
	if _, err := router.Put(context.Background(), &kvpb.PutRequest{Key: []byte("alpha2"), Value: []byte("v-alpha2")}); err != nil {
		t.Fatalf("Put(alpha2) after leader kill: %v", err)
	}
	waitForValue(t, router, []byte("alpha2"), "v-alpha2")

	// shard-2 (also missing a replica, since one physical node hosted
	// both shards) must still work too — 2 of 3 replicas is still quorum.
	// If the killed node also happened to be shard-2's leader, wait for
	// its own re-election first (same reasoning as shard-1 above:
	// Router's single retry can't out-wait an election still in
	// progress).
	if shard2Desc.GetCurrentLeaderNodeId() == string(killedID) {
		newShard2Desc := waitForLeader(t, metaClient, "shard-2", shard2Desc.GetCurrentLeaderTerm())
		t.Logf("shard-2 new leader=%s term=%d", newShard2Desc.GetCurrentLeaderNodeId(), newShard2Desc.GetCurrentLeaderTerm())
	}
	if _, err := router.Put(context.Background(), &kvpb.PutRequest{Key: []byte("zulu2"), Value: []byte("v-zulu2")}); err != nil {
		t.Fatalf("Put(zulu2) after leader kill: %v", err)
	}
	waitForValue(t, router, []byte("zulu2"), "v-zulu2")
}
