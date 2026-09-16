// Package shard is the Multi-Raft host: internal/shard.Manager hosts N
// independent raft.Node instances (one per shard) inside a single process,
// each with its own Storage/StateMachine, sharing one gRPC transport and
// one shared Ready-loop driver/ticker across every shard it hosts. It
// generalizes cmd/kvnode/main.go's original single-shard bootstrap (one
// Node, one Storage, one StateMachine, one Ready-loop goroutine, one
// ticker goroutine) to loop over many, plus adds the metaservice
// integration a single-shard deployment never needed: registering this
// replica for each hosted shard at startup, and reporting every observed
// leadership transition.
//
// Manager builds on the already-stable seams called out in
// pkg/raft/types.go: Transport.Send takes a raft.Message that already
// carries its own ShardID, and internal/transport/grpc.NodeRegistry is
// already a map[ShardID]raft.Node used by Server to dispatch inbound peer
// RPCs — Manager's AddShard populates exactly that registry, one shard at
// a time.
package shard

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/SushantPotu/raft-kv-store/internal/server"
	grpctransport "github.com/SushantPotu/raft-kv-store/internal/transport/grpc"
	"github.com/SushantPotu/raft-kv-store/pkg/kvpb"
	"github.com/SushantPotu/raft-kv-store/pkg/metapb"
	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// KVReader is satisfied by internal/statemachine.Adapter (and, in tests,
// any other per-shard local-read store) and is all Manager needs to build
// a hosted shard's client-facing server.KVServer.
type KVReader interface {
	Get(key []byte) (value []byte, found bool)
}

// Shard bundles one hosted shard's already-constructed Node with the
// Storage/StateMachine/KVReader the Manager's shared Ready-loop driver and
// client-facing dispatcher need. Constructing these — raftlog.Open,
// engine.Open, statemachine.NewAdapter, raftcore.NewNode, one of each per
// shard, in its own data subdirectory — is the caller's job, exactly like
// cmd/kvnode/main.go already does for its single shard (see
// cmd/kvnode/multishard.go for the N-shard version of that same
// construction); Manager only hosts and drives what it's handed.
type Shard struct {
	ID      raft.ShardID
	Node    raft.Node
	Storage raft.Storage
	SM      raft.StateMachine
	KV      KVReader
}

// hostedShard adds the bookkeeping Manager needs on top of a Shard: the
// per-shard KVServer built at AddShard time, and enough of the
// last-observed raft.Status to detect a leadership transition rather than
// re-reporting on every poll.
type hostedShard struct {
	Shard
	kvServer *server.KVServer

	reported     bool
	lastLeader   raft.NodeID
	lastTerm     raft.Term
	lastIsLeader bool
}

// Manager hosts N independent raft.Node instances (one per shard) in a
// single process — the Multi-Raft host itself. One shared raft.Transport
// and grpctransport.NodeRegistry are used for peer traffic across every
// hosted shard (both already support this by design — see this package's
// doc comment); Manager adds one shared Ready-loop driver and one shared
// ticker, generalized from cmd/kvnode's single-shard versions to loop over
// every hosted shard each cycle, plus metaservice integration.
type Manager struct {
	nodeID    raft.NodeID
	transport raft.Transport
	registry  *grpctransport.NodeRegistry
	meta      metapb.MetadataServiceClient // nil disables all metaservice integration

	tickInterval      time.Duration
	readyPollInterval time.Duration

	mu     sync.Mutex
	shards map[raft.ShardID]*hostedShard

	wg sync.WaitGroup
}

// Option configures a Manager constructed by NewManager.
type Option func(*Manager)

// WithMetadataClient enables metaservice integration: RegisterReplicas
// calls RegisterReplica for every hosted shard, and the Ready-loop driver
// calls ReportLeaderChange whenever a hosted shard's Status() transitions
// to/from IsLeader. Without this option (the default), Manager still
// hosts and drives every shard correctly — it just never talks to
// metaservice, which is what keeps this package usable for a
// single-shard deployment that has no metaservice running at all.
func WithMetadataClient(meta metapb.MetadataServiceClient) Option {
	return func(m *Manager) { m.meta = meta }
}

// WithTickInterval overrides the default tick interval (50ms, matching
// cmd/kvnode's single-shard default).
func WithTickInterval(d time.Duration) Option {
	return func(m *Manager) { m.tickInterval = d }
}

// WithReadyPollInterval overrides the default Ready() poll interval (5ms,
// matching cmd/kvnode's single-shard default).
func WithReadyPollInterval(d time.Duration) Option {
	return func(m *Manager) { m.readyPollInterval = d }
}

// NewManager constructs a Manager for nodeID, sharing transport (outbound
// raft.Transport) and registry (inbound shard_id-keyed dispatch —
// internal/transport/grpc.Server's backing map) across every shard AddShard
// hosts.
func NewManager(nodeID raft.NodeID, transport raft.Transport, registry *grpctransport.NodeRegistry, opts ...Option) *Manager {
	m := &Manager{
		nodeID:            nodeID,
		transport:         transport,
		registry:          registry,
		tickInterval:      50 * time.Millisecond,
		readyPollInterval: 5 * time.Millisecond,
		shards:            make(map[raft.ShardID]*hostedShard),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// AddShard hosts s: registers s.Node with the shared NodeRegistry (so
// inbound peer RPCs naming s.ID dispatch to it) and builds this shard's
// client-facing KVServer. Call it for every shard before Start.
func (m *Manager) AddShard(s Shard) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.registry.Register(s.ID, s.Node)
	m.shards[s.ID] = &hostedShard{
		Shard:    s,
		kvServer: server.NewKVServer(s.Node, s.KV),
	}
}

// ShardIDs returns the IDs of every shard currently hosted, in no
// particular order.
func (m *Manager) ShardIDs() []raft.ShardID {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]raft.ShardID, 0, len(m.shards))
	for id := range m.shards {
		ids = append(ids, id)
	}
	return ids
}

// RegisterReplicas calls metaservice's RegisterReplica once for every
// currently-hosted shard, advertising addr (this node's client-facing
// KVService address other components — kvrouter, kvctl — would dial, e.g.
// "kvnode-1:9001") and az (availability zone; may be empty). A no-op if
// this Manager was built without WithMetadataClient.
func (m *Manager) RegisterReplicas(ctx context.Context, addr, az string) error {
	if m.meta == nil {
		return nil
	}
	for _, id := range m.ShardIDs() {
		_, err := m.meta.RegisterReplica(ctx, &metapb.RegisterReplicaRequest{
			ShardId: string(id),
			Replica: &metapb.ReplicaDescriptor{
				NodeId:           string(m.nodeID),
				Address:          addr,
				AvailabilityZone: az,
			},
		})
		if err != nil {
			return fmt.Errorf("shard.Manager: RegisterReplica shard %q: %w", id, err)
		}
	}
	return nil
}

// KVServiceServer returns a kvpb.KVServiceServer that dispatches each
// request to the hosted shard named by its shard_id field — the
// client-facing analog of grpctransport.Server's shard_id-keyed dispatch
// for peer RPCs.
func (m *Manager) KVServiceServer() kvpb.KVServiceServer {
	return &kvMux{mgr: m}
}

func (m *Manager) kvServerFor(shardID string) (*server.KVServer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	hs, ok := m.shards[raft.ShardID(shardID)]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "shard.Manager: shard %q is not hosted on node %q", shardID, m.nodeID)
	}
	return hs.kvServer, nil
}

// kvMux implements kvpb.KVServiceServer by dispatching on each request's
// shard_id to the matching hosted shard's own server.KVServer.
type kvMux struct {
	kvpb.UnimplementedKVServiceServer
	mgr *Manager
}

var _ kvpb.KVServiceServer = (*kvMux)(nil)

func (x *kvMux) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	s, err := x.mgr.kvServerFor(req.GetShardId())
	if err != nil {
		return nil, err
	}
	return s.Get(ctx, req)
}

func (x *kvMux) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	s, err := x.mgr.kvServerFor(req.GetShardId())
	if err != nil {
		return nil, err
	}
	return s.Put(ctx, req)
}

func (x *kvMux) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	s, err := x.mgr.kvServerFor(req.GetShardId())
	if err != nil {
		return nil, err
	}
	return s.Delete(ctx, req)
}

func (x *kvMux) CompareAndSwap(ctx context.Context, req *kvpb.CompareAndSwapRequest) (*kvpb.CompareAndSwapResponse, error) {
	s, err := x.mgr.kvServerFor(req.GetShardId())
	if err != nil {
		return nil, err
	}
	return s.CompareAndSwap(ctx, req)
}

// Start launches the shared ticker and Ready-loop driver goroutines. It
// returns immediately; both goroutines run until ctx is canceled. Call
// Wait to block until they've fully exited (e.g. during graceful
// shutdown, before closing each shard's Storage/StateMachine).
func (m *Manager) Start(ctx context.Context) {
	m.wg.Add(2)
	go m.tickLoop(ctx)
	go m.readyLoop(ctx)
}

// Wait blocks until every goroutine Start launched has exited.
func (m *Manager) Wait() {
	m.wg.Wait()
}

func (m *Manager) snapshot() []*hostedShard {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*hostedShard, 0, len(m.shards))
	for _, hs := range m.shards {
		out = append(out, hs)
	}
	return out
}

// tickLoop drives Tick() on every hosted shard's Node once per
// tickInterval — the shared-ticker generalization of cmd/kvnode's
// single-shard ticker goroutine.
func (m *Manager) tickLoop(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, hs := range m.snapshot() {
				hs.Node.Tick()
			}
		}
	}
}

// readyLoop is the shared Ready-loop driver: each readyPollInterval tick,
// it polls every hosted shard's Ready() non-blockingly (see
// raft.Node.Ready's doc comment for why this has to be a poll, not a
// blocking receive) and, for whichever shards produced one, persists/
// sends/applies exactly as cmd/kvnode's single-shard version does — then
// checks that shard's Status() for a leadership transition to report to
// metaservice.
func (m *Manager) readyLoop(ctx context.Context) {
	defer m.wg.Done()
	poll := time.NewTicker(m.readyPollInterval)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
		}
		for _, hs := range m.snapshot() {
			select {
			case rd := <-hs.Node.Ready():
				m.handleReady(hs, rd)
				hs.Node.Advance()
			default:
			}
			m.maybeReportLeaderChange(hs)
		}
	}
}

// handleReady is cmd/kvnode's original single-shard handleReady,
// unchanged in behavior, generalized to operate on one hostedShard's own
// Storage/StateMachine instead of a single pair of package-level
// variables.
func (m *Manager) handleReady(hs *hostedShard, rd raft.Ready) {
	if rd.HardState != nil {
		if err := hs.Storage.SetHardState(*rd.HardState); err != nil {
			log.Printf("shard.Manager: shard %s: SetHardState: %v", hs.ID, err)
		}
	}
	if len(rd.Entries) > 0 {
		if err := hs.Storage.Append(rd.Entries); err != nil {
			log.Printf("shard.Manager: shard %s: Append: %v", hs.ID, err)
		}
	}

	// Fire each outbound message in its own goroutine: a dial failure or a
	// slow/unreachable peer must never block the shared Ready loop from
	// making progress on the next shard/round.
	for _, msg := range rd.Messages {
		msg := msg
		go func() {
			sendCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := raft.SendMessage(sendCtx, m.transport, msg); err != nil {
				log.Printf("shard.Manager: shard %s: Send to %s failed: %v", hs.ID, msg.To, err)
			}
		}()
	}

	for _, entry := range rd.CommittedEntries {
		if entry.Type == raft.EntryConfChange {
			// Membership changes aren't wired up by this workstream.
			continue
		}
		if len(entry.Data) == 0 {
			continue
		}
		if _, err := hs.SM.Apply(entry); err != nil {
			log.Printf("shard.Manager: shard %s: Apply entry %d: %v", hs.ID, entry.Index, err)
		}
	}

	if rd.Snapshot != nil {
		if err := hs.Storage.ApplySnapshot(*rd.Snapshot); err != nil {
			log.Printf("shard.Manager: shard %s: ApplySnapshot: %v", hs.ID, err)
		} else if err := hs.SM.RestoreSnapshot(rd.Snapshot.Data); err != nil {
			log.Printf("shard.Manager: shard %s: RestoreSnapshot: %v", hs.ID, err)
		}
	}
}

// maybeReportLeaderChange compares hs's current Status() against the last
// one this Manager reported (or, on the very first call, against the zero
// value) and, if this replica's own leadership just changed — became
// leader, stopped being leader, or advanced term while remaining leader —
// fires a ReportLeaderChange asynchronously so a slow/unreachable
// metaservice never blocks the Ready loop. It deliberately does NOT report
// every observed change to Status().Leader (which also updates on a plain
// follower just from processing AppendEntries) — metaservice only needs to
// hear from the replica whose own leadership actually transitioned.
func (m *Manager) maybeReportLeaderChange(hs *hostedShard) {
	if m.meta == nil {
		return
	}

	st := hs.Node.Status()
	changed := !hs.reported || st.Leader != hs.lastLeader || st.Term != hs.lastTerm || st.IsLeader != hs.lastIsLeader
	shouldReport := changed && (st.IsLeader || hs.lastIsLeader)

	hs.reported = true
	hs.lastLeader = st.Leader
	hs.lastTerm = st.Term
	hs.lastIsLeader = st.IsLeader

	if !shouldReport {
		return
	}

	shardID := hs.ID
	req := &metapb.ReportLeaderChangeRequest{
		ShardId:         string(shardID),
		NewLeaderNodeId: string(st.Leader),
		Term:            uint64(st.Term),
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := m.meta.ReportLeaderChange(ctx, req)
		if err != nil {
			log.Printf("shard.Manager: shard %s: ReportLeaderChange: %v", shardID, err)
			return
		}
		if !resp.GetAccepted() {
			log.Printf("shard.Manager: shard %s: ReportLeaderChange(term=%d) rejected, current term on record is %d", shardID, req.Term, resp.GetCurrentTermOnRecord())
		}
	}()
}
