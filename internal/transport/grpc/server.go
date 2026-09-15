package grpctransport

import (
	"context"
	"fmt"
	"io"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
	"github.com/SushantPotu/raft-kv-store/pkg/raftpb"
)

// NodeRegistry maps a ShardID to the local raft.Node hosting that shard's
// replica in this process. It stands in for the real internal/shard.Manager
// (a later workstream, not built yet), which will eventually own
// starting/stopping shards dynamically; for a single-shard deployment,
// something just needs to tell the gRPC server which Node an inbound RPC's
// shard_id maps to, and this is the simplest thing that could do that.
//
// Keep this simple — a more general multiplexer (dynamic registration on
// shard add/remove, routing metrics, etc.) belongs to whoever builds
// internal/shard.Manager, not here.
type NodeRegistry struct {
	mu    sync.RWMutex
	nodes map[raft.ShardID]raft.Node
}

// NewNodeRegistry constructs an empty registry.
func NewNodeRegistry() *NodeRegistry {
	return &NodeRegistry{nodes: make(map[raft.ShardID]raft.Node)}
}

// Register associates a shard with the local Node handling it. A
// single-shard deployment calls this once at startup with its one Node
// (or, for Workstream C's own testing, internal/server.SingleNodeStub).
func (r *NodeRegistry) Register(shard raft.ShardID, node raft.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[shard] = node
}

// Unregister removes a shard, e.g. when it's moved off this replica.
func (r *NodeRegistry) Unregister(shard raft.ShardID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.nodes, shard)
}

func (r *NodeRegistry) get(shard raft.ShardID) (raft.Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[shard]
	return n, ok
}

// Server implements raftpb.RaftTransportServiceServer, dispatching each
// inbound peer RPC to the right local raft.Node by shard_id via
// NodeRegistry, and translating it into a raft.InboundMessage for
// Node.Step.
//
// Response contract: per the Ready-loop design (docs/adr/0001), Node.Step
// only feeds a message in — it does not synchronously return the
// eventual reply (e.g. a RequestVote's vote_granted). That reply, if any,
// is produced later as an outbound raft.Message in a future Ready and
// sent back out through Transport like any other message. Server
// therefore returns a zero-value response immediately after Step
// succeeds, exactly as pkg/raft/rafttest.FakeTransport already does for
// the same reason. Bridging a synchronous RPC's response to the
// asynchronous Ready-produced reply is internal/shard.Manager's job once
// it exists, not this Server's.
type Server struct {
	raftpb.UnimplementedRaftTransportServiceServer

	registry *NodeRegistry
}

var _ raftpb.RaftTransportServiceServer = (*Server)(nil)

// NewServer constructs a Server dispatching through registry.
func NewServer(registry *NodeRegistry) *Server {
	return &Server{registry: registry}
}

func (s *Server) nodeFor(shardID string) (raft.Node, error) {
	node, ok := s.registry.get(raft.ShardID(shardID))
	if !ok {
		return nil, status.Errorf(codes.NotFound, "grpctransport: no node registered for shard %q", shardID)
	}
	return node, nil
}

// RequestVote implements raftpb.RaftTransportServiceServer.
func (s *Server) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	node, err := s.nodeFor(req.GetShardId())
	if err != nil {
		return nil, err
	}
	msg := raft.InboundMessage{
		From:    raft.NodeID(req.GetCandidateId()),
		Shard:   raft.ShardID(req.GetShardId()),
		Kind:    raft.MsgRequestVote,
		Payload: req,
	}
	if err := node.Step(ctx, msg); err != nil {
		return nil, status.Errorf(codes.Internal, "grpctransport: Step: %v", err)
	}
	return &raftpb.RequestVoteResponse{}, nil
}

// AppendEntries implements raftpb.RaftTransportServiceServer.
func (s *Server) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	node, err := s.nodeFor(req.GetShardId())
	if err != nil {
		return nil, err
	}
	msg := raft.InboundMessage{
		From:    raft.NodeID(req.GetLeaderId()),
		Shard:   raft.ShardID(req.GetShardId()),
		Kind:    raft.MsgAppendEntries,
		Payload: req,
	}
	if err := node.Step(ctx, msg); err != nil {
		return nil, status.Errorf(codes.Internal, "grpctransport: Step: %v", err)
	}
	return &raftpb.AppendEntriesResponse{}, nil
}

// InstallSnapshot implements raftpb.RaftTransportServiceServer. Per
// Client.SendInstallSnapshot's doc comment, each stream on this server
// side currently carries exactly one chunk (sent, then the client closes
// its send side) — but this handler is written to loop on Recv until
// io.EOF regardless, so it keeps working unmodified if a future caller
// reuses one stream across multiple chunks.
func (s *Server) InstallSnapshot(stream raftpb.RaftTransportService_InstallSnapshotServer) error {
	var (
		node    raft.Node
		shardID string
	)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&raftpb.InstallSnapshotResponse{})
		}
		if err != nil {
			return fmt.Errorf("grpctransport: recv InstallSnapshot chunk: %w", err)
		}

		if node == nil {
			shardID = req.GetShardId()
			n, err := s.nodeFor(shardID)
			if err != nil {
				return err
			}
			node = n
		}

		msg := raft.InboundMessage{
			From:    raft.NodeID(req.GetLeaderId()),
			Shard:   raft.ShardID(shardID),
			Kind:    raft.MsgInstallSnapshot,
			Payload: req,
		}
		if err := node.Step(stream.Context(), msg); err != nil {
			return status.Errorf(codes.Internal, "grpctransport: Step: %v", err)
		}
	}
}
