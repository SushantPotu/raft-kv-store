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
// inbound RaftMessage envelope to the right local raft.Node by shard_id
// via NodeRegistry, and translating it into a raft.InboundMessage for
// Node.Step.
//
// Response contract: per raft.Transport.Send's doc comment, Node.Step only
// feeds a message in — it never synchronously returns the eventual reply
// (e.g. a RequestVote's vote_granted). That reply, if any, is produced
// later as its own outbound raft.Message and delivered back via a
// separate Send call in the other direction, which is what actually
// carries it — not this RPC's return value. Server therefore returns
// SendAck{} unconditionally once Step succeeds.
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

// Send implements raftpb.RaftTransportServiceServer. It unwraps the
// envelope's oneof body, and — since neither request nor response
// payloads carry the sender's NodeID at the raft.Message level (only the
// wire messages that need one do, per RequestVoteResponse.voter_id's and
// AppendEntriesResponse.follower_id's comments) — derives InboundMessage.From
// from whichever identity field the concrete payload type carries.
func (s *Server) Send(ctx context.Context, envelope *raftpb.RaftMessage) (*raftpb.SendAck, error) {
	node, err := s.nodeFor(envelope.GetShardId())
	if err != nil {
		return nil, err
	}

	var msg raft.InboundMessage
	msg.Shard = raft.ShardID(envelope.GetShardId())

	switch body := envelope.GetBody().(type) {
	case *raftpb.RaftMessage_RequestVoteRequest:
		msg.Kind = raft.MsgRequestVote
		msg.From = raft.NodeID(body.RequestVoteRequest.GetCandidateId())
		msg.Payload = body.RequestVoteRequest
	case *raftpb.RaftMessage_RequestVoteResponse:
		msg.Kind = raft.MsgRequestVote
		msg.From = raft.NodeID(body.RequestVoteResponse.GetVoterId())
		msg.Payload = body.RequestVoteResponse
	case *raftpb.RaftMessage_AppendEntriesRequest:
		msg.Kind = raft.MsgAppendEntries
		msg.From = raft.NodeID(body.AppendEntriesRequest.GetLeaderId())
		msg.Payload = body.AppendEntriesRequest
	case *raftpb.RaftMessage_AppendEntriesResponse:
		msg.Kind = raft.MsgAppendEntries
		msg.From = raft.NodeID(body.AppendEntriesResponse.GetFollowerId())
		msg.Payload = body.AppendEntriesResponse
	case *raftpb.RaftMessage_InstallSnapshotResponse:
		msg.Kind = raft.MsgInstallSnapshot
		msg.From = raft.NodeID(body.InstallSnapshotResponse.GetFollowerId())
		msg.Payload = body.InstallSnapshotResponse
	default:
		return nil, status.Errorf(codes.InvalidArgument, "grpctransport: RaftMessage with empty or unknown body (%T)", body)
	}

	if err := node.Step(ctx, msg); err != nil {
		return nil, status.Errorf(codes.Internal, "grpctransport: Step: %v", err)
	}
	return &raftpb.SendAck{}, nil
}

// InstallSnapshot implements raftpb.RaftTransportServiceServer. Per
// Client.SendInstallSnapshotChunk's doc comment, each stream on this
// server side currently carries exactly one chunk (sent, then the client
// closes its send side) — but this handler is written to loop on Recv
// until io.EOF regardless, so it keeps working unmodified if a future
// caller reuses one stream across multiple chunks.
func (s *Server) InstallSnapshot(stream raftpb.RaftTransportService_InstallSnapshotServer) error {
	var (
		node    raft.Node
		shardID string
	)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&raftpb.InstallSnapshotAck{})
		}
		if err != nil {
			return fmt.Errorf("grpctransport: recv InstallSnapshot chunk: %w", err)
		}

		if node == nil {
			shardID = chunk.GetShardId()
			n, err := s.nodeFor(shardID)
			if err != nil {
				return err
			}
			node = n
		}

		msg := raft.InboundMessage{
			From:    raft.NodeID(chunk.GetLeaderId()),
			Shard:   raft.ShardID(shardID),
			Kind:    raft.MsgInstallSnapshot,
			Payload: chunk,
		}
		if err := node.Step(stream.Context(), msg); err != nil {
			return status.Errorf(codes.Internal, "grpctransport: Step: %v", err)
		}
	}
}
