package grpctransport

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
	"github.com/SushantPotu/raft-kv-store/pkg/raftpb"
)

// steppingNode is a minimal raft.Node recording the last InboundMessage
// it received via Step, modeled on rafttest's recordingNode.
type steppingNode struct {
	lastMsg raft.InboundMessage
	stepped bool
}

var _ raft.Node = (*steppingNode)(nil)

func (n *steppingNode) Propose(ctx context.Context, data []byte) error                 { return nil }
func (n *steppingNode) ProposeConfChange(ctx context.Context, cc raft.ConfChange) error { return nil }
func (n *steppingNode) ReadIndex(ctx context.Context, ctxToken []byte) error            { return nil }
func (n *steppingNode) Step(ctx context.Context, msg raft.InboundMessage) error {
	n.lastMsg = msg
	n.stepped = true
	return nil
}
func (n *steppingNode) Ready() <-chan raft.Ready { return nil }
func (n *steppingNode) Advance()                 {}
func (n *steppingNode) Tick()                    {}
func (n *steppingNode) Status() raft.Status      { return raft.Status{} }

func TestServerDispatchesRequestByShardID(t *testing.T) {
	reg := NewNodeRegistry()
	node := &steppingNode{}
	reg.Register("shard-a", node)

	s := NewServer(reg)
	envelope := &raftpb.RaftMessage{
		ShardId: "shard-a",
		Body: &raftpb.RaftMessage_AppendEntriesRequest{
			AppendEntriesRequest: &raftpb.AppendEntriesRequest{LeaderId: "leader-1", Term: 5},
		},
	}
	if _, err := s.Send(context.Background(), envelope); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if !node.stepped {
		t.Fatal("expected registered node's Step to be called")
	}
	if node.lastMsg.Kind != raft.MsgAppendEntries {
		t.Fatalf("Step message kind = %v, want MsgAppendEntries", node.lastMsg.Kind)
	}
	if node.lastMsg.From != "leader-1" {
		t.Fatalf("Step message From = %q, want %q", node.lastMsg.From, "leader-1")
	}
}

// TestServerDispatchesResponseByShardID verifies a response-typed envelope
// body is routed too, with From derived from the response's own identity
// field (voter_id/follower_id) rather than a request field — this is the
// exact case the original per-RPC-type Transport design couldn't handle.
func TestServerDispatchesResponseByShardID(t *testing.T) {
	reg := NewNodeRegistry()
	node := &steppingNode{}
	reg.Register("shard-a", node)

	s := NewServer(reg)
	envelope := &raftpb.RaftMessage{
		ShardId: "shard-a",
		Body: &raftpb.RaftMessage_RequestVoteResponse{
			RequestVoteResponse: &raftpb.RequestVoteResponse{VoterId: "voter-2", Term: 5, VoteGranted: true},
		},
	}
	if _, err := s.Send(context.Background(), envelope); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if node.lastMsg.Kind != raft.MsgRequestVote {
		t.Fatalf("Step message kind = %v, want MsgRequestVote", node.lastMsg.Kind)
	}
	if node.lastMsg.From != "voter-2" {
		t.Fatalf("Step message From = %q, want %q", node.lastMsg.From, "voter-2")
	}
	if _, ok := node.lastMsg.Payload.(*raftpb.RequestVoteResponse); !ok {
		t.Fatalf("Step message payload type = %T, want *RequestVoteResponse", node.lastMsg.Payload)
	}
}

func TestServerUnknownShardReturnsNotFound(t *testing.T) {
	s := NewServer(NewNodeRegistry())
	envelope := &raftpb.RaftMessage{
		ShardId: "missing",
		Body:    &raftpb.RaftMessage_AppendEntriesRequest{AppendEntriesRequest: &raftpb.AppendEntriesRequest{}},
	}
	_, err := s.Send(context.Background(), envelope)
	if status.Code(err) != codes.NotFound {
		t.Fatalf("error = %v, want codes.NotFound", err)
	}
}

func TestServerEmptyBodyReturnsInvalidArgument(t *testing.T) {
	reg := NewNodeRegistry()
	reg.Register("shard-a", &steppingNode{})
	s := NewServer(reg)

	_, err := s.Send(context.Background(), &raftpb.RaftMessage{ShardId: "shard-a"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error = %v, want codes.InvalidArgument", err)
	}
}
