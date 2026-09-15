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

func (n *steppingNode) Propose(ctx context.Context, data []byte) error                  { return nil }
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

func TestServerDispatchesByShardID(t *testing.T) {
	reg := NewNodeRegistry()
	node := &steppingNode{}
	reg.Register("shard-a", node)

	s := NewServer(reg)
	_, err := s.AppendEntries(context.Background(), &raftpb.AppendEntriesRequest{
		ShardId:  "shard-a",
		LeaderId: "leader-1",
		Term:     5,
	})
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
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

func TestServerUnknownShardReturnsNotFound(t *testing.T) {
	s := NewServer(NewNodeRegistry())
	_, err := s.AppendEntries(context.Background(), &raftpb.AppendEntriesRequest{ShardId: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("error = %v, want codes.NotFound", err)
	}
}
