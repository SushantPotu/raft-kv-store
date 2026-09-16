package server

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/SushantPotu/raft-kv-store/pkg/kvpb"
	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

func TestKVServerPutGetDelete(t *testing.T) {
	s := NewSingleNodeStubServer("n1")
	ctx := context.Background()

	if _, err := s.Put(ctx, &kvpb.PutRequest{Key: []byte("foo"), Value: []byte("bar")}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	resp, err := s.Get(ctx, &kvpb.GetRequest{Key: []byte("foo")})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.GetFound() || string(resp.GetValue()) != "bar" {
		t.Fatalf("Get = %+v, want found=true value=bar", resp)
	}

	if _, err := s.Delete(ctx, &kvpb.DeleteRequest{Key: []byte("foo")}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	resp, err = s.Get(ctx, &kvpb.GetRequest{Key: []byte("foo")})
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if resp.GetFound() {
		t.Fatalf("Get after delete = %+v, want found=false", resp)
	}
}

func TestKVServerGetLinearizableIsUnimplemented(t *testing.T) {
	s := NewSingleNodeStubServer("n1")
	_, err := s.Get(context.Background(), &kvpb.GetRequest{
		Key:         []byte("foo"),
		Consistency: kvpb.Consistency_CONSISTENCY_LINEARIZABLE,
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("Get(LINEARIZABLE) error = %v, want codes.Unimplemented", err)
	}
}

func TestKVServerCompareAndSwap(t *testing.T) {
	s := NewSingleNodeStubServer("n1")
	ctx := context.Background()

	// CAS against an absent key with expect_absent=true should succeed.
	resp, err := s.CompareAndSwap(ctx, &kvpb.CompareAndSwapRequest{
		Key:          []byte("k"),
		ExpectAbsent: true,
		NewValue:     []byte("v1"),
	})
	if err != nil {
		t.Fatalf("CompareAndSwap (create): %v", err)
	}
	if !resp.GetSwapped() {
		t.Fatalf("CompareAndSwap (create) = %+v, want swapped=true", resp)
	}

	// CAS with the wrong expected value should fail and report the actual value.
	resp, err = s.CompareAndSwap(ctx, &kvpb.CompareAndSwapRequest{
		Key:           []byte("k"),
		ExpectedValue: []byte("wrong"),
		NewValue:      []byte("v2"),
	})
	if err != nil {
		t.Fatalf("CompareAndSwap (mismatch): %v", err)
	}
	if resp.GetSwapped() {
		t.Fatalf("CompareAndSwap (mismatch) = %+v, want swapped=false", resp)
	}
	if string(resp.GetActualValue()) != "v1" {
		t.Fatalf("CompareAndSwap (mismatch) actual_value = %q, want %q", resp.GetActualValue(), "v1")
	}

	// CAS with the correct expected value should succeed.
	resp, err = s.CompareAndSwap(ctx, &kvpb.CompareAndSwapRequest{
		Key:           []byte("k"),
		ExpectedValue: []byte("v1"),
		NewValue:      []byte("v2"),
	})
	if err != nil {
		t.Fatalf("CompareAndSwap (match): %v", err)
	}
	if !resp.GetSwapped() {
		t.Fatalf("CompareAndSwap (match) = %+v, want swapped=true", resp)
	}

	get, err := s.Get(ctx, &kvpb.GetRequest{Key: []byte("k")})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(get.GetValue()) != "v2" {
		t.Fatalf("final value = %q, want %q", get.GetValue(), "v2")
	}
}

func TestKVServerLeaderHintEmptyForStub(t *testing.T) {
	s := NewSingleNodeStubServer("n1")
	resp, err := s.Put(context.Background(), &kvpb.PutRequest{Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if resp.GetLeaderHint() != "" {
		t.Fatalf("leader_hint = %q, want empty (single-node stub is always leader)", resp.GetLeaderHint())
	}
}

// notLeaderNode is a minimal raft.Node that always rejects Propose with
// raft.ErrNotLeader, modeling what a real multi-node Node does on a
// follower. This is the exact case that motivated propose's ErrNotLeader
// handling: without it, KVServer would surface a raw gRPC error instead of
// the LeaderHint-carrying response kvctl is written to expect (see
// cmd/kvctl/main.go's "not leader, try %s" handling).
type notLeaderNode struct {
	leader raft.NodeID
}

var _ raft.Node = (*notLeaderNode)(nil)

func (n *notLeaderNode) Propose(ctx context.Context, data []byte) error {
	return fmt.Errorf("%w (leader is %q)", raft.ErrNotLeader, n.leader)
}
func (n *notLeaderNode) ProposeConfChange(ctx context.Context, cc raft.ConfChange) error {
	return raft.ErrNotLeader
}
func (n *notLeaderNode) ReadIndex(ctx context.Context, ctxToken []byte) error { return nil }
func (n *notLeaderNode) Step(ctx context.Context, msg raft.InboundMessage) error {
	return nil
}
func (n *notLeaderNode) Ready() <-chan raft.Ready { return nil }
func (n *notLeaderNode) Advance()                 {}
func (n *notLeaderNode) Tick()                    {}
func (n *notLeaderNode) Status() raft.Status {
	return raft.Status{ID: "follower", Leader: n.leader, IsLeader: false}
}

func TestKVServerNotLeaderReturnsHintNotError(t *testing.T) {
	node := &notLeaderNode{leader: "kvnode-2"}
	s := NewKVServer(node, newFakeStateMachine())

	putResp, err := s.Put(context.Background(), &kvpb.PutRequest{Key: []byte("k"), Value: []byte("v")})
	if err != nil {
		t.Fatalf("Put on a non-leader must not return an error, got: %v", err)
	}
	if putResp.GetLeaderHint() != "kvnode-2" {
		t.Fatalf("Put leader_hint = %q, want %q", putResp.GetLeaderHint(), "kvnode-2")
	}

	delResp, err := s.Delete(context.Background(), &kvpb.DeleteRequest{Key: []byte("k")})
	if err != nil {
		t.Fatalf("Delete on a non-leader must not return an error, got: %v", err)
	}
	if delResp.GetLeaderHint() != "kvnode-2" {
		t.Fatalf("Delete leader_hint = %q, want %q", delResp.GetLeaderHint(), "kvnode-2")
	}

	casResp, err := s.CompareAndSwap(context.Background(), &kvpb.CompareAndSwapRequest{
		Key: []byte("k"), ExpectAbsent: true, NewValue: []byte("v"),
	})
	if err != nil {
		t.Fatalf("CompareAndSwap on a non-leader must not return an error, got: %v", err)
	}
	if casResp.GetLeaderHint() != "kvnode-2" {
		t.Fatalf("CompareAndSwap leader_hint = %q, want %q", casResp.GetLeaderHint(), "kvnode-2")
	}
	if casResp.GetSwapped() {
		t.Fatal("CompareAndSwap on a non-leader must not report swapped=true — nothing was ever proposed")
	}
}

// TestKVServerNotLeaderAndLeaderUnknownReturnsError covers the case
// notLeaderNode{leader: "kvnode-2"} above can't: a follower that doesn't
// know who the leader is either (e.g. mid-election), so its
// leader_hint would be "" — the same value a *successful* write from
// the actual leader also produces (see leaderHint's doc comment). Without
// notLeaderResponse's explicit check, Put/Delete/CompareAndSwap would
// return that empty-hint response as if the write had succeeded, when it
// was never proposed anywhere. This is exactly the bug that made
// internal/localcluster's very first end-to-end test flake: a client
// racing the cluster's initial election could get a false "OK" for a
// write that silently vanished.
func TestKVServerNotLeaderAndLeaderUnknownReturnsError(t *testing.T) {
	node := &notLeaderNode{leader: ""}
	s := NewKVServer(node, newFakeStateMachine())

	if _, err := s.Put(context.Background(), &kvpb.PutRequest{Key: []byte("k"), Value: []byte("v")}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Put with leader unknown: err = %v, want codes.Unavailable", err)
	}
	if _, err := s.Delete(context.Background(), &kvpb.DeleteRequest{Key: []byte("k")}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Delete with leader unknown: err = %v, want codes.Unavailable", err)
	}
	_, err := s.CompareAndSwap(context.Background(), &kvpb.CompareAndSwapRequest{
		Key: []byte("k"), ExpectAbsent: true, NewValue: []byte("v"),
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("CompareAndSwap with leader unknown: err = %v, want codes.Unavailable", err)
	}
}
