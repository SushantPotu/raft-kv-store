package grpctransport

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
	"github.com/SushantPotu/raft-kv-store/pkg/raftpb"
)

// recordingRaftServer captures the last request it received for each RPC,
// so tests can assert on exactly what Client put on the wire (in
// particular, that shard_id was stamped correctly).
type recordingRaftServer struct {
	raftpb.UnimplementedRaftTransportServiceServer

	lastAppendEntries *raftpb.AppendEntriesRequest
	lastRequestVote   *raftpb.RequestVoteRequest
}

func (s *recordingRaftServer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	s.lastRequestVote = req
	return &raftpb.RequestVoteResponse{Term: req.GetTerm(), VoteGranted: true}, nil
}

func (s *recordingRaftServer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	s.lastAppendEntries = req
	return &raftpb.AppendEntriesResponse{Term: req.GetTerm(), Success: true}, nil
}

func (s *recordingRaftServer) InstallSnapshot(stream raftpb.RaftTransportService_InstallSnapshotServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	return stream.SendAndClose(&raftpb.InstallSnapshotResponse{Term: req.GetTerm()})
}

// startRecordingServer starts a real RaftTransportService gRPC server on
// an OS-assigned localhost port backed by srv, and returns its address.
func startRecordingServer(t *testing.T, srv *recordingRaftServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	raftpb.RegisterRaftTransportServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

func newTestClient(t *testing.T, id raft.NodeID, addr string) *Client {
	t.Helper()
	c := NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	c.AddPeer(id, addr)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestSendAppendEntriesRoundTripsShardID verifies Client.SendAppendEntries
// dials the registered peer, stamps the given shard onto the outgoing
// request (overriding whatever the caller already set, since Client is
// meant to be the single place this seam gets enforced), and returns the
// server's real response over a real gRPC connection.
func TestSendAppendEntriesRoundTripsShardID(t *testing.T) {
	srv := &recordingRaftServer{}
	addr := startRecordingServer(t, srv)
	c := newTestClient(t, "peer-1", addr)

	req := &raftpb.AppendEntriesRequest{
		ShardId:  "wrong-shard", // Client must overwrite this with the shard arg
		Term:     7,
		LeaderId: "leader-1",
		Entries: []*raftpb.LogEntry{
			{Term: 7, Index: 1, Type: raftpb.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("x")},
		},
	}
	resp, err := c.SendAppendEntries(context.Background(), "shard-42", "peer-1", req)
	if err != nil {
		t.Fatalf("SendAppendEntries: %v", err)
	}
	if resp.GetTerm() != 7 || !resp.GetSuccess() {
		t.Fatalf("unexpected response: %+v", resp)
	}

	if srv.lastAppendEntries == nil {
		t.Fatal("server never received AppendEntries")
	}
	if got := srv.lastAppendEntries.GetShardId(); got != "shard-42" {
		t.Fatalf("shard_id on wire = %q, want %q", got, "shard-42")
	}
	if got := srv.lastAppendEntries.GetLeaderId(); got != "leader-1" {
		t.Fatalf("leader_id on wire = %q, want %q", got, "leader-1")
	}
	if len(srv.lastAppendEntries.GetEntries()) != 1 {
		t.Fatalf("entries on wire = %d, want 1", len(srv.lastAppendEntries.GetEntries()))
	}
}

// TestSendRequestVoteRoundTripsShardID mirrors the AppendEntries test for
// RequestVote.
func TestSendRequestVoteRoundTripsShardID(t *testing.T) {
	srv := &recordingRaftServer{}
	addr := startRecordingServer(t, srv)
	c := newTestClient(t, "peer-1", addr)

	req := &raftpb.RequestVoteRequest{Term: 3, CandidateId: "candidate-1"}
	resp, err := c.SendRequestVote(context.Background(), "shard-7", "peer-1", req)
	if err != nil {
		t.Fatalf("SendRequestVote: %v", err)
	}
	if resp.GetTerm() != 3 || !resp.GetVoteGranted() {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if srv.lastRequestVote.GetShardId() != "shard-7" {
		t.Fatalf("shard_id on wire = %q, want %q", srv.lastRequestVote.GetShardId(), "shard-7")
	}
}

// TestSendInstallSnapshotRoundTrips verifies the client-streaming
// InstallSnapshot RPC is driven correctly from Client's unary-looking
// SendInstallSnapshot method (open stream, send one chunk, close and
// receive the response).
func TestSendInstallSnapshotRoundTrips(t *testing.T) {
	srv := &recordingRaftServer{}
	addr := startRecordingServer(t, srv)
	c := newTestClient(t, "peer-1", addr)

	req := &raftpb.InstallSnapshotRequest{Term: 9, LeaderId: "leader-1", Data: []byte("snap"), Done: true}
	resp, err := c.SendInstallSnapshot(context.Background(), "shard-1", "peer-1", req)
	if err != nil {
		t.Fatalf("SendInstallSnapshot: %v", err)
	}
	if resp.GetTerm() != 9 {
		t.Fatalf("response term = %d, want 9", resp.GetTerm())
	}
}

// TestSendToUnregisteredPeerFails confirms Client refuses to guess an
// address rather than dialing nothing / silently failing later.
func TestSendToUnregisteredPeerFails(t *testing.T) {
	c := NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	t.Cleanup(func() { _ = c.Close() })

	_, err := c.SendAppendEntries(context.Background(), "shard-1", "ghost", &raftpb.AppendEntriesRequest{})
	if err == nil {
		t.Fatal("expected error sending to a peer with no registered address")
	}
}
