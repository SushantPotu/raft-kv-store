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

// recordingRaftServer captures the last envelope/chunk it received, so
// tests can assert on exactly what Client put on the wire (in particular,
// that shard_id was stamped correctly and the oneof body round-trips).
type recordingRaftServer struct {
	raftpb.UnimplementedRaftTransportServiceServer

	lastEnvelope *raftpb.RaftMessage
	lastChunk    *raftpb.InstallSnapshotChunk
}

func (s *recordingRaftServer) Send(ctx context.Context, envelope *raftpb.RaftMessage) (*raftpb.SendAck, error) {
	s.lastEnvelope = envelope
	return &raftpb.SendAck{}, nil
}

func (s *recordingRaftServer) InstallSnapshot(stream raftpb.RaftTransportService_InstallSnapshotServer) error {
	chunk, err := stream.Recv()
	if err != nil {
		return err
	}
	s.lastChunk = chunk
	return stream.SendAndClose(&raftpb.InstallSnapshotAck{})
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

// TestSendAppendEntriesRequestRoundTrips verifies Client.Send dials the
// registered peer, wraps an AppendEntriesRequest payload in a RaftMessage
// envelope stamped with the given shard, and the server receives it intact.
func TestSendAppendEntriesRequestRoundTrips(t *testing.T) {
	srv := &recordingRaftServer{}
	addr := startRecordingServer(t, srv)
	c := newTestClient(t, "peer-1", addr)

	msg := raft.Message{
		To:    "peer-1",
		Shard: "shard-42",
		Kind:  raft.MsgAppendEntries,
		Payload: &raftpb.AppendEntriesRequest{
			Term:     7,
			LeaderId: "leader-1",
			Entries: []*raftpb.LogEntry{
				{Term: 7, Index: 1, Type: raftpb.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("x")},
			},
		},
	}
	if err := c.Send(context.Background(), msg); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if srv.lastEnvelope == nil {
		t.Fatal("server never received a RaftMessage")
	}
	if got := srv.lastEnvelope.GetShardId(); got != "shard-42" {
		t.Fatalf("shard_id on wire = %q, want %q", got, "shard-42")
	}
	body, ok := srv.lastEnvelope.GetBody().(*raftpb.RaftMessage_AppendEntriesRequest)
	if !ok {
		t.Fatalf("envelope body type = %T, want *RaftMessage_AppendEntriesRequest", srv.lastEnvelope.GetBody())
	}
	if got := body.AppendEntriesRequest.GetLeaderId(); got != "leader-1" {
		t.Fatalf("leader_id on wire = %q, want %q", got, "leader-1")
	}
	if len(body.AppendEntriesRequest.GetEntries()) != 1 {
		t.Fatalf("entries on wire = %d, want 1", len(body.AppendEntriesRequest.GetEntries()))
	}
}

// TestSendRequestVoteResponseRoundTrips verifies a *response*-typed
// payload (the case the original per-RPC-type Transport design couldn't
// carry — see raft.Transport's doc comment) travels through Send just as
// well as a request-typed one.
func TestSendRequestVoteResponseRoundTrips(t *testing.T) {
	srv := &recordingRaftServer{}
	addr := startRecordingServer(t, srv)
	c := newTestClient(t, "peer-1", addr)

	msg := raft.Message{
		To:    "peer-1",
		Shard: "shard-7",
		Kind:  raft.MsgRequestVote,
		Payload: &raftpb.RequestVoteResponse{
			VoterId:     "voter-9",
			Term:        3,
			VoteGranted: true,
		},
	}
	if err := c.Send(context.Background(), msg); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if srv.lastEnvelope.GetShardId() != "shard-7" {
		t.Fatalf("shard_id on wire = %q, want %q", srv.lastEnvelope.GetShardId(), "shard-7")
	}
	body, ok := srv.lastEnvelope.GetBody().(*raftpb.RaftMessage_RequestVoteResponse)
	if !ok {
		t.Fatalf("envelope body type = %T, want *RaftMessage_RequestVoteResponse", srv.lastEnvelope.GetBody())
	}
	if !body.RequestVoteResponse.GetVoteGranted() || body.RequestVoteResponse.GetVoterId() != "voter-9" {
		t.Fatalf("unexpected response payload: %+v", body.RequestVoteResponse)
	}
}

// TestSendInstallSnapshotChunkRoundTrips verifies the client-streaming
// InstallSnapshot RPC is driven correctly from Client's per-chunk
// SendInstallSnapshotChunk method (open stream, send one chunk, close).
func TestSendInstallSnapshotChunkRoundTrips(t *testing.T) {
	srv := &recordingRaftServer{}
	addr := startRecordingServer(t, srv)
	c := newTestClient(t, "peer-1", addr)

	chunk := &raftpb.InstallSnapshotChunk{Term: 9, LeaderId: "leader-1", Data: []byte("snap"), Done: true}
	if err := c.SendInstallSnapshotChunk(context.Background(), "shard-1", "peer-1", chunk); err != nil {
		t.Fatalf("SendInstallSnapshotChunk: %v", err)
	}

	if srv.lastChunk == nil {
		t.Fatal("server never received an InstallSnapshot chunk")
	}
	if srv.lastChunk.GetShardId() != "shard-1" {
		t.Fatalf("shard_id on wire = %q, want %q", srv.lastChunk.GetShardId(), "shard-1")
	}
	if srv.lastChunk.GetTerm() != 9 || string(srv.lastChunk.GetData()) != "snap" {
		t.Fatalf("unexpected chunk: %+v", srv.lastChunk)
	}
}

// TestSendToUnregisteredPeerFails confirms Client refuses to guess an
// address rather than dialing nothing / silently failing later.
func TestSendToUnregisteredPeerFails(t *testing.T) {
	c := NewClient(grpc.WithTransportCredentials(insecure.NewCredentials()))
	t.Cleanup(func() { _ = c.Close() })

	err := c.Send(context.Background(), raft.Message{
		To:      "ghost",
		Shard:   "shard-1",
		Kind:    raft.MsgAppendEntries,
		Payload: &raftpb.AppendEntriesRequest{},
	})
	if err == nil {
		t.Fatal("expected error sending to a peer with no registered address")
	}
}

// TestSendUnsupportedPayloadTypeFails confirms Client rejects a payload it
// doesn't recognize rather than silently sending an empty envelope.
func TestSendUnsupportedPayloadTypeFails(t *testing.T) {
	srv := &recordingRaftServer{}
	addr := startRecordingServer(t, srv)
	c := newTestClient(t, "peer-1", addr)

	err := c.Send(context.Background(), raft.Message{To: "peer-1", Shard: "shard-1", Payload: "not a raftpb type"})
	if err == nil {
		t.Fatal("expected error for unsupported payload type")
	}
}
