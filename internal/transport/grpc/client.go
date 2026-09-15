// Package grpctransport is the real gRPC-backed implementation of
// pkg/raft.Transport (Workstream C: Client/API Protocol Layer).
//
// Raft Core (internal/raft) never imports this package directly — it only
// depends on the pkg/raft.Transport interface, which is what lets it be
// tested with pkg/raft/rafttest's in-memory fakes with zero network I/O.
// Client here is what a real deployment wires in instead of those fakes.
//
// The package is named grpctransport rather than grpc so it doesn't
// shadow the imported google.golang.org/grpc package at every call site.
package grpctransport

import (
	"context"
	"fmt"
	"io"
	"sync"

	"google.golang.org/grpc"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
	"github.com/SushantPotu/raft-kv-store/pkg/raftpb"
)

// Client implements raft.Transport by dialing peers over gRPC and issuing
// RaftTransportService RPCs. One Client is shared by all shards a process
// hosts; every outbound call is parameterized by shard.ShardID, which
// Client stamps onto the outgoing request's shard_id field itself (rather
// than trusting the caller to have set it) — this is the seam a future
// Multi-Raft deployment relies on to multiplex many shards' traffic over
// the same peer connections.
type Client struct {
	dialOpts []grpc.DialOption

	mu    sync.Mutex
	conns map[raft.NodeID]*grpc.ClientConn
	addrs map[raft.NodeID]string
}

var _ raft.Transport = (*Client)(nil)

// NewClient constructs a Client. dialOpts are applied to every peer
// connection Client dials lazily (e.g. grpc.WithTransportCredentials);
// callers must supply transport credentials themselves — Client has no
// opinion on TLS vs. insecure.
func NewClient(dialOpts ...grpc.DialOption) *Client {
	return &Client{
		dialOpts: dialOpts,
		conns:    make(map[raft.NodeID]*grpc.ClientConn),
		addrs:    make(map[raft.NodeID]string),
	}
}

// AddPeer registers the dial address for a peer NodeID. The connection
// itself is established lazily (and lazily re-established) on first use,
// consistent with grpc.ClientConn's own connect-on-demand behavior.
func (c *Client) AddPeer(id raft.NodeID, addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addrs[id] = addr
}

// Close tears down every peer connection this Client has opened.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for id, conn := range c.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(c.conns, id)
	}
	return firstErr
}

func (c *Client) clientFor(target raft.NodeID) (raftpb.RaftTransportServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.conns[target]; ok {
		return raftpb.NewRaftTransportServiceClient(conn), nil
	}

	addr, ok := c.addrs[target]
	if !ok {
		return nil, fmt.Errorf("grpctransport: no address registered for peer %q (call AddPeer first)", target)
	}

	conn, err := grpc.NewClient(addr, c.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("grpctransport: dial %q at %q: %w", target, addr, err)
	}
	c.conns[target] = conn
	return raftpb.NewRaftTransportServiceClient(conn), nil
}

// Send implements raft.Transport. It wraps msg.Payload in a RaftMessage
// envelope (see proto/raftpb/raft.proto's design note) and fires it at
// msg.To as a single RPC. The call's own return carries no Raft-protocol
// information — per raft.Transport.Send's doc comment, the real reply (if
// msg was a request) arrives later as its own separate Send call in the
// other direction.
func (c *Client) Send(ctx context.Context, msg raft.Message) error {
	cli, err := c.clientFor(msg.To)
	if err != nil {
		return err
	}

	envelope := &raftpb.RaftMessage{ShardId: string(msg.Shard)}
	switch p := msg.Payload.(type) {
	case *raftpb.RequestVoteRequest:
		envelope.Body = &raftpb.RaftMessage_RequestVoteRequest{RequestVoteRequest: p}
	case *raftpb.RequestVoteResponse:
		envelope.Body = &raftpb.RaftMessage_RequestVoteResponse{RequestVoteResponse: p}
	case *raftpb.AppendEntriesRequest:
		envelope.Body = &raftpb.RaftMessage_AppendEntriesRequest{AppendEntriesRequest: p}
	case *raftpb.AppendEntriesResponse:
		envelope.Body = &raftpb.RaftMessage_AppendEntriesResponse{AppendEntriesResponse: p}
	case *raftpb.InstallSnapshotResponse:
		envelope.Body = &raftpb.RaftMessage_InstallSnapshotResponse{InstallSnapshotResponse: p}
	default:
		return fmt.Errorf("grpctransport: unsupported message payload type %T", msg.Payload)
	}

	_, err = cli.Send(ctx, envelope)
	return err
}

// SendInstallSnapshotChunk implements raft.Transport. The wire RPC
// (RaftTransportService.InstallSnapshot) is client-streaming, because a
// full-keyspace snapshot can be too large for one message — but this
// method's own signature is per-chunk, so each call here opens its own
// short-lived stream, sends the single chunk it was given, and immediately
// closes the send side.
//
// Tradeoff: a caller sending a multi-chunk snapshot (repeated
// SendInstallSnapshotChunk calls with offset/done fields tracking
// progress, per the proto's field comments) pays a new-stream setup cost
// per chunk instead of reusing one stream for the whole snapshot. That's
// an acceptable simplification for Checkpoint 1 — snapshotting isn't
// exercised by any real caller yet (Workstream E). Revisit if/when
// snapshot transfer performance actually matters. The stream's own
// completion value is discarded for the same reason Send's is: the real
// InstallSnapshotResponse travels back via a later Send call, not this
// RPC's return.
func (c *Client) SendInstallSnapshotChunk(ctx context.Context, shard raft.ShardID, target raft.NodeID, chunk *raftpb.InstallSnapshotChunk) error {
	cli, err := c.clientFor(target)
	if err != nil {
		return err
	}
	chunk.ShardId = string(shard)

	stream, err := cli.InstallSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("grpctransport: open InstallSnapshot stream to %q: %w", target, err)
	}
	if err := stream.Send(chunk); err != nil && err != io.EOF {
		return fmt.Errorf("grpctransport: send InstallSnapshot chunk to %q: %w", target, err)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("grpctransport: close InstallSnapshot stream to %q: %w", target, err)
	}
	return nil
}
