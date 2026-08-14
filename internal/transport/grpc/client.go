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

// SendRequestVote implements raft.Transport.
func (c *Client) SendRequestVote(ctx context.Context, shard raft.ShardID, target raft.NodeID, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	cli, err := c.clientFor(target)
	if err != nil {
		return nil, err
	}
	req.ShardId = string(shard)
	return cli.RequestVote(ctx, req)
}

// SendAppendEntries implements raft.Transport.
func (c *Client) SendAppendEntries(ctx context.Context, shard raft.ShardID, target raft.NodeID, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	cli, err := c.clientFor(target)
	if err != nil {
		return nil, err
	}
	req.ShardId = string(shard)
	return cli.AppendEntries(ctx, req)
}

// SendInstallSnapshot implements raft.Transport. Unlike RequestVote/
// AppendEntries, the wire RPC (RaftTransportService.InstallSnapshot) is
// client-streaming, because a full-keyspace snapshot can be too large for
// one message. But raft.Transport's method signature is unary — it takes
// and returns exactly one chunk — so each SendInstallSnapshot call here
// opens its own short-lived stream, sends the single chunk it was given,
// and immediately closes the send side and waits for the response.
//
// Tradeoff: a Raft Core caller sending a multi-chunk snapshot (repeated
// SendInstallSnapshot calls with offset/done fields tracking progress,
// per the proto's field comments) pays a new-stream setup cost per chunk
// instead of reusing one stream for the whole snapshot. That's an
// acceptable simplification for Checkpoint 1 — keeping this Transport
// implementation's shape a straightforward mirror of the other two
// methods matters more right now than optimizing a path (snapshot
// transfer) that isn't yet exercised by any real caller. Revisit if/when
// snapshot transfer performance actually matters.
func (c *Client) SendInstallSnapshot(ctx context.Context, shard raft.ShardID, target raft.NodeID, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	cli, err := c.clientFor(target)
	if err != nil {
		return nil, err
	}
	req.ShardId = string(shard)

	stream, err := cli.InstallSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("grpctransport: open InstallSnapshot stream to %q: %w", target, err)
	}
	if err := stream.Send(req); err != nil && err != io.EOF {
		return nil, fmt.Errorf("grpctransport: send InstallSnapshot chunk to %q: %w", target, err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return nil, fmt.Errorf("grpctransport: recv InstallSnapshot response from %q: %w", target, err)
	}
	return resp, nil
}
