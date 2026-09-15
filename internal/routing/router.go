// Package routing implements kvrouter: a stateless router that implements
// kvpb.KVServiceServer (the same client-facing interface
// internal/server.KVServer implements) but runs no raft.Node of its own.
// Instead, for every request it resolves the owning shard via
// internal/metaservice (by key, through GetShardForKey), forwards the call
// to whichever replica it currently believes is that shard's leader, and —
// on a "not leader" response (a non-empty leader_hint, exactly as
// internal/server/kvserver.go's leaderHint helper produces) or a
// transport-level failure reaching that replica at all (e.g. it was
// killed) — refreshes its belief and retries once.
//
// Router keeps a local in-memory cache of shard descriptors (key range +
// current leader address) so the common case — repeated requests against
// an already-known, still-correct leader — never touches metaservice.
// Per proto/metapb/meta.proto's MetadataService doc comment, metaservice
// is always the authoritative source; this cache is purely a self-healing
// optimization layered on top, never the other way around — a wrong cache
// entry can only cost one extra round trip (the retry), never an
// incorrect answer served to the client.
package routing

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"

	"github.com/SushantPotu/raft-kv-store/pkg/kvpb"
	"github.com/SushantPotu/raft-kv-store/pkg/metapb"
)

// KVClientFactory returns a kvpb.KVServiceClient for addr (a
// replica's client-facing "host:port"). Implementations are expected to
// cache/reuse connections across calls with the same addr — see
// NewGRPCDialer for the production implementation and this package's
// tests for a fake used to control exactly which client a given address
// resolves to.
type KVClientFactory func(addr string) (kvpb.KVServiceClient, error)

// grpcDialer is the production KVClientFactory: it lazily dials each
// distinct address over gRPC once and reuses the connection, mirroring
// internal/transport/grpc.Client's own lazy-dial-and-cache behavior for
// peer connections.
type grpcDialer struct {
	dialOpts []grpc.DialOption

	mu      sync.Mutex
	clients map[string]kvpb.KVServiceClient
}

// NewGRPCDialer returns a KVClientFactory that dials addr over gRPC with
// dialOpts (e.g. grpc.WithTransportCredentials(insecure.NewCredentials())
// for local/Docker Compose rehearsal), caching one connection per address.
func NewGRPCDialer(dialOpts ...grpc.DialOption) KVClientFactory {
	d := &grpcDialer{dialOpts: dialOpts, clients: make(map[string]kvpb.KVServiceClient)}
	return d.dial
}

func (d *grpcDialer) dial(addr string) (kvpb.KVServiceClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if c, ok := d.clients[addr]; ok {
		return c, nil
	}
	conn, err := grpc.NewClient(addr, d.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("routing: dial %q: %w", addr, err)
	}
	c := kvpb.NewKVServiceClient(conn)
	d.clients[addr] = c
	return c, nil
}

// shardCacheEntry is one shard's cached routing information: its key
// range (so Router can resolve a key to a shard locally, without a
// metaservice round trip) and the address Router currently believes hosts
// its leader.
type shardCacheEntry struct {
	rangeStart, rangeEnd []byte
	leaderAddr           string
}

// inRange mirrors internal/metaservice.inRange exactly (key_range_end
// empty means "no upper bound").
func inRange(key, start, end []byte) bool {
	if bytes.Compare(key, start) < 0 {
		return false
	}
	if len(end) == 0 {
		return true
	}
	return bytes.Compare(key, end) < 0
}

// Router implements kvpb.KVServiceServer by forwarding to the shard
// leader it believes is current, per this package's doc comment.
type Router struct {
	kvpb.UnimplementedKVServiceServer

	meta metapb.MetadataServiceClient
	dial KVClientFactory

	mu     sync.RWMutex
	shards map[string]*shardCacheEntry // shard_id -> cached descriptor
}

var _ kvpb.KVServiceServer = (*Router)(nil)

// NewRouter constructs a Router resolving shards via meta and reaching
// replicas via dial.
func NewRouter(meta metapb.MetadataServiceClient, dial KVClientFactory) *Router {
	return &Router{meta: meta, dial: dial, shards: make(map[string]*shardCacheEntry)}
}

// leaderAddrFromDescriptor picks the address of desc's recorded leader,
// falling back to the first known replica if the leader is unrecorded or
// not present in the replica list (e.g. right after a shard was seeded
// but before any replica has reported in) — a reasonable first guess that
// self-corrects via a leader_hint on the first request if it's wrong.
func leaderAddrFromDescriptor(desc *metapb.ShardDescriptor) (string, error) {
	leaderID := desc.GetCurrentLeaderNodeId()
	var fallback string
	for _, r := range desc.GetReplicas() {
		if fallback == "" {
			fallback = r.GetAddress()
		}
		if leaderID != "" && r.GetNodeId() == leaderID {
			return r.GetAddress(), nil
		}
	}
	if fallback != "" {
		return fallback, nil
	}
	return "", fmt.Errorf("routing: shard %q has no known replicas", desc.GetShardId())
}

// resolveShardForKey returns (shard_id, leader address) for key. It
// consults the local cache first unless forceRefresh is set (used after a
// transport-level failure talking to the cached leader address, per this
// package's doc comment), and always falls back to a fresh
// GetShardForKey call on a cache miss.
func (r *Router) resolveShardForKey(ctx context.Context, key []byte, forceRefresh bool) (shardID, leaderAddr string, err error) {
	if !forceRefresh {
		r.mu.RLock()
		for id, e := range r.shards {
			if inRange(key, e.rangeStart, e.rangeEnd) {
				addr := e.leaderAddr
				r.mu.RUnlock()
				return id, addr, nil
			}
		}
		r.mu.RUnlock()
	}

	resp, err := r.meta.GetShardForKey(ctx, &metapb.GetShardForKeyRequest{Key: key})
	if err != nil {
		return "", "", fmt.Errorf("routing: GetShardForKey: %w", err)
	}
	desc := resp.GetShard()
	addr, err := leaderAddrFromDescriptor(desc)
	if err != nil {
		return "", "", err
	}

	r.mu.Lock()
	r.shards[desc.GetShardId()] = &shardCacheEntry{
		rangeStart: desc.GetKeyRangeStart(),
		rangeEnd:   desc.GetKeyRangeEnd(),
		leaderAddr: addr,
	}
	r.mu.Unlock()

	return desc.GetShardId(), addr, nil
}

// updateLeaderHint overwrites shardID's cached leader address with hint,
// e.g. after a response's leader_hint field redirected us. If shardID
// isn't cached yet (shouldn't normally happen — we only ever get a hint
// back for a shard we just resolved) it's a no-op; the next request for
// that key just re-resolves from metaservice.
func (r *Router) updateLeaderHint(shardID, hint string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.shards[shardID]; ok {
		e.leaderAddr = hint
	}
}

// Get implements kvpb.KVServiceServer.
func (r *Router) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	shardID, addr, err := r.resolveShardForKey(ctx, req.GetKey(), false)
	if err != nil {
		return nil, err
	}
	req.ShardId = shardID

	resp, err := r.callGet(ctx, addr, req)
	if err != nil {
		// Transport-level failure reaching the cached leader (e.g. it was
		// killed) — bypass the cache, re-resolve, and retry once.
		_, addr2, rerr := r.resolveShardForKey(ctx, req.GetKey(), true)
		if rerr != nil {
			return nil, err
		}
		return r.callGet(ctx, addr2, req)
	}
	if hint := resp.GetLeaderHint(); hint != "" {
		r.updateLeaderHint(shardID, hint)
		if resp2, err2 := r.callGet(ctx, hint, req); err2 == nil {
			return resp2, nil
		}
		return resp, nil
	}
	return resp, nil
}

func (r *Router) callGet(ctx context.Context, addr string, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	cli, err := r.dial(addr)
	if err != nil {
		return nil, err
	}
	return cli.Get(ctx, req)
}

// Put implements kvpb.KVServiceServer.
func (r *Router) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	shardID, addr, err := r.resolveShardForKey(ctx, req.GetKey(), false)
	if err != nil {
		return nil, err
	}
	req.ShardId = shardID

	resp, err := r.callPut(ctx, addr, req)
	if err != nil {
		_, addr2, rerr := r.resolveShardForKey(ctx, req.GetKey(), true)
		if rerr != nil {
			return nil, err
		}
		return r.callPut(ctx, addr2, req)
	}
	if hint := resp.GetLeaderHint(); hint != "" {
		r.updateLeaderHint(shardID, hint)
		if resp2, err2 := r.callPut(ctx, hint, req); err2 == nil {
			return resp2, nil
		}
		return resp, nil
	}
	return resp, nil
}

func (r *Router) callPut(ctx context.Context, addr string, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	cli, err := r.dial(addr)
	if err != nil {
		return nil, err
	}
	return cli.Put(ctx, req)
}

// Delete implements kvpb.KVServiceServer.
func (r *Router) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	shardID, addr, err := r.resolveShardForKey(ctx, req.GetKey(), false)
	if err != nil {
		return nil, err
	}
	req.ShardId = shardID

	resp, err := r.callDelete(ctx, addr, req)
	if err != nil {
		_, addr2, rerr := r.resolveShardForKey(ctx, req.GetKey(), true)
		if rerr != nil {
			return nil, err
		}
		return r.callDelete(ctx, addr2, req)
	}
	if hint := resp.GetLeaderHint(); hint != "" {
		r.updateLeaderHint(shardID, hint)
		if resp2, err2 := r.callDelete(ctx, hint, req); err2 == nil {
			return resp2, nil
		}
		return resp, nil
	}
	return resp, nil
}

func (r *Router) callDelete(ctx context.Context, addr string, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	cli, err := r.dial(addr)
	if err != nil {
		return nil, err
	}
	return cli.Delete(ctx, req)
}

// CompareAndSwap implements kvpb.KVServiceServer.
func (r *Router) CompareAndSwap(ctx context.Context, req *kvpb.CompareAndSwapRequest) (*kvpb.CompareAndSwapResponse, error) {
	shardID, addr, err := r.resolveShardForKey(ctx, req.GetKey(), false)
	if err != nil {
		return nil, err
	}
	req.ShardId = shardID

	resp, err := r.callCAS(ctx, addr, req)
	if err != nil {
		_, addr2, rerr := r.resolveShardForKey(ctx, req.GetKey(), true)
		if rerr != nil {
			return nil, err
		}
		return r.callCAS(ctx, addr2, req)
	}
	if hint := resp.GetLeaderHint(); hint != "" {
		r.updateLeaderHint(shardID, hint)
		if resp2, err2 := r.callCAS(ctx, hint, req); err2 == nil {
			return resp2, nil
		}
		return resp, nil
	}
	return resp, nil
}

func (r *Router) callCAS(ctx context.Context, addr string, req *kvpb.CompareAndSwapRequest) (*kvpb.CompareAndSwapResponse, error) {
	cli, err := r.dial(addr)
	if err != nil {
		return nil, err
	}
	return cli.CompareAndSwap(ctx, req)
}
