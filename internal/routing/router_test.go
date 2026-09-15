package routing

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc"

	"github.com/SushantPotu/raft-kv-store/pkg/kvpb"
	"github.com/SushantPotu/raft-kv-store/pkg/metapb"
)

// fakeMetaClient implements metapb.MetadataServiceClient with a
// configurable GetShardForKey and a call counter, so tests can assert a
// cache hit never reaches it.
type fakeMetaClient struct {
	getShardForKey func(ctx context.Context, req *metapb.GetShardForKeyRequest) (*metapb.GetShardForKeyResponse, error)
	calls          int
}

var _ metapb.MetadataServiceClient = (*fakeMetaClient)(nil)

func (f *fakeMetaClient) GetShardForKey(ctx context.Context, req *metapb.GetShardForKeyRequest, _ ...grpc.CallOption) (*metapb.GetShardForKeyResponse, error) {
	f.calls++
	return f.getShardForKey(ctx, req)
}
func (f *fakeMetaClient) ListShards(context.Context, *metapb.ListShardsRequest, ...grpc.CallOption) (*metapb.ListShardsResponse, error) {
	return nil, errors.New("not used in this test")
}
func (f *fakeMetaClient) ReportLeaderChange(context.Context, *metapb.ReportLeaderChangeRequest, ...grpc.CallOption) (*metapb.ReportLeaderChangeResponse, error) {
	return nil, errors.New("not used in this test")
}
func (f *fakeMetaClient) RegisterReplica(context.Context, *metapb.RegisterReplicaRequest, ...grpc.CallOption) (*metapb.RegisterReplicaResponse, error) {
	return nil, errors.New("not used in this test")
}

// fakeKVClient implements kvpb.KVServiceClient for one specific address,
// with configurable per-call behavior and a call counter.
type fakeKVClient struct {
	name string
	get  func(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error)
	put  func(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error)
	gets int
	puts int
}

var _ kvpb.KVServiceClient = (*fakeKVClient)(nil)

func (f *fakeKVClient) Get(ctx context.Context, req *kvpb.GetRequest, _ ...grpc.CallOption) (*kvpb.GetResponse, error) {
	f.gets++
	return f.get(ctx, req)
}
func (f *fakeKVClient) Put(ctx context.Context, req *kvpb.PutRequest, _ ...grpc.CallOption) (*kvpb.PutResponse, error) {
	f.puts++
	return f.put(ctx, req)
}
func (f *fakeKVClient) Delete(context.Context, *kvpb.DeleteRequest, ...grpc.CallOption) (*kvpb.DeleteResponse, error) {
	return nil, errors.New("not used in this test")
}
func (f *fakeKVClient) CompareAndSwap(context.Context, *kvpb.CompareAndSwapRequest, ...grpc.CallOption) (*kvpb.CompareAndSwapResponse, error) {
	return nil, errors.New("not used in this test")
}

func dialerFor(clients map[string]kvpb.KVServiceClient) KVClientFactory {
	return func(addr string) (kvpb.KVServiceClient, error) {
		c, ok := clients[addr]
		if !ok {
			return nil, fmt.Errorf("no fake client registered for %q", addr)
		}
		return c, nil
	}
}

func shardDescriptor(shardID, leaderAddr string) *metapb.ShardDescriptor {
	return &metapb.ShardDescriptor{
		ShardId:             shardID,
		CurrentLeaderNodeId: "leader-1",
		Replicas: []*metapb.ReplicaDescriptor{
			{NodeId: "leader-1", Address: leaderAddr},
		},
	}
}

// TestCacheMissTriggersMetadataLookupThenHits verifies a cache miss calls
// GetShardForKey exactly once, and a second request for the same key is
// served entirely from the cache (no second metaservice call).
func TestCacheMissTriggersMetadataLookupThenHits(t *testing.T) {
	meta := &fakeMetaClient{
		getShardForKey: func(_ context.Context, req *metapb.GetShardForKeyRequest) (*metapb.GetShardForKeyResponse, error) {
			return &metapb.GetShardForKeyResponse{Shard: shardDescriptor("shard-1", "leader-addr:9001")}, nil
		},
	}
	leader := &fakeKVClient{
		get: func(_ context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
			return &kvpb.GetResponse{Value: []byte("v1"), Found: true}, nil
		},
	}
	r := NewRouter(meta, dialerFor(map[string]kvpb.KVServiceClient{"leader-addr:9001": leader}))

	resp, err := r.Get(context.Background(), &kvpb.GetRequest{Key: []byte("a")})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.GetFound() || string(resp.GetValue()) != "v1" {
		t.Fatalf("resp = %+v, want found v1", resp)
	}
	if meta.calls != 1 {
		t.Fatalf("metaservice calls = %d, want 1 (cache miss)", meta.calls)
	}

	// Second request, same key: must be served from cache.
	if _, err := r.Get(context.Background(), &kvpb.GetRequest{Key: []byte("a")}); err != nil {
		t.Fatalf("Get (2nd): %v", err)
	}
	if meta.calls != 1 {
		t.Fatalf("metaservice calls after 2nd request = %d, want 1 (cache hit)", meta.calls)
	}
	if leader.gets != 2 {
		t.Fatalf("leader.gets = %d, want 2", leader.gets)
	}
}

// TestLeaderHintTriggersCacheUpdateAndRetry verifies a response carrying a
// non-empty leader_hint causes Router to update its cache and retry once
// against the hinted address, returning that retry's response.
func TestLeaderHintTriggersCacheUpdateAndRetry(t *testing.T) {
	meta := &fakeMetaClient{
		getShardForKey: func(_ context.Context, req *metapb.GetShardForKeyRequest) (*metapb.GetShardForKeyResponse, error) {
			return &metapb.GetShardForKeyResponse{Shard: shardDescriptor("shard-1", "old-leader:9001")}, nil
		},
	}
	oldLeader := &fakeKVClient{
		get: func(_ context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
			return &kvpb.GetResponse{LeaderHint: "new-leader:9001"}, nil
		},
	}
	newLeader := &fakeKVClient{
		get: func(_ context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
			return &kvpb.GetResponse{Value: []byte("v2"), Found: true}, nil
		},
	}
	r := NewRouter(meta, dialerFor(map[string]kvpb.KVServiceClient{
		"old-leader:9001": oldLeader,
		"new-leader:9001": newLeader,
	}))

	resp, err := r.Get(context.Background(), &kvpb.GetRequest{Key: []byte("a")})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.GetFound() || string(resp.GetValue()) != "v2" {
		t.Fatalf("resp = %+v, want the retried response (found v2)", resp)
	}
	if oldLeader.gets != 1 {
		t.Fatalf("oldLeader.gets = %d, want 1", oldLeader.gets)
	}
	if newLeader.gets != 1 {
		t.Fatalf("newLeader.gets = %d, want 1 (the retry)", newLeader.gets)
	}

	// A subsequent request for the same key should go straight to the new
	// leader — the cache should have been updated by the hint, not just
	// the one retried call.
	if _, err := r.Get(context.Background(), &kvpb.GetRequest{Key: []byte("a")}); err != nil {
		t.Fatalf("Get (2nd): %v", err)
	}
	if oldLeader.gets != 1 {
		t.Fatalf("oldLeader.gets after 2nd request = %d, want still 1 (cache should route to new leader now)", oldLeader.gets)
	}
	if newLeader.gets != 2 {
		t.Fatalf("newLeader.gets after 2nd request = %d, want 2", newLeader.gets)
	}
	if meta.calls != 1 {
		t.Fatalf("metaservice calls = %d, want 1 (hint update must not re-trigger a lookup)", meta.calls)
	}
}

// TestTransportErrorTriggersForcedRefreshAndRetry verifies that when the
// cached leader address is entirely unreachable (a transport-level error,
// not a leader_hint response — what actually happens when a leader
// process is killed outright), Router bypasses its cache, re-resolves via
// metaservice, and retries once against the freshly resolved address.
func TestTransportErrorTriggersForcedRefreshAndRetry(t *testing.T) {
	callCount := 0
	meta := &fakeMetaClient{
		getShardForKey: func(_ context.Context, req *metapb.GetShardForKeyRequest) (*metapb.GetShardForKeyResponse, error) {
			callCount++
			addr := "dead-leader:9001"
			if callCount > 1 {
				addr = "live-leader:9001"
			}
			return &metapb.GetShardForKeyResponse{Shard: shardDescriptor("shard-1", addr)}, nil
		},
	}
	liveLeader := &fakeKVClient{
		get: func(_ context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
			return &kvpb.GetResponse{Value: []byte("v3"), Found: true}, nil
		},
	}
	// dead-leader:9001 has no registered fake client at all — dialing it
	// (or calling it) fails, simulating a killed process.
	r := NewRouter(meta, dialerFor(map[string]kvpb.KVServiceClient{
		"live-leader:9001": liveLeader,
	}))

	resp, err := r.Get(context.Background(), &kvpb.GetRequest{Key: []byte("a")})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !resp.GetFound() || string(resp.GetValue()) != "v3" {
		t.Fatalf("resp = %+v, want the retried response (found v3)", resp)
	}
	if callCount != 2 {
		t.Fatalf("metaservice calls = %d, want 2 (initial + forced refresh after transport failure)", callCount)
	}
}
