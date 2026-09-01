package metaservice

import (
	"context"
	"testing"

	"github.com/SushantPotu/raft-kv-store/pkg/metapb"
)

func seedService(t *testing.T) *Service {
	t.Helper()
	store := NewMemStore()
	ctx := context.Background()
	if err := store.Put(ctx, &metapb.ShardDescriptor{
		ShardId:       "shard-1",
		KeyRangeStart: nil,
		KeyRangeEnd:   []byte("m"),
	}); err != nil {
		t.Fatalf("seed shard-1: %v", err)
	}
	if err := store.Put(ctx, &metapb.ShardDescriptor{
		ShardId:       "shard-2",
		KeyRangeStart: []byte("m"),
		KeyRangeEnd:   nil, // no upper bound
	}); err != nil {
		t.Fatalf("seed shard-2: %v", err)
	}
	return NewService(store)
}

func TestGetShardForKeyInclusiveStartExclusiveEnd(t *testing.T) {
	s := seedService(t)
	ctx := context.Background()

	// "m" is shard-2's inclusive start, and (not coincidentally) shard-1's
	// exclusive end — it must resolve to shard-2, never shard-1.
	resp, err := s.GetShardForKey(ctx, &metapb.GetShardForKeyRequest{Key: []byte("m")})
	if err != nil {
		t.Fatalf("GetShardForKey(m): %v", err)
	}
	if resp.GetShard().GetShardId() != "shard-2" {
		t.Fatalf("shard for key %q = %q, want shard-2", "m", resp.GetShard().GetShardId())
	}

	// Just below "m" belongs to shard-1.
	resp, err = s.GetShardForKey(ctx, &metapb.GetShardForKeyRequest{Key: []byte("lz")})
	if err != nil {
		t.Fatalf("GetShardForKey(lz): %v", err)
	}
	if resp.GetShard().GetShardId() != "shard-1" {
		t.Fatalf("shard for key %q = %q, want shard-1", "lz", resp.GetShard().GetShardId())
	}
}

func TestGetShardForKeyNoUpperBound(t *testing.T) {
	s := seedService(t)
	// shard-2 has an empty key_range_end — "no upper bound" per the proto
	// comment, so even a very large key must still resolve to it.
	resp, err := s.GetShardForKey(context.Background(), &metapb.GetShardForKeyRequest{Key: []byte("zzzzzzzzzzzzzzzz")})
	if err != nil {
		t.Fatalf("GetShardForKey: %v", err)
	}
	if resp.GetShard().GetShardId() != "shard-2" {
		t.Fatalf("shard = %q, want shard-2", resp.GetShard().GetShardId())
	}
}

func TestGetShardForKeyNoOwnerNotFound(t *testing.T) {
	store := NewMemStore()
	s := NewService(store)
	_, err := s.GetShardForKey(context.Background(), &metapb.GetShardForKeyRequest{Key: []byte("a")})
	if err == nil {
		t.Fatal("GetShardForKey with no shards on record: want error")
	}
}

func TestReportLeaderChangeRejectsStaleReport(t *testing.T) {
	s := seedService(t)
	ctx := context.Background()

	resp, err := s.ReportLeaderChange(ctx, &metapb.ReportLeaderChangeRequest{ShardId: "shard-1", NewLeaderNodeId: "node-a", Term: 5})
	if err != nil {
		t.Fatalf("ReportLeaderChange: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatal("first report at term 5 was rejected")
	}

	resp, err = s.ReportLeaderChange(ctx, &metapb.ReportLeaderChangeRequest{ShardId: "shard-1", NewLeaderNodeId: "node-stale", Term: 2})
	if err != nil {
		t.Fatalf("ReportLeaderChange: %v", err)
	}
	if resp.GetAccepted() {
		t.Fatal("stale (lower-term) report was accepted")
	}
	if resp.GetCurrentTermOnRecord() != 5 {
		t.Fatalf("CurrentTermOnRecord = %d, want 5", resp.GetCurrentTermOnRecord())
	}

	shards, err := s.ListShards(ctx, &metapb.ListShardsRequest{})
	if err != nil {
		t.Fatalf("ListShards: %v", err)
	}
	for _, d := range shards.GetShards() {
		if d.GetShardId() == "shard-1" && d.GetCurrentLeaderNodeId() != "node-a" {
			t.Fatalf("shard-1 leader = %q, want node-a (unchanged by stale report)", d.GetCurrentLeaderNodeId())
		}
	}
}

func TestRegisterReplicaThenListShards(t *testing.T) {
	s := seedService(t)
	ctx := context.Background()

	_, err := s.RegisterReplica(ctx, &metapb.RegisterReplicaRequest{
		ShardId: "shard-1",
		Replica: &metapb.ReplicaDescriptor{NodeId: "node-a", Address: "kvnode-1:9001"},
	})
	if err != nil {
		t.Fatalf("RegisterReplica: %v", err)
	}

	resp, err := s.ListShards(ctx, &metapb.ListShardsRequest{})
	if err != nil {
		t.Fatalf("ListShards: %v", err)
	}
	found := false
	for _, d := range resp.GetShards() {
		if d.GetShardId() != "shard-1" {
			continue
		}
		for _, r := range d.GetReplicas() {
			if r.GetNodeId() == "node-a" && r.GetAddress() == "kvnode-1:9001" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("registered replica not present in ListShards")
	}
}
