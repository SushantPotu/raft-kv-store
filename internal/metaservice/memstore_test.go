package metaservice

import (
	"context"
	"testing"

	"github.com/SushantPotu/raft-kv-store/pkg/metapb"
)

func TestConditionalUpdateLeaderAcceptsNewerTerm(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()

	accepted, term, err := m.ConditionalUpdateLeader(ctx, "shard-1", "node-a", 3)
	if err != nil {
		t.Fatalf("ConditionalUpdateLeader: %v", err)
	}
	if !accepted || term != 3 {
		t.Fatalf("first report: accepted=%v term=%d, want true/3", accepted, term)
	}

	accepted, term, err = m.ConditionalUpdateLeader(ctx, "shard-1", "node-b", 5)
	if err != nil {
		t.Fatalf("ConditionalUpdateLeader: %v", err)
	}
	if !accepted || term != 5 {
		t.Fatalf("newer report: accepted=%v term=%d, want true/5", accepted, term)
	}

	d, ok, err := m.Get(ctx, "shard-1")
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if d.GetCurrentLeaderNodeId() != "node-b" || d.GetCurrentLeaderTerm() != 5 {
		t.Fatalf("record = (%q, %d), want (node-b, 5)", d.GetCurrentLeaderNodeId(), d.GetCurrentLeaderTerm())
	}
}

func TestConditionalUpdateLeaderRejectsStaleTerm(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()

	if _, _, err := m.ConditionalUpdateLeader(ctx, "shard-1", "node-b", 5); err != nil {
		t.Fatalf("ConditionalUpdateLeader: %v", err)
	}

	// A deposed leader's delayed report at a lower term must never win.
	accepted, term, err := m.ConditionalUpdateLeader(ctx, "shard-1", "node-a", 3)
	if err != nil {
		t.Fatalf("ConditionalUpdateLeader: %v", err)
	}
	if accepted {
		t.Fatal("stale report was accepted, want rejected")
	}
	if term != 5 {
		t.Fatalf("currentTermOnRecord = %d, want 5 (unchanged)", term)
	}

	d, _, err := m.Get(ctx, "shard-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if d.GetCurrentLeaderNodeId() != "node-b" {
		t.Fatalf("record leader = %q, want node-b (unchanged by stale report)", d.GetCurrentLeaderNodeId())
	}
}

func TestConditionalUpdateLeaderIdempotentSameTermSameLeader(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()

	if _, _, err := m.ConditionalUpdateLeader(ctx, "shard-1", "node-a", 3); err != nil {
		t.Fatalf("ConditionalUpdateLeader: %v", err)
	}
	accepted, term, err := m.ConditionalUpdateLeader(ctx, "shard-1", "node-a", 3)
	if err != nil {
		t.Fatalf("ConditionalUpdateLeader: %v", err)
	}
	if !accepted || term != 3 {
		t.Fatalf("idempotent re-affirmation: accepted=%v term=%d, want true/3", accepted, term)
	}
}

func TestConditionalUpdateLeaderRejectsConflictingSameTerm(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()

	if _, _, err := m.ConditionalUpdateLeader(ctx, "shard-1", "node-a", 3); err != nil {
		t.Fatalf("ConditionalUpdateLeader: %v", err)
	}
	accepted, _, err := m.ConditionalUpdateLeader(ctx, "shard-1", "node-b", 3)
	if err != nil {
		t.Fatalf("ConditionalUpdateLeader: %v", err)
	}
	if accepted {
		t.Fatal("conflicting same-term claim from a different leader was accepted, want rejected")
	}
}

func TestUpsertReplicaCreatesAndUpdatesShard(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()

	if err := m.UpsertReplica(ctx, "shard-1", &metapb.ReplicaDescriptor{NodeId: "node-a", Address: "host-a:9001"}); err != nil {
		t.Fatalf("UpsertReplica: %v", err)
	}
	if err := m.UpsertReplica(ctx, "shard-1", &metapb.ReplicaDescriptor{NodeId: "node-b", Address: "host-b:9001"}); err != nil {
		t.Fatalf("UpsertReplica: %v", err)
	}
	// Replace node-a's address (e.g. it restarted with a new address).
	if err := m.UpsertReplica(ctx, "shard-1", &metapb.ReplicaDescriptor{NodeId: "node-a", Address: "host-a-new:9001"}); err != nil {
		t.Fatalf("UpsertReplica: %v", err)
	}

	d, ok, err := m.Get(ctx, "shard-1")
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if len(d.GetReplicas()) != 2 {
		t.Fatalf("len(replicas) = %d, want 2 (no duplicate for node-a)", len(d.GetReplicas()))
	}
	for _, r := range d.GetReplicas() {
		if r.GetNodeId() == "node-a" && r.GetAddress() != "host-a-new:9001" {
			t.Fatalf("node-a address = %q, want updated address", r.GetAddress())
		}
	}
}

func TestGetUnknownShardNotFound(t *testing.T) {
	m := NewMemStore()
	_, ok, err := m.Get(context.Background(), "no-such-shard")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Fatal("Get on unknown shard reported ok=true")
	}
}

// TestGetReturnsIndependentCopy verifies mutating a returned descriptor
// can't corrupt the store's own state — important because Service hands
// these straight back to gRPC callers who have no business mutating
// MemStore's internals.
func TestGetReturnsIndependentCopy(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	if err := m.Put(ctx, &metapb.ShardDescriptor{ShardId: "shard-1", CurrentLeaderNodeId: "node-a"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	d, _, err := m.Get(ctx, "shard-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	d.CurrentLeaderNodeId = "tampered"

	d2, _, err := m.Get(ctx, "shard-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if d2.GetCurrentLeaderNodeId() != "node-a" {
		t.Fatalf("store state mutated via caller's copy: got %q", d2.GetCurrentLeaderNodeId())
	}
}
