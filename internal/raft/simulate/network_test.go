package simulate

import (
	"testing"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

func TestNetworkDeliversAfterDelay(t *testing.T) {
	n := NewNetwork(1)
	n.SetFaultProfile(0, 2, 2) // fixed 2-tick delay, no drops

	n.Send("a", "b", "shard-1", raft.Message{To: "b", Kind: raft.MsgAppendEntries})

	if envs := n.Tick(); len(envs) != 0 {
		t.Fatalf("tick 1: expected no delivery yet, got %d", len(envs))
	}
	if envs := n.Tick(); len(envs) != 1 {
		t.Fatalf("tick 2: expected exactly 1 delivery, got %d", len(envs))
	}
}

func TestNetworkPartitionBlocksDelivery(t *testing.T) {
	n := NewNetwork(2)
	n.SetFaultProfile(0, 1, 1)
	n.Partition("a", "b")

	n.Send("a", "b", "shard-1", raft.Message{To: "b"})
	if envs := n.Tick(); len(envs) != 0 {
		t.Fatalf("expected partitioned send to be dropped, got %d delivered", len(envs))
	}

	n.Heal("a", "b")
	n.Send("a", "b", "shard-1", raft.Message{To: "b"})
	if envs := n.Tick(); len(envs) != 1 {
		t.Fatalf("expected healed partition to allow delivery, got %d", len(envs))
	}
}

func TestNetworkDropRateZeroNeverDrops(t *testing.T) {
	n := NewNetwork(3)
	n.SetFaultProfile(0, 0, 0)
	for i := 0; i < 50; i++ {
		n.Send("a", "b", "shard-1", raft.Message{To: "b"})
	}
	if envs := n.Tick(); len(envs) != 50 {
		t.Fatalf("expected all 50 messages delivered with zero drop rate, got %d", len(envs))
	}
}
