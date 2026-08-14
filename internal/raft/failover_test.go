package raftcore

import (
	"context"
	"fmt"
	"testing"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// TestLeaderKillReelectsWithoutLosingCommits commits some entries, kills
// the leader, and asserts (a) a new leader gets elected among the
// survivors and (b) every previously committed entry is still present, in
// order, in the remaining nodes' applied logs — the durability half of
// Raft's safety guarantee (paper §5.4.3, the Leader Completeness
// Property).
func TestLeaderKillReelectsWithoutLosingCommits(t *testing.T) {
	h := newTestHarness(t, 5, 7)

	firstLeader := electLeader(t, h, 2000)

	const n = 10
	var committed []string
	for i := 0; i < n; i++ {
		cmd := fmt.Sprintf("pre-kill-%02d", i)
		committed = append(committed, cmd)
		if err := h.nodes[firstLeader].Propose(context.Background(), []byte(cmd)); err != nil {
			t.Fatalf("Propose(%q): %v", cmd, err)
		}
	}
	h.run(200) // let it fully replicate & commit everywhere

	for _, id := range h.ids {
		got := stripNoops(h.sms[id].commands())
		if !equalStringSlices(got, committed) {
			t.Fatalf("before kill: node %s applied = %v, want %v", id, got, committed)
		}
	}

	h.kill(firstLeader)

	// Elect a new leader among the 4 survivors.
	var newLeader raft.NodeID
	found := false
	for i := 0; i < 2000; i++ {
		h.step()
		leaders := h.leaders()
		if len(leaders) > 1 {
			t.Fatalf("safety violation: %d simultaneous leaders after killing %s: %+v", len(leaders), firstLeader, leaders)
		}
		if len(leaders) == 1 {
			newLeader = leaders[0].ID
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("no new leader elected among survivors within 2000 rounds after killing %s", firstLeader)
	}
	if newLeader == firstLeader {
		t.Fatalf("new leader is the killed node %s, should be impossible", firstLeader)
	}

	// Propose a bit more through the new leader and let it settle.
	for i := 0; i < 5; i++ {
		cmd := fmt.Sprintf("post-kill-%02d", i)
		committed = append(committed, cmd)
		if err := h.nodes[newLeader].Propose(context.Background(), []byte(cmd)); err != nil {
			t.Fatalf("Propose(%q) via new leader %s: %v", cmd, newLeader, err)
		}
	}
	h.run(300)

	for _, id := range h.alive() {
		got := stripNoops(h.sms[id].commands())
		if len(got) < len(committed) {
			t.Fatalf("node %s lost committed entries: applied = %v, want at least %v", id, got, committed)
		}
		if !equalStringSlices(got[:len(committed)], committed) {
			t.Fatalf("node %s applied = %v, want prefix %v", id, got, committed)
		}
	}
}
