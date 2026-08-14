package raftcore

import (
	"context"
	"fmt"
	"testing"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// electLeader runs the cluster until exactly one node claims leadership,
// failing the test if that doesn't happen within maxRounds. Returns the
// leader's NodeID.
func electLeader(t testing.TB, h *testHarness, maxRounds int) raft.NodeID {
	t.Helper()
	for i := 0; i < maxRounds; i++ {
		h.step()
		if leaders := h.leaders(); len(leaders) == 1 {
			return leaders[0].ID
		} else if len(leaders) > 1 {
			t.Fatalf("safety violation: %d simultaneous leaders after %d rounds: %+v", len(leaders), i+1, leaders)
		}
	}
	t.Fatalf("no leader elected within %d rounds", maxRounds)
	return ""
}

// TestCleanReplicationConverges proposes several entries via the leader
// and asserts every live node's state machine converges to the same
// applied command sequence.
func TestCleanReplicationConverges(t *testing.T) {
	h := newTestHarness(t, 5, 42)

	leader := electLeader(t, h, 2000)

	const n = 20
	var want []string
	for i := 0; i < n; i++ {
		cmd := fmt.Sprintf("cmd-%03d", i)
		want = append(want, cmd)
		if err := h.nodes[leader].Propose(context.Background(), []byte(cmd)); err != nil {
			t.Fatalf("Propose(%q): %v", cmd, err)
		}
		// A couple of rounds between proposals so replication has a
		// chance to make progress incrementally, closer to how a real
		// client workload would look; not strictly necessary since
		// broadcastAppendEntriesLocked pipelines regardless.
		h.step()
	}

	// Run forward long enough for replication + commit + apply to settle
	// across every follower.
	h.run(500)

	for _, id := range h.ids {
		got := stripNoops(h.sms[id].commands())
		if !equalStringSlices(got, want) {
			t.Fatalf("node %s applied commands = %v, want %v", id, got, want)
		}
	}
}

// stripNoops removes the empty-Data entries produced by the no-op every
// new leader appends on election (state.go's becomeLeaderLocked), leaving
// only the client-visible command sequence to compare against.
func stripNoops(cmds []string) []string {
	var out []string
	for _, c := range cmds {
		if c == "" {
			continue
		}
		out = append(out, c)
	}
	return out
}
