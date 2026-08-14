package raftcore

import (
	"context"
	"fmt"
	"testing"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// TestMinorityPartitionNeverElectsMajorityKeepsOperating splits a 5-node
// cluster into a 2-node minority and a 3-node majority, and asserts the
// minority can never elect a leader (it can't reach a majority of the
// *whole* cluster) while the majority continues to elect a leader and
// commit new proposals. After healing the partition, the whole cluster
// must reconverge on one applied log.
func TestMinorityPartitionNeverElectsMajorityKeepsOperating(t *testing.T) {
	h := newTestHarness(t, 5, 99)

	leader := electLeader(t, h, 2000)

	// Build the groups so the already-elected leader lands on the
	// majority side. A network *partition* (as opposed to a kill) gives a
	// leader no signal that it's been isolated — without the
	// CheckQuorum/PreVote extensions (out of scope for this workstream)
	// it will just keep believing itself leader in its old term forever,
	// unable to reach anyone to prove otherwise. That's expected Raft
	// behavior, not a bug (see fuzz_test.go's doc comment for more), but
	// it means a minority-side node that happened to be the pre-partition
	// leader would make the "minority never reports itself as leader"
	// assertion below fail for a reason that has nothing to do with the
	// property this test is actually meant to prove. Keeping the
	// pre-existing leader on the majority side keeps the test focused on
	// that property regardless of which node the earlier election picked.
	var minority, majority []raft.NodeID
	for _, id := range h.ids {
		if id == leader {
			majority = append(majority, id)
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, id)
		} else {
			majority = append(majority, id)
		}
	}

	partitionGroups(h, minority, majority)

	// Run forward long enough for the minority to repeatedly time out and
	// fail to win elections, and for the majority to (re-)elect if needed.
	checkNoMinorityLeaderAndFindMajorityLeader := func() raft.NodeID {
		var majLeader raft.NodeID
		for i := 0; i < 3000; i++ {
			h.step()

			for _, st := range statusesFor(h, minority) {
				if st.IsLeader {
					t.Fatalf("minority node %s claims leadership during a network partition — safety violation", st.ID)
				}
			}
			majLeaders := statusesFor(h, majority)
			var claiming []raft.Status
			for _, st := range majLeaders {
				if st.IsLeader {
					claiming = append(claiming, st)
				}
			}
			if len(claiming) > 1 {
				t.Fatalf("safety violation: %d simultaneous leaders in majority side: %+v", len(claiming), claiming)
			}
			if len(claiming) == 1 {
				majLeader = claiming[0].ID
			}
		}
		return majLeader
	}

	majLeader := checkNoMinorityLeaderAndFindMajorityLeader()
	if majLeader == "" {
		t.Fatalf("majority side never elected a leader during the partition")
	}

	// The majority side must still be able to commit new proposals while
	// partitioned.
	const n = 8
	var committed []string
	for i := 0; i < n; i++ {
		cmd := fmt.Sprintf("during-partition-%02d", i)
		committed = append(committed, cmd)
		if err := h.nodes[majLeader].Propose(context.Background(), []byte(cmd)); err != nil {
			t.Fatalf("Propose(%q) on majority leader %s: %v", cmd, majLeader, err)
		}
	}
	h.run(300)

	for _, id := range majority {
		got := stripNoops(h.sms[id].commands())
		if !containsPrefixOf(got, committed) {
			t.Fatalf("majority node %s applied = %v, want to contain %v in order", id, got, committed)
		}
	}
	for _, id := range minority {
		for _, cmd := range committed {
			for _, got := range h.sms[id].commands() {
				if got == cmd {
					t.Fatalf("minority node %s applied %q, which was only proposed during the partition — should be unreachable", id, cmd)
				}
			}
		}
	}

	// Heal and let the whole cluster reconverge.
	h.network.HealAll()
	h.run(1500)

	leaders := h.leaders()
	if len(leaders) != 1 {
		t.Fatalf("after healing: got %d leaders, want exactly 1: %+v", len(leaders), leaders)
	}

	var want []string
	for _, id := range h.ids {
		got := stripNoops(h.sms[id].commands())
		if len(got) >= len(want) {
			want = got
		}
	}
	if len(want) == 0 {
		t.Fatalf("no node applied anything after healing")
	}
	for _, id := range h.ids {
		got := stripNoops(h.sms[id].commands())
		if len(got) != len(want) || !equalStringSlices(got, want[:len(got)]) {
			t.Fatalf("after healing: node %s applied = %v, want a prefix/match of %v", id, got, want)
		}
	}
}

func partitionGroups(h *testHarness, a, b []raft.NodeID) {
	for _, x := range a {
		for _, y := range b {
			h.network.Partition(x, y)
		}
	}
}

func statusesFor(h *testHarness, ids []raft.NodeID) []raft.Status {
	var out []raft.Status
	for _, id := range ids {
		out = append(out, h.nodes[id].Status())
	}
	return out
}

// containsPrefixOf reports whether want (in order, though not necessarily
// contiguous with respect to interleaved no-ops already stripped) appears
// as a subsequence-suffix of got — specifically here, that every element
// of want appears in got in the same relative order starting from
// wherever want's first element is found. This tolerates got having
// additional earlier entries (e.g. commands committed before the
// partition test phase began, if any).
func containsPrefixOf(got, want []string) bool {
	gi := 0
	for _, w := range want {
		found := false
		for ; gi < len(got); gi++ {
			if got[gi] == w {
				found = true
				gi++
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
