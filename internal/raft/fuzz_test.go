package raftcore

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// TestSafetyFuzzNeverTwoLeadersSameTerm is the "Jepsen-lite" fuzz check:
// across 100+ seeded runs it randomly injects Kill/Restart/Partition/Heal
// and occasional proposals into a 5-node cluster, and after every single
// round asserts Raft's single most important safety property — no two
// live nodes ever simultaneously claim leadership for the same term
// (paper §5.2's Election Safety property; two leaders in the *same* term
// would mean split-brain, which would be a correctness bug, not just a
// liveness hiccup).
//
// Deliberately NOT asserted: "at most one leader overall, across any
// term." A leader that gets network-*partitioned* away (as opposed to
// killed) has no way to notice its own isolation without the
// CheckQuorum/PreVote extensions to the base algorithm (out of scope for
// this workstream — see the final report), so it will happily go on
// believing itself leader in its old term while the reachable majority
// times out and elects a successor in a higher term. That's expected,
// well-understood Raft behavior (paper §8 discusses exactly this via the
// ReadIndex protocol, itself out of scope here) — not a safety violation,
// since a stale leader that can't reach a majority can never get anything
// new committed. Only a same-term collision is a genuine bug.
//
// It also asserts that whatever gets committed never gets lost or
// reordered on any node that stays alive to the end.
func TestSafetyFuzzNeverTwoLeadersSameTerm(t *testing.T) {
	seeds := 100
	if testing.Short() {
		seeds = 20
	}
	const nodes = 5
	const rounds = 4000

	for seed := int64(0); seed < int64(seeds); seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			h := newTestHarness(t, nodes, seed)
			fuzzRng := rand.New(rand.NewSource(seed ^ 0x5a5a5a5a))

			proposed := 0
			for round := 0; round < rounds; round++ {
				injectRandomFault(h, fuzzRng, nodes)

				// Occasionally propose through whoever the fuzzer
				// believes is leader; errors (not leader, etc.) are
				// expected and ignored — this is about safety under
				// chaos, not making every proposal succeed.
				if fuzzRng.Intn(5) == 0 {
					if leaders := h.leaders(); len(leaders) == 1 {
						cmd := fmt.Sprintf("fuzz-%d-%d", seed, proposed)
						if err := h.nodes[leaders[0].ID].Propose(context.Background(), []byte(cmd)); err == nil {
							proposed++
						}
					}
				}

				h.step()

				assertNoSameTermLeaderSplit(t, h, seed, round)
			}

			// Heal everything and revive everyone so the cluster has a
			// chance to reconverge, then check no committed entry was
			// lost or reordered on any node that's alive at the end.
			h.network.HealAll()
			for _, id := range h.ids {
				h.restart(id)
			}
			h.run(2000)
			assertNoSameTermLeaderSplit(t, h, seed, rounds)

			assertNoDivergence(t, h)
		})
	}
}

// injectRandomFault applies zero or one random chaos action per round:
// killing/restarting a random node, or partitioning/healing a random pair.
// Kept deliberately mild in frequency (checked via the modulus below) so
// the cluster gets a realistic chance to make progress between faults,
// which is what makes "committed entries are never lost" a meaningful
// assertion rather than a vacuous one.
func injectRandomFault(h *testHarness, r *rand.Rand, nodes int) {
	switch r.Intn(8) {
	case 0:
		id := h.ids[r.Intn(nodes)]
		h.kill(id)
	case 1:
		id := h.ids[r.Intn(nodes)]
		h.restart(id)
	case 2:
		a, b := h.ids[r.Intn(nodes)], h.ids[r.Intn(nodes)]
		if a != b {
			h.network.Partition(a, b)
		}
	case 3:
		a, b := h.ids[r.Intn(nodes)], h.ids[r.Intn(nodes)]
		if a != b {
			h.network.Heal(a, b)
		}
	default:
		// no-op this round
	}
}

// assertNoSameTermLeaderSplit checks Raft's Election Safety property
// (paper §5.2): among live nodes, no two ever simultaneously claim
// leadership for the *same* term. See the TestSafetyFuzzNeverTwoLeadersSameTerm
// doc comment for why two leaders in *different* terms is deliberately not
// treated as a violation here.
func assertNoSameTermLeaderSplit(t *testing.T, h *testHarness, seed int64, round int) {
	t.Helper()
	leaders := h.leaders()
	if len(leaders) <= 1 {
		return
	}
	termSeen := make(map[raft.Term][]raft.NodeID)
	for _, st := range leaders {
		termSeen[st.Term] = append(termSeen[st.Term], st.ID)
	}
	for term, ids := range termSeen {
		if len(ids) > 1 {
			t.Fatalf("seed %d round %d: SAFETY VIOLATION: %d nodes (%v) simultaneously claim leadership in the SAME term %d", seed, round, len(ids), ids, term)
		}
	}
}

// assertNoDivergence checks that among nodes still alive at the end of
// the run, every applied command sequence is a prefix of the longest one
// — i.e. nothing committed was ever lost or reordered, even after all the
// chaos injectRandomFault threw at the cluster.
func assertNoDivergence(t *testing.T, h *testHarness) {
	t.Helper()
	alive := h.alive()
	var longest []string
	for _, id := range alive {
		got := stripNoops(h.sms[id].commands())
		if len(got) > len(longest) {
			longest = got
		}
	}
	for _, id := range alive {
		got := stripNoops(h.sms[id].commands())
		if !equalStringSlices(got, longest[:len(got)]) {
			t.Fatalf("node %s diverged from the majority applied sequence: got %v, want a prefix of %v", id, got, longest)
		}
	}
}
