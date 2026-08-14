package raftcore

import "testing"

// TestSingleLeaderPerTerm runs a 5-node cluster with no faults to
// completion of an election and asserts exactly one node claims
// leadership, across 100+ seeded runs (paper §5.2's core safety property:
// at most one leader per term).
func TestSingleLeaderPerTerm(t *testing.T) {
	seeds := 100
	if testing.Short() {
		seeds = 15
	}

	for seed := int64(0); seed < int64(seeds); seed++ {
		seed := seed
		t.Run("", func(t *testing.T) {
			h := newTestHarness(t, 5, seed)

			// DefaultElectionTimeoutMaxTicks is 300; run comfortably past
			// that so an election has definitely happened even in the
			// unlucky case of a split vote needing a second round.
			h.run(2000)

			leaders := h.leaders()
			if len(leaders) != 1 {
				t.Fatalf("seed %d: got %d leaders after 2000 ticks, want exactly 1: %+v", seed, len(leaders), leaders)
			}
		})
	}
}

// TestSingleNodeClusterBecomesLeaderImmediately is a degenerate but useful
// edge case: with zero peers, a node should become its own leader without
// ever needing a vote round-trip.
func TestSingleNodeClusterBecomesLeaderImmediately(t *testing.T) {
	h := newTestHarness(t, 1, 1)
	h.run(400) // comfortably past DefaultElectionTimeoutMaxTicks

	leaders := h.leaders()
	if len(leaders) != 1 {
		t.Fatalf("got %d leaders, want 1: %+v", len(leaders), leaders)
	}
}
