package raftcore

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// TestSnapshotCompactsPastThreshold proves the size-based trigger: once a
// leader's applied-but-uncompacted log grows past WithSnapshotThreshold,
// Node snapshots and Storage.FirstIndex() advances past 1.
func TestSnapshotCompactsPastThreshold(t *testing.T) {
	h := newTestHarness(t, 3, 11, WithSnapshotThreshold(5))
	leader := electLeader(t, h, 2000)

	for i := 0; i < 25; i++ {
		cmd := fmt.Sprintf("cmd-%02d", i)
		if err := h.nodes[leader].Propose(context.Background(), []byte(cmd)); err != nil {
			t.Fatalf("Propose(%q): %v", cmd, err)
		}
		h.step()
	}
	h.run(300)

	first, err := h.storage[leader].FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex: %v", err)
	}
	if first <= 1 {
		t.Fatalf("expected leader %s to have compacted its log, FirstIndex = %d (want > 1)", leader, first)
	}

	snap, err := h.storage[leader].Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.LastIncludedIndex+1 != first {
		t.Fatalf("snapshot boundary %d doesn't match FirstIndex-1 %d", snap.LastIncludedIndex, first-1)
	}

	// Every live node must still agree on the applied command sequence —
	// compacting the leader's own log must never change what it (or
	// anyone else) has already applied.
	want := stripNoops(h.sms[leader].commands())
	for _, id := range h.ids {
		got := stripNoops(h.sms[id].commands())
		if !equalStringSlices(got, want) {
			t.Fatalf("node %s applied = %v, want %v", id, got, want)
		}
	}
}

// TestPartitionedFollowerCatchesUpViaInstallSnapshot partitions a follower
// away long enough that the leader compacts its log past what the follower
// still needs, then heals the partition and asserts the follower catches up
// via InstallSnapshot (rather than getting stuck retrying AppendEntries
// forever) and converges to the correct applied state.
func TestPartitionedFollowerCatchesUpViaInstallSnapshot(t *testing.T) {
	// A generously long, uniform election timeout keeps the partitioned
	// follower from spontaneously starting elections (and thereby
	// inflating its term) purely because it sits isolated for hundreds of
	// simulated ticks while the leader accumulates enough log to compact —
	// that's the base algorithm's well-known (documented in fuzz_test.go,
	// and in Ongaro's thesis §9.6) PreVote-shaped liveness gap, not
	// something this test is about; it would otherwise let the follower
	// disrupt the healthy leader on reconnection with an inflated term
	// before InstallSnapshot even gets a chance to run.
	h := newTestHarness(t, 3, 12, WithSnapshotThreshold(5), WithElectionTimeoutTicks(1000, 1200))
	leader := electLeader(t, h, 3000)

	var follower raft.NodeID
	for _, id := range h.ids {
		if id != leader {
			follower = id
			break
		}
	}
	for _, id := range h.ids {
		if id != follower {
			h.network.Partition(follower, id)
		}
	}

	const n = 40
	var want []string
	for i := 0; i < n; i++ {
		cmd := fmt.Sprintf("cmd-%03d", i)
		want = append(want, cmd)
		if err := h.nodes[leader].Propose(context.Background(), []byte(cmd)); err != nil {
			t.Fatalf("Propose(%q): %v", cmd, err)
		}
		h.step()
	}
	// Let the reachable majority (leader + the other follower) commit and
	// the leader compact well past what the partitioned follower has.
	h.run(500)

	first, err := h.storage[leader].FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex: %v", err)
	}
	followerLast, err := h.storage[follower].LastIndex()
	if err != nil {
		t.Fatalf("follower LastIndex: %v", err)
	}
	if first <= followerLast+1 {
		t.Fatalf("test setup didn't actually get the leader ahead of the follower's needs: leader FirstIndex=%d, follower LastIndex=%d", first, followerLast)
	}

	for _, id := range h.ids {
		if id != follower {
			h.network.Heal(follower, id)
		}
	}
	h.run(500)

	for _, id := range h.ids {
		got := stripNoops(h.sms[id].commands())
		if !equalStringSlices(got, want) {
			t.Fatalf("node %s applied = %v, want %v", id, got, want)
		}
	}

	followerFirst, err := h.storage[follower].FirstIndex()
	if err != nil {
		t.Fatalf("follower FirstIndex: %v", err)
	}
	if followerFirst <= 1 {
		t.Fatalf("follower %s never actually received a snapshot (FirstIndex = %d)", follower, followerFirst)
	}
}

// TestRestartAfterInstallSnapshotResumes proves that a node which received
// a snapshot (installing it into its raft.Storage), and is then restarted —
// modeled here by constructing a brand-new raftcore.Node against the very
// same rafttest.FakeStorage instance, exactly as NewNode's doc comment
// describes recovery working — resumes correctly: its in-memory log view
// picks up at the snapshot boundary (not from index 1), and it goes on to
// participate correctly in the cluster (voting, replicating further
// entries) afterward.
func TestRestartAfterInstallSnapshotResumes(t *testing.T) {
	// See TestPartitionedFollowerCatchesUpViaInstallSnapshot's comment on
	// why a long, uniform election timeout is used here.
	h := newTestHarness(t, 3, 13, WithSnapshotThreshold(5), WithElectionTimeoutTicks(1000, 1200))
	leader := electLeader(t, h, 3000)

	var restarted raft.NodeID
	for _, id := range h.ids {
		if id != leader {
			restarted = id
			break
		}
	}
	for _, id := range h.ids {
		if id != restarted {
			h.network.Partition(restarted, id)
		}
	}

	var want []string
	for i := 0; i < 40; i++ {
		cmd := fmt.Sprintf("pre-%03d", i)
		want = append(want, cmd)
		if err := h.nodes[leader].Propose(context.Background(), []byte(cmd)); err != nil {
			t.Fatalf("Propose(%q): %v", cmd, err)
		}
		h.step()
	}
	h.run(500)

	for _, id := range h.ids {
		if id != restarted {
			h.network.Heal(restarted, id)
		}
	}
	h.run(500) // restarted node installs the snapshot

	if first, _ := h.storage[restarted].FirstIndex(); first <= 1 {
		t.Fatalf("precondition failed: node %s never received a snapshot", restarted)
	}

	// Simulate a process restart: rebuild the Node purely from the
	// existing Storage (same instance — its persisted log/HardState/
	// snapshot survive a restart; only Node's in-memory view doesn't).
	//
	// The state machine is a separate story: per this project's own
	// architecture (see snapshot.go's package doc comment, and
	// cmd/kvnode's run(), which never calls RestoreSnapshot at startup),
	// the *engine's own* durable persistence — not Raft's snapshot
	// mechanism — is what a real restart relies on to recover already-
	// applied state; Raft Core only guarantees that whatever it doesn't
	// already know is applied (everything after the snapshot boundary)
	// gets correctly redelivered via ordinary CommittedEntries. So this
	// models the pessimistic case (only the last snapshot's data survived)
	// with a *fresh* state machine manually seeded from it, to prove that
	// redelivery-from-the-boundary half without relying on the mock engine
	// happening to already have every entry cached in memory from before
	// the simulated crash.
	restartedSM := newFakeStateMachine()
	if snap, err := h.storage[restarted].Snapshot(); err == nil && snap.Data != nil {
		if err := restartedSM.RestoreSnapshot(snap.Data); err != nil {
			t.Fatalf("RestoreSnapshot: %v", err)
		}
	}
	h.sms[restarted] = restartedSM

	var peers []raft.NodeID
	for _, id := range h.ids {
		if id != restarted {
			peers = append(peers, id)
		}
	}
	freshNode := NewNode(restarted, h.shard, peers, h.storage[restarted], nil, restartedSM,
		WithRandSource(rand.New(rand.NewSource(999))),
		WithSnapshotThreshold(5),
		WithElectionTimeoutTicks(1000, 1200),
	)
	h.nodes[restarted] = freshNode
	h.cluster.AddNode(restarted, freshNode)

	first, err := h.storage[restarted].FirstIndex()
	if err != nil {
		t.Fatalf("FirstIndex: %v", err)
	}
	if st := freshNode.Status(); st.CommitIndex < first-1 {
		t.Fatalf("restarted node's recovered commitIndex %d is behind its own snapshot boundary %d", st.CommitIndex, first-1)
	}

	// Propose more commands and make sure the restarted node keeps up.
	for i := 0; i < 10; i++ {
		cmd := fmt.Sprintf("post-%03d", i)
		want = append(want, cmd)
		leaderNow, _, ok := h.cluster.Leader()
		if !ok {
			h.run(50)
			leaderNow, _, ok = h.cluster.Leader()
			if !ok {
				t.Fatalf("no leader after restart")
			}
		}
		if err := h.nodes[leaderNow].Propose(context.Background(), []byte(cmd)); err != nil {
			t.Fatalf("Propose(%q): %v", cmd, err)
		}
		h.step()
	}
	h.run(500)

	for _, id := range h.ids {
		got := stripNoops(h.sms[id].commands())
		if !equalStringSlices(got, want) {
			t.Fatalf("node %s applied = %v, want %v", id, got, want)
		}
	}
}
