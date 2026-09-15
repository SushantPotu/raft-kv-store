package raftcore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// TestReadIndexSucceedsWithHealthyMajority proves the common case: a
// leader with a healthy majority resolves ReadIndex quickly.
func TestReadIndexSucceedsWithHealthyMajority(t *testing.T) {
	h := newTestHarness(t, 3, 21)
	leader := electLeader(t, h, 2000)
	h.run(5) // let the no-op commit so the §8 caveat is already satisfied

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- h.nodes[leader].ReadIndex(context.Background(), []byte("k"))
	}()

	// Drive the cluster forward so the confirmation heartbeat round can
	// actually be exchanged (ReadIndex blocks the calling goroutine, but
	// progress still requires this goroutine's h.step() calls).
	deadline := time.After(5 * time.Second)
	for {
		select {
		case err := <-resultCh:
			if err != nil {
				t.Fatalf("ReadIndex: %v", err)
			}
			return
		case <-deadline:
			t.Fatal("ReadIndex never resolved within the deadline")
		default:
			h.step()
		}
	}
}

// TestReadIndexOnNonLeaderFailsImmediately proves ReadIndex on a follower
// returns raft.ErrNotLeader right away, without blocking.
func TestReadIndexOnNonLeaderFailsImmediately(t *testing.T) {
	h := newTestHarness(t, 3, 22)
	leader := electLeader(t, h, 2000)

	var follower raft.NodeID
	for _, id := range h.ids {
		if id != leader {
			follower = id
			break
		}
	}

	done := make(chan error, 1)
	go func() { done <- h.nodes[follower].ReadIndex(context.Background(), nil) }()

	select {
	case err := <-done:
		if !errors.Is(err, raft.ErrNotLeader) {
			t.Fatalf("ReadIndex on follower = %v, want ErrNotLeader", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReadIndex on a non-leader blocked instead of returning immediately")
	}
}

// TestReadIndexNeverFalselySucceedsUnderMinorityPartition partitions the
// leader into a minority *after* the ReadIndex call starts, and asserts it
// never falsely reports success — it must either time out (respecting ctx)
// or simply never resolve; it must NOT return nil.
func TestReadIndexNeverFalselySucceedsUnderMinorityPartition(t *testing.T) {
	h := newTestHarness(t, 5, 23)
	leader := electLeader(t, h, 2000)
	h.run(5)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- h.nodes[leader].ReadIndex(ctx, []byte("k"))
	}()

	// Partition the leader into a minority (itself + 1 of 4 peers) right
	// after starting the call, then keep driving simulated time forward —
	// but never deliver the majority acks the leader would need.
	var others []raft.NodeID
	for _, id := range h.ids {
		if id != leader {
			others = append(others, id)
		}
	}
	for i, id := range others {
		if i >= 1 { // keep exactly one peer reachable — still a minority (2 of 5)
			h.network.Partition(leader, id)
		}
	}

	timeout := time.After(3 * time.Second)
	for {
		select {
		case err := <-resultCh:
			if err == nil {
				t.Fatal("SAFETY VIOLATION: ReadIndex returned success while the leader was in a minority partition")
			}
			// Any non-nil error (ctx.Err() from the deadline above, or
			// ErrNotLeader if it happened to lose leadership) is
			// acceptable — the only unacceptable outcome is nil.
			return
		case <-timeout:
			t.Fatal("ReadIndex neither resolved nor respected its context deadline")
		default:
			h.step()
		}
	}
}

// TestReadIndexWaitsForCurrentTermCommit proves the paper §8 caveat: a
// freshly elected leader's ReadIndex must wait for its own current-term
// no-op entry to commit before resolving, rather than serving a read off
// stale, pre-election state. This is exercised indirectly (Raft Core
// always appends and immediately tries to commit a no-op on election, so
// under a healthy majority the wait is normally invisible) by checking
// that ReadIndex issued in the very first round after leadership is won —
// before any Ready/Advance has had a chance to run — still only resolves
// once that commit has actually gone through, never earlier.
func TestReadIndexWaitsForCurrentTermCommit(t *testing.T) {
	h := newTestHarness(t, 3, 24)
	leader := electLeader(t, h, 2000)

	// electLeader already ran h.step() in a loop, which (for a healthy
	// majority) is normally enough for the no-op to commit too. To
	// actually exercise the "not yet committed" branch, call ReadIndex
	// before handing the cluster any further chance to exchange the
	// acks the no-op's commit needs, and confirm it still only resolves
	// once that has happened rather than immediately.
	n := h.nodes[leader].(*Node)
	n.mu.Lock()
	hasCommit := n.hasCommittedCurrentTermEntryLocked()
	n.mu.Unlock()

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- h.nodes[leader].ReadIndex(context.Background(), []byte("k"))
	}()

	select {
	case err := <-resultCh:
		if !hasCommit {
			t.Fatalf("ReadIndex resolved (err=%v) before this leader's current-term entry had committed", err)
		}
		if err != nil {
			t.Fatalf("ReadIndex: %v", err)
		}
	case <-time.After(20 * time.Millisecond):
		// Didn't resolve instantly — acceptable (and expected if hasCommit
		// was false); drive the cluster forward and confirm it does
		// eventually resolve successfully once the commit lands.
		deadline := time.After(5 * time.Second)
		for {
			select {
			case err := <-resultCh:
				if err != nil {
					t.Fatalf("ReadIndex: %v", err)
				}
				return
			case <-deadline:
				t.Fatal("ReadIndex never resolved")
			default:
				h.step()
			}
		}
	}
}
