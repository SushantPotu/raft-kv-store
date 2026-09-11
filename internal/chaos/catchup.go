package chaos

import (
	"context"
	"fmt"
	"time"

	"github.com/SushantPotu/raft-kv-store/pkg/kvpb"
	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// CatchUpResult is the outcome of RunRestartCatchUp.
type CatchUpResult struct {
	KilledNode raft.NodeID
	NewLeader  raft.NodeID
	CaughtUp   bool
	Elapsed    time.Duration
}

// RunRestartCatchUp is a correctness check, not a timing benchmark: it
// kills the leader, confirms the survivors elect a new one and keep
// accepting writes, restarts the killed node (reopening its same on-disk
// raft log and KV engine — see localcluster.Cluster.Restart), and polls
// until that node serves the write that happened while it was dead.
//
// This exists because RunTrial/RunTrials only ever exercise "kill and
// measure" — they never restart a node, so Kill+Restart working together
// under an actual chaos run (as opposed to internal/localcluster's own
// unit test, which is the only other place that path is exercised) would
// otherwise go unverified by this package.
func RunRestartCatchUp(ctx context.Context, newCluster Factory) (CatchUpResult, error) {
	c, err := newCluster()
	if err != nil {
		return CatchUpResult{}, fmt.Errorf("chaos: start cluster: %w", err)
	}
	defer c.Stop()

	leader, err := c.AwaitHealthy(ctx)
	if err != nil {
		return CatchUpResult{}, fmt.Errorf("chaos: await healthy: %w", err)
	}

	if err := c.Kill(leader); err != nil {
		return CatchUpResult{}, fmt.Errorf("chaos: kill %s: %w", leader, err)
	}

	key := []byte("chaos-catchup-key")
	value := []byte(fmt.Sprintf("v-%d", time.Now().UnixNano()))
	newLeader, err := c.Put(ctx, key, value, "")
	if err != nil {
		return CatchUpResult{}, fmt.Errorf("chaos: put after killing %s: %w", leader, err)
	}
	if newLeader == leader {
		return CatchUpResult{}, fmt.Errorf("chaos: write succeeded against %s, which was just killed", leader)
	}

	if err := c.Restart(leader); err != nil {
		return CatchUpResult{}, fmt.Errorf("chaos: restart %s: %w", leader, err)
	}
	if !c.IsAlive(leader) {
		return CatchUpResult{}, fmt.Errorf("chaos: %s not alive immediately after Restart", leader)
	}

	t0 := time.Now()
	var found bool
	for {
		_, found, err = c.Get(ctx, leader, key, kvpb.Consistency_CONSISTENCY_STALE)
		if err == nil && found {
			break
		}
		select {
		case <-ctx.Done():
			return CatchUpResult{KilledNode: leader, NewLeader: newLeader, CaughtUp: false, Elapsed: time.Since(t0)}, nil
		case <-time.After(20 * time.Millisecond):
		}
	}

	return CatchUpResult{
		KilledNode: leader,
		NewLeader:  newLeader,
		CaughtUp:   found,
		Elapsed:    time.Since(t0),
	}, nil
}
