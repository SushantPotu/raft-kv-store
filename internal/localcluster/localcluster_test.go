package localcluster

import (
	"context"
	"testing"
	"time"

	"github.com/SushantPotu/raft-kv-store/pkg/kvpb"
)

func TestClusterElectsAndServesReadsWrites(t *testing.T) {
	c, err := Start(Config{NumNodes: 3, TickInterval: 5 * time.Millisecond, ReadyPollInterval: 2 * time.Millisecond})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(c.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	leader, err := c.AwaitHealthy(ctx)
	if err != nil {
		t.Fatalf("AwaitHealthy: %v", err)
	}
	t.Logf("initial leader: %s", leader)

	if _, err := c.Put(ctx, []byte("foo"), []byte("bar"), leader); err != nil {
		t.Fatalf("Put: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var found bool
	var value []byte
	for time.Now().Before(deadline) {
		value, found, err = c.Get(ctx, leader, []byte("foo"), kvpb.Consistency_CONSISTENCY_STALE)
		if err == nil && found {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !found || string(value) != "bar" {
		for _, id := range c.NodeIDs() {
			st, _ := c.Status(id)
			t.Logf("status %s: %+v", id, st)
		}
		t.Fatalf("Get(foo) = (%q, %v, %v), want (bar, true, nil)", value, found, err)
	}
}

func TestClusterSurvivesLeaderKillAndRestart(t *testing.T) {
	c, err := Start(Config{NumNodes: 3, TickInterval: 5 * time.Millisecond, ReadyPollInterval: 2 * time.Millisecond})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(c.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	leader, err := c.AwaitHealthy(ctx)
	if err != nil {
		t.Fatalf("AwaitHealthy: %v", err)
	}

	killed := leader
	t0 := time.Now()
	if err := c.Kill(killed); err != nil {
		t.Fatalf("Kill(%s): %v", killed, err)
	}

	newLeader, err := c.Put(ctx, []byte("after-kill"), []byte("v1"), roundRobinNext(c.NodeIDs(), killed))
	if err != nil {
		t.Fatalf("Put after killing leader: %v", err)
	}
	failoverTime := time.Since(t0)
	t.Logf("killed=%s newLeader=%s failoverTime=%s", killed, newLeader, failoverTime)
	if newLeader == killed {
		t.Fatalf("write succeeded against the killed node %s — impossible, it's dead", killed)
	}

	if err := c.Restart(killed); err != nil {
		t.Fatalf("Restart(%s): %v", killed, err)
	}
	if !c.IsAlive(killed) {
		t.Fatalf("IsAlive(%s) = false after Restart", killed)
	}

	// The restarted node should catch up and eventually serve the write
	// that happened while it was dead.
	deadline := time.Now().Add(5 * time.Second)
	var found bool
	for time.Now().Before(deadline) {
		_, found, err = c.Get(ctx, killed, []byte("after-kill"), kvpb.Consistency_CONSISTENCY_STALE)
		if err == nil && found {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !found {
		t.Fatalf("restarted node %s never caught up on the write made while it was dead", killed)
	}
}
