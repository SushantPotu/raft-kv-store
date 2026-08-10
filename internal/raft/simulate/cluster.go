package simulate

import (
	"context"
	"fmt"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// Cluster drives a set of raft.Node instances against a shared Network,
// advancing logical time and delivering messages only when the test tells
// it to. This is the harness Workstream B's correctness tests (leader
// election, replication, partition safety, "Jepsen-lite" fuzzing) are
// built on top of.
//
// Cluster is deliberately generic over raft.Node: it doesn't construct
// nodes itself (Raft Core's real constructor doesn't exist yet at Phase 0)
// — callers pass in already-constructed nodes wired to a per-node
// transport adapter that forwards through Network.Send.
type Cluster struct {
	Network *Network
	Shard   raft.ShardID
	nodes   map[raft.NodeID]raft.Node
	killed  map[raft.NodeID]bool
}

func NewCluster(shard raft.ShardID, network *Network) *Cluster {
	return &Cluster{
		Network: network,
		Shard:   shard,
		nodes:   make(map[raft.NodeID]raft.Node),
		killed:  make(map[raft.NodeID]bool),
	}
}

func (c *Cluster) AddNode(id raft.NodeID, node raft.Node) {
	c.nodes[id] = node
}

// Kill stops delivering ticks/messages to a node without removing it,
// modeling a crashed-but-not-yet-restarted process.
func (c *Cluster) Kill(id raft.NodeID) { c.killed[id] = true }

// Restart resumes ticking/delivering to a previously killed node. The
// node's own raft.Node implementation is responsible for recovering its
// state from its raft.Storage, exactly as it would after a real process
// restart.
func (c *Cluster) Restart(id raft.NodeID) { delete(c.killed, id) }

// Step advances one logical round: every live node's clock ticks once,
// then every message the Network says is now deliverable gets handed to
// its destination's Step, and every resulting Ready is drained (Advance()
// called) so the node makes progress each round. Returns an error only if
// a node's Step/Ready handling returns one — test code decides whether
// that's fatal.
func (c *Cluster) Step(ctx context.Context) error {
	for id, n := range c.nodes {
		if c.killed[id] {
			continue
		}
		n.Tick()
	}

	for _, env := range c.Network.Tick() {
		if c.killed[env.To] {
			continue
		}
		n, ok := c.nodes[env.To]
		if !ok {
			continue
		}
		inbound := raft.InboundMessage{From: env.From, Shard: env.Shard, Kind: env.Msg.Kind, Payload: env.Msg.Payload}
		if err := n.Step(ctx, inbound); err != nil {
			return fmt.Errorf("simulate: node %s Step: %w", env.To, err)
		}
	}

	for id, n := range c.nodes {
		if c.killed[id] {
			continue
		}
		c.drainReady(id, n)
	}
	return nil
}

// drainReady pulls any currently-buffered Ready off the node without
// blocking, persists nothing itself (that's the real shard.Manager's job —
// here we just forward outbound Messages onto the Network so the
// simulation progresses), and calls Advance().
func (c *Cluster) drainReady(id raft.NodeID, n raft.Node) {
	select {
	case rd := <-n.Ready():
		for _, msg := range rd.Messages {
			c.Network.Send(id, msg.To, c.Shard, msg)
		}
		n.Advance()
	default:
	}
}

// Run steps the cluster n times.
func (c *Cluster) Run(ctx context.Context, rounds int) error {
	for i := 0; i < rounds; i++ {
		if err := c.Step(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Leader returns the NodeID of whichever live node currently believes
// itself to be leader, provided exactly one does. If zero nodes claim
// leadership (mid-election) it returns ok=false; if more than one does
// (a safety violation — two leaders in the same or different terms) it
// also returns ok=false but the caller should treat that case as a test
// failure, not as "no leader yet" — check ambiguous separately if that
// distinction matters.
func (c *Cluster) Leader() (id raft.NodeID, term raft.Term, ok bool) {
	var found []raft.Status
	for nodeID, n := range c.nodes {
		if c.killed[nodeID] {
			continue
		}
		if st := n.Status(); st.IsLeader {
			found = append(found, st)
		}
	}
	if len(found) != 1 {
		return "", 0, false
	}
	return found[0].ID, found[0].Term, true
}

// Leaders returns the Status of every live node that currently believes
// itself to be leader. Tests use this directly to assert "at most one"
// rather than relying on Leader()'s ok=false conflating "none" with "more
// than one."
func (c *Cluster) Leaders() []raft.Status {
	var found []raft.Status
	for nodeID, n := range c.nodes {
		if c.killed[nodeID] {
			continue
		}
		if st := n.Status(); st.IsLeader {
			found = append(found, st)
		}
	}
	return found
}
