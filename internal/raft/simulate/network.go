// Package simulate provides a deterministic, in-process test harness for
// Raft Core (Workstream B): a fake network with configurable message
// drop/delay/partition, and a fake clock advanced only by explicit Tick()
// calls. Raft Core's correctness tests (leader election, replication,
// partition safety) run entirely through this harness — no real disk,
// network, or goroutine scheduling nondeterminism involved, which is what
// makes randomized-seed fuzz runs reproducible.
package simulate

import (
	"math/rand"
	"sync"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// Envelope is one in-flight message between two simulated nodes.
type Envelope struct {
	From, To raft.NodeID
	Shard    raft.ShardID
	Msg      raft.Message
	// DeliverAtTick is when this envelope becomes eligible for delivery;
	// lets tests model network delay in logical-clock terms.
	DeliverAtTick uint64
}

// Network is a fake, fully-controllable transport shared by every
// simulated node in one test run. It is not a raft.Transport itself —
// each simulated node wraps it in a small adapter scoped to that node's
// outbound sends, so Raft Core still only ever depends on raft.Transport.
type Network struct {
	mu   sync.Mutex
	rng  *rand.Rand
	tick uint64

	inboxes map[raft.NodeID][]Envelope
	// partitioned[a][b] == true means a and b cannot currently exchange
	// messages in either direction.
	partitioned map[raft.NodeID]map[raft.NodeID]bool
	dropRate    float64 // [0,1); applied per-envelope at send time
	minDelay    uint64  // ticks
	maxDelay    uint64  // ticks
}

func NewNetwork(seed int64) *Network {
	return &Network{
		rng:         rand.New(rand.NewSource(seed)),
		inboxes:     make(map[raft.NodeID][]Envelope),
		partitioned: make(map[raft.NodeID]map[raft.NodeID]bool),
	}
}

// SetFaultProfile configures drop rate and delay range for all subsequent
// sends. Call between test phases to model e.g. "healthy network" vs.
// "lossy network" without constructing a new Network.
func (n *Network) SetFaultProfile(dropRate float64, minDelay, maxDelay uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate, n.minDelay, n.maxDelay = dropRate, minDelay, maxDelay
}

// Partition prevents a and b from exchanging messages in either direction
// until Heal is called. Used by Raft Core's minority/majority partition
// safety tests.
func (n *Network) Partition(a, b raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.setPartitioned(a, b, true)
}

func (n *Network) Heal(a, b raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.setPartitioned(a, b, false)
}

// HealAll clears every partition, e.g. simulating a network fully
// recovering after a split-brain scenario.
func (n *Network) HealAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitioned = make(map[raft.NodeID]map[raft.NodeID]bool)
}

func (n *Network) setPartitioned(a, b raft.NodeID, v bool) {
	if n.partitioned[a] == nil {
		n.partitioned[a] = make(map[raft.NodeID]bool)
	}
	if n.partitioned[b] == nil {
		n.partitioned[b] = make(map[raft.NodeID]bool)
	}
	n.partitioned[a][b] = v
	n.partitioned[b][a] = v
}

func (n *Network) isPartitioned(a, b raft.NodeID) bool {
	return n.partitioned[a] != nil && n.partitioned[a][b]
}

// Send enqueues msg for delivery to `to`, subject to the current fault
// profile and any active partition. Called by each node's transport
// adapter, never directly by Raft Core.
func (n *Network) Send(from, to raft.NodeID, shard raft.ShardID, msg raft.Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.isPartitioned(from, to) {
		return
	}
	if n.dropRate > 0 && n.rng.Float64() < n.dropRate {
		return
	}
	delay := n.minDelay
	if n.maxDelay > n.minDelay {
		delay += uint64(n.rng.Int63n(int64(n.maxDelay - n.minDelay + 1)))
	}
	n.inboxes[to] = append(n.inboxes[to], Envelope{
		From: from, To: to, Shard: shard, Msg: msg, DeliverAtTick: n.tick + delay,
	})
}

// Tick advances the logical clock by one and returns every envelope now
// eligible for delivery, removing them from their inbox. The caller
// (the test driving the simulated cluster) is responsible for calling
// each destination node's Step with the returned envelopes, then calling
// Tick() on every node's own logical clock.
func (n *Network) Tick() []Envelope {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.tick++

	var ready []Envelope
	for id, inbox := range n.inboxes {
		var remaining []Envelope
		for _, e := range inbox {
			if e.DeliverAtTick <= n.tick {
				ready = append(ready, e)
			} else {
				remaining = append(remaining, e)
			}
		}
		n.inboxes[id] = remaining
	}
	return ready
}

func (n *Network) CurrentTick() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.tick
}
