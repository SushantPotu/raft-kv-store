package raftcore

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/SushantPotu/raft-kv-store/internal/raft/simulate"
	"github.com/SushantPotu/raft-kv-store/pkg/raft"
	"github.com/SushantPotu/raft-kv-store/pkg/raft/rafttest"
)

// fakeStateMachine is a trivial raft.StateMachine that just records
// applied commands in order, for tests to assert convergence against.
// It is NOT the real KV engine (that's a different workstream) — just
// enough to prove Raft Core delivers a correct, agreed-upon log to
// whatever sits behind it.
type fakeStateMachine struct {
	mu      sync.Mutex
	applied []string // one entry per applied LogEntry, in order
}

func newFakeStateMachine() *fakeStateMachine { return &fakeStateMachine{} }

func (f *fakeStateMachine) Apply(entry raft.LogEntry) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, string(entry.Data))
	return nil, nil
}

// Snapshot/RestoreSnapshot round-trip the full applied-commands slice as
// JSON, so snapshot tests can actually verify state survives a
// compact-then-InstallSnapshot (or restart-from-snapshot) cycle, rather
// than just checking Storage-level bookkeeping.
func (f *fakeStateMachine) Snapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return json.Marshal(f.applied)
}

func (f *fakeStateMachine) RestoreSnapshot(data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(data) == 0 {
		f.applied = nil
		return nil
	}
	return json.Unmarshal(data, &f.applied)
}

func (f *fakeStateMachine) commands() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.applied))
	copy(out, f.applied)
	return out
}

// equalStringSlices compares two string slices by length and contents,
// treating a nil slice and a zero-length non-nil slice as equal (unlike
// reflect.DeepEqual, which does not) — the empty-log case comes up
// naturally whenever a node hasn't applied anything yet.
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// testHarness wires up a simulate.Cluster of n raftcore Nodes, each with
// its own FakeStorage and fakeStateMachine, and drains every Ready itself
// (applying CommittedEntries to that node's state machine and persisting
// Entries/HardState into that node's FakeStorage) so tests only have to
// deal with Propose/Tick/Run and then inspect the state machines.
//
// This plays the role of internal/shard.Manager in the real system — see
// docs/adr/0001-raft-core-io-separation.md's "Ready-loop contract".
type testHarness struct {
	t       testing.TB
	shard   raft.ShardID
	network *simulate.Network
	cluster *simulate.Cluster

	ids     []raft.NodeID
	nodes   map[raft.NodeID]raft.Node
	storage map[raft.NodeID]*rafttest.FakeStorage
	sms     map[raft.NodeID]*fakeStateMachine

	// killed mirrors simulate.Cluster's own (private, so inaccessible from
	// here) killed-node bookkeeping. Kept in lockstep via kill()/restart()
	// below so this harness's own step() loop and simulate.Cluster.Leader/
	// Leaders (which do consult their own private copy) agree.
	killed map[raft.NodeID]bool
}

func newTestHarness(t testing.TB, n int, seed int64, opts ...Option) *testHarness {
	t.Helper()
	shard := raft.ShardID("shard-1")
	network := simulate.NewNetwork(seed)
	cluster := simulate.NewCluster(shard, network)

	h := &testHarness{
		t:       t,
		shard:   shard,
		network: network,
		cluster: cluster,
		nodes:   make(map[raft.NodeID]raft.Node, n),
		storage: make(map[raft.NodeID]*rafttest.FakeStorage, n),
		sms:     make(map[raft.NodeID]*fakeStateMachine, n),
		killed:  make(map[raft.NodeID]bool, n),
	}

	ids := make([]raft.NodeID, n)
	for i := 0; i < n; i++ {
		ids[i] = raft.NodeID(fmt.Sprintf("n%d", i+1))
	}
	h.ids = ids

	for i, id := range ids {
		var peers []raft.NodeID
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		st := rafttest.NewFakeStorage()
		sm := newFakeStateMachine()
		// Give every node a distinctly-seeded RNG derived from the test
		// seed so a whole run is reproducible from one seed, per node,
		// without relying on wall-clock time.
		nodeOpts := append([]Option{WithRandSource(rand.New(rand.NewSource(seed*1000 + int64(i))))}, opts...)
		node := NewNode(id, shard, peers, st, nil, sm, nodeOpts...)

		h.storage[id] = st
		h.sms[id] = sm
		h.nodes[id] = node
		cluster.AddNode(id, node)
	}

	return h
}

// drainAll processes every currently-buffered Ready for every live node:
// persists Entries/HardState into that node's FakeStorage, applies
// CommittedEntries to that node's fakeStateMachine, and forwards Messages
// onto the network — mirroring what simulate.Cluster.Step's drainReady
// does for Messages, but additionally doing the persistence/application
// halves of the Ready-loop contract that Cluster (deliberately generic
// over raft.Node) doesn't do for us.
func (h *testHarness) drainAll() {
	for _, id := range h.ids {
		if h.killed[id] {
			continue
		}
		node := h.nodes[id]
		select {
		case rd := <-node.Ready():
			st := h.storage[id]
			if len(rd.Entries) > 0 {
				if err := st.Append(rd.Entries); err != nil {
					h.t.Fatalf("node %s: Storage.Append: %v", id, err)
				}
			}
			if rd.HardState != nil {
				if err := st.SetHardState(*rd.HardState); err != nil {
					h.t.Fatalf("node %s: Storage.SetHardState: %v", id, err)
				}
			}
			for _, e := range rd.CommittedEntries {
				if e.Type == raft.EntryNormal {
					if _, err := h.sms[id].Apply(e); err != nil {
						h.t.Fatalf("node %s: StateMachine.Apply: %v", id, err)
					}
				}
			}
			for _, m := range rd.Messages {
				h.network.Send(id, m.To, h.shard, m)
			}
			if rd.Snapshot != nil {
				// Mirrors cmd/kvnode's handleReady: Node itself never calls
				// ApplySnapshot/RestoreSnapshot for a *received* snapshot —
				// that's the Ready-loop driver's job, which this harness
				// plays here.
				if err := st.ApplySnapshot(*rd.Snapshot); err != nil {
					h.t.Fatalf("node %s: Storage.ApplySnapshot: %v", id, err)
				}
				if err := h.sms[id].RestoreSnapshot(rd.Snapshot.Data); err != nil {
					h.t.Fatalf("node %s: StateMachine.RestoreSnapshot: %v", id, err)
				}
			}
			node.Advance()
		default:
		}
	}
}

// step advances the cluster by one round: every live node ticks, inbound
// messages get delivered, and every Ready gets drained via drainAll
// (instead of simulate.Cluster.Step's own drainReady, which only forwards
// Messages) so persistence/application actually happen in these tests.
func (h *testHarness) step() {
	for _, id := range h.ids {
		if h.killed[id] {
			continue
		}
		h.nodes[id].Tick()
	}
	for _, env := range h.network.Tick() {
		if h.killed[env.To] {
			continue
		}
		node, ok := h.nodes[env.To]
		if !ok {
			continue
		}
		inbound := raft.InboundMessage{From: env.From, Shard: env.Shard, Kind: env.Msg.Kind, Payload: env.Msg.Payload}
		if err := node.Step(context.Background(), inbound); err != nil {
			h.t.Fatalf("node %s Step: %v", env.To, err)
		}
	}
	h.drainAll()
}

func (h *testHarness) run(rounds int) {
	for i := 0; i < rounds; i++ {
		h.step()
	}
}

// leaders returns the Status of every live node that currently believes
// itself to be leader (simulate.Cluster.Leaders already excludes killed
// nodes using its own bookkeeping, kept in sync by kill()/restart()).
func (h *testHarness) leaders() []raft.Status {
	return h.cluster.Leaders()
}

// kill marks a node as crashed: it stops being ticked/stepped/drained by
// this harness, and simulate.Cluster.Leader(s) stops counting it too.
func (h *testHarness) kill(id raft.NodeID) {
	h.killed[id] = true
	h.cluster.Kill(id)
}

// restart resumes a previously killed node. Its raftcore.Node instance is
// unchanged (this is a simulated process pause, not a real restart that
// would reload from Storage) — exactly like simulate.Cluster.Restart's own
// doc comment describes.
func (h *testHarness) restart(id raft.NodeID) {
	delete(h.killed, id)
	h.cluster.Restart(id)
}

// addNode dynamically adds a new node to a running harness, mid-test — used
// by membership-change tests to bring up a replica beyond the harness's
// initial fixed set. peers lists every other voting member the new node
// should start out knowing about, from its own perspective (excluding
// itself); it doesn't need to already match every existing node's view,
// since AppendEntries will bring it into agreement once a leader replicates
// to it.
func (h *testHarness) addNode(id raft.NodeID, peers []raft.NodeID, seed int64, opts ...Option) raft.Node {
	st := rafttest.NewFakeStorage()
	sm := newFakeStateMachine()
	nodeOpts := append([]Option{WithRandSource(rand.New(rand.NewSource(seed)))}, opts...)
	node := NewNode(id, h.shard, peers, st, nil, sm, nodeOpts...)

	h.ids = append(h.ids, id)
	h.storage[id] = st
	h.sms[id] = sm
	h.nodes[id] = node
	h.cluster.AddNode(id, node)
	return node
}

// alive returns every node id that hasn't been killed.
func (h *testHarness) alive() []raft.NodeID {
	var out []raft.NodeID
	for _, id := range h.ids {
		if !h.killed[id] {
			out = append(out, id)
		}
	}
	return out
}
