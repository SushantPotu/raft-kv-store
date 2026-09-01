package metaservice

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/SushantPotu/raft-kv-store/pkg/metapb"
)

// MemStore is an in-memory Store, guarded by a single mutex. It's the
// production Store implementation until Checkpoint 2 wires up DynamoDB
// (needs a live AWS account) — see this package's doc comment for why
// that swap should be a small, isolated change.
//
// Every getter returns a deep copy (via proto.Clone) so a caller mutating
// the returned message can never corrupt MemStore's own state out from
// under a concurrent reader — the same isolation a real network round trip
// to DynamoDB would give for free.
type MemStore struct {
	mu     sync.Mutex
	shards map[string]*metapb.ShardDescriptor
}

var _ Store = (*MemStore)(nil)

// NewMemStore constructs an empty MemStore.
func NewMemStore() *MemStore {
	return &MemStore{shards: make(map[string]*metapb.ShardDescriptor)}
}

func (m *MemStore) Get(_ context.Context, shardID string) (*metapb.ShardDescriptor, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.shards[shardID]
	if !ok {
		return nil, false, nil
	}
	return proto.Clone(d).(*metapb.ShardDescriptor), true, nil
}

func (m *MemStore) List(_ context.Context) ([]*metapb.ShardDescriptor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*metapb.ShardDescriptor, 0, len(m.shards))
	for _, d := range m.shards {
		out = append(out, proto.Clone(d).(*metapb.ShardDescriptor))
	}
	return out, nil
}

func (m *MemStore) Put(_ context.Context, desc *metapb.ShardDescriptor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shards[desc.GetShardId()] = proto.Clone(desc).(*metapb.ShardDescriptor)
	return nil
}

func (m *MemStore) UpsertReplica(_ context.Context, shardID string, replica *metapb.ReplicaDescriptor) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	d, ok := m.shards[shardID]
	if !ok {
		d = &metapb.ShardDescriptor{ShardId: shardID}
		m.shards[shardID] = d
	}

	replacement := proto.Clone(replica).(*metapb.ReplicaDescriptor)
	for i, r := range d.Replicas {
		if r.GetNodeId() == replica.GetNodeId() {
			d.Replicas[i] = replacement
			return nil
		}
	}
	d.Replicas = append(d.Replicas, replacement)
	return nil
}

func (m *MemStore) ConditionalUpdateLeader(_ context.Context, shardID, leaderNodeID string, term uint64) (bool, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	d, ok := m.shards[shardID]
	if !ok {
		d = &metapb.ShardDescriptor{ShardId: shardID}
		m.shards[shardID] = d
	}

	switch {
	case term > d.CurrentLeaderTerm:
		d.CurrentLeaderNodeId = leaderNodeID
		d.CurrentLeaderTerm = term
		return true, d.CurrentLeaderTerm, nil
	case term == d.CurrentLeaderTerm && leaderNodeID == d.CurrentLeaderNodeId:
		// Idempotent re-affirmation of the leader already on record.
		return true, d.CurrentLeaderTerm, nil
	default:
		// Stale (lower term) or a conflicting claim at the current term —
		// reject either way. The condition guarding this in a real
		// DynamoDB table would be:
		//   attribute_not_exists(shard_id) OR current_leader_term < :term
		return false, d.CurrentLeaderTerm, nil
	}
}
