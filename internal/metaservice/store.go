// Package metaservice implements metapb.MetadataServiceServer: the
// cluster's source of truth for "which shard owns this key, and who is its
// current leader" (see proto/metapb/meta.proto).
//
// The RPC-facing Service type (service.go) is deliberately kept thin and
// backed by a Store interface (this file) rather than talking to any
// particular backing database directly. Checkpoint 1/2 of this workstream
// only needs an in-memory Store (memstore.go) — no live AWS account is
// available yet — but production is meant to run against DynamoDB (see
// deploy/terraform/modules/dynamodb-metadata/main.tf: a single table keyed
// by shard_id, supporting conditional writes natively). Swapping in a
// DynamoDB-backed Store later should only require a new type satisfying
// this interface; Service itself, and everything upstream of it (kvrouter,
// kvnode's shard.Manager), never need to change.
package metaservice

import (
	"context"
	"errors"

	"github.com/SushantPotu/raft-kv-store/pkg/metapb"
)

// ErrShardNotFound is returned by a Store when no ShardDescriptor is on
// record for the requested shard ID.
var ErrShardNotFound = errors.New("metaservice: shard not found")

// Store is the persistence seam behind Service. Every method here maps
// onto an operation DynamoDB supports natively against a table keyed by
// shard_id (see this package's doc comment):
//
//   - Get/List are plain reads (GetItem/Scan).
//   - Put is an unconditional upsert (PutItem), used for initial shard-map
//     bootstrap/seeding — not exposed over the RPC surface at all, only used
//     by whatever seeds the shard map initially (a migration script in
//     production; tests and local rehearsal call it directly).
//   - UpsertReplica is a read-modify-write that only touches the replicas
//     list, creating the shard record (with an empty/unbounded key range)
//     if it doesn't exist yet — DynamoDB would do this with an UpdateItem
//     list-append expression; the in-memory Store just does it under a
//     lock.
//   - ConditionalUpdateLeader is the one operation that MUST be a real
//     compare-and-swap, not read-then-write from the caller's side: it maps
//     onto DynamoDB's ConditionExpression (e.g.
//     "attribute_not_exists(shard_id) OR current_leader_term < :term"),
//     which is atomic on the server side. The in-memory implementation
//     mimics that atomicity with a mutex held for the whole check-and-set.
type Store interface {
	// Get returns the ShardDescriptor on record for shardID. The second
	// return is false (with a nil descriptor and nil error) if no such
	// shard exists yet.
	Get(ctx context.Context, shardID string) (*metapb.ShardDescriptor, bool, error)
	// List returns every ShardDescriptor on record, in no particular
	// order.
	List(ctx context.Context) ([]*metapb.ShardDescriptor, error)
	// Put unconditionally creates or replaces the full ShardDescriptor for
	// desc.ShardId. Used for bootstrap/seeding (see this type's doc
	// comment) — never called from the RPC-facing Service.
	Put(ctx context.Context, desc *metapb.ShardDescriptor) error
	// UpsertReplica adds replica to shardID's replica list, replacing any
	// existing entry with the same NodeId. Creates the shard record (with
	// an empty key range — bootstrap is expected to fill that in
	// separately) if shardID has no record yet.
	UpsertReplica(ctx context.Context, shardID string, replica *metapb.ReplicaDescriptor) error
	// ConditionalUpdateLeader atomically sets shardID's current leader to
	// (leaderNodeID, term) IFF term is strictly newer than the currently
	// recorded current_leader_term (or no record exists yet, treated as
	// term 0). accepted reports whether the write happened;
	// currentTermOnRecord is whatever term ends up on record afterward
	// (the just-written term if accepted, the pre-existing term
	// otherwise) — exactly the pair ReportLeaderChangeResponse needs.
	//
	// A report at the same term as the one on record is accepted only if
	// it names the same leader (an idempotent re-affirmation, e.g. a
	// duplicate RPC after a timeout) — two different leaders claiming the
	// same term is a correctness violation Raft itself should never
	// produce, so it's rejected rather than silently picked.
	//
	// Returns ErrShardNotFound only if... it never does: an unknown
	// shardID is treated as an implicit record at term 0, so the first
	// report for a shard always succeeds and creates it (with an empty key
	// range, same as UpsertReplica) rather than erroring.
	ConditionalUpdateLeader(ctx context.Context, shardID, leaderNodeID string, term uint64) (accepted bool, currentTermOnRecord uint64, err error)
}
