// Package engine defines the KV storage engine interface — the "database
// internals" layer underneath the Raft state machine. A real
// implementation (Workstream A) is a Bitcask-style engine: an in-memory
// index over append-only, CRC-checked WAL segments, with background
// compaction. This file defines only the interface plus a trivial fake so
// the statemachine adapter and Raft Core can be built/tested before the
// real engine exists.
package engine

// Engine is a local, single-node key-value store. It has no knowledge of
// Raft, sharding, or replication — internal/statemachine.Adapter is the
// only caller, translating committed Raft log entries into calls here.
//
// Owned by: Storage Engine workstream (internal/storage/wal, engine, snapshot).
// Consumed by: internal/statemachine.Adapter.
type Engine interface {
	Get(key []byte) (value []byte, found bool, err error)
	Put(key, value []byte) error
	Delete(key []byte) error
	// Scan calls fn for each key in [start, end) in ascending key order.
	// fn returns false to stop the scan early.
	Scan(start, end []byte, fn func(k, v []byte) bool) error
	// Compact merges live records into fresh WAL segment(s) and removes
	// obsolete ones. Safe to call concurrently with reads/writes.
	Compact() error
	// SnapshotAll dumps the entire keyspace, used by Raft's
	// snapshot/InstallSnapshot path (Workstream E) to bring a lagging or
	// new replica up to date without replaying the full log.
	SnapshotAll() ([]byte, error)
	RestoreAll(data []byte) error
	Close() error
}
