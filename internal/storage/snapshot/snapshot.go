// Package snapshot provides small, shared serialization helpers used by
// both snapshot consumers in this codebase:
//
//   - internal/storage/engine.Engine.SnapshotAll/RestoreAll, which dumps
//     and restores the entire KV keyspace (used by Raft's
//     InstallSnapshot path to bring a lagging/new replica up to date).
//   - internal/storage/raftlog, which persists its own raft.Snapshot
//     metadata (the compacted log boundary + ConfState) to disk so it
//     survives a process restart.
//
// The two use cases serialize different Go types, so there isn't much
// snapshot-specific logic worth sharing beyond this: both want a
// corruption-detecting envelope around an arbitrary gob-encodable value,
// and that envelope is built directly on top of
// internal/storage/wal's CRC frame format so the corruption-detection
// logic itself lives in exactly one place in the codebase rather than
// being reimplemented here.
package snapshot

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/SushantPotu/raft-kv-store/internal/storage/wal"
)

// Encode gob-encodes v and wraps it in a single CRC-checked wal frame.
func Encode(v any) ([]byte, error) {
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(v); err != nil {
		return nil, fmt.Errorf("snapshot: gob encode: %w", err)
	}
	var out bytes.Buffer
	if err := wal.WriteFrame(&out, payload.Bytes()); err != nil {
		return nil, fmt.Errorf("snapshot: write frame: %w", err)
	}
	return out.Bytes(), nil
}

// Decode reads back a value written by Encode into v (which must be a
// pointer, per encoding/gob's usual contract). It returns an error if the
// frame's CRC does not match (corrupted snapshot) or is truncated.
func Decode(data []byte, v any) error {
	payload, err := wal.ReadFrame(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("snapshot: read frame: %w", err)
	}
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(v); err != nil {
		return fmt.Errorf("snapshot: gob decode: %w", err)
	}
	return nil
}

// KV is one key/value pair as it appears inside an engine keyspace dump.
type KV struct {
	Key   []byte
	Value []byte
}
