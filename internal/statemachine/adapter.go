package statemachine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/SushantPotu/raft-kv-store/internal/storage/engine"
	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// Adapter wraps a real engine.Engine as a raft.StateMachine, decoding the
// Command wire format (see command.go) that KVServer encodes when it calls
// Node.Propose. It is the production replacement for internal/server's
// fakeStateMachine — same wire format, same CAS semantics, backed by a real
// disk-resident engine instead of an in-memory map.
type Adapter struct {
	engine engine.Engine

	// casMu serializes the read-modify-write CAS sequence (Get then
	// conditionally Put) against itself. engine.Engine's own DiskEngine
	// implementation guards each individual Get/Put call with its own
	// RWMutex, but that only makes each call atomic in isolation — it does
	// nothing to stop two goroutines from interleaving between this
	// Adapter's Get and its follow-up Put on the same key (classic
	// check-then-act race). In this deployment, Apply is only ever driven
	// from the single Ready-loop goroutine (internal/raft.Node's contract
	// serializes CommittedEntries application), so in practice CAS calls
	// never actually overlap today — but relying on that as the only thing
	// keeping CAS correct would silently break the moment anything (a
	// future multi-shard driver reusing one Adapter, a retry path, a test)
	// calls Apply concurrently. The mutex costs nothing on the (already
	// serialized) hot path and removes that latent hazard entirely.
	casMu sync.Mutex
}

var _ raft.StateMachine = (*Adapter)(nil)

// NewAdapter constructs an Adapter wrapping e.
func NewAdapter(e engine.Engine) *Adapter {
	return &Adapter{engine: e}
}

// Apply decodes a Command from entry.Data, dispatches on Op against the
// wrapped engine, and returns a JSON-encoded CommandResult — mirroring
// internal/server's fakeStateMachine.Apply exactly, so real and fake state
// machines produce identical results for identical inputs.
func (a *Adapter) Apply(entry raft.LogEntry) ([]byte, error) {
	var cmd Command
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		return nil, fmt.Errorf("statemachine: decode command: %w", err)
	}

	var res CommandResult
	switch cmd.Op {
	case OpPut:
		if err := a.engine.Put(cmd.Key, cmd.Value); err != nil {
			return nil, fmt.Errorf("statemachine: put: %w", err)
		}
	case OpDel:
		if err := a.engine.Delete(cmd.Key); err != nil {
			return nil, fmt.Errorf("statemachine: delete: %w", err)
		}
	case OpCAS:
		a.casMu.Lock()
		err := a.applyCASLocked(cmd, &res)
		a.casMu.Unlock()
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("statemachine: unknown op %q", cmd.Op)
	}

	return json.Marshal(res)
}

// applyCASLocked performs the compare-and-swap read-modify-write sequence.
// Caller must hold a.casMu.
func (a *Adapter) applyCASLocked(cmd Command, res *CommandResult) error {
	current, exists, err := a.engine.Get(cmd.Key)
	if err != nil {
		return fmt.Errorf("statemachine: cas get: %w", err)
	}

	switch {
	case cmd.ExpectAbsent:
		if !exists {
			if err := a.engine.Put(cmd.Key, cmd.Value); err != nil {
				return fmt.Errorf("statemachine: cas put: %w", err)
			}
			res.Swapped = true
		} else {
			res.ActualValue = current
			res.Found = true
		}
	case exists && bytes.Equal(current, cmd.ExpectedValue):
		if err := a.engine.Put(cmd.Key, cmd.Value); err != nil {
			return fmt.Errorf("statemachine: cas put: %w", err)
		}
		res.Swapped = true
	default:
		res.ActualValue = current
		res.Found = exists
	}
	return nil
}

// Get delegates directly to the wrapped engine, matching
// fakeStateMachine.Get's signature exactly (KVServer's localReader
// interface requires it). Unlike the in-memory fake, a real disk-backed
// engine can fail (I/O error, corrupt record, etc.) — since the interface
// this satisfies has no room for an error return, log it via the standard
// "log" package rather than silently discarding it.
func (a *Adapter) Get(key []byte) (value []byte, found bool) {
	v, ok, err := a.engine.Get(key)
	if err != nil {
		log.Printf("statemachine: Get(%q): %v", key, err)
		return nil, false
	}
	return v, ok
}

// Snapshot dumps the entire keyspace via the wrapped engine, for Raft's
// snapshot/InstallSnapshot path.
func (a *Adapter) Snapshot() ([]byte, error) {
	return a.engine.SnapshotAll()
}

// RestoreSnapshot replaces the entire keyspace with the contents of a blob
// produced by Snapshot.
func (a *Adapter) RestoreSnapshot(data []byte) error {
	return a.engine.RestoreAll(data)
}
