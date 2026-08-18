// Package server implements the client-facing KVService gRPC server
// (Workstream C: Client/API Protocol Layer). See kvserver.go for the
// service implementation and stubnode.go for an important note about the
// temporary raft.Node stand-in used here.
package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/SushantPotu/raft-kv-store/internal/statemachine"
	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// fakeStateMachine is a trivial in-memory raft.StateMachine used only by
// Workstream C to prove out its own gRPC wiring end to end. It is NOT the
// real KV engine — that's internal/storage (a different, parallel
// workstream) via internal/statemachine.Adapter. Do not reach for this
// type outside of internal/server's own tests/stub wiring.
type fakeStateMachine struct {
	mu   sync.Mutex
	data map[string][]byte
}

var _ raft.StateMachine = (*fakeStateMachine)(nil)

func newFakeStateMachine() *fakeStateMachine {
	return &fakeStateMachine{data: make(map[string][]byte)}
}

// Get performs a direct, non-consensus read of local state. Used by
// kvserver for CONSISTENCY_STALE Gets, which by design bypass Node
// entirely (see proto/kvpb/kv.proto's Consistency enum doc comment).
func (f *fakeStateMachine) Get(key []byte) (value []byte, found bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[string(key)]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

// Apply decodes a command from entry.Data, mutates the map accordingly,
// and returns a JSON-encoded commandResult. This satisfies
// raft.StateMachine.Apply.
func (f *fakeStateMachine) Apply(entry raft.LogEntry) ([]byte, error) {
	var cmd statemachine.Command
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		return nil, fmt.Errorf("fakeStateMachine: decode command: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	var res statemachine.CommandResult
	switch cmd.Op {
	case statemachine.OpPut:
		f.data[string(cmd.Key)] = cmd.Value
	case statemachine.OpDel:
		delete(f.data, string(cmd.Key))
	case statemachine.OpCAS:
		current, exists := f.data[string(cmd.Key)]
		switch {
		case cmd.ExpectAbsent:
			if !exists {
				f.data[string(cmd.Key)] = cmd.Value
				res.Swapped = true
			} else {
				res.ActualValue = current
				res.Found = true
			}
		case exists && bytes.Equal(current, cmd.ExpectedValue):
			f.data[string(cmd.Key)] = cmd.Value
			res.Swapped = true
		default:
			res.ActualValue = current
			res.Found = exists
		}
	default:
		return nil, fmt.Errorf("fakeStateMachine: unknown op %q", cmd.Op)
	}

	return json.Marshal(res)
}

// Snapshot/RestoreSnapshot make fakeStateMachine a complete
// raft.StateMachine, but neither is exercised by Workstream C's own
// tests — snapshotting a fake is out of scope for what this stub exists
// to prove.
func (f *fakeStateMachine) Snapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return json.Marshal(f.data)
}

func (f *fakeStateMachine) RestoreSnapshot(data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := make(map[string][]byte)
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	f.data = m
	return nil
}
