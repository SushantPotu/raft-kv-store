// Package enginetest provides FakeEngine, a trivial in-memory
// engine.Engine with no WAL, no persistence, and no compaction. It exists
// so internal/statemachine and Raft Core can be built and tested before
// Workstream A's real engine (internal/storage/engine, WAL-backed,
// crash-recoverable) lands.
package enginetest

import (
	"bytes"
	"encoding/json"
	"sort"
	"sync"

	"github.com/SushantPotu/raft-kv-store/internal/storage/engine"
)

var _ engine.Engine = (*FakeEngine)(nil)

type FakeEngine struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewFakeEngine() *FakeEngine {
	return &FakeEngine{data: make(map[string][]byte)}
}

func (e *FakeEngine) Get(key []byte) ([]byte, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	v, ok := e.data[string(key)]
	if !ok {
		return nil, false, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true, nil
}

func (e *FakeEngine) Put(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	v := make([]byte, len(value))
	copy(v, value)
	e.data[string(key)] = v
	return nil
}

func (e *FakeEngine) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.data, string(key))
	return nil
}

func (e *FakeEngine) Scan(start, end []byte, fn func(k, v []byte) bool) error {
	e.mu.RLock()
	keys := make([]string, 0, len(e.data))
	for k := range e.data {
		keys = append(keys, k)
	}
	e.mu.RUnlock()
	sort.Strings(keys)
	for _, k := range keys {
		kb := []byte(k)
		if start != nil && bytes.Compare(kb, start) < 0 {
			continue
		}
		if end != nil && bytes.Compare(kb, end) >= 0 {
			break
		}
		e.mu.RLock()
		v := e.data[k]
		e.mu.RUnlock()
		if !fn(kb, v) {
			break
		}
	}
	return nil
}

// Compact is a no-op: there is nothing to compact without a WAL.
func (e *FakeEngine) Compact() error { return nil }

func (e *FakeEngine) SnapshotAll() ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return json.Marshal(e.data)
}

func (e *FakeEngine) RestoreAll(data []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	m := make(map[string][]byte)
	if len(data) > 0 {
		if err := json.Unmarshal(data, &m); err != nil {
			return err
		}
	}
	e.data = m
	return nil
}

func (e *FakeEngine) Close() error { return nil }
