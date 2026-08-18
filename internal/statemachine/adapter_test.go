package statemachine

import (
	"encoding/json"
	"testing"

	"github.com/SushantPotu/raft-kv-store/internal/storage/engine"
	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

func newTestAdapter(t *testing.T) *Adapter {
	t.Helper()
	e, err := engine.Open(t.TempDir(), engine.Options{})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return NewAdapter(e)
}

func apply(t *testing.T, a *Adapter, cmd Command) CommandResult {
	t.Helper()
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	out, err := a.Apply(raft.LogEntry{Type: raft.EntryNormal, Data: data})
	if err != nil {
		t.Fatalf("Apply(%+v): %v", cmd, err)
	}
	var res CommandResult
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	return res
}

func TestAdapterPutGet(t *testing.T) {
	a := newTestAdapter(t)

	apply(t, a, Command{RequestID: "r1", Op: OpPut, Key: []byte("foo"), Value: []byte("bar")})

	v, found := a.Get([]byte("foo"))
	if !found || string(v) != "bar" {
		t.Fatalf("Get(foo) = (%q, %v), want (bar, true)", v, found)
	}
}

func TestAdapterDelete(t *testing.T) {
	a := newTestAdapter(t)

	apply(t, a, Command{RequestID: "r1", Op: OpPut, Key: []byte("foo"), Value: []byte("bar")})
	apply(t, a, Command{RequestID: "r2", Op: OpDel, Key: []byte("foo")})

	if _, found := a.Get([]byte("foo")); found {
		t.Fatalf("Get(foo) found=true after delete, want false")
	}
}

// TestAdapterCompareAndSwap mirrors internal/server's
// TestKVServerCompareAndSwap case-for-case (create-on-absent, mismatch,
// match) to verify behavior parity between the real Adapter and the fake
// state machine those tests exercise.
func TestAdapterCompareAndSwap(t *testing.T) {
	a := newTestAdapter(t)

	// CAS against an absent key with expect_absent=true should succeed.
	res := apply(t, a, Command{RequestID: "r1", Op: OpCAS, Key: []byte("k"), ExpectAbsent: true, Value: []byte("v1")})
	if !res.Swapped {
		t.Fatalf("CAS (create) = %+v, want swapped=true", res)
	}

	// CAS with the wrong expected value should fail and report the actual value.
	res = apply(t, a, Command{RequestID: "r2", Op: OpCAS, Key: []byte("k"), ExpectedValue: []byte("wrong"), Value: []byte("v2")})
	if res.Swapped {
		t.Fatalf("CAS (mismatch) = %+v, want swapped=false", res)
	}
	if string(res.ActualValue) != "v1" {
		t.Fatalf("CAS (mismatch) actual_value = %q, want %q", res.ActualValue, "v1")
	}
	if !res.Found {
		t.Fatalf("CAS (mismatch) found=false, want true")
	}

	// CAS with the correct expected value should succeed.
	res = apply(t, a, Command{RequestID: "r3", Op: OpCAS, Key: []byte("k"), ExpectedValue: []byte("v1"), Value: []byte("v2")})
	if !res.Swapped {
		t.Fatalf("CAS (match) = %+v, want swapped=true", res)
	}

	v, found := a.Get([]byte("k"))
	if !found || string(v) != "v2" {
		t.Fatalf("final Get(k) = (%q, %v), want (v2, true)", v, found)
	}
}

func TestAdapterSnapshotRoundTrip(t *testing.T) {
	a := newTestAdapter(t)

	apply(t, a, Command{RequestID: "r1", Op: OpPut, Key: []byte("a"), Value: []byte("1")})
	apply(t, a, Command{RequestID: "r2", Op: OpPut, Key: []byte("b"), Value: []byte("2")})

	snap, err := a.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	b := newTestAdapter(t)
	if err := b.RestoreSnapshot(snap); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}

	v, found := b.Get([]byte("a"))
	if !found || string(v) != "1" {
		t.Fatalf("Get(a) after restore = (%q, %v), want (1, true)", v, found)
	}
	v, found = b.Get([]byte("b"))
	if !found || string(v) != "2" {
		t.Fatalf("Get(b) after restore = (%q, %v), want (2, true)", v, found)
	}
}
