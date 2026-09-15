package enginetest

import (
	"bytes"
	"testing"
)

func TestFakeEnginePutGetDelete(t *testing.T) {
	e := NewFakeEngine()

	if _, found, _ := e.Get([]byte("k")); found {
		t.Fatal("expected key not found before Put")
	}
	if err := e.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, found, err := e.Get([]byte("k"))
	if err != nil || !found || !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("Get after Put: v=%q found=%v err=%v", v, found, err)
	}

	if err := e.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, found, _ := e.Get([]byte("k")); found {
		t.Fatal("expected key not found after Delete")
	}
}

func TestFakeEngineScanOrderAndBounds(t *testing.T) {
	e := NewFakeEngine()
	for _, k := range []string{"b", "d", "a", "c"} {
		if err := e.Put([]byte(k), []byte(k)); err != nil {
			t.Fatalf("Put(%s): %v", k, err)
		}
	}

	var got []string
	err := e.Scan([]byte("b"), []byte("d"), func(k, v []byte) bool {
		got = append(got, string(k))
		return true
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	want := []string{"b", "c"} // [b, d) excludes d
	if len(got) != len(want) {
		t.Fatalf("Scan range: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Scan order/range: got %v, want %v", got, want)
		}
	}
}

func TestFakeEngineScanEarlyStop(t *testing.T) {
	e := NewFakeEngine()
	for _, k := range []string{"a", "b", "c"} {
		_ = e.Put([]byte(k), []byte(k))
	}
	count := 0
	_ = e.Scan(nil, nil, func(k, v []byte) bool {
		count++
		return false // stop after first
	})
	if count != 1 {
		t.Fatalf("expected Scan to stop after 1 callback, got %d", count)
	}
}

func TestFakeEngineSnapshotRoundTrip(t *testing.T) {
	src := NewFakeEngine()
	_ = src.Put([]byte("k1"), []byte("v1"))
	_ = src.Put([]byte("k2"), []byte("v2"))

	snap, err := src.SnapshotAll()
	if err != nil {
		t.Fatalf("SnapshotAll: %v", err)
	}

	dst := NewFakeEngine()
	_ = dst.Put([]byte("stale"), []byte("should be wiped"))
	if err := dst.RestoreAll(snap); err != nil {
		t.Fatalf("RestoreAll: %v", err)
	}

	if _, found, _ := dst.Get([]byte("stale")); found {
		t.Fatal("expected RestoreAll to replace prior state entirely")
	}
	wantValues := map[string]string{"k1": "v1", "k2": "v2"}
	for k, want := range wantValues {
		v, found, err := dst.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
		if !found {
			t.Fatalf("expected %s to survive snapshot round trip", k)
		}
		if string(v) != want {
			t.Fatalf("Get(%s) = %q, want %q", k, v, want)
		}
	}
}
