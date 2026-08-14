package engine

import (
	"fmt"
	"os"
	"testing"

	"github.com/SushantPotu/raft-kv-store/internal/storage/wal"
)

func mustOpen(t *testing.T, dir string, opts Options) *DiskEngine {
	t.Helper()
	e, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func TestBasicPutGetDelete(t *testing.T) {
	e := mustOpen(t, t.TempDir(), Options{})

	if _, found, err := e.Get([]byte("missing")); err != nil || found {
		t.Fatalf("Get(missing) = found=%v err=%v", found, err)
	}

	if err := e.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, found, err := e.Get([]byte("k1"))
	if err != nil || !found || string(v) != "v1" {
		t.Fatalf("Get(k1) = %q, found=%v, err=%v", v, found, err)
	}

	if err := e.Put([]byte("k1"), []byte("v2")); err != nil {
		t.Fatalf("Put overwrite: %v", err)
	}
	v, _, _ = e.Get([]byte("k1"))
	if string(v) != "v2" {
		t.Fatalf("Get(k1) after overwrite = %q, want v2", v)
	}

	if err := e.Delete([]byte("k1")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, found, _ := e.Get([]byte("k1")); found {
		t.Fatal("k1 should be gone after Delete")
	}
}

func TestScanAscendingOrder(t *testing.T) {
	e := mustOpen(t, t.TempDir(), Options{})
	keys := []string{"c", "a", "e", "b", "d"}
	for _, k := range keys {
		if err := e.Put([]byte(k), []byte("val-"+k)); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	var got []string
	if err := e.Scan(nil, nil, func(k, v []byte) bool {
		got = append(got, string(k))
		return true
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	want := []string{"a", "b", "c", "d", "e"}
	if len(got) != len(want) {
		t.Fatalf("Scan returned %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Scan order = %v, want %v", got, want)
		}
	}

	// Bounded scan.
	got = nil
	if err := e.Scan([]byte("b"), []byte("d"), func(k, v []byte) bool {
		got = append(got, string(k))
		return true
	}); err != nil {
		t.Fatalf("Scan bounded: %v", err)
	}
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("bounded Scan = %v, want [b c]", got)
	}
}

// TestCompactionPreservesLiveDataAndRemovesOldSegments puts/deletes/
// overwrites a mix of keys, forcing multiple segment rotations, then
// calls Compact and asserts (a) the live key/value set is byte-identical
// to what it was pre-compaction and (b) the old segment files are
// actually gone from disk afterward.
func TestCompactionPreservesLiveDataAndRemovesOldSegments(t *testing.T) {
	dir := t.TempDir()
	e := mustOpen(t, dir, Options{SegmentBytes: 256})

	// Build up a mix: some keys overwritten multiple times, some deleted,
	// some deleted-then-recreated, some untouched.
	for i := 0; i < 40; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		if err := e.Put(key, []byte(fmt.Sprintf("v0-%d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	for i := 0; i < 40; i += 2 {
		key := []byte(fmt.Sprintf("key-%03d", i))
		if err := e.Put(key, []byte(fmt.Sprintf("v1-%d", i))); err != nil {
			t.Fatalf("overwrite Put: %v", err)
		}
	}
	for i := 0; i < 40; i += 5 {
		key := []byte(fmt.Sprintf("key-%03d", i))
		if err := e.Delete(key); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	// key-000 was deleted above (i=0 is a multiple of 5); recreate it with
	// a new value to exercise delete-then-recreate through compaction.
	if err := e.Put([]byte("key-000"), []byte("resurrected")); err != nil {
		t.Fatalf("Put after delete: %v", err)
	}

	preCompact := snapshotLiveState(t, e)

	segsBefore, err := e.segmentFilesOnDisk()
	if err != nil {
		t.Fatalf("segmentFilesOnDisk: %v", err)
	}
	if len(segsBefore) < 2 {
		t.Fatalf("expected multiple segments before compaction (rotation), got %d", len(segsBefore))
	}

	if err := e.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	postCompact := snapshotLiveState(t, e)
	if len(preCompact) != len(postCompact) {
		t.Fatalf("key count changed by Compact: before=%d after=%d", len(preCompact), len(postCompact))
	}
	for k, v := range preCompact {
		v2, ok := postCompact[k]
		if !ok {
			t.Fatalf("key %q missing after Compact", k)
		}
		if v2 != v {
			t.Fatalf("key %q value changed by Compact: before=%q after=%q", k, v, v2)
		}
	}

	segsAfter, err := e.segmentFilesOnDisk()
	if err != nil {
		t.Fatalf("segmentFilesOnDisk: %v", err)
	}
	for _, old := range segsBefore {
		for _, cur := range segsAfter {
			if old == cur {
				t.Fatalf("old segment %s still present after Compact", old)
			}
		}
	}
}

func snapshotLiveState(t *testing.T, e *DiskEngine) map[string]string {
	t.Helper()
	out := make(map[string]string)
	if err := e.Scan(nil, nil, func(k, v []byte) bool {
		out[string(k)] = string(v)
		return true
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	return out
}

// TestRestartReopensWithAllData writes data, closes the engine, reopens it
// pointing at the same directory, and verifies every key/value is still
// there.
func TestRestartReopensWithAllData(t *testing.T) {
	dir := t.TempDir()
	e := Open2(t, dir)

	want := map[string]string{}
	for i := 0; i < 25; i++ {
		k := fmt.Sprintf("k%d", i)
		v := fmt.Sprintf("v%d", i)
		if err := e.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Put: %v", err)
		}
		want[k] = v
	}
	if err := e.Delete([]byte("k3")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	delete(want, "k3")

	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	e2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()

	got := snapshotLiveState(t, e2)
	if len(got) != len(want) {
		t.Fatalf("after restart: got %d keys, want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("after restart: key %q = %q, want %q", k, got[k], v)
		}
	}
	if _, found, _ := e2.Get([]byte("k3")); found {
		t.Fatal("deleted key k3 resurrected after restart")
	}
}

func Open2(t *testing.T, dir string) *DiskEngine {
	t.Helper()
	e, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return e
}

// TestSnapshotAllRestoreAllRoundTrip exercises the full-keyspace
// dump/restore path used by Raft's InstallSnapshot flow.
func TestSnapshotAllRestoreAllRoundTrip(t *testing.T) {
	e := mustOpen(t, t.TempDir(), Options{})
	for i := 0; i < 10; i++ {
		if err := e.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	blob, err := e.SnapshotAll()
	if err != nil {
		t.Fatalf("SnapshotAll: %v", err)
	}

	e2 := mustOpen(t, t.TempDir(), Options{})
	// Pre-existing state that should be wiped by RestoreAll.
	if err := e2.Put([]byte("stale"), []byte("x")); err != nil {
		t.Fatalf("Put stale: %v", err)
	}
	if err := e2.RestoreAll(blob); err != nil {
		t.Fatalf("RestoreAll: %v", err)
	}

	if _, found, _ := e2.Get([]byte("stale")); found {
		t.Fatal("stale key survived RestoreAll")
	}
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("k%d", i)
		v, found, err := e2.Get([]byte(k))
		if err != nil || !found || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("Get(%s) after restore = %q found=%v err=%v", k, v, found, err)
		}
	}
}

// TestOpenReplaysAcrossCrashTornSegment verifies the engine-level
// (not just wal-level) crash recovery contract: if the on-disk WAL's
// final segment is torn mid-record, Open must still succeed and expose
// every record that preceded the tear, with no panic.
func TestOpenReplaysAcrossCrashTornSegment(t *testing.T) {
	dir := t.TempDir()
	e := Open2(t, dir)
	for i := 0; i < 20; i++ {
		if err := e.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("fixed-size-value")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	segID := e.writer.ActiveSegmentID()
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := wal.SegmentPath(dir, segID, wal.SegmentExt)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	// Truncate off roughly the last third of the file to guarantee we cut
	// through the middle of some record's frame.
	truncateAt := info.Size() * 2 / 3
	if err := os.Truncate(path, truncateAt); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	e2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open after simulated crash: %v", err)
	}
	defer e2.Close()

	got := snapshotLiveState(t, e2)
	if len(got) == 0 {
		t.Fatal("expected at least some records recovered before the tear")
	}
	if len(got) >= 20 {
		t.Fatalf("expected fewer than 20 records recovered (tear should have dropped some), got %d", len(got))
	}
	// Everything recovered must be a genuine, uncorrupted prefix.
	for k, v := range got {
		if v != "fixed-size-value" {
			t.Fatalf("corrupted recovered value for %q: %q", k, v)
		}
	}
}
