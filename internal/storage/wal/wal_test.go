package wal

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestWriterReplayRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir, WriterOptions{Sync: SyncEveryWrite})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	want := []Record{
		{Op: OpPut, Key: []byte("a"), Value: []byte("1")},
		{Op: OpPut, Key: []byte("b"), Value: []byte("2")},
		{Op: OpDelete, Key: []byte("a")},
	}
	for _, r := range want {
		if _, _, err := w.Append(r); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, torn, err := ReplayDir(dir)
	if err != nil {
		t.Fatalf("ReplayDir: %v", err)
	}
	if torn {
		t.Fatal("expected clean (non-torn) replay")
	}
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d", len(got), len(want))
	}
	for i, r := range got {
		if r.Op != want[i].Op || string(r.Key) != string(want[i].Key) || string(r.Value) != string(want[i].Value) {
			t.Fatalf("record %d = %+v, want %+v", i, r, want[i])
		}
		if r.Seq != uint64(i) {
			t.Fatalf("record %d seq = %d, want %d", i, r.Seq, i)
		}
	}
}

func TestWriterRotatesSegments(t *testing.T) {
	dir := t.TempDir()
	// Tiny threshold forces a rotation after just a couple of records.
	w, err := OpenWriter(dir, WriterOptions{SegmentBytes: 64})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	for i := 0; i < 20; i++ {
		if _, _, err := w.Append(Record{Op: OpPut, Key: []byte("key"), Value: []byte("value-value-value")}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ids, err := ListSegmentIDs(dir, SegmentExt)
	if err != nil {
		t.Fatalf("ListSegmentIDs: %v", err)
	}
	if len(ids) < 2 {
		t.Fatalf("expected multiple segments from rotation, got %d", len(ids))
	}

	recs, torn, err := ReplayDir(dir)
	if err != nil {
		t.Fatalf("ReplayDir: %v", err)
	}
	if torn {
		t.Fatal("expected clean replay across rotated segments")
	}
	if len(recs) != 20 {
		t.Fatalf("got %d records across segments, want 20", len(recs))
	}
}

// TestCrashRecoveryTornTail simulates a crash mid-write: N valid records
// are written and fsynced, then the underlying segment file is truncated
// at a random byte offset that falls inside the final record's frame
// (never at a clean record boundary). Replay must reconstruct exactly the
// records that precede the tear, report the tear, and must not panic or
// silently fabricate data for the torn record.
func TestCrashRecoveryTornTail(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(dir, WriterOptions{Sync: SyncEveryWrite})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	const n = 50
	for i := 0; i < n; i++ {
		rec := Record{Op: OpPut, Key: []byte{byte(i)}, Value: []byte("some-value-payload")}
		if _, _, err := w.Append(rec); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	segID := w.ActiveSegmentID()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := SegmentPath(dir, segID, SegmentExt)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	fullSize := info.Size()

	// Every record here is the same encoded size, so we know exactly
	// where the last record's frame starts and can truncate at a random
	// offset strictly inside it (never at a record boundary), guaranteeing
	// we're exercising a genuine torn record rather than accidentally
	// landing on a clean boundary.
	perRecordSize := fullSize / n
	lastRecordStart := perRecordSize * (n - 1)
	rng := rand.New(rand.NewSource(1))
	// Offset somewhere in (lastRecordStart, fullSize) exclusive of the
	// exact boundary and the exact end.
	truncateAt := lastRecordStart + 1 + rng.Int63n(fullSize-lastRecordStart-1)

	if err := os.Truncate(path, truncateAt); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	recs, torn, err := ReplaySegmentFile(path)
	if err != nil {
		t.Fatalf("ReplaySegmentFile after simulated crash: %v", err)
	}
	if !torn {
		t.Fatal("expected torn=true after truncating mid-record")
	}
	if len(recs) != n-1 {
		t.Fatalf("recovered %d records, want %d (all but the torn one)", len(recs), n-1)
	}
	for i, r := range recs {
		if r.Key[0] != byte(i) {
			t.Fatalf("record %d key = %d, want %d (corrupted recovery)", i, r.Key[0], i)
		}
	}
}

// TestCrashRecoveryAcrossManyOffsets sweeps a range of truncation points
// (not just one random offset) to make sure replay never panics and always
// recovers a prefix of records regardless of exactly where the crash
// landed.
func TestCrashRecoveryAcrossManyOffsets(t *testing.T) {
	baseDir := t.TempDir()
	writeFresh := func(dir string) (path string, n int) {
		w, err := OpenWriter(dir, WriterOptions{Sync: SyncEveryWrite})
		if err != nil {
			t.Fatalf("OpenWriter: %v", err)
		}
		n = 10
		for i := 0; i < n; i++ {
			if _, _, err := w.Append(Record{Op: OpPut, Key: []byte{byte(i)}, Value: []byte("payload")}); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
		segID := w.ActiveSegmentID()
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return SegmentPath(dir, segID, SegmentExt), n
	}

	srcDir := filepath.Join(baseDir, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatal(err)
	}
	srcPath, _ := writeFresh(srcDir)
	full, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatal(err)
	}

	for off := 0; off <= len(full); off += 3 {
		dir := filepath.Join(baseDir, "trial")
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		path := SegmentPath(dir, 0, SegmentExt)
		if err := os.WriteFile(path, full[:off], 0o644); err != nil {
			t.Fatal(err)
		}
		recs, _, err := ReplaySegmentFile(path)
		if err != nil {
			t.Fatalf("offset %d: ReplaySegmentFile: %v", off, err)
		}
		for i, r := range recs {
			if r.Key[0] != byte(i) {
				t.Fatalf("offset %d: record %d corrupted: key=%d", off, i, r.Key[0])
			}
		}
	}
}
