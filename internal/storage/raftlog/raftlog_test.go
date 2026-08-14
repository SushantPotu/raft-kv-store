package raftlog

import (
	"testing"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

func mustOpen(t *testing.T, dir string) *Storage {
	t.Helper()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// The following four tests mirror pkg/raft/rafttest/fakes_test.go's
// FakeStorage tests line for line (same scenarios, same assertions) to
// prove the real, disk-backed Storage matches the fake's behavioral
// contract exactly, as required by ADR-0002 / the workstream spec.

func TestStorageAppendAndEntries(t *testing.T) {
	s := mustOpen(t, t.TempDir())

	if err := s.Append([]raft.LogEntry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 2, Data: []byte("c")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	last, err := s.LastIndex()
	if err != nil || last != 3 {
		t.Fatalf("LastIndex = %d, err=%v, want 3", last, err)
	}

	entries, err := s.Entries(1, 4, 0)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("Entries: got %d entries, want 3", len(entries))
	}

	term, err := s.Term(3)
	if err != nil || term != 2 {
		t.Fatalf("Term(3) = %d, err=%v, want 2", term, err)
	}
}

func TestStorageAppendOverwritesConflictingTail(t *testing.T) {
	s := mustOpen(t, t.TempDir())
	_ = s.Append([]raft.LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})

	// Leader with a higher term overwrites the conflicting tail starting
	// at index 2.
	if err := s.Append([]raft.LogEntry{
		{Index: 2, Term: 2},
	}); err != nil {
		t.Fatalf("Append (overwrite): %v", err)
	}

	last, _ := s.LastIndex()
	if last != 2 {
		t.Fatalf("LastIndex after overwrite = %d, want 2 (tail truncated)", last)
	}
	term, _ := s.Term(2)
	if term != 2 {
		t.Fatalf("Term(2) after overwrite = %d, want 2", term)
	}
}

func TestStorageSnapshotCompactsLog(t *testing.T) {
	s := mustOpen(t, t.TempDir())
	_ = s.Append([]raft.LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	})

	snap, err := s.CreateSnapshot(2, raft.ConfState{Voters: []raft.NodeID{"a", "b", "c"}}, []byte("snapshot-data"))
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if snap.LastIncludedIndex != 2 || snap.LastIncludedTerm != 1 {
		t.Fatalf("snapshot metadata = %+v, want index=2 term=1", snap)
	}

	first, err := s.FirstIndex()
	if err != nil || first != 3 {
		t.Fatalf("FirstIndex after compaction = %d, err=%v, want 3", first, err)
	}

	if _, err := s.Term(1); err == nil {
		t.Fatal("expected error reading Term for a compacted-away index")
	}
	if _, err := s.Entries(1, 3, 0); err == nil {
		t.Fatal("expected error reading Entries at/before compacted base")
	}
}

func TestStorageApplySnapshotResetsLog(t *testing.T) {
	s := mustOpen(t, t.TempDir())
	cs := raft.ConfState{Voters: []raft.NodeID{"a", "b"}}
	if err := s.ApplySnapshot(raft.Snapshot{Data: []byte("x"), LastIncludedIndex: 10, LastIncludedTerm: 5, ConfState: cs}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}

	last, _ := s.LastIndex()
	first, _ := s.FirstIndex()
	if last != 10 || first != 11 {
		t.Fatalf("after ApplySnapshot: first=%d last=%d, want first=11 last=10", first, last)
	}

	_, cs2, err := s.InitialState()
	if err != nil {
		t.Fatalf("InitialState: %v", err)
	}
	if len(cs2.Voters) != 2 {
		t.Fatalf("ConfState not restored from snapshot: %+v", cs2)
	}
}

// TestStorageCompactionMovesFirstIndexAndFreesSegments is the
// raftlog-specific acceptance test: append enough entries to span
// multiple segments, CreateSnapshot at an index inside an earlier
// segment, and verify FirstIndex moves past the boundary, Entries/Term
// error for indices before it, and the segment files strictly before the
// compacted range are actually removed from disk.
func TestStorageCompactionMovesFirstIndexAndFreesSegments(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{SegmentBytes: 256})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	const n = 40
	entries := make([]raft.LogEntry, 0, n)
	for i := 1; i <= n; i++ {
		entries = append(entries, raft.LogEntry{
			Index: raft.LogIndex(i),
			Term:  raft.Term(1 + i/10),
			Data:  []byte("entry-payload-entry-payload"),
		})
	}
	if err := s.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}

	segsBefore, err := listEntrySegmentIDs(s.segs.dir)
	if err != nil {
		t.Fatalf("listEntrySegmentIDs: %v", err)
	}
	if len(segsBefore) < 2 {
		t.Fatalf("expected multiple entry segments from rotation, got %d", len(segsBefore))
	}

	const snapIndex = 30
	term30, err := s.Term(snapIndex)
	if err != nil {
		t.Fatalf("Term(%d): %v", snapIndex, err)
	}
	snap, err := s.CreateSnapshot(snapIndex, raft.ConfState{Voters: []raft.NodeID{"a"}}, []byte("snap"))
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if snap.LastIncludedIndex != snapIndex || snap.LastIncludedTerm != term30 {
		t.Fatalf("snapshot = %+v, want index=%d term=%d", snap, snapIndex, term30)
	}

	first, err := s.FirstIndex()
	if err != nil || first != snapIndex+1 {
		t.Fatalf("FirstIndex = %d, err=%v, want %d", first, err, snapIndex+1)
	}
	if _, err := s.Term(snapIndex - 1); err == nil {
		t.Fatal("expected error for Term before compacted boundary")
	}
	if _, err := s.Entries(snapIndex, snapIndex+2, 0); err == nil {
		t.Fatal("expected error for Entries at/before compacted boundary")
	}

	// Entries after the boundary must still be intact.
	remaining, err := s.Entries(snapIndex+1, n+1, 0)
	if err != nil {
		t.Fatalf("Entries after compaction: %v", err)
	}
	if len(remaining) != n-snapIndex {
		t.Fatalf("got %d remaining entries, want %d", len(remaining), n-snapIndex)
	}
	for i, e := range remaining {
		wantIdx := raft.LogIndex(snapIndex + 1 + i)
		if e.Index != wantIdx {
			t.Fatalf("remaining entry %d has index %d, want %d", i, e.Index, wantIdx)
		}
	}

	segsAfter, err := listEntrySegmentIDs(s.segs.dir)
	if err != nil {
		t.Fatalf("listEntrySegmentIDs: %v", err)
	}
	// Every segment id strictly less than the id backing the new
	// sentinel must be gone.
	keepFrom := s.locs[0].segID
	for _, id := range segsAfter {
		if id < keepFrom {
			t.Fatalf("segment %d should have been removed by compaction (keepFrom=%d)", id, keepFrom)
		}
	}
	if len(segsAfter) >= len(segsBefore) {
		t.Fatalf("expected compaction to reduce segment count: before=%d after=%d", len(segsBefore), len(segsAfter))
	}
}

// TestStorageRestartSurvivesEntriesHardStateAndSnapshot writes entries,
// hard state, and a snapshot, closes the log, reopens it pointing at the
// same directory, and verifies everything is still there — the
// restart/reopen acceptance test for raftlog.
func TestStorageRestartSurvivesEntriesHardStateAndSnapshot(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := s.Append([]raft.LogEntry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 2, Data: []byte("c")},
		{Index: 4, Term: 2, Data: []byte("d")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	hs := raft.HardState{Term: 2, VotedFor: "node-1", CommitIdx: 3}
	if err := s.SetHardState(hs); err != nil {
		t.Fatalf("SetHardState: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	gotHS, _, err := s2.InitialState()
	if err != nil || gotHS != hs {
		t.Fatalf("InitialState after restart = %+v, err=%v, want %+v", gotHS, err, hs)
	}
	last, _ := s2.LastIndex()
	if last != 4 {
		t.Fatalf("LastIndex after restart = %d, want 4", last)
	}
	entries, err := s2.Entries(1, 5, 0)
	if err != nil {
		t.Fatalf("Entries after restart: %v", err)
	}
	want := []string{"a", "b", "c", "d"}
	if len(entries) != len(want) {
		t.Fatalf("got %d entries after restart, want %d", len(entries), len(want))
	}
	for i, e := range entries {
		if string(e.Data) != want[i] {
			t.Fatalf("entry %d data = %q, want %q", i, e.Data, want[i])
		}
		if e.Index != raft.LogIndex(i+1) {
			t.Fatalf("entry %d index = %d, want %d", i, e.Index, i+1)
		}
	}
}

// TestStorageRestartAfterConflictingTailTruncation makes sure a
// truncated-and-overwritten tail does not reappear after a restart — the
// on-disk truncation performed by Append must be durable, not just an
// in-memory effect.
func TestStorageRestartAfterConflictingTailTruncation(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Append([]raft.LogEntry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("stale-b")},
		{Index: 3, Term: 1, Data: []byte("stale-c")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Append([]raft.LogEntry{
		{Index: 2, Term: 2, Data: []byte("new-b")},
	}); err != nil {
		t.Fatalf("Append (overwrite): %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	last, _ := s2.LastIndex()
	if last != 2 {
		t.Fatalf("LastIndex after restart = %d, want 2 (truncation must survive restart)", last)
	}
	entries, err := s2.Entries(1, 3, 0)
	if err != nil {
		t.Fatalf("Entries after restart: %v", err)
	}
	if len(entries) != 2 || string(entries[1].Data) != "new-b" || entries[1].Term != 2 {
		t.Fatalf("entries after restart = %+v, want [a, new-b(term=2)]", entries)
	}
}

// TestStorageRestartAfterApplySnapshot verifies ApplySnapshot's
// wholesale-reset behavior (and the ConfState it sets) survives a
// restart.
func TestStorageRestartAfterApplySnapshot(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Append([]raft.LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	cs := raft.ConfState{Voters: []raft.NodeID{"a", "b", "c"}}
	if err := s.ApplySnapshot(raft.Snapshot{Data: []byte("snap-data"), LastIncludedIndex: 100, LastIncludedTerm: 7, ConfState: cs}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	first, _ := s2.FirstIndex()
	last, _ := s2.LastIndex()
	if first != 101 || last != 100 {
		t.Fatalf("after restart: first=%d last=%d, want first=101 last=100", first, last)
	}
	snap, err := s2.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.LastIncludedIndex != 100 || snap.LastIncludedTerm != 7 || string(snap.Data) != "snap-data" {
		t.Fatalf("Snapshot after restart = %+v", snap)
	}
	_, gotCS, err := s2.InitialState()
	if err != nil || len(gotCS.Voters) != 3 {
		t.Fatalf("ConfState after restart = %+v, err=%v", gotCS, err)
	}
}
