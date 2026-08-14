// Package raftlog is the real, durable implementation of pkg/raft.Storage
// (see ADR-0002 for why this is a separate log/layer from
// internal/storage/engine's KV WAL). It persists Raft log entries in its
// own append-only, CRC-checked segment files (reusing
// internal/storage/wal's generic framing — see segments.go) plus a small
// atomically-rewritten metadata file for HardState/ConfState/Snapshot (see
// meta.go).
//
// Concurrency model: a single sync.Mutex guards all state (unlike
// engine.DiskEngine, this uses a plain Mutex rather than RWMutex — every
// Storage method here is cheap relative to an fsync, and Raft Core calls
// these methods from a single goroutine per shard in the etcd/raft "Ready
// loop" pattern, so read/write concurrency isn't the bottleneck the way it
// can be for the KV engine under many client goroutines).
//
// In-memory entry representation: like pkg/raft/rafttest.FakeStorage,
// Storage keeps every unsompacted raft.LogEntry fully in memory
// (entries[0] is a sentinel holding the compaction boundary's Index/Term,
// exactly mirroring FakeStorage's convention) for fast Term/Entries
// access, backed by the segment files for durability and restart
// recovery. This matches how real Raft implementations such as etcd/raft's
// MemoryStorage behave — the log between the last snapshot and the last
// entry is not typically large enough to warrant paging it in from disk on
// every access, unlike the KV engine's full keyspace.
package raftlog

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

var _ raft.Storage = (*Storage)(nil)

// entryLoc records where one in-memory entry's frame lives on disk, so a
// later Append that needs to truncate a conflicting tail knows exactly
// which segment/offset to truncate from.
type entryLoc struct {
	segID  uint64
	offset int64
}

// Options configures Open.
type Options struct {
	// SegmentBytes is the entry-segment rotation threshold; zero means
	// the package default (64MB, matching internal/storage/wal).
	SegmentBytes int64
}

// Storage is the durable raft.Storage implementation for one shard.
type Storage struct {
	mu  sync.Mutex
	dir string

	segs *entrySegments

	entries []raft.LogEntry // entries[0] is the sentinel at the compaction boundary
	locs    []entryLoc      // parallel to entries; locs[0] is meaningless until a real CreateSnapshot populates it

	hardState   raft.HardState
	confState   raft.ConfState
	snapshot    raft.Snapshot
	hasSnapshot bool
}

// Open opens (creating if necessary) a Storage rooted at dir, replaying
// its segment files and metadata to reconstruct the in-memory log and
// hard state. This is what makes an instance survive a process restart:
// everything InitialState/Entries/Term/FirstIndex/LastIndex/Snapshot
// report is rebuilt purely from what's durably on disk.
func Open(dir string, opts Options) (*Storage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("raftlog: mkdir %s: %w", dir, err)
	}
	segs, err := openEntrySegments(filepath.Join(dir, "entries"), opts.SegmentBytes)
	if err != nil {
		return nil, err
	}

	s := &Storage{
		dir:     dir,
		segs:    segs,
		entries: []raft.LogEntry{{Index: 0, Term: 0}},
		locs:    []entryLoc{{}},
	}

	// Load metadata (HardState/ConfState/Snapshot) first: if a snapshot
	// was ever applied/created, it moves the sentinel's Index/Term, and
	// entry replay below needs that boundary to know which on-disk
	// frames are stale leftovers versus live entries.
	if err := s.loadState(); err != nil {
		return nil, err
	}
	if err := s.replayEntries(); err != nil {
		return nil, err
	}
	return s, nil
}

// replayEntries scans every entry segment file in order and rebuilds
// s.entries/s.locs. Frames at or before the current sentinel boundary are
// skipped (they're either genuinely obsolete leftovers in a partially
// reclaimed segment, or — when the index exactly matches the boundary —
// the sentinel's own on-disk location, which is recorded so a future
// Append can still truncate from it correctly).
func (s *Storage) replayEntries() error {
	boundary := s.entries[0].Index

	ids, err := listEntrySegmentIDs(s.segs.dir)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := s.replayOneSegment(id, boundary); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) replayOneSegment(id uint64, boundary raft.LogIndex) error {
	path := entrySegmentPath(s.segs.dir, id)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	offset := int64(0)
	_, _, err = replayEntryFrames(f, func(payload []byte) error {
		frameLen := frameSize(payload)
		e, derr := decodeEntry(payload)
		curOffset := offset
		offset += frameLen
		if derr != nil {
			return derr
		}

		switch {
		case e.Index < boundary:
			// Stale leftover in a not-yet-fully-reclaimed segment; ignore.
			return nil
		case e.Index == boundary:
			s.locs[0] = entryLoc{segID: id, offset: curOffset}
			return nil
		}

		expected := boundary + raft.LogIndex(len(s.entries))
		if e.Index != expected {
			return fmt.Errorf("raftlog: replay: out-of-order entry index %d in segment %d, expected %d", e.Index, id, expected)
		}
		s.entries = append(s.entries, e)
		s.locs = append(s.locs, entryLoc{segID: id, offset: curOffset})
		return nil
	})
	return err
}

// InitialState returns the persisted HardState/ConfState as of the last
// SetHardState/ApplySnapshot call (or their zero values for a brand-new
// log).
func (s *Storage) InitialState() (raft.HardState, raft.ConfState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hardState, s.confState, nil
}

// Entries returns log entries in [lo, hi), bounded by maxSize bytes of
// entry Data. Semantics mirror rafttest.FakeStorage.Entries exactly.
func (s *Storage) Entries(lo, hi raft.LogIndex, maxSize uint64) ([]raft.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	base := s.entries[0].Index
	if lo <= base {
		return nil, fmt.Errorf("raftlog: requested index %d at or before compacted base %d", lo, base)
	}
	loOff, hiOff := int(lo-base), int(hi-base)
	if hiOff > len(s.entries) {
		hiOff = len(s.entries)
	}
	if loOff >= hiOff {
		return nil, nil
	}
	out := make([]raft.LogEntry, 0, hiOff-loOff)
	var size uint64
	for _, e := range s.entries[loOff:hiOff] {
		size += uint64(len(e.Data))
		if maxSize > 0 && size > maxSize && len(out) > 0 {
			break
		}
		out = append(out, e)
	}
	return out, nil
}

// Term returns the term of the entry at index i.
func (s *Storage) Term(i raft.LogIndex) (raft.Term, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	base := s.entries[0].Index
	if i < base || int(i-base) >= len(s.entries) {
		return 0, fmt.Errorf("raftlog: index %d out of range", i)
	}
	return s.entries[i-base].Term, nil
}

// FirstIndex is the index after the most recent compaction boundary.
func (s *Storage) FirstIndex() (raft.LogIndex, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries[0].Index + 1, nil
}

// LastIndex is the index of the most recently appended entry (or the
// compaction boundary itself if the log is otherwise empty).
func (s *Storage) LastIndex() (raft.LogIndex, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries[len(s.entries)-1].Index, nil
}

// Append durably persists entries, truncating any conflicting tail first.
// Semantics mirror rafttest.FakeStorage.Append exactly: entries[0] (index
// `first`) is spliced into the log at position first-base, discarding
// whatever previously followed that position — this is what lets a
// leader's AppendEntries overwrite a follower's divergent tail per the
// Raft log-matching property.
func (s *Storage) Append(entries []raft.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	base := s.entries[0].Index
	first := entries[0].Index
	if uint64(first-base) > uint64(len(s.entries)) {
		return fmt.Errorf("raftlog: append gap: have up to %d, got first=%d", s.entries[len(s.entries)-1].Index, first)
	}
	truncateIdx := int(first - base)

	if truncateIdx > 0 && truncateIdx < len(s.entries) {
		loc := s.locs[truncateIdx]
		if err := s.segs.truncateTo(loc.segID, loc.offset); err != nil {
			return fmt.Errorf("raftlog: truncate conflicting tail: %w", err)
		}
	}
	s.entries = s.entries[:truncateIdx]
	s.locs = s.locs[:truncateIdx]

	for _, e := range entries {
		payload := encodeEntry(e)
		segID, offset, err := s.segs.appendEntry(payload)
		if err != nil {
			return err
		}
		s.entries = append(s.entries, e)
		s.locs = append(s.locs, entryLoc{segID: segID, offset: offset})
	}
	return nil
}

// SetHardState durably persists hs. Raft Core must call this (and wait
// for it to return) before responding to or acting on the RPC that
// produced this HardState.
func (s *Storage) SetHardState(hs raft.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hardState = hs
	return s.persistState()
}

// CreateSnapshot compacts the log up to and including index: everything
// before it becomes reachable only via the returned Snapshot, and the
// now-unreachable segment files are deleted from disk. Semantics mirror
// rafttest.FakeStorage.CreateSnapshot exactly, including that ConfState
// tracked by Storage is deliberately left untouched here (only
// ApplySnapshot updates it) — cs is stored solely inside the returned/
// persisted Snapshot for a future ApplySnapshot on some other replica to
// consume.
func (s *Storage) CreateSnapshot(index raft.LogIndex, cs raft.ConfState, data []byte) (raft.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	base := s.entries[0].Index
	if index < base || int(index-base) >= len(s.entries) {
		return raft.Snapshot{}, fmt.Errorf("raftlog: snapshot index %d out of range", index)
	}
	term := s.entries[index-base].Term
	snap := raft.Snapshot{Data: data, LastIncludedIndex: index, LastIncludedTerm: term, ConfState: cs}

	keepFromLoc := s.locs[index-base]
	s.entries = s.entries[index-base:]
	s.locs = s.locs[index-base:]
	s.snapshot = snap
	s.hasSnapshot = true

	if err := s.persistState(); err != nil {
		return raft.Snapshot{}, err
	}
	if err := s.segs.deleteSegmentsBefore(keepFromLoc.segID); err != nil {
		return raft.Snapshot{}, err
	}
	return snap, nil
}

// ApplySnapshot installs snap wholesale, discarding the entire existing
// log (used on the InstallSnapshot receive path for a lagging or brand
// new replica). Semantics mirror rafttest.FakeStorage.ApplySnapshot
// exactly.
func (s *Storage) ApplySnapshot(snap raft.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.segs.resetAll(); err != nil {
		return err
	}
	s.snapshot = snap
	s.hasSnapshot = true
	s.entries = []raft.LogEntry{{Index: snap.LastIncludedIndex, Term: snap.LastIncludedTerm}}
	s.locs = []entryLoc{{}}
	s.confState = snap.ConfState

	return s.persistState()
}

// Snapshot returns the most recently created/applied Snapshot (the zero
// value if none yet).
func (s *Storage) Snapshot() (raft.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot, nil
}

// Close flushes and closes the active entry segment.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.segs.close()
}
