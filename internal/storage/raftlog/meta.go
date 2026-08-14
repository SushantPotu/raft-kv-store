package raftlog

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/SushantPotu/raft-kv-store/internal/storage/snapshot"
	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// metaFileName holds the small, infrequently-changing pieces of Storage
// state: HardState (term/votedFor/commitIndex), ConfState, and the most
// recent Raft Snapshot's metadata (its LastIncludedIndex/Term/ConfState —
// the opaque Data is included too since ApplySnapshot's caller expects
// Snapshot() to return it unchanged after a restart).
//
// Unlike entries (append-only, one frame per record), this file is
// rewritten in full on every change via a write-tmp-then-rename, which is
// atomic on both POSIX and Windows (os.Rename replaces an existing
// destination file on both). That means a crash can never leave this file
// half-written — the reader either sees the old value or the new one,
// never a torn mix — trading a little more I/O per SetHardState for not
// needing any CRC/replay logic on this path at all.
const metaFileName = "state.dat"

// persistedState is the gob-encoded payload written into metaFileName.
type persistedState struct {
	HardState   raft.HardState
	ConfState   raft.ConfState
	Snapshot    raft.Snapshot
	HasSnapshot bool
}

func (s *Storage) metaPath() string {
	return filepath.Join(s.dir, metaFileName)
}

// persistState atomically overwrites the metadata file with the Storage's
// current HardState/ConfState/Snapshot. Caller must hold s.mu.
func (s *Storage) persistState() error {
	ps := persistedState{
		HardState:   s.hardState,
		ConfState:   s.confState,
		Snapshot:    s.snapshot,
		HasSnapshot: s.hasSnapshot,
	}
	blob, err := snapshot.Encode(ps)
	if err != nil {
		return fmt.Errorf("raftlog: encode state: %w", err)
	}

	tmpPath := s.metaPath() + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("raftlog: create temp state file: %w", err)
	}
	if _, err := f.Write(blob); err != nil {
		f.Close()
		return fmt.Errorf("raftlog: write temp state file: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("raftlog: fsync temp state file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("raftlog: close temp state file: %w", err)
	}
	if err := os.Rename(tmpPath, s.metaPath()); err != nil {
		return fmt.Errorf("raftlog: rename temp state file into place: %w", err)
	}
	return nil
}

// loadState reads the metadata file if present, populating
// hardState/confState/snapshot. Absence of the file (a brand-new log) is
// not an error.
func (s *Storage) loadState() error {
	data, err := os.ReadFile(s.metaPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("raftlog: read state file: %w", err)
	}
	var ps persistedState
	if err := snapshot.Decode(data, &ps); err != nil {
		return fmt.Errorf("raftlog: decode state file: %w", err)
	}
	s.hardState = ps.HardState
	s.confState = ps.ConfState
	s.snapshot = ps.Snapshot
	s.hasSnapshot = ps.HasSnapshot
	if ps.HasSnapshot {
		s.entries[0] = raft.LogEntry{Index: ps.Snapshot.LastIncludedIndex, Term: ps.Snapshot.LastIncludedTerm}
	}
	return nil
}
