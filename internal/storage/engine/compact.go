package engine

import (
	"fmt"
	"os"
	"sort"

	"github.com/SushantPotu/raft-kv-store/internal/storage/wal"
)

// Compact rewrites every live record into a fresh sequence of segments,
// then removes every old segment file from disk (both the ones that held
// only obsolete records — overwritten values and tombstones — and the
// ones that held live records now duplicated into the new segments).
//
// Safe to call concurrently with reads/writes: Compact holds the engine's
// write lock for its full duration, so a concurrent Get/Put/Delete/Scan
// simply blocks until compaction finishes rather than observing a
// half-migrated index. (A production engine would do this online, e.g. by
// compacting into new segments in the background and only briefly locking
// to swap the index over; this project takes the simpler stop-the-world
// approach since correctness, not compaction throughput, is what's being
// demonstrated here.)
func (e *DiskEngine) Compact() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	oldIDs, err := wal.ListSegmentIDs(e.dir, wal.SegmentExt)
	if err != nil {
		return err
	}

	// Snapshot the live key set (sorted, for deterministic output) and
	// read each value from its current location before anything on disk
	// changes.
	keys := make([]string, 0, len(e.index))
	for k := range e.index {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	type liveEntry struct {
		key, value []byte
	}
	live := make([]liveEntry, 0, len(keys))
	for _, k := range keys {
		loc := e.index[k]
		f, err := e.segmentFileLocked(loc.segID)
		if err != nil {
			return fmt.Errorf("engine: compact: open segment %d: %w", loc.segID, err)
		}
		rec, err := readRecordAt(f, loc.offset)
		if err != nil {
			return fmt.Errorf("engine: compact: read live record for %q: %w", k, err)
		}
		live = append(live, liveEntry{key: []byte(k), value: rec.Value})
	}

	// Close the current writer and every cached read handle before
	// touching segment files on disk, so nothing holds a stale fd across
	// the swap.
	if err := e.writer.Close(); err != nil {
		return fmt.Errorf("engine: compact: close writer: %w", err)
	}
	for id, f := range e.openSegments {
		f.Close()
		delete(e.openSegments, id)
	}

	// Write live records into a fresh, isolated staging directory first,
	// so a crash mid-compaction leaves the original segments untouched
	// (the staging directory is simply abandoned and cleaned up on next
	// Open, or left as harmless garbage — never mistaken for real WAL
	// segments since it lives outside e.dir).
	stagingDir, err := os.MkdirTemp(e.dir, ".compact-*")
	if err != nil {
		return fmt.Errorf("engine: compact: create staging dir: %w", err)
	}
	defer os.RemoveAll(stagingDir)

	stagingWriter, err := wal.OpenWriter(stagingDir, wal.WriterOptions{SegmentBytes: e.opts.SegmentBytes, Sync: e.opts.Sync})
	if err != nil {
		return fmt.Errorf("engine: compact: open staging writer: %w", err)
	}
	newIndex := make(map[string]location, len(live))
	for _, le := range live {
		segID, offset, err := stagingWriter.Append(wal.Record{Op: wal.OpPut, Key: le.key, Value: le.value})
		if err != nil {
			stagingWriter.Close()
			return fmt.Errorf("engine: compact: write staged record: %w", err)
		}
		newIndex[string(le.key)] = location{segID: segID, offset: offset}
	}
	if err := stagingWriter.Close(); err != nil {
		return fmt.Errorf("engine: compact: close staging writer: %w", err)
	}

	// Move the staged segments into place under fresh, never-before-used
	// segment ids (continuing on from the highest id ever used) so they
	// can't collide with anything a partially-failed compaction left
	// behind.
	nextID := uint64(0)
	if len(oldIDs) > 0 {
		nextID = oldIDs[len(oldIDs)-1] + 1
	}
	stagedIDs, err := wal.ListSegmentIDs(stagingDir, wal.SegmentExt)
	if err != nil {
		return err
	}
	idRemap := make(map[uint64]uint64, len(stagedIDs))
	for _, sid := range stagedIDs {
		idRemap[sid] = nextID
		nextID++
	}
	for _, sid := range stagedIDs {
		src := wal.SegmentPath(stagingDir, sid, wal.SegmentExt)
		dst := wal.SegmentPath(e.dir, idRemap[sid], wal.SegmentExt)
		if err := os.Rename(src, dst); err != nil {
			return fmt.Errorf("engine: compact: install staged segment: %w", err)
		}
	}
	for k, loc := range newIndex {
		loc.segID = idRemap[loc.segID]
		newIndex[k] = loc
	}

	// Only now remove the old segment files — the new ones are already
	// durably in place under e.dir.
	for _, id := range oldIDs {
		if err := os.Remove(wal.SegmentPath(e.dir, id, wal.SegmentExt)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("engine: compact: remove old segment %d: %w", id, err)
		}
	}

	// Reopen a live writer positioned after the newly-installed segments.
	w, err := wal.OpenWriter(e.dir, wal.WriterOptions{SegmentBytes: e.opts.SegmentBytes, Sync: e.opts.Sync})
	if err != nil {
		return fmt.Errorf("engine: compact: reopen writer: %w", err)
	}
	e.writer = w
	e.index = newIndex
	return nil
}
