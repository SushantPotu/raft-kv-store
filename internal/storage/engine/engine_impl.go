// Package engine's real implementation (Engine, in this file and
// compact.go) is a Bitcask-style storage engine: an in-memory index of
// *locations* (segment id + byte offset + length) backed by
// internal/storage/wal append-only, CRC-checked segment files. Values are
// not cached in memory — Get seeks into the segment file and re-decodes
// the record on every call, which is the deliberate tradeoff Bitcask makes
// (RAM proportional to key count and index overhead, not to total data
// size).
//
// Concurrency model: a single sync.RWMutex guards both the in-memory index
// and all segment-file I/O. Reads (Get/Scan) take RLock; writes
// (Put/Delete) and maintenance (Compact) take the full Lock. This is
// coarser than a production engine (which would let concurrent Gets touch
// different segment files without blocking each other), but it keeps the
// crash-recovery and compaction invariants easy to reason about, which
// matters more for this project than raw concurrent-read throughput.
package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/SushantPotu/raft-kv-store/internal/storage/snapshot"
	"github.com/SushantPotu/raft-kv-store/internal/storage/wal"
)

// location pinpoints one record's frame inside a segment file.
type location struct {
	segID     uint64
	offset    int64
	tombstone bool
}

// DiskEngine is the real, WAL-backed engine.Engine implementation.
type DiskEngine struct {
	mu   sync.RWMutex
	dir  string
	opts Options

	writer *wal.Writer
	index  map[string]location // live keys only (tombstones are removed from the index)

	// openSegments caches read-only file handles for segments other than
	// the active one, so Get doesn't reopen a file on every call.
	openSegments map[uint64]*os.File
}

var _ Engine = (*DiskEngine)(nil)

// Options configures Open.
type Options struct {
	// SegmentBytes is the WAL rotation threshold; zero means
	// wal.DefaultSegmentBytes.
	SegmentBytes int64
	// Sync selects the WAL's fsync policy; zero value is wal.SyncNone.
	Sync wal.SyncMode
}

// Open opens (creating if necessary) a DiskEngine rooted at dir, replaying
// any existing WAL segments to reconstruct the in-memory index. This is
// what makes the engine crash- and restart-safe: everything needed to
// serve Get/Scan is rebuilt purely from the segment files on disk.
func Open(dir string, opts Options) (*DiskEngine, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("engine: mkdir %s: %w", dir, err)
	}

	e := &DiskEngine{
		dir:          dir,
		opts:         opts,
		index:        make(map[string]location),
		openSegments: make(map[uint64]*os.File),
	}

	ids, err := wal.ListSegmentIDs(dir, wal.SegmentExt)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if err := e.replaySegment(id); err != nil {
			return nil, err
		}
	}

	w, err := wal.OpenWriter(dir, wal.WriterOptions{SegmentBytes: opts.SegmentBytes, Sync: opts.Sync})
	if err != nil {
		return nil, err
	}
	e.writer = w
	return e, nil
}

// replaySegment applies every valid record in segment id to the in-memory
// index. A torn tail is tolerated (the record simply isn't applied) since
// wal.ReplaySegmentFile already stopped at the last valid record.
func (e *DiskEngine) replaySegment(id uint64) error {
	path := wal.SegmentPath(e.dir, id, wal.SegmentExt)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	offset := int64(0)
	_, _, err = wal.ReplayFrames(f, func(payload []byte) error {
		rec, loc, frameLen, derr := decodeIndexedRecord(payload, id, offset)
		offset += frameLen
		if derr != nil {
			return derr
		}
		e.applyIndex(rec.Key, loc)
		return nil
	})
	return err
}

// applyIndex updates the in-memory index for one decoded record. Because
// segments are replayed in ascending (segID, offset) order, a later record
// for the same key always wins, which is exactly bitcask's "last write
// wins" rule for both live values and tombstones.
func (e *DiskEngine) applyIndex(key []byte, loc location) {
	if loc.tombstone {
		delete(e.index, string(key))
		return
	}
	e.index[string(key)] = loc
}

// Get looks up key in the index and, if present and live, reads its value
// straight from the owning segment file (never from an in-memory cache of
// values).
func (e *DiskEngine) Get(key []byte) ([]byte, bool, error) {
	e.mu.RLock()
	loc, ok := e.index[string(key)]
	e.mu.RUnlock()
	if !ok {
		return nil, false, nil
	}

	e.mu.Lock()
	f, err := e.segmentFileLocked(loc.segID)
	e.mu.Unlock()
	if err != nil {
		return nil, false, err
	}

	rec, err := readRecordAt(f, loc.offset)
	if err != nil {
		return nil, false, fmt.Errorf("engine: read value for key %q: %w", key, err)
	}
	return rec.Value, true, nil
}

// segmentFileLocked returns a cached read handle for segment id, opening
// it if necessary. Caller must hold e.mu (for write, since it may mutate
// openSegments).
func (e *DiskEngine) segmentFileLocked(id uint64) (*os.File, error) {
	if f, ok := e.openSegments[id]; ok {
		return f, nil
	}
	path := wal.SegmentPath(e.dir, id, wal.SegmentExt)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	e.openSegments[id] = f
	return f, nil
}

// Put appends a PUT record to the WAL and updates the index.
func (e *DiskEngine) Put(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	segID, offset, err := e.writer.Append(wal.Record{Op: wal.OpPut, Key: key, Value: value})
	if err != nil {
		return err
	}
	e.applyIndex(key, location{segID: segID, offset: offset})
	return nil
}

// Delete appends a tombstone (DEL) record to the WAL and removes key from
// the index. The tombstone itself is retained on disk until the next
// Compact drops it, which is what lets a crash-recovery replay correctly
// forget a key that was deleted before the crash.
func (e *DiskEngine) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	segID, offset, err := e.writer.Append(wal.Record{Op: wal.OpDelete, Key: key})
	if err != nil {
		return err
	}
	e.applyIndex(key, location{segID: segID, offset: offset, tombstone: true})
	return nil
}

// Scan calls fn for each live key in [start, end) in ascending order.
// Since the index is a plain map, Scan takes a point-in-time snapshot of
// the matching keys (sorted), then reads each value under the read lock
// individually — this means a concurrent Put/Delete may or may not be
// reflected in a given Scan, but each individual key/value pair returned
// is always internally consistent.
func (e *DiskEngine) Scan(start, end []byte, fn func(k, v []byte) bool) error {
	e.mu.RLock()
	keys := make([]string, 0, len(e.index))
	for k := range e.index {
		if start != nil && k < string(start) {
			continue
		}
		if end != nil && k >= string(end) {
			continue
		}
		keys = append(keys, k)
	}
	e.mu.RUnlock()
	sort.Strings(keys)

	for _, k := range keys {
		v, found, err := e.Get([]byte(k))
		if err != nil {
			return err
		}
		if !found {
			// Deleted between building the key list and reading it; skip.
			continue
		}
		if !fn([]byte(k), v) {
			break
		}
	}
	return nil
}

// SnapshotAll dumps the entire live keyspace as a corruption-checked blob
// (see internal/storage/snapshot), used by Raft's InstallSnapshot path to
// bring a lagging or new replica up to date without replaying the full
// log.
func (e *DiskEngine) SnapshotAll() ([]byte, error) {
	e.mu.RLock()
	keys := make([]string, 0, len(e.index))
	for k := range e.index {
		keys = append(keys, k)
	}
	e.mu.RUnlock()
	sort.Strings(keys)

	// Reuse Get for each key rather than reading segment files directly:
	// Get already manages the openSegments cache under the correct lock
	// discipline (a plain os.Open here without caching/closing would leak
	// a file descriptor per call, which is exactly the kind of bug this
	// project's crash/restart tests are meant to catch).
	pairs := make([]snapshot.KV, 0, len(keys))
	for _, k := range keys {
		v, found, err := e.Get([]byte(k))
		if err != nil {
			return nil, err
		}
		if !found {
			continue // deleted concurrently between the two steps above
		}
		pairs = append(pairs, snapshot.KV{Key: []byte(k), Value: v})
	}

	return snapshot.Encode(pairs)
}

// RestoreAll replaces the entire keyspace with the contents of a blob
// produced by SnapshotAll. It durably persists the restored state (as
// fresh Put records) rather than only updating the in-memory index, so a
// restore survives a subsequent crash.
func (e *DiskEngine) RestoreAll(data []byte) error {
	var pairs []snapshot.KV
	if len(data) > 0 {
		if err := snapshot.Decode(data, &pairs); err != nil {
			return fmt.Errorf("engine: decode snapshot: %w", err)
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Tombstone every currently-live key that won't be present in the
	// restored set, so replay after a crash mid-restore can't resurrect
	// stale data.
	restored := make(map[string]bool, len(pairs))
	for _, p := range pairs {
		restored[string(p.Key)] = true
	}
	for k := range e.index {
		if !restored[k] {
			if _, _, err := e.writer.Append(wal.Record{Op: wal.OpDelete, Key: []byte(k)}); err != nil {
				return err
			}
		}
	}

	e.index = make(map[string]location)
	for _, p := range pairs {
		segID, offset, err := e.writer.Append(wal.Record{Op: wal.OpPut, Key: p.Key, Value: p.Value})
		if err != nil {
			return err
		}
		e.index[string(p.Key)] = location{segID: segID, offset: offset}
	}
	return nil
}

// Close flushes and closes the active WAL segment and every cached
// read-only segment handle.
func (e *DiskEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	var firstErr error
	if err := e.writer.Close(); err != nil {
		firstErr = err
	}
	for _, f := range e.openSegments {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	e.openSegments = make(map[uint64]*os.File)
	return firstErr
}

// Dir returns the directory this engine is rooted at. Exposed for tests
// that want to inspect the segment-file listing directly (e.g. asserting
// Compact actually removed obsolete files from disk).
func (e *DiskEngine) Dir() string { return e.dir }

// segmentFilesOnDisk lists the current *.wal segment files in the
// engine's directory. Test helper.
func (e *DiskEngine) segmentFilesOnDisk() ([]string, error) {
	entries, err := os.ReadDir(e.dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, ent := range entries {
		if filepath.Ext(ent.Name()) == wal.SegmentExt {
			names = append(names, ent.Name())
		}
	}
	return names, nil
}
