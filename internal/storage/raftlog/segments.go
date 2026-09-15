package raftlog

import (
	"bytes"
	"fmt"
	"os"

	"github.com/SushantPotu/raft-kv-store/internal/storage/wal"
)

// entrySegmentExt distinguishes raftlog's own entry segment files from
// internal/storage/engine's *.wal files — the two are independent
// directories/logs per ADR-0002, but sharing the wal package's naming
// helpers means both must pass a distinct extension.
const entrySegmentExt = ".rlog"

// defaultSegmentBytes mirrors internal/storage/wal's default rotation
// threshold.
const defaultSegmentBytes = 64 * 1024 * 1024

// entrySegments manages the append-only, CRC-framed sequence of segment
// files that back the Raft log's entries. It reuses
// internal/storage/wal's frame read/write/replay primitives (WriteFrame,
// ReadFrame, ReplayFrames, ListSegmentIDs, SegmentPath) rather than
// duplicating the CRC/torn-tail logic — only the higher-level segment
// bookkeeping (rotation, and the truncate-on-conflicting-tail operation
// the KV engine's append-only WAL never needs) is specific to raftlog.
type entrySegments struct {
	dir          string
	segmentBytes int64

	file  *os.File
	segID uint64
	size  int64
}

func openEntrySegments(dir string, segmentBytes int64) (*entrySegments, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("raftlog: mkdir %s: %w", dir, err)
	}
	if segmentBytes <= 0 {
		segmentBytes = defaultSegmentBytes
	}
	ids, err := wal.ListSegmentIDs(dir, entrySegmentExt)
	if err != nil {
		return nil, err
	}
	s := &entrySegments{dir: dir, segmentBytes: segmentBytes}
	var startID uint64
	if len(ids) > 0 {
		startID = ids[len(ids)-1]
	}
	if err := s.open(startID); err != nil {
		return nil, err
	}
	return s, nil
}

// open opens segment id for read/write WITHOUT O_APPEND. Writes are done
// via WriteAt at explicitly tracked offsets (see appendEntry) rather than
// relying on the OS to position writes at end-of-file: on Windows, a file
// handle opened with O_APPEND cannot be truncated (File.Truncate returns
// "Access is denied"), which would break truncateTo — the operation this
// package needs for the Raft log-matching / conflicting-tail-overwrite
// semantics. Avoiding O_APPEND entirely keeps segment files truncatable
// on every platform.
func (s *entrySegments) open(id uint64) error {
	path := wal.SegmentPath(s.dir, id, entrySegmentExt)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("raftlog: open segment %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	s.file = f
	s.segID = id
	s.size = info.Size()
	return nil
}

// appendEntry writes one entry frame to the active segment (rotating
// first if needed) and fsyncs before returning — Raft's correctness
// depends on an entry being durable before a node acts on it (e.g.
// responds successfully to AppendEntries or casts a vote), so unlike
// internal/storage/wal's KV writer, raftlog does not offer a "skip
// fsync" mode.
func (s *entrySegments) appendEntry(payload []byte) (segID uint64, offset int64, err error) {
	frameLen := int64(wal.FrameHeaderSize + len(payload))
	if s.size > 0 && s.size+frameLen > s.segmentBytes {
		if err := s.rotate(); err != nil {
			return 0, 0, err
		}
	}
	offset = s.size

	var buf bytes.Buffer
	buf.Grow(int(frameLen))
	if err := wal.WriteFrame(&buf, payload); err != nil {
		return 0, 0, fmt.Errorf("raftlog: encode entry frame: %w", err)
	}
	if _, err := s.file.WriteAt(buf.Bytes(), offset); err != nil {
		return 0, 0, fmt.Errorf("raftlog: write entry frame: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return 0, 0, fmt.Errorf("raftlog: fsync entry: %w", err)
	}
	s.size += frameLen
	return s.segID, offset, nil
}

func (s *entrySegments) rotate() error {
	if err := s.file.Sync(); err != nil {
		return err
	}
	if err := s.file.Close(); err != nil {
		return err
	}
	return s.open(s.segID + 1)
}

// truncateTo discards every entry frame from (segID, offset) onward,
// physically shrinking the file(s) on disk so a subsequent replay never
// resurrects a conflicting tail that a leader's AppendEntries has just
// overwritten. If segID is an already-rotated-away segment (i.e. the
// conflict spans one or more full segments), those later segment files
// are deleted outright and segID's file is reopened as the new active
// segment, truncated to offset.
func (s *entrySegments) truncateTo(segID uint64, offset int64) error {
	if segID == s.segID {
		if err := s.file.Truncate(offset); err != nil {
			return fmt.Errorf("raftlog: truncate active segment: %w", err)
		}
		s.size = offset
		return nil
	}

	if err := s.file.Close(); err != nil {
		return err
	}
	ids, err := wal.ListSegmentIDs(s.dir, entrySegmentExt)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if id > segID {
			if err := os.Remove(wal.SegmentPath(s.dir, id, entrySegmentExt)); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("raftlog: remove superseded segment %d: %w", id, err)
			}
		}
	}
	path := wal.SegmentPath(s.dir, segID, entrySegmentExt)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("raftlog: reopen segment %d for truncation: %w", segID, err)
	}
	if err := f.Truncate(offset); err != nil {
		f.Close()
		return fmt.Errorf("raftlog: truncate segment %d: %w", segID, err)
	}
	f.Close()

	return s.open(segID)
}

// deleteSegmentsBefore permanently removes every segment file strictly
// older than keepFromSegID. Called after CreateSnapshot compacts the
// in-memory log, to actually reclaim the disk space of entries that are
// now unreachable except via the snapshot.
func (s *entrySegments) deleteSegmentsBefore(keepFromSegID uint64) error {
	ids, err := wal.ListSegmentIDs(s.dir, entrySegmentExt)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if id < keepFromSegID {
			if err := os.Remove(wal.SegmentPath(s.dir, id, entrySegmentExt)); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("raftlog: remove compacted segment %d: %w", id, err)
			}
		}
	}
	return nil
}

// resetAll discards every entry segment file, used when ApplySnapshot
// replaces the entire log wholesale (the InstallSnapshot path: a lagging
// or brand-new replica's existing log, if any, is entirely superseded).
func (s *entrySegments) resetAll() error {
	if err := s.file.Close(); err != nil {
		return err
	}
	ids, err := wal.ListSegmentIDs(s.dir, entrySegmentExt)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := os.Remove(wal.SegmentPath(s.dir, id, entrySegmentExt)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return s.open(0)
}

// listEntrySegmentIDs, entrySegmentPath, replayEntryFrames, and frameSize
// are thin wrappers around internal/storage/wal's generic helpers, kept
// here so raftlog.go doesn't need to import wal directly just to spell
// out the entrySegmentExt each time.
func listEntrySegmentIDs(dir string) ([]uint64, error) {
	return wal.ListSegmentIDs(dir, entrySegmentExt)
}

func entrySegmentPath(dir string, id uint64) string {
	return wal.SegmentPath(dir, id, entrySegmentExt)
}

func replayEntryFrames(f *os.File, fn func(payload []byte) error) (count int, torn bool, err error) {
	return wal.ReplayFrames(f, fn)
}

func frameSize(payload []byte) int64 {
	return int64(wal.FrameHeaderSize + len(payload))
}

func (s *entrySegments) close() error {
	if err := s.file.Sync(); err != nil {
		s.file.Close()
		return err
	}
	return s.file.Close()
}
