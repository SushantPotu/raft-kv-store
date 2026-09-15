package wal

import (
	"fmt"
	"os"
	"sync"
)

// SegmentExt is the file extension used for KV WAL segments.
const SegmentExt = ".wal"

// DefaultSegmentBytes is the default rotation threshold for a segment
// file: once the active segment would exceed this size, a new one is
// started.
const DefaultSegmentBytes = 64 * 1024 * 1024 // 64MB

// SyncMode controls when a Writer fsyncs the active segment to disk. This
// is deliberately exposed as a knob (rather than picked once and hidden)
// because the fsync-per-write vs. batched-fsync tradeoff is exactly the
// durability/throughput tradeoff this package's benchmark quantifies.
type SyncMode int

const (
	// SyncNone never calls fsync explicitly; durability is whatever the OS
	// page cache flushes on its own schedule (or on Close/Flush). Fastest,
	// weakest durability — a power loss can lose recently-appended
	// records even though Append returned nil.
	SyncNone SyncMode = iota
	// SyncEveryWrite calls fsync after every single Append. Strongest
	// durability (an acknowledged write survives a crash), at the cost of
	// one fsync syscall per record.
	SyncEveryWrite
	// SyncBatch defers fsync to explicit Flush() calls, so a caller can
	// batch many Appends into one fsync (e.g. once per N records or once
	// per tick). Durability is "everything since the last Flush may be
	// lost," which is the same window most production WALs (Kafka,
	// Postgres with commit_delay, etc.) accept in exchange for much higher
	// throughput.
	SyncBatch
)

// WriterOptions configures a Writer.
type WriterOptions struct {
	// SegmentBytes is the rotation threshold. Zero means
	// DefaultSegmentBytes.
	SegmentBytes int64
	// Sync selects the fsync policy. Zero value is SyncNone; callers
	// wanting durability should set SyncEveryWrite or SyncBatch
	// explicitly.
	Sync SyncMode
}

func (o WriterOptions) segmentBytes() int64 {
	if o.SegmentBytes <= 0 {
		return DefaultSegmentBytes
	}
	return o.SegmentBytes
}

// Writer appends KV Records to a rotating sequence of segment files in a
// directory.
//
// Concurrency: Writer is safe for concurrent use; all mutating operations
// (Append, Flush, Close) take an internal mutex. Callers that need
// higher-throughput concurrent writers typically serialize at a higher
// level (a single writer goroutine fed by a channel) instead, but
// correctness does not depend on that.
type Writer struct {
	mu      sync.Mutex
	dir     string
	opts    WriterOptions
	file    *os.File
	segID   uint64
	size    int64
	nextSeq uint64
	closed  bool
}

// OpenWriter opens (creating if necessary) dir and positions a Writer to
// append after whatever segments already exist there, resuming the
// sequence-number counter from the highest Seq found during a quick tail
// scan of the last segment. Most callers instead go through
// engine.Open, which performs a full replay anyway and can seed nextSeq
// precisely; OpenWriter's own scan exists so the Writer is independently
// correct if used standalone (e.g. in wal package tests/benchmarks).
func OpenWriter(dir string, opts WriterOptions) (*Writer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %s: %w", dir, err)
	}
	ids, err := ListSegmentIDs(dir, SegmentExt)
	if err != nil {
		return nil, err
	}
	w := &Writer{dir: dir, opts: opts}
	var lastID uint64
	if len(ids) > 0 {
		lastID = ids[len(ids)-1]
	}
	if err := w.openSegment(lastID, len(ids) == 0); err != nil {
		return nil, err
	}
	// Seed nextSeq by scanning every existing segment for the max Seq
	// seen. This keeps sequence numbers strictly increasing across
	// restarts even though Seq's only real job is intra-process ordering.
	maxSeq, seen, err := scanMaxSeq(dir, ids)
	if err != nil {
		return nil, err
	}
	if seen {
		w.nextSeq = maxSeq + 1
	}
	return w, nil
}

func scanMaxSeq(dir string, ids []uint64) (maxSeq uint64, seen bool, err error) {
	for _, id := range ids {
		recs, _, err := ReplaySegmentFile(SegmentPath(dir, id, SegmentExt))
		if err != nil {
			return 0, false, err
		}
		for _, r := range recs {
			if !seen || r.Seq > maxSeq {
				maxSeq = r.Seq
				seen = true
			}
		}
	}
	return maxSeq, seen, nil
}

func (w *Writer) openSegment(id uint64, create bool) error {
	path := SegmentPath(w.dir, id, SegmentExt)
	flags := os.O_RDWR | os.O_APPEND
	if create {
		flags |= os.O_CREATE
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if os.IsNotExist(err) {
		f, err = os.OpenFile(path, flags|os.O_CREATE, 0o644)
	}
	if err != nil {
		return fmt.Errorf("wal: open segment %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	w.file = f
	w.segID = id
	w.size = info.Size()
	return nil
}

// Append writes rec to the active segment, rotating to a new segment first
// if the write would exceed the configured threshold. It returns the
// segment id and byte offset the record's frame was written at, which
// callers (the engine's index, or raftlog) can use to seek straight to the
// value later without re-scanning.
func (w *Writer) Append(rec Record) (segID uint64, offset int64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, 0, fmt.Errorf("wal: append on closed writer")
	}

	rec.Seq = w.nextSeq
	payload := encodeRecord(rec)
	frameLen := int64(frameHeaderSize + len(payload))

	if w.size > 0 && w.size+frameLen > w.opts.segmentBytes() {
		if err := w.rotateLocked(); err != nil {
			return 0, 0, err
		}
	}

	offset = w.size
	if err := WriteFrame(w.file, payload); err != nil {
		return 0, 0, fmt.Errorf("wal: write frame: %w", err)
	}

	switch w.opts.Sync {
	case SyncEveryWrite:
		if err := w.file.Sync(); err != nil {
			return 0, 0, fmt.Errorf("wal: fsync: %w", err)
		}
	case SyncBatch, SyncNone:
		// no-op here; SyncBatch durability comes from explicit Flush().
	}

	w.size += frameLen
	w.nextSeq++
	return w.segID, offset, nil
}

// LastAppendedSeq returns the sequence number that will be assigned to the
// *next* Append call, i.e. one past the last record actually written.
// Exposed for tests/diagnostics.
func (w *Writer) NextSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextSeq
}

func (w *Writer) rotateLocked() error {
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync before rotate: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal: close before rotate: %w", err)
	}
	return w.openSegment(w.segID+1, true)
}

// Flush fsyncs the active segment. Used explicitly under SyncBatch to
// control the durability/throughput tradeoff (batch N Appends, then one
// Flush).
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	return w.file.Sync()
}

// ActiveSegmentID returns the id of the segment currently being written
// to.
func (w *Writer) ActiveSegmentID() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segID
}

// Close flushes and closes the active segment file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.file.Sync(); err != nil {
		w.file.Close()
		return err
	}
	return w.file.Close()
}
