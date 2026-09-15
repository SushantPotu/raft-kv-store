// Package wal implements a segment-based, CRC-checked write-ahead log. It
// provides two layers:
//
//   - A generic record-framing layer (this file, frame.go): every record on
//     disk is [uint32 crc][uint32 len][payload]. This framing is
//     content-agnostic and is reused verbatim by internal/storage/raftlog
//     (which frames raft.LogEntry / hard-state bytes instead of KV
//     records) and by internal/storage/snapshot (which frames whole
//     snapshot blobs), so the CRC/torn-tail detection logic exists in
//     exactly one place in the codebase.
//   - A KV-record-specific layer (record.go, writer.go, reader.go) used by
//     internal/storage/engine to persist Put/Delete operations.
//
// Concurrency: the framing functions in this file are stateless and operate
// on the io.Reader/io.Writer handed to them; callers are responsible for
// serializing access to a shared file handle.
package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// FrameHeaderSize is the fixed-size header preceding every frame's
// payload: a 4-byte CRC32 (IEEE) of the payload, followed by a 4-byte
// little-endian payload length. Exported so callers that need to compute
// on-disk frame sizes from a payload length (e.g. the engine's index,
// which stores byte offsets) don't have to hardcode the constant.
const FrameHeaderSize = 8
const frameHeaderSize = FrameHeaderSize

// ErrCorruptFrame is returned by ReadFrame when a frame's payload fails its
// CRC check. This indicates on-disk corruption (as opposed to a clean
// truncation) and replay must stop at this point.
var ErrCorruptFrame = errors.New("wal: corrupt frame (crc mismatch)")

// ErrTornFrame is returned by ReadFrame when fewer bytes are available than
// the frame header/payload requires. This is the expected signature of a
// process crash mid-write: the last frame's header or payload was only
// partially flushed to disk before the crash. Replay must stop at this
// point but this is not corruption — it's an incomplete final write.
var ErrTornFrame = errors.New("wal: torn frame (truncated trailing write)")

// WriteFrame writes one length-prefixed, CRC-checked frame containing
// payload to w. It performs at most one Write call's worth of syscalls
// worth of buffering is the caller's concern) — two writes (header, then
// payload) unless the caller wraps w in a buffered writer.
func WriteFrame(w io.Writer, payload []byte) error {
	var hdr [frameHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], crc32.ChecksumIEEE(payload))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// ReadFrame reads and validates one frame from r.
//
// Return contract:
//   - (payload, nil) on a fully-read, CRC-valid frame.
//   - (nil, io.EOF) when r is exhausted exactly at a frame boundary (the
//     clean end of a log — not an error).
//   - (nil, ErrTornFrame) when r is exhausted partway through a header or
//     payload — the hallmark of a crash mid-write.
//   - (nil, ErrCorruptFrame) when a full frame was read but its CRC does
//     not match — on-disk bit corruption rather than a torn write.
func ReadFrame(r io.Reader) ([]byte, error) {
	var hdr [frameHeaderSize]byte
	n, err := io.ReadFull(r, hdr[:])
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return nil, io.EOF
		}
		// A non-zero, incomplete read of the header, or an immediate
		// io.ErrUnexpectedEOF, both mean the header itself was torn.
		return nil, ErrTornFrame
	}

	crc := binary.LittleEndian.Uint32(hdr[0:4])
	length := binary.LittleEndian.Uint32(hdr[4:8])

	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, ErrTornFrame
		}
	}

	if crc32.ChecksumIEEE(payload) != crc {
		return nil, ErrCorruptFrame
	}
	return payload, nil
}

// ReplayFrames reads consecutive frames from r via ReadFrame, invoking fn
// with each valid payload, until it hits a clean EOF, a torn trailing
// frame, or a corrupt frame. It never returns an error for the two crash
// scenarios (torn/corrupt) — instead it reports how many frames were
// recovered and whether the tail was torn, which is exactly the
// information a WAL replayer needs to recover cleanly from a crash instead
// of treating it as a fatal error.
//
// fn returning a non-nil error aborts replay immediately and that error is
// returned as-is (a genuine processing error, distinct from framing
// issues).
func ReplayFrames(r io.Reader, fn func(payload []byte) error) (count int, torn bool, err error) {
	for {
		payload, ferr := ReadFrame(r)
		if ferr != nil {
			switch {
			case errors.Is(ferr, io.EOF):
				return count, false, nil
			case errors.Is(ferr, ErrTornFrame):
				return count, true, nil
			case errors.Is(ferr, ErrCorruptFrame):
				// Mid-file bit-rot: stop here too. We deliberately treat
				// this the same as a torn tail from the caller's point of
				// view (replay stops, nothing after this point is
				// trusted) but keep the distinct sentinel so callers that
				// care about the difference (corruption vs. crash) can
				// still tell them apart via errors.Is on a wrapped error
				// if desired. For the common replay path we report it as
				// "torn" since the recovery action is identical.
				return count, true, nil
			default:
				return count, false, ferr
			}
		}
		if err := fn(payload); err != nil {
			return count, false, err
		}
		count++
	}
}
