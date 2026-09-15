package engine

import (
	"os"

	"github.com/SushantPotu/raft-kv-store/internal/storage/wal"
)

// decodeIndexedRecord decodes one frame payload (as handed to a
// wal.ReplayFrames callback) into a wal.Record plus the location that
// frame occupies in segment id starting at byte offset, along with the
// total on-disk size of the frame (header + payload) so the caller can
// advance its own offset tracker.
func decodeIndexedRecord(payload []byte, segID uint64, offset int64) (wal.Record, location, int64, error) {
	rec, err := wal.DecodeRecord(payload)
	if err != nil {
		return wal.Record{}, location{}, 0, err
	}
	frameLen := int64(wal.FrameHeaderSize + len(payload))
	loc := location{segID: segID, offset: offset, tombstone: rec.Op == wal.OpDelete}
	return rec, loc, frameLen, nil
}

// readRecordAt reads and decodes exactly one record frame from f at
// offset.
func readRecordAt(f *os.File, offset int64) (wal.Record, error) {
	return wal.ReadRecordAt(f, offset)
}
