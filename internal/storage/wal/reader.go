package wal

import (
	"fmt"
	"io"
	"os"
)

// ReplayResult summarizes one segment-file replay: the valid records
// recovered, in file order, and whether the file ended mid-record
// (torn == true) rather than at a clean frame boundary.
type ReplayResult struct {
	Records []Record
	Torn    bool
}

// ReplaySegmentFile reads every valid record from the segment file at
// path. If the file ends with a partially-written record (the signature
// of a crash mid-append) or a CRC-corrupt record, replay stops at that
// point, torn is true, and every record read before it is still returned
// — this is the "stop cleanly and report how many valid records were
// recovered" contract the storage engine's crash-recovery test exercises.
func ReplaySegmentFile(path string) (records []Record, torn bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer f.Close()

	var recs []Record
	_, tornOut, err := ReplayFrames(f, func(payload []byte) error {
		rec, derr := decodeRecord(payload)
		if derr != nil {
			return fmt.Errorf("wal: decode record in %s: %w", path, derr)
		}
		recs = append(recs, rec)
		return nil
	})
	if err != nil {
		return recs, false, err
	}
	return recs, tornOut, nil
}

// ReplayResultFor is a convenience wrapper returning a ReplayResult
// instead of separate return values, used where a struct reads more
// clearly (e.g. building a map of segment id -> result while replaying a
// whole directory).
func ReplayResultFor(path string) (ReplayResult, error) {
	recs, torn, err := ReplaySegmentFile(path)
	if err != nil {
		return ReplayResult{}, err
	}
	return ReplayResult{Records: recs, Torn: torn}, nil
}

// ReadRecordAt seeks to offset in f and decodes exactly one record's
// frame there. Used for the engine's Bitcask-style random-access reads:
// the in-memory index stores (segment, offset) locations, and Get uses
// this to fetch a value without scanning the whole segment.
func ReadRecordAt(f *os.File, offset int64) (Record, error) {
	sr := io.NewSectionReader(f, offset, 1<<63-1-offset)
	payload, err := ReadFrame(sr)
	if err != nil {
		return Record{}, err
	}
	return DecodeRecord(payload)
}

// ReplayDir replays every segment file (in ascending segment-id order) in
// dir and returns the concatenation of their valid records plus whether
// the final segment's tail was torn. A torn tail may only legitimately
// appear on the *last* segment (an earlier segment being torn while a
// later one exists cleanly would mean a segment was written out of
// order, which never happens with this package's single-active-writer
// model) — callers that want to assert that invariant can compare
// len(returned Records) against a full per-segment breakdown.
func ReplayDir(dir string) (records []Record, torn bool, err error) {
	ids, err := ListSegmentIDs(dir, SegmentExt)
	if err != nil {
		return nil, false, err
	}
	var all []Record
	for _, id := range ids {
		recs, segTorn, err := ReplaySegmentFile(SegmentPath(dir, id, SegmentExt))
		if err != nil {
			return nil, false, err
		}
		all = append(all, recs...)
		if segTorn {
			// Stop at the first torn segment; anything after it (there
			// shouldn't be anything, but be defensive) is not trusted.
			return all, true, nil
		}
	}
	return all, false, nil
}
