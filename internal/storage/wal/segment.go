package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// segmentFileName renders a segment id as a zero-padded, lexically-sortable
// file name so directory listings sort in creation order without needing
// to parse every name first.
func segmentFileName(id uint64, ext string) string {
	return fmt.Sprintf("%020d%s", id, ext)
}

// parseSegmentID extracts the numeric id from a segment file name produced
// by segmentFileName, or ok=false if name doesn't match the expected
// pattern (e.g. it's some unrelated file in the directory).
func parseSegmentID(name, ext string) (id uint64, ok bool) {
	if !strings.HasSuffix(name, ext) {
		return 0, false
	}
	base := strings.TrimSuffix(name, ext)
	n, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// ListSegmentIDs returns the ids of all segment files with the given
// extension in dir, sorted ascending. Used by both the KV WAL writer and
// internal/storage/raftlog to discover existing segments on open/replay.
func ListSegmentIDs(dir, ext string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var ids []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if id, ok := parseSegmentID(e.Name(), ext); ok {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// SegmentPath returns the full path of segment id within dir.
func SegmentPath(dir string, id uint64, ext string) string {
	return filepath.Join(dir, segmentFileName(id, ext))
}
