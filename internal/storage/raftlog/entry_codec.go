package raftlog

import (
	"encoding/binary"
	"fmt"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// encodeEntry serializes one raft.LogEntry into the flat binary layout:
//
//	[8 bytes index][8 bytes term][4 bytes type][4 bytes datalen][data]
//
// This is framed with internal/storage/wal's generic CRC frame (see
// segments.go), the same way internal/storage/engine frames its own KV
// records — the framing/corruption-detection code is shared, only the
// payload layout differs.
func encodeEntry(e raft.LogEntry) []byte {
	buf := make([]byte, 8+8+4+4+len(e.Data))
	off := 0
	binary.LittleEndian.PutUint64(buf[off:], uint64(e.Index))
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], uint64(e.Term))
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], uint32(e.Type))
	off += 4
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(e.Data)))
	off += 4
	copy(buf[off:], e.Data)
	return buf
}

func decodeEntry(b []byte) (raft.LogEntry, error) {
	const minLen = 8 + 8 + 4 + 4
	if len(b) < minLen {
		return raft.LogEntry{}, fmt.Errorf("raftlog: entry payload too short: %d bytes", len(b))
	}
	off := 0
	index := raft.LogIndex(binary.LittleEndian.Uint64(b[off:]))
	off += 8
	term := raft.Term(binary.LittleEndian.Uint64(b[off:]))
	off += 8
	typ := raft.EntryType(binary.LittleEndian.Uint32(b[off:]))
	off += 4
	dataLen := binary.LittleEndian.Uint32(b[off:])
	off += 4
	if uint64(off)+uint64(dataLen) > uint64(len(b)) {
		return raft.LogEntry{}, fmt.Errorf("raftlog: entry data length %d overruns payload", dataLen)
	}
	data := append([]byte(nil), b[off:off+int(dataLen)]...)
	return raft.LogEntry{Index: index, Term: term, Type: typ, Data: data}, nil
}
