package wal

import (
	"encoding/binary"
	"fmt"
)

// Op identifies the kind of mutation a KV WAL record represents.
type Op byte

const (
	// OpPut records a key/value write.
	OpPut Op = 1
	// OpDelete records a tombstone for key. Value is always empty for a
	// delete record.
	OpDelete Op = 2
)

func (o Op) String() string {
	switch o {
	case OpPut:
		return "PUT"
	case OpDelete:
		return "DEL"
	default:
		return fmt.Sprintf("Op(%d)", byte(o))
	}
}

// Record is one KV mutation as persisted in a WAL segment. Seq is a
// monotonically increasing sequence number assigned by the Writer,
// independent of any Raft index — it exists purely so the engine can order
// records within and across segments deterministically (e.g. to resolve
// which of two records for the same key is newer).
type Record struct {
	Seq   uint64
	Op    Op
	Key   []byte
	Value []byte
}

// encode serializes a Record into the flat binary layout:
//
//	[8 bytes seq][1 byte op][4 bytes keylen][key][4 bytes vallen][value]
//
// This is a hand-rolled, node-local binary format (not a wire protocol),
// so there is no need to route it through the shared proto/ definitions.
func encodeRecord(rec Record) []byte {
	buf := make([]byte, 8+1+4+len(rec.Key)+4+len(rec.Value))
	off := 0
	binary.LittleEndian.PutUint64(buf[off:], rec.Seq)
	off += 8
	buf[off] = byte(rec.Op)
	off++
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(rec.Key)))
	off += 4
	off += copy(buf[off:], rec.Key)
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(rec.Value)))
	off += 4
	off += copy(buf[off:], rec.Value)
	return buf
}

// DecodeRecord is the inverse of encodeRecord, exported so callers that
// need to interpret a raw frame payload directly (e.g. engine.DiskEngine
// reading a single record at a known offset without a full replay) don't
// have to reimplement the layout.
func DecodeRecord(b []byte) (Record, error) { return decodeRecord(b) }

// decodeRecord is the inverse of encodeRecord. It returns an error (rather
// than panicking) on malformed input so a corrupt-but-CRC-matching payload
// (vanishingly unlikely, but defensive coding costs nothing here) can never
// crash a replay.
func decodeRecord(b []byte) (Record, error) {
	const minLen = 8 + 1 + 4 + 4
	if len(b) < minLen {
		return Record{}, fmt.Errorf("wal: record payload too short: %d bytes", len(b))
	}
	off := 0
	seq := binary.LittleEndian.Uint64(b[off:])
	off += 8
	op := Op(b[off])
	off++
	keyLen := binary.LittleEndian.Uint32(b[off:])
	off += 4
	if uint64(off)+uint64(keyLen) > uint64(len(b)) {
		return Record{}, fmt.Errorf("wal: record key length %d overruns payload", keyLen)
	}
	key := append([]byte(nil), b[off:off+int(keyLen)]...)
	off += int(keyLen)
	if off+4 > len(b) {
		return Record{}, fmt.Errorf("wal: record payload truncated before value length")
	}
	valLen := binary.LittleEndian.Uint32(b[off:])
	off += 4
	if uint64(off)+uint64(valLen) > uint64(len(b)) {
		return Record{}, fmt.Errorf("wal: record value length %d overruns payload", valLen)
	}
	val := append([]byte(nil), b[off:off+int(valLen)]...)
	off += int(valLen)
	return Record{Seq: seq, Op: op, Key: key, Value: val}, nil
}
