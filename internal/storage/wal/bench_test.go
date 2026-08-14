package wal

import (
	"fmt"
	"testing"
)

// benchAppend appends b.N records under the given sync policy, calling
// Flush every batchSize records when mode == SyncBatch (batchSize is
// ignored for the other modes). This is the harness both benchmarks below
// share, so the only variable between them is the sync policy itself.
func benchAppend(b *testing.B, mode SyncMode, batchSize int) {
	dir := b.TempDir()
	w, err := OpenWriter(dir, WriterOptions{Sync: mode})
	if err != nil {
		b.Fatalf("OpenWriter: %v", err)
	}
	defer w.Close()

	value := make([]byte, 128) // representative small-value KV workload
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := Record{Op: OpPut, Key: []byte(fmt.Sprintf("key-%d", i)), Value: value}
		if _, _, err := w.Append(rec); err != nil {
			b.Fatalf("Append: %v", err)
		}
		if mode == SyncBatch && batchSize > 0 && (i+1)%batchSize == 0 {
			if err := w.Flush(); err != nil {
				b.Fatalf("Flush: %v", err)
			}
		}
	}
	if mode == SyncBatch {
		if err := w.Flush(); err != nil {
			b.Fatalf("final Flush: %v", err)
		}
	}
	b.StopTimer()
}

// BenchmarkAppend_SyncEveryWrite fsyncs after every single Append — the
// strongest durability guarantee (an Append that returned nil has
// survived a crash) at the cost of one fsync syscall per record.
func BenchmarkAppend_SyncEveryWrite(b *testing.B) {
	benchAppend(b, SyncEveryWrite, 0)
}

// BenchmarkAppend_SyncBatch100 fsyncs once every 100 records — the
// throughput-oriented end of the tradeoff: up to 100 acknowledged records
// can be lost on a crash, in exchange for amortizing the fsync cost.
func BenchmarkAppend_SyncBatch100(b *testing.B) {
	benchAppend(b, SyncBatch, 100)
}

// BenchmarkAppend_SyncNone never calls fsync explicitly (only Close does).
// Included as a reference point for "no durability at all," which is
// never appropriate for a real WAL but shows the ceiling batching is
// approaching.
func BenchmarkAppend_SyncNone(b *testing.B) {
	benchAppend(b, SyncNone, 0)
}
