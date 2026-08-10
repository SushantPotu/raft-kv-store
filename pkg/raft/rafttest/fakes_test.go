package rafttest

import (
	"context"
	"testing"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

func TestFakeStorageAppendAndEntries(t *testing.T) {
	s := NewFakeStorage()

	if err := s.Append([]raft.LogEntry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 2, Data: []byte("c")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	last, err := s.LastIndex()
	if err != nil || last != 3 {
		t.Fatalf("LastIndex = %d, err=%v, want 3", last, err)
	}

	entries, err := s.Entries(1, 4, 0)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("Entries: got %d entries, want 3", len(entries))
	}

	term, err := s.Term(3)
	if err != nil || term != 2 {
		t.Fatalf("Term(3) = %d, err=%v, want 2", term, err)
	}
}

func TestFakeStorageAppendOverwritesConflictingTail(t *testing.T) {
	s := NewFakeStorage()
	_ = s.Append([]raft.LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	})

	// Leader with a higher term overwrites the conflicting tail starting
	// at index 2 — this is the log-matching-property scenario Raft Core's
	// AppendEntries handler relies on Storage.Append to support correctly.
	if err := s.Append([]raft.LogEntry{
		{Index: 2, Term: 2},
	}); err != nil {
		t.Fatalf("Append (overwrite): %v", err)
	}

	last, _ := s.LastIndex()
	if last != 2 {
		t.Fatalf("LastIndex after overwrite = %d, want 2 (tail truncated)", last)
	}
	term, _ := s.Term(2)
	if term != 2 {
		t.Fatalf("Term(2) after overwrite = %d, want 2", term)
	}
}

func TestFakeStorageSnapshotCompactsLog(t *testing.T) {
	s := NewFakeStorage()
	_ = s.Append([]raft.LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	})

	snap, err := s.CreateSnapshot(2, raft.ConfState{Voters: []raft.NodeID{"a", "b", "c"}}, []byte("snapshot-data"))
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if snap.LastIncludedIndex != 2 || snap.LastIncludedTerm != 1 {
		t.Fatalf("snapshot metadata = %+v, want index=2 term=1", snap)
	}

	first, err := s.FirstIndex()
	if err != nil || first != 3 {
		t.Fatalf("FirstIndex after compaction = %d, err=%v, want 3", first, err)
	}

	// Index 2 is now only reachable via the snapshot, not via Entries/Term.
	if _, err := s.Term(1); err == nil {
		t.Fatal("expected error reading Term for a compacted-away index")
	}
}

func TestFakeStorageApplySnapshotResetsLog(t *testing.T) {
	s := NewFakeStorage()
	cs := raft.ConfState{Voters: []raft.NodeID{"a", "b"}}
	if err := s.ApplySnapshot(raft.Snapshot{Data: []byte("x"), LastIncludedIndex: 10, LastIncludedTerm: 5, ConfState: cs}); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}

	last, _ := s.LastIndex()
	first, _ := s.FirstIndex()
	if last != 10 || first != 11 {
		t.Fatalf("after ApplySnapshot: first=%d last=%d, want first=11 last=10", first, last)
	}

	_, cs2, err := s.InitialState()
	if err != nil {
		t.Fatalf("InitialState: %v", err)
	}
	if len(cs2.Voters) != 2 {
		t.Fatalf("ConfState not restored from snapshot: %+v", cs2)
	}
}

func TestFakeTransportRoutesToRegisteredNode(t *testing.T) {
	reg := NewRegistry()
	received := make(chan struct{}, 1)
	reg.Register("b", &recordingNode{onStep: func() { received <- struct{}{} }})

	tr := NewFakeTransport(reg)
	if _, err := tr.SendAppendEntries(context.Background(), "shard-1", "b", nil); err != nil {
		t.Fatalf("SendAppendEntries: %v", err)
	}

	select {
	case <-received:
	default:
		t.Fatal("expected registered node's Step to be called")
	}
}

// recordingNode is a minimal raft.Node stub satisfying the interface just
// enough to prove FakeTransport routes to the right node — not a real
// Raft implementation (that's Workstream B's job).
type recordingNode struct {
	onStep func()
}

var _ raft.Node = (*recordingNode)(nil)

func (n *recordingNode) Propose(ctx context.Context, data []byte) error { return nil }
func (n *recordingNode) ProposeConfChange(ctx context.Context, cc raft.ConfChange) error {
	return nil
}
func (n *recordingNode) ReadIndex(ctx context.Context, ctxToken []byte) error { return nil }
func (n *recordingNode) Step(ctx context.Context, msg raft.InboundMessage) error {
	if n.onStep != nil {
		n.onStep()
	}
	return nil
}
func (n *recordingNode) Ready() <-chan raft.Ready { return nil }
func (n *recordingNode) Advance()                 {}
func (n *recordingNode) Tick()                    {}
func (n *recordingNode) Status() raft.Status      { return raft.Status{} }
