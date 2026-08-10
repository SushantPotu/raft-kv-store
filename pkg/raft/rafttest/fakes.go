// Package rafttest provides trivial, in-memory implementations of the
// pkg/raft interfaces (Storage, Transport) for use in tests and by
// workstreams that haven't landed yet. None of these are durable or
// production-ready — see internal/storage (Workstream A) for the real
// Storage implementation and internal/transport/grpc (Workstream C) for
// the real Transport implementation.
package rafttest

import (
	"context"
	"fmt"
	"sync"

	"github.com/SushantPotu/raft-kv-store/pkg/raft"
	"github.com/SushantPotu/raft-kv-store/pkg/raftpb"
)

var (
	_ raft.Storage   = (*FakeStorage)(nil)
	_ raft.Transport = (*FakeTransport)(nil)
)

// FakeStorage is an in-memory raft.Storage with no persistence — state is
// lost on process exit. Sufficient for unit tests of anything that
// consumes raft.Storage without needing crash-recovery behavior (that's
// specifically what Workstream A's real implementation is tested for).
type FakeStorage struct {
	mu        sync.Mutex
	hardState raft.HardState
	confState raft.ConfState
	entries   []raft.LogEntry // index 0 holds the entry at FirstIndex-1 (dummy/snapshot boundary)
	snapshot  raft.Snapshot
}

func NewFakeStorage() *FakeStorage {
	return &FakeStorage{
		entries: []raft.LogEntry{{Index: 0, Term: 0}}, // sentinel
	}
}

func (s *FakeStorage) InitialState() (raft.HardState, raft.ConfState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hardState, s.confState, nil
}

func (s *FakeStorage) Entries(lo, hi raft.LogIndex, maxSize uint64) ([]raft.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	base := s.entries[0].Index
	if lo <= base {
		return nil, fmt.Errorf("rafttest: requested index %d at or before compacted base %d", lo, base)
	}
	loOff, hiOff := int(lo-base), int(hi-base)
	if hiOff > len(s.entries) {
		hiOff = len(s.entries)
	}
	if loOff >= hiOff {
		return nil, nil
	}
	out := make([]raft.LogEntry, 0, hiOff-loOff)
	var size uint64
	for _, e := range s.entries[loOff:hiOff] {
		size += uint64(len(e.Data))
		if maxSize > 0 && size > maxSize && len(out) > 0 {
			break
		}
		out = append(out, e)
	}
	return out, nil
}

func (s *FakeStorage) Term(i raft.LogIndex) (raft.Term, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	base := s.entries[0].Index
	if i < base || int(i-base) >= len(s.entries) {
		return 0, fmt.Errorf("rafttest: index %d out of range", i)
	}
	return s.entries[i-base].Term, nil
}

func (s *FakeStorage) FirstIndex() (raft.LogIndex, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries[0].Index + 1, nil
}

func (s *FakeStorage) LastIndex() (raft.LogIndex, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries[len(s.entries)-1].Index, nil
}

func (s *FakeStorage) Append(entries []raft.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	base := s.entries[0].Index
	first := entries[0].Index
	if uint64(first-base) > uint64(len(s.entries)) {
		return fmt.Errorf("rafttest: append gap: have up to %d, got first=%d", s.entries[len(s.entries)-1].Index, first)
	}
	s.entries = append(s.entries[:first-base], entries...)
	return nil
}

func (s *FakeStorage) SetHardState(hs raft.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hardState = hs
	return nil
}

func (s *FakeStorage) CreateSnapshot(index raft.LogIndex, cs raft.ConfState, data []byte) (raft.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	base := s.entries[0].Index
	if index < base || int(index-base) >= len(s.entries) {
		return raft.Snapshot{}, fmt.Errorf("rafttest: snapshot index %d out of range", index)
	}
	term := s.entries[index-base].Term
	snap := raft.Snapshot{Data: data, LastIncludedIndex: index, LastIncludedTerm: term, ConfState: cs}
	s.entries = s.entries[index-base:]
	s.snapshot = snap
	return snap, nil
}

func (s *FakeStorage) ApplySnapshot(snap raft.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot = snap
	s.entries = []raft.LogEntry{{Index: snap.LastIncludedIndex, Term: snap.LastIncludedTerm}}
	s.confState = snap.ConfState
	return nil
}

func (s *FakeStorage) Snapshot() (raft.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot, nil
}

// FakeTransport routes messages between FakeTransport instances registered
// in the same Registry, entirely in-process (no network, no serialization).
// Useful for integration-style tests that want real Node instances talking
// to each other without Docker. internal/raft/simulate provides a more
// controllable (drop/delay/partition-capable) alternative for Raft Core's
// own correctness tests; FakeTransport is for other workstreams that just
// need "it works end to end in one process."
type Registry struct {
	mu       sync.Mutex
	handlers map[raft.NodeID]raft.Node
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[raft.NodeID]raft.Node)}
}

func (r *Registry) Register(id raft.NodeID, node raft.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[id] = node
}

type FakeTransport struct {
	registry *Registry
}

func NewFakeTransport(registry *Registry) *FakeTransport {
	return &FakeTransport{registry: registry}
}

func (t *FakeTransport) target(id raft.NodeID) (raft.Node, error) {
	t.registry.mu.Lock()
	defer t.registry.mu.Unlock()
	n, ok := t.registry.handlers[id]
	if !ok {
		return nil, fmt.Errorf("rafttest: no node registered for %q", id)
	}
	return n, nil
}

func (t *FakeTransport) SendRequestVote(ctx context.Context, shard raft.ShardID, target raft.NodeID, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	n, err := t.target(target)
	if err != nil {
		return nil, err
	}
	if err := n.Step(ctx, raft.InboundMessage{Shard: shard, Kind: raft.MsgRequestVote, Payload: req}); err != nil {
		return nil, err
	}
	return &raftpb.RequestVoteResponse{}, nil
}

func (t *FakeTransport) SendAppendEntries(ctx context.Context, shard raft.ShardID, target raft.NodeID, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	n, err := t.target(target)
	if err != nil {
		return nil, err
	}
	if err := n.Step(ctx, raft.InboundMessage{Shard: shard, Kind: raft.MsgAppendEntries, Payload: req}); err != nil {
		return nil, err
	}
	return &raftpb.AppendEntriesResponse{}, nil
}

func (t *FakeTransport) SendInstallSnapshot(ctx context.Context, shard raft.ShardID, target raft.NodeID, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	n, err := t.target(target)
	if err != nil {
		return nil, err
	}
	if err := n.Step(ctx, raft.InboundMessage{Shard: shard, Kind: raft.MsgInstallSnapshot, Payload: req}); err != nil {
		return nil, err
	}
	return &raftpb.InstallSnapshotResponse{}, nil
}
