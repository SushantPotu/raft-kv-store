package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/SushantPotu/raft-kv-store/pkg/kvpb"
	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// KVServer implements kvpb.KVServiceServer, the client-facing KV gRPC
// API. It translates each RPC into a call against a raft.Node: writes go
// through Propose, and reads honor the request's Consistency setting
// (CONSISTENCY_STALE reads the local fake state machine directly;
// CONSISTENCY_LINEARIZABLE goes through Node.ReadIndex).
//
// KVServer is written against the raft.Node and raft.StateMachine
// interfaces so it keeps working unmodified once SingleNodeStub (see
// stubnode.go) is replaced by the real internal/raft.Node — the one
// exception is the fakeStateMachine.Get helper used for stale reads,
// which is specific to this package's own testing fake and will need to
// be replaced by whatever read path the real internal/statemachine
// adapter exposes.
type KVServer struct {
	kvpb.UnimplementedKVServiceServer

	node raft.Node
	sm   *fakeStateMachine

	reqSeq atomic.Uint64
}

// NewKVServer wires a KVServer against a raft.Node and this package's
// fake state machine. Production wiring will eventually pass a real
// internal/raft.Node and a real internal/statemachine.Adapter-backed
// store instead of NewSingleNodeStub/newFakeStateMachine (see
// NewSingleNodeStubServer below for the all-in-one constructor
// Workstream C's own tests use).
func NewKVServer(node raft.Node, sm *fakeStateMachine) *KVServer {
	return &KVServer{node: node, sm: sm}
}

// NewSingleNodeStubServer builds a KVServer backed by the temporary
// SingleNodeStub raft.Node and an in-memory fake state machine — see the
// prominent warning atop stubnode.go. This is what Workstream C's own
// end-to-end test (test/integration) and any local manual testing should
// use until the real internal/raft.Node lands.
func NewSingleNodeStubServer(id raft.NodeID) *KVServer {
	sm := newFakeStateMachine()
	stub := newSingleNodeStub(id, sm)
	return NewKVServer(stub, sm)
}

var _ kvpb.KVServiceServer = (*KVServer)(nil)

// syncApplier is implemented by raft.Node stand-ins (currently just
// SingleNodeStub) whose Propose applies synchronously and can therefore
// hand back a per-request result immediately, instead of requiring the
// caller to consume a Ready loop. Not part of pkg/raft — it's a
// package-local escape hatch that only exists because of the parallel
// Raft Core workstream not having landed yet.
type syncApplier interface {
	takeResult(requestID string) (commandResult, bool)
}

// leaderHint returns the empty string when node believes itself to be the
// leader (always true for SingleNodeStub), and the known leader's ID
// otherwise. This plumbing matters once a real multi-node Node exists;
// for the stub it is always "".
func leaderHint(node raft.Node) string {
	st := node.Status()
	if st.IsLeader {
		return ""
	}
	return string(st.Leader)
}

func (s *KVServer) nextRequestID() string {
	return fmt.Sprintf("req-%d", s.reqSeq.Add(1))
}

func (s *KVServer) propose(ctx context.Context, cmd command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return status.Errorf(codes.Internal, "encode command: %v", err)
	}
	if err := s.node.Propose(ctx, data); err != nil {
		return status.Errorf(codes.Unavailable, "propose: %v", err)
	}
	return nil
}

// Get implements kvpb.KVServiceServer.
func (s *KVServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	switch req.GetConsistency() {
	case kvpb.Consistency_CONSISTENCY_LINEARIZABLE:
		// Node.ReadIndex implements the linearizable-read protocol
		// (Workstream F / Raft Core). It is not implemented yet in this
		// parallel-development timeline (SingleNodeStub.ReadIndex always
		// errors, and the real internal/raft.Node hadn't landed ReadIndex
		// either as of this writing) — surface that plainly instead of
		// silently downgrading to a stale read, which would be a
		// correctness lie about the guarantee the client asked for.
		if err := s.node.ReadIndex(ctx, req.GetKey()); err != nil {
			return nil, status.Errorf(codes.Unimplemented,
				"linearizable reads not yet available: %v", err)
		}
		// If ReadIndex ever succeeds (a real Node), fall through and read
		// local state now that leadership/commit-index has been confirmed.
	case kvpb.Consistency_CONSISTENCY_STALE, kvpb.Consistency_CONSISTENCY_UNSPECIFIED:
		// Served from local state with no consensus round — documented,
		// intentional tradeoff (see proto/kvpb/kv.proto).
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown consistency %v", req.GetConsistency())
	}

	value, found := s.sm.Get(req.GetKey())
	return &kvpb.GetResponse{
		Value:      value,
		Found:      found,
		LeaderHint: leaderHint(s.node),
	}, nil
}

// Put implements kvpb.KVServiceServer.
func (s *KVServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	cmd := command{
		RequestID: s.nextRequestID(),
		Op:        opPut,
		Key:       req.GetKey(),
		Value:     req.GetValue(),
	}
	if err := s.propose(ctx, cmd); err != nil {
		return nil, err
	}
	return &kvpb.PutResponse{LeaderHint: leaderHint(s.node)}, nil
}

// Delete implements kvpb.KVServiceServer.
func (s *KVServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	cmd := command{
		RequestID: s.nextRequestID(),
		Op:        opDel,
		Key:       req.GetKey(),
	}
	if err := s.propose(ctx, cmd); err != nil {
		return nil, err
	}
	return &kvpb.DeleteResponse{LeaderHint: leaderHint(s.node)}, nil
}

// CompareAndSwap implements kvpb.KVServiceServer.
func (s *KVServer) CompareAndSwap(ctx context.Context, req *kvpb.CompareAndSwapRequest) (*kvpb.CompareAndSwapResponse, error) {
	requestID := s.nextRequestID()
	cmd := command{
		RequestID:     requestID,
		Op:            opCAS,
		Key:           req.GetKey(),
		Value:         req.GetNewValue(),
		ExpectedValue: req.GetExpectedValue(),
		ExpectAbsent:  req.GetExpectAbsent(),
	}
	if err := s.propose(ctx, cmd); err != nil {
		return nil, err
	}

	// SingleNodeStub applies synchronously, so by the time Propose above
	// returned, the result is already recorded. A real Node applies
	// asynchronously via its Ready loop — wiring that up (waiting for the
	// committed entry matching requestID to be applied) is exactly the
	// kind of change documented as required at Integration Checkpoint 1
	// when SingleNodeStub is retired; see the warning atop stubnode.go.
	sync, ok := s.node.(syncApplier)
	if !ok {
		return nil, status.Error(codes.Unimplemented,
			"CompareAndSwap result retrieval is only wired for a synchronous Node like SingleNodeStub; "+
				"a real raft.Node requires consuming Ready().CommittedEntries instead")
	}
	res, ok := sync.takeResult(requestID)
	if !ok {
		return nil, status.Error(codes.Internal, "CompareAndSwap: no result recorded for proposed command")
	}

	return &kvpb.CompareAndSwapResponse{
		Swapped:     res.Swapped,
		ActualValue: res.ActualValue,
		LeaderHint:  leaderHint(s.node),
	}, nil
}
