package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/SushantPotu/raft-kv-store/internal/statemachine"
	"github.com/SushantPotu/raft-kv-store/pkg/kvpb"
	"github.com/SushantPotu/raft-kv-store/pkg/raft"
)

// localReader is satisfied by both fakeStateMachine (Workstream C's testing
// fake) and internal/statemachine.Adapter (the real one), so KVServer can be
// wired against either without caring which.
type localReader interface {
	Get(key []byte) (value []byte, found bool)
}

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
	sm   localReader

	reqSeq atomic.Uint64
}

// NewKVServer wires a KVServer against a raft.Node and this package's
// fake state machine. Production wiring will eventually pass a real
// internal/raft.Node and a real internal/statemachine.Adapter-backed
// store instead of NewSingleNodeStub/newFakeStateMachine (see
// NewSingleNodeStubServer below for the all-in-one constructor
// Workstream C's own tests use).
func NewKVServer(node raft.Node, sm localReader) *KVServer {
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
	takeResult(requestID string) (statemachine.CommandResult, bool)
}

// leaderHint returns the empty string when node believes itself to be the
// leader (always true for SingleNodeStub), and the known leader's ID
// otherwise. This plumbing matters once a real multi-node Node exists;
// for the stub it is always "".
//
// Ambiguity callers must handle: "" is also what this returns when node
// is a follower that doesn't know who the leader is yet (st.Leader == "",
// e.g. mid-election) — indistinguishable at this layer from "I am the
// leader." notLeaderResponse below is what turns that ambiguity back into
// a real error instead of a response a client would misread as success.
func leaderHint(node raft.Node) string {
	st := node.Status()
	if st.IsLeader {
		return ""
	}
	return string(st.Leader)
}

// notLeaderResponse computes what a write handler should return after
// propose returned raft.ErrNotLeader. Normally that's not a client-visible
// failure — just a redirect via LeaderHint (see propose's doc comment).
// But if this replica doesn't know who the leader is either, leaderHint
// returns "" — the same value it returns when this replica *is* the
// leader (see that function's doc comment) — so returning a normal
// response with an empty LeaderHint here would be silently
// indistinguishable from success: the write was never proposed anywhere,
// yet a client checking only "was LeaderHint set" would conclude it
// succeeded. Surface that case as a real error instead.
func (s *KVServer) notLeaderResponse() (hint string, err error) {
	hint = leaderHint(s.node)
	if hint == "" {
		return "", status.Error(codes.Unavailable, "not leader, and no leader is currently known")
	}
	return hint, nil
}

func (s *KVServer) nextRequestID() string {
	return fmt.Sprintf("req-%d", s.reqSeq.Add(1))
}

// propose encodes cmd and calls Node.Propose. On any failure OTHER than
// "this replica isn't the leader," it returns an already gRPC-status-wrapped
// error. A not-leader rejection is returned unwrapped (satisfying
// errors.Is(err, raft.ErrNotLeader)) precisely so Put/Delete/CompareAndSwap
// can tell the two cases apart: not-leader isn't a failure from the
// client's point of view, it's a normal response carrying a LeaderHint to
// redirect to (see leaderHint and each handler below) — kvctl already
// expects exactly that shape.
func (s *KVServer) propose(ctx context.Context, cmd statemachine.Command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return status.Errorf(codes.Internal, "encode command: %v", err)
	}
	if err := s.node.Propose(ctx, data); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return err
		}
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
	cmd := statemachine.Command{
		RequestID: s.nextRequestID(),
		Op:        statemachine.OpPut,
		Key:       req.GetKey(),
		Value:     req.GetValue(),
	}
	if err := s.propose(ctx, cmd); err != nil {
		if !errors.Is(err, raft.ErrNotLeader) {
			return nil, err
		}
		hint, uerr := s.notLeaderResponse()
		if uerr != nil {
			return nil, uerr
		}
		return &kvpb.PutResponse{LeaderHint: hint}, nil
	}
	return &kvpb.PutResponse{LeaderHint: leaderHint(s.node)}, nil
}

// Delete implements kvpb.KVServiceServer.
func (s *KVServer) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	cmd := statemachine.Command{
		RequestID: s.nextRequestID(),
		Op:        statemachine.OpDel,
		Key:       req.GetKey(),
	}
	if err := s.propose(ctx, cmd); err != nil {
		if !errors.Is(err, raft.ErrNotLeader) {
			return nil, err
		}
		hint, uerr := s.notLeaderResponse()
		if uerr != nil {
			return nil, uerr
		}
		return &kvpb.DeleteResponse{LeaderHint: hint}, nil
	}
	return &kvpb.DeleteResponse{LeaderHint: leaderHint(s.node)}, nil
}

// CompareAndSwap implements kvpb.KVServiceServer.
func (s *KVServer) CompareAndSwap(ctx context.Context, req *kvpb.CompareAndSwapRequest) (*kvpb.CompareAndSwapResponse, error) {
	requestID := s.nextRequestID()
	cmd := statemachine.Command{
		RequestID:     requestID,
		Op:            statemachine.OpCAS,
		Key:           req.GetKey(),
		Value:         req.GetNewValue(),
		ExpectedValue: req.GetExpectedValue(),
		ExpectAbsent:  req.GetExpectAbsent(),
	}
	if err := s.propose(ctx, cmd); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			// Never proposed anywhere — no result to retrieve, just redirect
			// (or a real error if even the leader's identity is unknown —
			// see notLeaderResponse).
			hint, uerr := s.notLeaderResponse()
			if uerr != nil {
				return nil, uerr
			}
			return &kvpb.CompareAndSwapResponse{LeaderHint: hint}, nil
		}
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
