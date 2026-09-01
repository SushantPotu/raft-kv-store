package metaservice

import (
	"bytes"
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/SushantPotu/raft-kv-store/pkg/metapb"
)

// Service implements metapb.MetadataServiceServer against a Store. See
// this package's doc comment for the Store seam's purpose.
type Service struct {
	metapb.UnimplementedMetadataServiceServer

	store Store
}

var _ metapb.MetadataServiceServer = (*Service)(nil)

// NewService constructs a Service backed by store.
func NewService(store Store) *Service {
	return &Service{store: store}
}

// inRange reports whether key falls in [start, end) — start inclusive, end
// exclusive with an empty end meaning "no upper bound", exactly as
// ShardDescriptor.key_range_end's doc comment specifies. An empty start
// needs no special case: bytes.Compare(key, nil) >= 0 is true for every
// key, which is already "no lower bound".
func inRange(key, start, end []byte) bool {
	if bytes.Compare(key, start) < 0 {
		return false
	}
	if len(end) == 0 {
		return true
	}
	return bytes.Compare(key, end) < 0
}

// GetShardForKey implements metapb.MetadataServiceServer.
func (s *Service) GetShardForKey(ctx context.Context, req *metapb.GetShardForKeyRequest) (*metapb.GetShardForKeyResponse, error) {
	shards, err := s.store.List(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "metaservice: list shards: %v", err)
	}
	key := req.GetKey()
	for _, d := range shards {
		if inRange(key, d.GetKeyRangeStart(), d.GetKeyRangeEnd()) {
			return &metapb.GetShardForKeyResponse{Shard: d}, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "metaservice: no shard owns key %q", key)
}

// ListShards implements metapb.MetadataServiceServer.
func (s *Service) ListShards(ctx context.Context, _ *metapb.ListShardsRequest) (*metapb.ListShardsResponse, error) {
	shards, err := s.store.List(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "metaservice: list shards: %v", err)
	}
	return &metapb.ListShardsResponse{Shards: shards}, nil
}

// ReportLeaderChange implements metapb.MetadataServiceServer. Per the
// proto's doc comment, this is a conditional write keyed on term so a
// stale report from a deposed leader can never clobber a newer leader's
// record — see Store.ConditionalUpdateLeader.
func (s *Service) ReportLeaderChange(ctx context.Context, req *metapb.ReportLeaderChangeRequest) (*metapb.ReportLeaderChangeResponse, error) {
	accepted, currentTerm, err := s.store.ConditionalUpdateLeader(ctx, req.GetShardId(), req.GetNewLeaderNodeId(), req.GetTerm())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "metaservice: conditional update leader: %v", err)
	}
	return &metapb.ReportLeaderChangeResponse{
		Accepted:            accepted,
		CurrentTermOnRecord: currentTerm,
	}, nil
}

// RegisterReplica implements metapb.MetadataServiceServer.
func (s *Service) RegisterReplica(ctx context.Context, req *metapb.RegisterReplicaRequest) (*metapb.RegisterReplicaResponse, error) {
	if req.GetReplica() == nil {
		return nil, status.Error(codes.InvalidArgument, "metaservice: replica is required")
	}
	if err := s.store.UpsertReplica(ctx, req.GetShardId(), req.GetReplica()); err != nil {
		return nil, status.Errorf(codes.Internal, "metaservice: upsert replica: %v", err)
	}
	return &metapb.RegisterReplicaResponse{}, nil
}
