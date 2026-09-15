// Command metaservice runs internal/metaservice.Service as a standalone
// gRPC server: the cluster's source of truth for "which shard owns this
// key, and who is its current leader" (see proto/metapb/meta.proto).
//
// Backed by an in-memory Store for now (internal/metaservice.MemStore) —
// wiring a DynamoDB-backed Store is Checkpoint 2's job, gated on having a
// live AWS account (see internal/metaservice's package doc comment for why
// that swap is designed to be small and isolated).
//
// Configuration is entirely via environment variables:
//
//	LISTEN_ADDR   address to listen on, default ":9200" (matches
//	              Dockerfile.metaservice's EXPOSE and the multi-shard
//	              compose file's port mapping)
//	SHARD_SEEDS   optional bootstrap seeding of the shard map's key ranges,
//	              "<shard_id>:<range_start_hex>:<range_end_hex>,...".
//	              Either hex value may be empty (start empty = no lower
//	              bound, end empty = no upper bound, matching
//	              ShardDescriptor.key_range_end's doc comment). Production
//	              would instead seed DynamoDB directly (e.g. via a
//	              migration script) before any kvnode starts; this env var
//	              is a local-rehearsal/Docker Compose convenience so
//	              GetShardForKey has something to resolve against before
//	              any RegisterReplica call has landed. A shard a kvnode
//	              registers against that was never seeded here still works
//	              (RegisterReplica creates it with an unbounded empty
//	              range) — it just won't be resolvable by GetShardForKey
//	              until its range is set some other way.
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"

	"github.com/SushantPotu/raft-kv-store/internal/metaservice"
	"github.com/SushantPotu/raft-kv-store/pkg/metapb"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("metaservice: %v", err)
	}
}

func run() error {
	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = ":9200"
	}

	store := metaservice.NewMemStore()

	seeds, err := parseShardSeeds(os.Getenv("SHARD_SEEDS"))
	if err != nil {
		return fmt.Errorf("SHARD_SEEDS: %w", err)
	}
	ctx := context.Background()
	for _, sd := range seeds {
		if err := store.Put(ctx, sd); err != nil {
			return fmt.Errorf("seed shard %q: %w", sd.GetShardId(), err)
		}
		log.Printf("metaservice: seeded shard %q range=[%x, %x)", sd.GetShardId(), sd.GetKeyRangeStart(), sd.GetKeyRangeEnd())
	}

	svc := metaservice.NewService(store)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", listenAddr, err)
	}

	srv := grpc.NewServer()
	metapb.RegisterMetadataServiceServer(srv, svc)

	errCh := make(chan error, 1)
	go func() {
		log.Printf("metaservice: MetadataService listening on %s", listenAddr)
		errCh <- srv.Serve(lis)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("serve: %w", err)
		}
	case <-sigCh:
		log.Printf("metaservice: shutting down")
		srv.GracefulStop()
	}
	return nil
}

// parseShardSeeds parses SHARD_SEEDS' "<shard_id>:<start_hex>:<end_hex>,..."
// format. Either hex field may be empty.
func parseShardSeeds(s string) ([]*metapb.ShardDescriptor, error) {
	if s == "" {
		return nil, nil
	}
	var out []*metapb.ShardDescriptor
	for _, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, ":", 3)
		if len(parts) != 3 || parts[0] == "" {
			return nil, fmt.Errorf("malformed SHARD_SEEDS entry %q (want shard_id:start_hex:end_hex)", entry)
		}
		start, err := decodeHexAllowEmpty(parts[1])
		if err != nil {
			return nil, fmt.Errorf("entry %q: range_start: %w", entry, err)
		}
		end, err := decodeHexAllowEmpty(parts[2])
		if err != nil {
			return nil, fmt.Errorf("entry %q: range_end: %w", entry, err)
		}
		out = append(out, &metapb.ShardDescriptor{
			ShardId:       parts[0],
			KeyRangeStart: start,
			KeyRangeEnd:   end,
		})
	}
	return out, nil
}

func decodeHexAllowEmpty(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	return hex.DecodeString(s)
}
