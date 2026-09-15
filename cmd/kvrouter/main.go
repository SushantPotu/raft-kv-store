// Command kvrouter runs internal/routing.Router as a standalone gRPC
// server: a stateless router implementing the same client-facing
// kvpb.KVServiceServer interface kvnode itself does, but with no raft.Node
// of its own — it resolves each request's owning shard via metaservice
// and forwards to whichever replica it believes is that shard's current
// leader, self-correcting via leader_hint redirects and metaservice
// refreshes (see internal/routing's package doc comment).
//
// Configuration is entirely via environment variables:
//
//	METASERVICE_ADDR   required: address of the metaservice gRPC server
//	                    (see cmd/metaservice) this router resolves shards
//	                    against.
//	LISTEN_ADDR         address to listen on, default ":9100" (matches
//	                    Dockerfile.kvrouter's EXPOSE and the multi-shard
//	                    compose file's port mapping).
package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/SushantPotu/raft-kv-store/internal/routing"
	"github.com/SushantPotu/raft-kv-store/pkg/kvpb"
	"github.com/SushantPotu/raft-kv-store/pkg/metapb"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("kvrouter: %v", err)
	}
}

func run() error {
	metaAddr := os.Getenv("METASERVICE_ADDR")
	if metaAddr == "" {
		return fmt.Errorf("METASERVICE_ADDR is required")
	}
	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		listenAddr = ":9100"
	}

	metaConn, err := grpc.NewClient(metaAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial metaservice %s: %w", metaAddr, err)
	}
	defer metaConn.Close()
	metaClient := metapb.NewMetadataServiceClient(metaConn)

	dial := routing.NewGRPCDialer(grpc.WithTransportCredentials(insecure.NewCredentials()))
	router := routing.NewRouter(metaClient, dial)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", listenAddr, err)
	}

	srv := grpc.NewServer()
	kvpb.RegisterKVServiceServer(srv, router)

	errCh := make(chan error, 1)
	go func() {
		log.Printf("kvrouter: KVService listening on %s, routing via metaservice at %s", listenAddr, metaAddr)
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
		log.Printf("kvrouter: shutting down")
		srv.GracefulStop()
	}
	return nil
}
