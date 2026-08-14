package main

import (
	"bytes"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"github.com/SushantPotu/raft-kv-store/internal/server"
	"github.com/SushantPotu/raft-kv-store/pkg/kvpb"
)

// TestEndToEndPutGet is Workstream C's acceptance test: it proves the full
// stack wires together correctly — kvctl (CLI) -> gRPC -> KVService ->
// SingleNodeStub (temporary raft.Node stand-in, see internal/server/
// stubnode.go) -> the fake in-memory state machine.
//
// It starts a real KVService gRPC server on an OS-assigned localhost
// port, then drives kvctl's Run function in-process (no subprocess) to
// `put foo bar` followed by `get foo`, and asserts the round trip.
func TestEndToEndPutGet(t *testing.T) {
	addr := startTestServer(t)

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"put", "foo", "bar", "--addr=" + addr}, &stdout, &stderr); code != 0 {
		t.Fatalf("kvctl put exited %d, stderr=%q", code, stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "OK" {
		t.Fatalf("kvctl put stdout = %q, want OK", got)
	}

	stdout.Reset()
	stderr.Reset()
	if code := Run([]string{"get", "foo", "--addr=" + addr}, &stdout, &stderr); code != 0 {
		t.Fatalf("kvctl get exited %d, stderr=%q", code, stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "bar" {
		t.Fatalf("kvctl get stdout = %q, want %q", got, "bar")
	}
}

// TestEndToEndGetMissingKey exercises the "not found" path through the
// same full stack.
func TestEndToEndGetMissingKey(t *testing.T) {
	addr := startTestServer(t)

	var stdout, stderr bytes.Buffer
	if code := Run([]string{"get", "does-not-exist", "--addr=" + addr}, &stdout, &stderr); code != 0 {
		t.Fatalf("kvctl get exited %d, stderr=%q", code, stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "(not found)" {
		t.Fatalf("kvctl get stdout = %q, want %q", got, "(not found)")
	}
}

// TestEndToEndLinearizableGetIsUnimplemented confirms that a
// linearizable read is surfaced as a clear "not yet implemented" error
// rather than silently served stale, since Raft Core's ReadIndex isn't
// available yet (see stubnode.go and kvserver.go's Get method).
func TestEndToEndLinearizableGetIsUnimplemented(t *testing.T) {
	addr := startTestServer(t)

	var stdout, stderr bytes.Buffer
	code := Run([]string{"get", "foo", "--addr=" + addr, "--consistency=linearizable"}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("expected non-zero exit for unimplemented linearizable read, stdout=%q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "not yet available") {
		t.Fatalf("stderr = %q, want it to mention linearizable reads not yet being available", stderr.String())
	}
}

// startTestServer starts a KVService server backed by
// server.NewSingleNodeStubServer on an OS-assigned localhost port and
// returns its address. The server is stopped when the test completes.
func startTestServer(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	kvpb.RegisterKVServiceServer(grpcServer, server.NewSingleNodeStubServer("test-node"))

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(grpcServer.Stop)

	return lis.Addr().String()
}
