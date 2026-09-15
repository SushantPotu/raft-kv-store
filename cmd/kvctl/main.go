// Command kvctl is a small CLI client for KVService (Workstream C:
// Client/API Protocol Layer). It deliberately uses only the standard
// library's flag package plus a hand-rolled subcommand dispatch — no
// third-party CLI framework.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/SushantPotu/raft-kv-store/pkg/kvpb"
)

func main() {
	os.Exit(Run(os.Args[1:], os.Stdout, os.Stderr))
}

// Run executes kvctl's CLI logic and returns a process exit code. It's
// exported (rather than living unexported in main) so tests — notably
// Workstream C's own end-to-end integration test — can drive the CLI
// in-process against a local KVService server without shelling out to a
// built binary.
func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usage())
		return 2
	}

	sub := args[0]
	rest := args[1:]

	switch sub {
	case "get":
		return runGet(rest, stdout, stderr)
	case "put":
		return runPut(rest, stdout, stderr)
	case "delete":
		return runDelete(rest, stdout, stderr)
	case "-h", "--help", "help":
		fmt.Fprintln(stdout, usage())
		return 0
	default:
		fmt.Fprintf(stderr, "kvctl: unknown subcommand %q\n\n%s\n", sub, usage())
		return 2
	}
}

func usage() string {
	return `kvctl - client for KVService

Usage:
  kvctl get <key> [--addr=host:port] [--consistency=stale|linearizable] [--shard=id]
  kvctl put <key> <value> [--addr=host:port] [--shard=id]
  kvctl delete <key> [--addr=host:port] [--shard=id]

Flags:
  --addr         KVService gRPC target (default "localhost:7000")
  --consistency  "stale" (default) or "linearizable" (get only)
  --shard        shard_id to send on the request (default "")
  --timeout      per-request timeout (default 5s)`
}

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// splitArgs separates args into flags ("-x", "--x", "--x=y") and
// positional arguments, preserving each group's relative order. The
// standard library's flag package stops parsing at the first non-flag
// argument, which would otherwise force callers to always write flags
// before positional arguments (e.g. "kvctl get --addr=x foo" but not
// "kvctl get foo --addr=x"); splitting up front lets kvctl accept either
// order, e.g. `kvctl get foo --consistency=linearizable` as shown in
// usage(), while still using flag.FlagSet for the actual flag parsing.
func splitArgs(args []string) (flags, positional []string) {
	for _, a := range args {
		if strings.HasPrefix(a, "-") {
			flags = append(flags, a)
		} else {
			positional = append(positional, a)
		}
	}
	return flags, positional
}

func runGet(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", "localhost:7000", "KVService gRPC target")
	consistency := fs.String("consistency", "stale", `"stale" or "linearizable"`)
	shard := fs.String("shard", "", "shard_id")
	timeout := fs.Duration("timeout", 5*time.Second, "request timeout")
	flagArgs, positional := splitArgs(args)
	if err := fs.Parse(flagArgs); err != nil {
		return 2
	}
	if len(positional) != 1 {
		fmt.Fprintln(stderr, "kvctl get: expected exactly one argument: <key>")
		return 2
	}
	key := positional[0]

	var c kvpb.Consistency
	switch *consistency {
	case "stale", "":
		c = kvpb.Consistency_CONSISTENCY_STALE
	case "linearizable":
		c = kvpb.Consistency_CONSISTENCY_LINEARIZABLE
	default:
		fmt.Fprintf(stderr, "kvctl get: unknown --consistency %q (want stale|linearizable)\n", *consistency)
		return 2
	}

	conn, err := dial(*addr)
	if err != nil {
		fmt.Fprintf(stderr, "kvctl get: dial %s: %v\n", *addr, err)
		return 1
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	resp, err := kvpb.NewKVServiceClient(conn).Get(ctx, &kvpb.GetRequest{
		ShardId:     *shard,
		Key:         []byte(key),
		Consistency: c,
	})
	if err != nil {
		fmt.Fprintf(stderr, "kvctl get: %v\n", err)
		return 1
	}
	if !resp.GetFound() {
		fmt.Fprintln(stdout, "(not found)")
		return 0
	}
	fmt.Fprintln(stdout, string(resp.GetValue()))
	return 0
}

func runPut(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("put", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", "localhost:7000", "KVService gRPC target")
	shard := fs.String("shard", "", "shard_id")
	timeout := fs.Duration("timeout", 5*time.Second, "request timeout")
	flagArgs, positional := splitArgs(args)
	if err := fs.Parse(flagArgs); err != nil {
		return 2
	}
	if len(positional) != 2 {
		fmt.Fprintln(stderr, "kvctl put: expected exactly two arguments: <key> <value>")
		return 2
	}
	key, value := positional[0], positional[1]

	conn, err := dial(*addr)
	if err != nil {
		fmt.Fprintf(stderr, "kvctl put: dial %s: %v\n", *addr, err)
		return 1
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	resp, err := kvpb.NewKVServiceClient(conn).Put(ctx, &kvpb.PutRequest{
		ShardId: *shard,
		Key:     []byte(key),
		Value:   []byte(value),
	})
	if err != nil {
		fmt.Fprintf(stderr, "kvctl put: %v\n", err)
		return 1
	}
	if hint := resp.GetLeaderHint(); hint != "" {
		fmt.Fprintf(stderr, "kvctl put: not leader, try %s\n", hint)
		return 1
	}
	fmt.Fprintln(stdout, "OK")
	return 0
}

func runDelete(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("delete", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", "localhost:7000", "KVService gRPC target")
	shard := fs.String("shard", "", "shard_id")
	timeout := fs.Duration("timeout", 5*time.Second, "request timeout")
	flagArgs, positional := splitArgs(args)
	if err := fs.Parse(flagArgs); err != nil {
		return 2
	}
	if len(positional) != 1 {
		fmt.Fprintln(stderr, "kvctl delete: expected exactly one argument: <key>")
		return 2
	}
	key := positional[0]

	conn, err := dial(*addr)
	if err != nil {
		fmt.Fprintf(stderr, "kvctl delete: dial %s: %v\n", *addr, err)
		return 1
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	resp, err := kvpb.NewKVServiceClient(conn).Delete(ctx, &kvpb.DeleteRequest{
		ShardId: *shard,
		Key:     []byte(key),
	})
	if err != nil {
		fmt.Fprintf(stderr, "kvctl delete: %v\n", err)
		return 1
	}
	if hint := resp.GetLeaderHint(); hint != "" {
		fmt.Fprintf(stderr, "kvctl delete: not leader, try %s\n", hint)
		return 1
	}
	fmt.Fprintln(stdout, "OK")
	return 0
}
