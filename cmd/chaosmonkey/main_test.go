package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRunEndToEnd drives chaosmonkey's Run function in-process (no
// subprocess) against real, small, fast clusters — proving the CLI
// wiring (flag parsing, running real trials via internal/chaos, writing
// CSV, printing a summary, running the catch-up check) all actually
// works end to end, not just that internal/chaos's own unit tests pass
// in isolation. Trial/cluster counts are kept tiny so this runs quickly
// as part of the normal test suite.
func TestRunEndToEnd(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "results.csv")

	var stdout, stderr bytes.Buffer
	code := Run([]string{
		"--nodes=3",
		"--trials=2",
		"--also-sizes=",
		"--out=" + outPath,
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("Run exited %d, stderr=%q", code, stderr.String())
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read %s: %v", outPath, err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 3 { // header + 2 trial rows
		t.Fatalf("csv has %d lines, want 3 (header + 2 trials); content:\n%s", len(lines), data)
	}
	if !strings.HasPrefix(lines[0], "trial,nodes,killed_node,new_leader,failover_ms") {
		t.Fatalf("unexpected csv header: %q", lines[0])
	}

	out := stdout.String()
	if !strings.Contains(out, "wrote 2 trial rows") {
		t.Fatalf("stdout missing row-count line; got:\n%s", out)
	}
	if !strings.Contains(out, "nodes=3") {
		t.Fatalf("stdout missing per-size summary; got:\n%s", out)
	}
	if !strings.Contains(out, "catch-up check OK") {
		t.Fatalf("stdout missing catch-up check result; got:\n%s", out)
	}
}

// TestRunValidatesFlags confirms obviously-invalid flag values are
// rejected before any cluster is started, rather than failing confusingly
// deep inside internal/chaos.
func TestRunValidatesFlags(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"zero nodes", []string{"--nodes=0"}},
		{"negative trials", []string{"--trials=-1"}},
		{"bad also-sizes", []string{"--also-sizes=abc"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := Run(tc.args, &stdout, &stderr); code != 2 {
				t.Fatalf("Run(%v) = %d, want 2; stderr=%q", tc.args, code, stderr.String())
			}
		})
	}
}

func TestParseSizes(t *testing.T) {
	cases := []struct {
		in      string
		want    []int
		wantErr bool
	}{
		{"", nil, false},
		{"  ", nil, false},
		{"5", []int{5}, false},
		{"3,5,7", []int{3, 5, 7}, false},
		{" 3 , 5 ", []int{3, 5}, false},
		{"0", nil, true},
		{"abc", nil, true},
	}
	for _, tc := range cases {
		got, err := parseSizes(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseSizes(%q) = %v, nil; want error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseSizes(%q) unexpected error: %v", tc.in, err)
			continue
		}
		if len(got) != len(tc.want) {
			t.Errorf("parseSizes(%q) = %v, want %v", tc.in, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("parseSizes(%q) = %v, want %v", tc.in, got, tc.want)
				break
			}
		}
	}
}
