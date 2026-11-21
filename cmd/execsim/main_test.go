package main

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/fillwire/internal/testutil"
)

const (
	evidence = "../../internal/exec/sim/testdata/sim.evidence.jsonl"
	intents  = "../../internal/exec/sim/testdata/sim.intents.jsonl"
	golden   = "testdata/sim.golden.jsonl"
)

func execsim(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestReplayFixtureIsDeterministicAndPrintsBothRules(t *testing.T) {
	code, first, stderr := execsim(t, "-in", evidence, "-intents", intents)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if _, second, _ := execsim(t, "-in", evidence, "-intents", intents); second != first {
		t.Fatalf("second run differs:\n%s\n%s", first, second)
	}
	testutil.Golden(t, golden, []byte(first))
	lines := strings.Split(strings.TrimSpace(first), "\n")
	if len(lines) != 4 {
		t.Fatalf("%d lines, want 2 intents x 2 rules", len(lines))
	}
	for i, rule := range []string{"through", "touch", "through", "touch"} {
		if !strings.Contains(lines[i], `"rule":"`+rule+`"`) || !strings.Contains(lines[i], `"slippage_bps":`) {
			t.Fatalf("line %d is not the %s rule: %s", i, rule, lines[i])
		}
	}
}

func TestUsageErrors(t *testing.T) {
	for _, args := range [][]string{nil, {"-in", evidence}, {"-in", evidence, "-intents", intents, "-step", "0s"}} {
		if code, _, _ := execsim(t, args...); code != 2 {
			t.Errorf("%q: exit %d, want 2", args, code)
		}
	}
	if code, _, _ := execsim(t, "-in", "missing.jsonl", "-intents", intents); code != 1 {
		t.Errorf("missing evidence: exit %d, want 1", code)
	}
}

const (
	upbitBooks   = "testdata/upbit.books.jsonl"
	upbitIntents = "testdata/upbit.intents.jsonl"
	upbitGolden  = "testdata/upbit.golden.jsonl"
)

func TestUpbitReplayIsDeterministicAndPrintsBothRules(t *testing.T) {
	args := []string{"-venue", "upbit", "-intents", upbitIntents, "-books", upbitBooks}
	code, first, stderr := execsim(t, args...)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if _, second, _ := execsim(t, args...); second != first {
		t.Fatal("second run differs")
	}
	testutil.Golden(t, upbitGolden, []byte(first))

	// 5 intents; the stop-loss ones run under two triggers: 3 plain x 2 rules
	// and 2 stops x 2 triggers x 2 rules = 14 results.
	var results []string
	for _, line := range strings.Split(strings.TrimSpace(first), "\n") {
		if strings.HasPrefix(line, `{"kind":"result"`) {
			results = append(results, line)
		}
	}
	if len(results) != 14 {
		t.Fatalf("%d result lines, want 14", len(results))
	}
	for _, rule := range []string{"through", "touch"} {
		if !strings.Contains(first, `"rule":"`+rule+`"`) {
			t.Fatalf("no %s lines", rule)
		}
	}
}

// A recording split across days (the closed day gzip-compressed) replays the
// same as the whole file.
func TestUpbitReplayReadsSplitDaysAndGzip(t *testing.T) {
	whole, err := os.ReadFile(upbitBooks)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(string(whole), "\n")
	cut := len(lines) / 2
	dir := t.TempDir()
	first := filepath.Join(dir, "KRW-XRP.jsonl.gz")
	var gzBuf bytes.Buffer
	zw := gzip.NewWriter(&gzBuf)
	_, _ = zw.Write([]byte(strings.Join(lines[:cut], "")))
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	second := filepath.Join(dir, "second.jsonl")
	if err := os.WriteFile(first, gzBuf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte(strings.Join(lines[cut:], "")), 0o644); err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(upbitGolden)
	if err != nil {
		t.Fatal(err)
	}
	code, got, stderr := execsim(t, "-venue", "upbit", "-intents", upbitIntents, "-books", first, "-books", second)
	if code != 0 || got != string(want) {
		t.Fatalf("exit %d, differs from golden: %s", code, stderr)
	}
	// Out of order files are refused, not replayed.
	if code, _, _ := execsim(t, "-venue", "upbit", "-intents", upbitIntents, "-books", second, "-books", first); code != 1 {
		t.Errorf("out-of-order files: exit %d, want 1", code)
	}
}

func TestUpbitUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		{"-venue", "upbit"},
		{"-venue", "upbit", "-intents", upbitIntents},
		{"-venue", "upbit", "-books", upbitBooks},
		{"-venue", "upbit", "-intents", upbitIntents, "-books", upbitBooks, "-in", evidence},
		{"-venue", "upbit", "-intents", upbitIntents, "-books", upbitBooks, "-step", "5s"},
		{"-venue", "upbit", "-intents", upbitIntents, "-books", upbitBooks, "-stale", "0s"},
		{"-venue", "krx", "-in", evidence, "-intents", intents},
		{"-in", evidence, "-intents", intents, "-books", upbitBooks}, // -books is Upbit's
	} {
		if code, _, _ := execsim(t, args...); code != 2 {
			t.Errorf("%q: exit %d, want 2", args, code)
		}
	}
	if code, _, _ := execsim(t, "-venue", "upbit", "-intents", upbitIntents, "-books", "missing.jsonl"); code != 1 {
		t.Errorf("missing recording: exit %d, want 1", code)
	}
	// A KIS intent is not an Upbit one.
	if code, _, stderr := execsim(t, "-venue", "upbit", "-intents", intents, "-books", upbitBooks); code != 1 || !strings.Contains(stderr, "intents line 1") {
		t.Errorf("KIS intent: exit %d, %q", code, stderr)
	}
}

func TestUpbitRefusesADuplicateIntent(t *testing.T) {
	data, err := os.ReadFile(upbitIntents)
	if err != nil {
		t.Fatal(err)
	}
	first := strings.SplitN(string(data), "\n", 2)[0]
	path := filepath.Join(t.TempDir(), "dup.jsonl")
	if err := os.WriteFile(path, []byte(first+"\n"+first+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code, _, stderr := execsim(t, "-venue", "upbit", "-intents", path, "-books", upbitBooks); code != 1 || !strings.Contains(stderr, "duplicate intent_id") {
		t.Errorf("exit %d, %q", code, stderr)
	}
}
