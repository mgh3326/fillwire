package main

import (
	"bytes"
	"flag"
	"os"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite testdata/sim.golden.jsonl")

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
	if *update {
		if err := os.WriteFile(golden, []byte(first), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if first != string(want) {
		t.Fatalf("output differs from %s:\n%s", golden, first)
	}
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
