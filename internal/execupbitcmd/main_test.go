package execupbitcmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func runArgs(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := Run(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

// TestDryRunReplayGolden pins the whole recorded sequence: book lines,
// child requests, the expiry cancel and the finish.
func TestDryRunReplayGolden(t *testing.T) {
	const dir = "../../cmd/execupbit/testdata"
	golden, err := os.ReadFile(filepath.Join(dir, "dry-run.golden.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr := runArgs(t,
		"-dry-run",
		"-intent", filepath.Join(dir, "intent.json"),
		"-books", filepath.Join(dir, "books.jsonl"),
	)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if stdout != string(golden) {
		t.Fatalf("output differs from golden\n--- got ---\n%s\n--- want ---\n%s", stdout, golden)
	}
	// The sequence must actually carry the decisions the brief names.
	for _, want := range []string{`"reason":"open"`, `"reason":"requote"`, `"reason":"band"`, `"reason":"requote_limit"`, `"reason":"deadline"`, `"done":"deadline"`, `"quantity":"33"`} {
		if !strings.Contains(stdout, want) {
			t.Errorf("output lacks %s", want)
		}
	}
}

// TestShadowOnlyModes proves the command cannot send: it has no mode but
// -dry-run, and a send-shaped flag is rejected as an unknown flag.
func TestShadowOnlyModes(t *testing.T) {
	cases := []struct {
		name   string
		args   []string
		code   int
		stderr string
	}{
		{"no dry-run", []string{"-intent", "../../cmd/execupbit/testdata/intent.json", "-books", "../../cmd/execupbit/testdata/books.jsonl"}, 2, "shadow only"},
		{"send flag", []string{"-dry-run", "-send", "-intent", "../../cmd/execupbit/testdata/intent.json", "-books", "../../cmd/execupbit/testdata/books.jsonl"}, 2, "flag provided but not defined: -send"},
		{"no intent", []string{"-dry-run", "-books", "../../cmd/execupbit/testdata/books.jsonl"}, 2, "usage:"},
		{"two sources", []string{"-dry-run", "-intent", "../../cmd/execupbit/testdata/intent.json", "-books", "../../cmd/execupbit/testdata/books.jsonl", "-live"}, 2, "usage:"},
		{"no source", []string{"-dry-run", "-intent", "../../cmd/execupbit/testdata/intent.json"}, 2, "usage:"},
		{"live without root", []string{"-dry-run", "-intent", "../../cmd/execupbit/testdata/intent.json", "-live"}, 2, "-live needs -root"},
		{"zero stale", []string{"-dry-run", "-intent", "../../cmd/execupbit/testdata/intent.json", "-books", "../../cmd/execupbit/testdata/books.jsonl", "-stale", "0s"}, 2, "usage:"},
	}
	for _, c := range cases {
		code, stdout, stderr := runArgs(t, c.args...)
		if code != c.code || !strings.Contains(stderr, c.stderr) {
			t.Errorf("%s: exit %d, stderr %q; want %d with %q", c.name, code, stderr, c.code, c.stderr)
		}
		if stdout != "" {
			t.Errorf("%s: wrote %q before failing", c.name, stdout)
		}
	}
}

func TestInvalidIntentFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "intent.json")
	if err := os.WriteFile(path, []byte(`{"schema_version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	code, _, stderr := runArgs(t, "-dry-run", "-intent", path, "-books", "../../cmd/execupbit/testdata/books.jsonl")
	if code != 1 || !strings.Contains(stderr, "invalid intent") {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
}

func TestUnreadableBookFileFails(t *testing.T) {
	code, _, stderr := runArgs(t, "-dry-run", "-intent", "../../cmd/execupbit/testdata/intent.json", "-books", filepath.Join(t.TempDir(), "absent.jsonl"))
	if code != 1 || stderr == "" {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
}
