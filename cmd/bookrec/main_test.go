package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeClock advances 100 ms per call, so a recording is reproducible.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(100 * time.Millisecond)
	return c.now
}

func TestRecordThenReplay(t *testing.T) {
	dir := t.TempDir()
	evidence := filepath.Join(dir, "book.evidence.jsonl")
	trade := func(qty, acml string) string {
		f := make([]string, 46)
		for i := range f {
			f[i] = "0"
		}
		f[0], f[1], f[2], f[12], f[13], f[21] = "005930", "090000", "71000", qty, acml, "1"
		return "0|H0STCNT0|001|" + strings.Join(f, "^")
	}
	frames := strings.Join([]string{
		`{"header":{"tr_id":"PINGPONG"}}`,
		trade("10", "10"),
		"",
		trade("10", "10"), // duplicate
		trade("5", "40"),  // gap of 25
	}, "\n") + "\n"

	clock := &fakeClock{now: time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)}
	var stderr bytes.Buffer
	if code := run([]string{"record", "-out", evidence}, strings.NewReader(frames), &stderr, &stderr, clock.Now); code != 0 {
		t.Fatalf("record exit %d: %s", code, stderr.String())
	}

	replayOnce := func() string {
		t.Helper()
		var stdout, stderr bytes.Buffer
		if code := run([]string{"replay", "-in", evidence}, nil, &stdout, &stderr, clock.Now); code != 0 {
			t.Fatalf("replay exit %d: %s", code, stderr.String())
		}
		if !strings.Contains(stderr.String(), `"applied":2,"duplicates":1,"regressed":0,"gaps":1,"missing_vol":25`) {
			t.Fatalf("summary: %s", stderr.String())
		}
		return stdout.String()
	}
	first := replayOnce()
	if first != replayOnce() {
		t.Fatal("two replays differ")
	}
	if got := strings.Count(first, "\n"); got != 4 { // session, trade, gap, trade
		t.Fatalf("%d events:\n%s", got, first)
	}

	out := filepath.Join(dir, "events.jsonl")
	if code := run([]string{"replay", "-in", evidence, "-out", out}, nil, &bytes.Buffer{}, &bytes.Buffer{}, clock.Now); code != 0 {
		t.Fatal("replay -out failed")
	}
	if written, _ := os.ReadFile(out); string(written) != first {
		t.Fatal("-out differs from stdout")
	}
}

func TestUsageErrors(t *testing.T) {
	for _, args := range [][]string{nil, {"send"}, {"record"}, {"replay"}, {"record", "-out", "x", "-stale", "0s"}} {
		var stderr bytes.Buffer
		if code := run(args, strings.NewReader(""), &stderr, &stderr, time.Now); code == 0 {
			t.Errorf("%v: exit 0", args)
		}
	}
}
