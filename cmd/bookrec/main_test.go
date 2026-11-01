package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/book"
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
	for _, args := range [][]string{nil, {"send"}, {"record"}, {"replay"}, {"record", "-out", "x", "-stale", "0s"}, {"record", "-out", "x", "-stale", "3ns"}} {
		var stderr bytes.Buffer
		if code := run(args, strings.NewReader(""), &stderr, &stderr, time.Now); code == 0 {
			t.Errorf("%v: exit 0", args)
		}
	}
}

func TestReplayRefusesToOverwriteItsInput(t *testing.T) {
	dir := t.TempDir()
	evidence := filepath.Join(dir, "book.evidence.jsonl")
	clock := &fakeClock{now: time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)}
	if code := run([]string{"record", "-out", evidence}, strings.NewReader(""), io.Discard, io.Discard, clock.Now); code != 0 {
		t.Fatal("record failed")
	}
	before, _ := os.ReadFile(evidence)
	link := filepath.Join(dir, "link.jsonl")
	if err := os.Symlink(evidence, link); err != nil {
		t.Fatal(err)
	}
	for _, out := range []string{evidence, link, filepath.Join(dir, ".", "book.evidence.jsonl")} {
		var stderr bytes.Buffer
		if code := run([]string{"replay", "-in", evidence, "-out", out}, nil, io.Discard, &stderr, clock.Now); code != 1 || !strings.Contains(stderr.String(), "-out names the -in") {
			t.Fatalf("-out %s: exit %d, %s", out, code, stderr.String())
		}
	}
	if after, _ := os.ReadFile(evidence); !bytes.Equal(before, after) || len(after) == 0 {
		t.Fatal("evidence changed")
	}
}

type failAfter struct{ writes int }

func (w *failAfter) Write(p []byte) (int, error) {
	if w.writes == 0 {
		return 0, errors.New("disk full")
	}
	w.writes--
	return len(p), nil
}

// A Tick that fails to write ends the recording even though stdin is open
// and idle.
func TestRecordStopsOnTickFailureWhileStdinIdle(t *testing.T) {
	start := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	rec, err := book.NewRecorder(&failAfter{writes: 2}, time.Second, start) // session + one frame
	if err != nil {
		t.Fatal(err)
	}
	stdin, feed := io.Pipe()
	defer feed.Close()
	tick := make(chan time.Time)
	// The first reading stamps the frame; every later one (the Tick) is 5 s
	// on, when the stream is stale and its stale line cannot be written.
	var calls atomic.Int32
	clock := func() time.Time {
		if calls.Add(1) == 1 {
			return start
		}
		return start.Add(5 * time.Second)
	}
	result := make(chan error, 1)
	go func() { result <- recordFrom(rec, stdin, tick, clock) }()

	frame := "0|H0STCNT0|001|005930^090000^71000" + strings.Repeat("^0", 9) + "^10^10" + strings.Repeat("^0", 7) + "^1" + strings.Repeat("^0", 24) + "\n"
	if _, err := io.WriteString(feed, frame); err != nil {
		t.Fatal(err)
	}
	for calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	select {
	case tick <- start:
	case <-time.After(5 * time.Second):
		t.Fatal("record loop did not take the tick")
	}
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "disk full") {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("record kept running after a failed Tick")
	}
}
