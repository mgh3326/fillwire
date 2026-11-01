package book

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"testing"
)

// replayBytes replays evidence and returns the JSON event stream followed by
// the final books, the form a consumer would persist or diff.
func replayBytes(t *testing.T, evidence []byte) ([]byte, State) {
	t.Helper()
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	state, err := Replay(bytes.NewReader(evidence), func(e Event) error { return enc.Encode(e) })
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range state.SortedBooks() {
		if err := enc.Encode(b); err != nil {
			t.Fatal(err)
		}
	}
	return out.Bytes(), state
}

// The committed evidence file is what recordScenario writes, byte for byte.
func TestRecorderWritesScenarioEvidence(t *testing.T) {
	golden(t, "scenario.evidence.jsonl", recordScenario(t))
}

func TestReplayIsByteIdentical(t *testing.T) {
	evidence, err := os.ReadFile("testdata/scenario.evidence.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	first, state := replayBytes(t, evidence)
	second, _ := replayBytes(t, evidence)
	if !bytes.Equal(first, second) {
		t.Fatalf("two replays of the same file differ:\n%s\n---\n%s", first, second)
	}
	golden(t, "scenario.replay.jsonl", first)

	want := Summary{
		Sessions: 1, Frames: 12, Records: 12, Applied: 10, Duplicates: 2,
		Gaps: 2, MissingVol: 180, Stale: 4, Reconnects: 1, BadFrames: 1,
	}
	if state.Summary != want {
		t.Fatalf("summary = %+v, want %+v", state.Summary, want)
	}
	if len(state.Tape) != 5 || state.Tape[4].AcmlVol != 450 {
		t.Fatalf("tape = %+v", state.Tape)
	}
	books := state.SortedBooks()
	if len(books) != 2 || books[0].Symbol != "000660" || books[1].Asks[0].Price != 71300 || !books[1].Synced {
		t.Fatalf("books = %+v", books)
	}
}

func TestReplayCountsVerdictMismatch(t *testing.T) {
	evidence := recordScenario(t)
	tampered := bytes.Replace(evidence, []byte(`"class":"duplicate"`), []byte(`"class":"ok"`), 1)
	if bytes.Equal(tampered, evidence) {
		t.Fatal("fixture has no duplicate verdict to tamper with")
	}
	got, state := replayBytes(t, tampered)
	want, _ := replayBytes(t, evidence)
	if state.Summary.VerdictMismatches != 1 {
		t.Fatalf("mismatches = %d, want 1", state.Summary.VerdictMismatches)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("recorded verdicts changed the replay; replay must recompute them")
	}
}

func TestReplayRejectsUnknownFormat(t *testing.T) {
	for name, evidence := range map[string]string{
		"version":    `{"v":2,"seq":1,"kind":"session","at_ns":0}` + "\n",
		"kind":       `{"v":1,"seq":1,"kind":"session","at_ns":0}` + "\n" + `{"v":1,"seq":2,"kind":"order","at_ns":0}` + "\n",
		"no session": `{"v":1,"seq":1,"kind":"reconnect","at_ns":0}` + "\n",
	} {
		if _, err := Replay(bytes.NewReader([]byte(evidence)), nil); !errors.Is(err, ErrFormat) {
			t.Fatalf("%s: err = %v, want ErrFormat", name, err)
		}
	}
}

func TestReplayStopsOnEmitError(t *testing.T) {
	stop := errors.New("consumer full")
	calls := 0
	_, err := Replay(bytes.NewReader(recordScenario(t)), func(Event) error {
		calls++
		if calls == 3 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || calls != 3 {
		t.Fatalf("err = %v after %d calls", err, calls)
	}
}

// Only a crash can tear a line, and a crash ends the session: the torn line
// is the last one, or the next line opens a new session. Anything else is
// corruption that would silently drop frames, so replay fails.
func TestReplayRejectsMidFileCorruption(t *testing.T) {
	evidence := recordScenario(t)
	nl := bytes.IndexByte(evidence, '\n') + 1
	second := bytes.IndexByte(evidence[nl:], '\n') + nl + 1
	torn := []byte(`{"v":1,"seq":99,"ki` + "\n")
	session := []byte(`{"v":1,"seq":1,"kind":"session","at_ns":1}` + "\n")

	cases := map[string]struct {
		evidence []byte
		want     error
	}{
		"torn tail":           {append(append([]byte{}, evidence...), torn[:len(torn)-1]...), nil},
		"torn before session": {append(append(append([]byte{}, evidence...), torn...), session...), nil},
		"mid-session":         {append(append(append([]byte{}, evidence[:second]...), torn...), evidence[second:]...), ErrCorrupt},
		"torn before frame":   {append(append(append([]byte{}, evidence[:nl]...), torn...), evidence[nl:]...), ErrCorrupt},
		"two invalid at end":  {append(append(append([]byte{}, evidence...), torn...), torn...), ErrCorrupt},
	}
	for name, c := range cases {
		state, err := Replay(bytes.NewReader(c.evidence), nil)
		if !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", name, err, c.want)
		}
		if c.want == nil && state.Summary.TornLines != 1 {
			t.Errorf("%s: torn lines = %d, want 1", name, state.Summary.TornLines)
		}
	}
}
