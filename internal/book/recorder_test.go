package book

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func newTestRecorder(t *testing.T) (*Recorder, *bytes.Buffer) {
	t.Helper()
	var out bytes.Buffer
	rec, err := NewRecorder(&out, DefaultStaleAfter, at(0))
	if err != nil {
		t.Fatal(err)
	}
	return rec, &out
}

func lines(t *testing.T, raw []byte) []Line {
	t.Helper()
	var out []Line
	for _, chunk := range bytes.Split(bytes.TrimRight(raw, "\n"), []byte("\n")) {
		var l Line
		if err := json.Unmarshal(chunk, &l); err != nil {
			t.Fatalf("line %q: %v", chunk, err)
		}
		out = append(out, l)
	}
	return out
}

// verdicts returns the verdict of each frame line, one per record, in order.
func verdicts(t *testing.T, raw []byte) []Verdict {
	t.Helper()
	var out []Verdict
	for _, l := range lines(t, raw) {
		if l.Kind == KindFrame {
			out = append(out, l.Verdicts...)
		}
	}
	return out
}

func staleLines(t *testing.T, raw []byte) []Line {
	t.Helper()
	var out []Line
	for _, l := range lines(t, raw) {
		if l.Kind == KindStale {
			out = append(out, l)
		}
	}
	return out
}

func feed(t *testing.T, rec *Recorder, frames ...[]byte) {
	t.Helper()
	for i, f := range frames {
		if err := rec.Frame(f, at(i*100)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRecorderGap(t *testing.T) {
	rec, out := newTestRecorder(t)
	feed(t, rec,
		tradeFrame("005930", "090000", 71000, 100, 100), // baseline
		tradeFrame("005930", "090000", 71000, 50, 150),  // continues: 150-50 == 100
		tradeFrame("005930", "090001", 71100, 30, 300),  // 300-30 = 270, 120 shares never arrived
		tradeFrame("005930", "090001", 71100, 10, 310),  // continues from the new baseline
		tradeFrame("000660", "090001", 180000, 5, 900),  // other symbol: own baseline
	)
	want := []Verdict{
		{Symbol: "005930", Class: ClassOK},
		{Symbol: "005930", Class: ClassOK},
		{Symbol: "005930", Class: ClassGap, MissingVol: 120},
		{Symbol: "005930", Class: ClassOK},
		{Symbol: "000660", Class: ClassOK},
	}
	if got := verdicts(t, out.Bytes()); !reflect.DeepEqual(got, want) {
		t.Fatalf("verdicts = %+v, want %+v", got, want)
	}
}

func TestRecorderDuplicate(t *testing.T) {
	rec, out := newTestRecorder(t)
	trade := tradeFrame("005930", "090000", 71000, 50, 150)
	book := bookFrame("005930", "090000", 71100, 71000, 10)
	feed(t, rec,
		tradeFrame("005930", "090000", 71000, 100, 100),
		trade,
		trade, // byte-identical resend
		frame(TRTrade, tradeRecord("005930", "090000", 71000, 50, 150, "5")), // same volume, other side code
		tradeFrame("005930", "085959", 70900, 40, 140),                       // cumulative volume went backwards
		book,
		book, // byte-identical resend
		bookFrame("005930", "085959", 71000, 70900, 10), // older exchange time
		bookFrame("005930", "090000", 71200, 71100, 10), // same second, new levels: applied
	)
	want := []Verdict{
		{Symbol: "005930", Class: ClassOK},
		{Symbol: "005930", Class: ClassOK},
		{Symbol: "005930", Class: ClassDuplicate},
		{Symbol: "005930", Class: ClassDuplicate},
		{Symbol: "005930", Class: ClassRegress},
		{Symbol: "005930", Class: ClassOK},
		{Symbol: "005930", Class: ClassDuplicate},
		{Symbol: "005930", Class: ClassRegress},
		{Symbol: "005930", Class: ClassOK},
	}
	if got := verdicts(t, out.Bytes()); !reflect.DeepEqual(got, want) {
		t.Fatalf("verdicts = %+v, want %+v", got, want)
	}
}

func TestRecorderStale(t *testing.T) {
	rec, out := newTestRecorder(t)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(rec.Frame(bookFrame("005930", "090000", 71100, 71000, 10), at(0)))
	must(rec.Frame(tradeFrame("005930", "090000", 71000, 10, 10), at(500)))

	must(rec.Tick(at(2000))) // book silent exactly 2 s: not stale
	if got := staleLines(t, out.Bytes()); len(got) != 0 {
		t.Fatalf("stale at exactly 2 s: %+v", got)
	}
	must(rec.Tick(at(2001))) // book silent 2.001 s: stale, trade (1.501 s) not
	must(rec.Tick(at(2400))) // flagged once per silence
	want := []Line{{V: 1, Seq: 4, Kind: KindStale, AtNS: at(2001).UnixNano(), TR: TRBook, Symbol: "005930", SinceNS: at(0).UnixNano()}}
	if got := staleLines(t, out.Bytes()); !reflect.DeepEqual(got, want) {
		t.Fatalf("stale lines = %+v, want %+v", got, want)
	}

	// The flagged book recovers without another stale line. The trade stream,
	// never flagged by a Tick, is flagged on arrival after 2.6 s of silence,
	// and its stale line precedes the frame that ended the silence.
	must(rec.Frame(bookFrame("005930", "090003", 71200, 71100, 10), at(3000)))
	must(rec.Frame(tradeFrame("005930", "090003", 71100, 10, 20), at(3100)))
	all := lines(t, out.Bytes())
	tail := all[len(all)-3:]
	if tail[0].Kind != KindFrame || tail[1].Kind != KindStale || tail[2].Kind != KindFrame {
		t.Fatalf("tail kinds = %s %s %s, want frame stale frame", tail[0].Kind, tail[1].Kind, tail[2].Kind)
	}
	if tail[1].TR != TRTrade || tail[1].SinceNS != at(500).UnixNano() || tail[1].AtNS != at(3100).UnixNano() {
		t.Fatalf("arrival stale line = %+v", tail[1])
	}
	if got := len(staleLines(t, out.Bytes())); got != 2 {
		t.Fatalf("%d stale lines, want 2", got)
	}
}

func TestRecorderReconnectResync(t *testing.T) {
	rec, out := newTestRecorder(t)
	book := bookFrame("005930", "090000", 71100, 71000, 10)
	feed(t, rec,
		book,
		tradeFrame("005930", "090000", 71000, 100, 100),
		tradeFrame("000660", "090000", 180000, 900, 900),
	)
	if err := rec.Reconnect(at(1000), 1); err != nil {
		t.Fatal(err)
	}
	must := func(raw []byte, ms int) {
		t.Helper()
		if err := rec.Frame(raw, at(ms)); err != nil {
			t.Fatal(err)
		}
	}
	must(book, 1100)                                           // identical snapshot after reconnect is a resync, not a duplicate
	must(book, 1150)                                           // now it is a duplicate again
	must(tradeFrame("005930", "090010", 71200, 10, 160), 1200) // 50 shares traded during the outage
	must(tradeFrame("005930", "090010", 71200, 10, 170), 1250) // resync done: plain continuation
	must(tradeFrame("000660", "090000", 180000, 30, 30), 1300) // cumulative volume restarted: new day

	want := []Verdict{
		{Symbol: "005930", Class: ClassOK},
		{Symbol: "005930", Class: ClassOK},
		{Symbol: "000660", Class: ClassOK},
		{Symbol: "005930", Class: ClassOK, Resync: true},
		{Symbol: "005930", Class: ClassDuplicate},
		{Symbol: "005930", Class: ClassGap, Resync: true, MissingVol: 50},
		{Symbol: "005930", Class: ClassOK},
		{Symbol: "000660", Class: ClassOK, Resync: true},
	}
	if got := verdicts(t, out.Bytes()); !reflect.DeepEqual(got, want) {
		t.Fatalf("verdicts = %+v, want %+v", got, want)
	}
	reconnects := 0
	for _, l := range lines(t, out.Bytes()) {
		if l.Kind == KindReconnect {
			reconnects++
			if l.Attempt != 1 || l.AtNS != at(1000).UnixNano() {
				t.Fatalf("reconnect line = %+v", l)
			}
		}
	}
	if reconnects != 1 {
		t.Fatalf("%d reconnect lines, want 1", reconnects)
	}

	// Replay holds the book unsynced from the reconnect until its snapshot.
	raw := out.Bytes()
	cut := bytes.Index(raw, []byte(`"kind":"reconnect"`))
	cut += bytes.IndexByte(raw[cut:], '\n') + 1
	before, err := Replay(bytes.NewReader(raw[:cut]), nil)
	if err != nil {
		t.Fatal(err)
	}
	after, err := Replay(bytes.NewReader(raw), nil)
	if err != nil {
		t.Fatal(err)
	}
	if before.Books["005930"].Synced || !after.Books["005930"].Synced {
		t.Fatalf("synced before snapshot = %v, after = %v", before.Books["005930"].Synced, after.Books["005930"].Synced)
	}
}

func TestRecorderIgnoresAndKeepsBadFrames(t *testing.T) {
	rec, out := newTestRecorder(t)
	bad := []byte("0|H0STCNT0|001|005930^09")
	feed(t, rec,
		[]byte(`{"header":{"tr_id":"PINGPONG"}}`),
		[]byte("0|H0STCNI0|001|secret^fill"),
		[]byte("1|H0STCNT0|001|ciphertext"),
		bad,
		[]byte("0|H0STASP0|002|"+bookRecord("005930", "090000", 1, 1, 1)), // 59 fields do not split into 2 records
	)
	got := lines(t, out.Bytes())
	if len(got) != 4 { // session + three recorded bad frames
		t.Fatalf("%d lines, want 4: %s", len(got), out.Bytes())
	}
	for _, l := range got[1:] {
		if l.Kind != KindFrame || l.Error == "" || l.Verdicts != nil {
			t.Fatalf("bad frame line = %+v", l)
		}
	}
	if got[2].Raw != string(bad) {
		t.Fatalf("raw = %q, want %q", got[2].Raw, bad)
	}
	if strings.Contains(out.String(), "secret") {
		t.Fatal("execution notice recorded")
	}
}

func TestRecorderKeepsNonUTF8RawExact(t *testing.T) {
	rec, out := newTestRecorder(t)
	raw := []byte("0|H0STCNT0|001|\xff\xfe")
	feed(t, rec, raw)
	got := lines(t, out.Bytes())[1]
	if got.Raw != "" || !bytes.Equal(got.raw(), raw) {
		t.Fatalf("line = %+v", got)
	}
}

type failingWriter struct{ n int }

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.n == 0 {
		return 0, errors.New("disk full")
	}
	w.n--
	return len(p), nil
}

func TestRecorderWriteErrorIsSticky(t *testing.T) {
	rec, err := NewRecorder(&failingWriter{n: 1}, 0, at(0))
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Frame(bookFrame("005930", "090000", 2, 1, 1), at(1)); err == nil {
		t.Fatal("write error not returned")
	}
	if rec.Tick(at(9000)) == nil || rec.Reconnect(at(9001), 1) == nil || rec.Err() == nil {
		t.Fatal("write error not sticky")
	}
}

func TestOpenEvidenceAppendsOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "book.evidence.jsonl")
	record := func(ms int) {
		t.Helper()
		f, err := OpenEvidence(path)
		if err != nil {
			t.Fatal(err)
		}
		rec, err := NewRecorder(f, 0, at(ms))
		if err != nil {
			t.Fatal(err)
		}
		if err := rec.Frame(bookFrame("005930", "090000", 2, 1, 1), at(ms+1)); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	record(0)
	first, _ := os.ReadFile(path)
	// A crash mid-write leaves a torn last line.
	if err := os.WriteFile(path, append(first, `{"v":1,"seq":3,"ki`...), 0o640); err != nil {
		t.Fatal(err)
	}
	record(1000)
	all, _ := os.ReadFile(path)
	if !bytes.HasPrefix(all, first) {
		t.Fatal("second session rewrote the first")
	}
	state, err := Replay(bytes.NewReader(all), nil)
	if err != nil {
		t.Fatal(err)
	}
	s := state.Summary
	if s.Sessions != 2 || s.Frames != 2 || s.TornLines != 1 || s.VerdictMismatches != 0 {
		t.Fatalf("summary = %+v", s)
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0o640 {
		t.Fatalf("mode = %v", info.Mode().Perm())
	}
}
