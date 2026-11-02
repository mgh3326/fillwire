package sim_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/book"
	"github.com/mgh3326/fillwire/internal/exec"
	"github.com/mgh3326/fillwire/internal/exec/sim"
)

var update = flag.Bool("update", false, "rewrite testdata/sim.evidence.jsonl")

// t0 is 2026-11-02 09:00:00 KST, the fixture session start.
var t0 = time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)

func hour(at time.Time) string { return at.Add(9 * time.Hour).Format("150405") }

// bookRaw builds an H0STASP0 frame with a 100 KRW ladder from the touch.
func bookRaw(at time.Time, bid, ask int64) []byte {
	f := []string{"005930", hour(at), "0"}
	for i := int64(0); i < book.Depth; i++ {
		f = append(f, strconv.FormatInt(ask+i*100, 10))
	}
	for i := int64(0); i < book.Depth; i++ {
		f = append(f, strconv.FormatInt(bid-i*100, 10))
	}
	for i := 0; i < 2*book.Depth; i++ {
		f = append(f, "10")
	}
	f = append(f, "100", "100")
	for len(f) < 59 {
		f = append(f, "0")
	}
	return []byte("0|H0STASP0|001|" + strings.Join(f, "^"))
}

// tradeRaw builds an H0STCNT0 frame.
func tradeRaw(at time.Time, price, qty, acml int64) []byte {
	f := make([]string, 46)
	for i := range f {
		f[i] = "0"
	}
	f[0], f[1], f[2] = "005930", hour(at), strconv.FormatInt(price, 10)
	f[12], f[13], f[21] = strconv.FormatInt(qty, 10), strconv.FormatInt(acml, 10), "1"
	return []byte("0|H0STCNT0|001|" + strings.Join(f, "^"))
}

// writeFixture records 40 s of one symbol: the touch is 70,000/70,200 until
// 20 s, then 70,100/70,300; a reconnect at 25 s; trades every 5 s.
func writeFixture(t *testing.T, path string) {
	var buf bytes.Buffer
	rec, err := book.NewRecorder(&buf, 2*time.Second, t0)
	if err != nil {
		t.Fatal(err)
	}
	trades := map[int][2]int64{ // second -> price, qty, printed at +0.5 s
		2: {70_000, 3}, 7: {70_100, 2}, 12: {70_000, 5}, 17: {70_200, 4},
		22: {70_300, 6}, 27: {70_100, 8}, 32: {70_200, 3},
	}
	var acml int64
	for s := 0; s <= 40; s++ {
		at := t0.Add(time.Duration(s) * time.Second)
		if s == 25 {
			if err := rec.Reconnect(at, 1); err != nil {
				t.Fatal(err)
			}
			continue
		}
		bid, ask := int64(70_000), int64(70_200)
		if s >= 20 {
			bid, ask = 70_100, 70_300
		}
		if err := rec.Frame(bookRaw(at, bid, ask), at); err != nil {
			t.Fatal(err)
		}
		if tr, ok := trades[s]; ok {
			acml += tr[1]
			at := at.Add(500 * time.Millisecond)
			if err := rec.Frame(tradeRaw(at, tr[0], tr[1], acml), at); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func load(t *testing.T) ([]byte, []exec.Intent) {
	t.Helper()
	if *update {
		writeFixture(t, "testdata/sim.evidence.jsonl")
	}
	evidence, err := os.ReadFile("testdata/sim.evidence.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("testdata/sim.intents.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	var intents []exec.Intent
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		in, err := exec.ParseIntent(line)
		if err != nil {
			t.Fatal(err)
		}
		intents = append(intents, in)
	}
	return evidence, intents
}

func runAll(t *testing.T, evidence []byte, intents []exec.Intent) string {
	t.Helper()
	cfg := exec.DefaultConfig()
	cfg.SendEnabled = true
	var out bytes.Buffer
	for _, rule := range sim.Rules {
		res, err := sim.Run(evidence, intents, cfg, rule)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range res {
			b, _ := json.Marshal(r)
			fmt.Fprintf(&out, "%s\n", b)
		}
	}
	return out.String()
}

func TestReplayIsDeterministic(t *testing.T) {
	evidence, intents := load(t)
	first, second := runAll(t, evidence, intents), runAll(t, evidence, intents)
	if first != second {
		t.Fatalf("replays differ:\n%s\n%s", first, second)
	}
}

func TestReplayFillRules(t *testing.T) {
	evidence, intents := load(t)
	cfg := exec.DefaultConfig()
	cfg.SendEnabled = true
	through, err := sim.Run(evidence, intents, cfg, sim.Through)
	if err != nil {
		t.Fatal(err)
	}
	touch, err := sim.Run(evidence, intents, cfg, sim.Touch)
	if err != nil {
		t.Fatal(err)
	}
	for i := range intents {
		c, o := through[i], touch[i]
		t.Logf("%s %s: through %+v", c.Side, c.IntentID, c)
		t.Logf("%s %s: touch   %+v", o.Side, o.IntentID, o)
		if c.Violations != 0 || o.Violations != 0 {
			t.Fatalf("violations: through %d touch %d", c.Violations, o.Violations)
		}
		if c.Filled > o.Filled {
			t.Fatalf("conservative filled %d more than optimistic %d", c.Filled, o.Filled)
		}
		if o.Filled == 0 || c.ArrivalMid != 70_100 {
			t.Fatalf("fixture exercises nothing: touch filled %d, mid %v", o.Filled, c.ArrivalMid)
		}
	}
	if _, err := sim.Run(evidence, intents, cfg, "mid"); err == nil {
		t.Fatal("unknown rule accepted")
	}
}

// TestOtherSymbolEventsAdvanceTheClock: after one 005930 book, only another
// symbol's books arrive. Their times must still reach the executor, which
// cancels after 30 s of book silence, before the deadline (35 s).
func TestOtherSymbolEventsAdvanceTheClock(t *testing.T) {
	_, intents := load(t)
	var buf bytes.Buffer
	rec, err := book.NewRecorder(&buf, 2*time.Second, t0)
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Frame(bookRaw(t0, 70_000, 70_200), t0); err != nil {
		t.Fatal(err)
	}
	for s := 1; s <= 40; s++ {
		at := t0.Add(time.Duration(s) * time.Second)
		other := bytes.Replace(bookRaw(at, 180_000, 180_500), []byte("005930"), []byte("000660"), 1)
		if err := rec.Frame(other, at); err != nil {
			t.Fatal(err)
		}
	}
	cfg := exec.DefaultConfig()
	cfg.SendEnabled = true
	res, err := sim.Run(buf.Bytes(), intents[:1], cfg, sim.Touch)
	if err != nil {
		t.Fatal(err)
	}
	if r := res[0]; r.Done != exec.DoneStaleBook || r.Sends != 2 || r.Filled != 0 {
		t.Fatalf("got %+v, want submit then a stale-book cancel", r)
	}
}

// TestNoFillAtOrAfterTheDeadline: after one book at t0, the next event is a
// trade through the buy's price. At 34 s (before the 35 s deadline) it fills;
// at or after 35 s the order must already count as cancelled, under both
// rules.
func TestNoFillAtOrAfterTheDeadline(t *testing.T) {
	_, intents := load(t)
	cfg := exec.DefaultConfig()
	cfg.SendEnabled = true
	for _, c := range []struct {
		sec        float64
		wantFilled int64
		wantDone   string
	}{{34, 10, exec.DoneFilled}, {35, 0, exec.DoneDeadline}, {36, 0, exec.DoneDeadline}} {
		var buf bytes.Buffer
		rec, err := book.NewRecorder(&buf, 2*time.Second, t0)
		if err != nil {
			t.Fatal(err)
		}
		at := t0.Add(time.Duration(c.sec * float64(time.Second)))
		if err := rec.Frame(bookRaw(t0, 70_000, 70_200), t0); err != nil {
			t.Fatal(err)
		}
		if err := rec.Frame(tradeRaw(at, 69_900, 10, 10), at); err != nil {
			t.Fatal(err)
		}
		for _, rule := range sim.Rules {
			res, err := sim.Run(buf.Bytes(), intents[:1], cfg, rule)
			if err != nil {
				t.Fatal(err)
			}
			if r := res[0]; r.Filled != c.wantFilled || r.Done != c.wantDone {
				t.Errorf("trade at %vs, %s: filled %d done %q, want %d %q", c.sec, rule, r.Filled, r.Done, c.wantFilled, c.wantDone)
			}
		}
	}
}
