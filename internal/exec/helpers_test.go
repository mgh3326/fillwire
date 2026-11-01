package exec_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/exec"
	"github.com/mgh3326/fillwire/internal/exec/exectest"
)

// t0 is 2026-11-02 09:00:00 KST.
var t0 = time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)

func at(sec float64) int64 { return t0.Add(time.Duration(sec * float64(time.Second))).UnixNano() }

// fixtureIntent is testdata/e1_intent.json: a buy of 10 capped at 70,300,
// expiring 60 s after t0.
func fixtureIntent(t *testing.T) exec.Intent {
	t.Helper()
	data, err := os.ReadFile("testdata/e1_intent.json")
	if err != nil {
		t.Fatal(err)
	}
	in, err := exec.ParseIntent(data)
	if err != nil {
		t.Fatal(err)
	}
	return in
}

// sellIntent sells 10 of a 10 share position, capped at 69,700.
func sellIntent(t *testing.T) exec.Intent {
	in := fixtureIntent(t)
	in.Side, in.LimitPriceKRW = exec.Sell, 69_700
	in.Inventory = exec.Inventory{PositionAtAuthorization: 10, MinPositionAfter: 0, MaxPositionAfter: 10}
	return in
}

func enabled() exec.Config {
	c := exec.DefaultConfig()
	c.SendEnabled = true
	return c
}

type rig struct {
	t    *testing.T
	path string
	cfg  exec.Config
	j    *exec.Journal
	e    *exec.Executor
	g    *exectest.Gateway
	kill *exec.KillSwitch
	top  exec.Top
}

func newRig(t *testing.T, in exec.Intent, cfg exec.Config) *rig {
	t.Helper()
	r := &rig{t: t, path: filepath.Join(t.TempDir(), "exec.journal"), cfg: cfg, g: exectest.New(in), kill: &exec.KillSwitch{}}
	r.open()
	if err := r.e.Start(in, at(0)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.j.Close() })
	return r
}

func (r *rig) open() exec.State {
	r.t.Helper()
	j, st, err := exec.OpenJournal(r.path)
	if err != nil {
		r.t.Fatal(err)
	}
	r.j, r.e = j, exec.New(r.cfg, j, st, r.g, r.kill)
	return st
}

// restart simulates a process kill: the executor and its book are dropped
// and a new one resumes from the journal alone.
func (r *rig) restart() exec.State {
	r.t.Helper()
	r.j.Close()
	return r.open()
}

// book sets a fresh synced touch at sec.
func (r *rig) book(sec float64, bid, ask int64) {
	r.top = exec.Top{Bid: bid, Ask: ask, AtNS: at(sec), Synced: true}
	r.e.OnBook(r.top)
}

// step feeds a fresh book with the same touch and ticks at sec.
func (r *rig) step(sec float64) error {
	r.book(sec, r.top.Bid, r.top.Ask)
	return r.e.Tick(at(sec))
}

// tickRecover ticks at sec and reports whether the fake killed the process.
func (r *rig) tickRecover(sec float64) (killed bool) {
	defer func() {
		if v := recover(); v != nil {
			if _, ok := v.(exectest.Killed); !ok {
				panic(v)
			}
			killed = true
		}
	}()
	_ = r.step(sec)
	return false
}

// fill executes qty at the broker and delivers the ledger evidence.
func (r *rig) fill(qty int64, sec float64) exec.Fill {
	r.t.Helper()
	f, ok := r.g.Fill(qty, at(sec))
	if !ok {
		r.t.Fatal("no live order to fill")
	}
	if err := r.e.OnFill(f); err != nil {
		r.t.Fatal(err)
	}
	return f
}

// safe asserts the fake saw no broken rule and no key twice.
func (r *rig) safe() {
	r.t.Helper()
	if len(r.g.Violations) > 0 {
		r.t.Fatalf("violations: %v", r.g.Violations)
	}
	keys := map[string]bool{}
	for _, c := range r.g.Sends {
		if keys[c.Key] {
			r.t.Fatalf("key %s sent twice", c.Key)
		}
		keys[c.Key] = true
	}
	if st := r.e.State(); st.Filled > st.Target {
		r.t.Fatalf("filled %d beyond target %d", st.Filled, st.Target)
	}
}

func (r *rig) actions() []string {
	var out []string
	for _, c := range r.g.Sends {
		out = append(out, describe(c))
	}
	return out
}

func describe(c exec.Child) string {
	if c.Action == exec.ActCancel {
		return "cancel"
	}
	return c.Action + "@" + itoa(c.Price) + "x" + itoa(c.Qty)
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func stateJSON(t *testing.T, s exec.State) string {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func equal(t *testing.T, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
}
