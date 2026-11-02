package exec_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/mgh3326/fillwire/internal/exec"
)

// richRun drives one intent through every record kind and an ambiguous send.
func richRun(t *testing.T) *rig {
	r := newRig(t, fixtureIntent(t), enabled())
	r.top = exec.Top{Bid: 70_000, Ask: 70_400}
	r.step(0)    // submit
	r.fill(3, 1) // partial fill
	r.g.DropResponse = true
	r.step(5) // amend, answer lost, link down -> uncertain
	r.g.Down = false
	r.step(5.5)  // reconcile
	r.fill(2, 6) // fill on the amended order
	r.step(10)   // amend
	r.step(15)   // amend to the cap
	r.step(20)   // cancel at the cap
	if st := r.e.State(); st.Done != exec.DoneCap || st.Filled != 5 {
		t.Fatalf("rich run ended done %q filled %d", st.Done, st.Filled)
	}
	r.safe()
	return r
}

// TestKillAtEveryTransition cuts the journal after each record, as a kill
// right after that write would, and with a torn next line, and checks that a
// restart rebuilds exactly the state the live executor had at that point.
func TestKillAtEveryTransition(t *testing.T) {
	r := richRun(t)
	data, err := os.ReadFile(r.path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.SplitAfter(data, []byte("\n"))
	lines = lines[:len(lines)-1] // trailing empty split
	kinds := map[string]bool{}
	for k := 1; k <= len(lines); k++ {
		var l struct {
			Rec   exec.Record `json:"rec"`
			State string      `json:"state"`
		}
		if err := json.Unmarshal(lines[k-1], &l); err != nil {
			t.Fatal(err)
		}
		kinds[l.Rec.Kind] = true
		prefix := bytes.Join(lines[:k], nil)
		for _, tail := range [][]byte{nil, []byte(`{"v":1,"seq":`)} {
			path := filepath.Join(t.TempDir(), "cut.journal")
			if err := os.WriteFile(path, append(bytes.Clone(prefix), tail...), 0o600); err != nil {
				t.Fatal(err)
			}
			j, st, err := exec.OpenJournal(path)
			if err != nil {
				t.Fatalf("record %d tail %q: %v", k, tail, err)
			}
			j.Close()
			if st.Digest() != l.State {
				t.Fatalf("record %d (%s) tail %q: restored state differs from the live one", k, l.Rec.Kind, tail)
			}
			if got, _ := os.ReadFile(path); !bytes.Equal(got, prefix) {
				t.Fatalf("record %d: torn tail was not cut", k)
			}
		}
	}
	for _, kind := range []string{exec.RecStart, exec.RecSend, exec.RecOutcome, exec.RecReconcile, exec.RecFill} {
		if !kinds[kind] {
			t.Fatalf("scenario never wrote a %s record", kind)
		}
	}
	if got, want := stateJSON(t, r.restart()), stateJSON(t, r.e.State()); got != want {
		t.Fatalf("full replay differs:\n got %s\nwant %s", got, want)
	}
}

func TestJournalReplayIsDeterministic(t *testing.T) {
	r := richRun(t)
	data, _ := os.ReadFile(r.path)
	a, errA := exec.ReplayJournal(bytes.NewReader(data))
	b, errB := exec.ReplayJournal(bytes.NewReader(data))
	if errA != nil || errB != nil || stateJSON(t, a) != stateJSON(t, b) || a.Digest() != r.e.State().Digest() {
		t.Fatalf("replays differ: %v %v", errA, errB)
	}
}

func TestJournalRefusesCorruption(t *testing.T) {
	r := richRun(t)
	data, _ := os.ReadFile(r.path)
	lines := bytes.SplitAfter(data, []byte("\n"))
	for name, bad := range map[string][]byte{
		"bad middle line": bytes.Join([][]byte{lines[0], []byte("garbage\n"), lines[1]}, nil),
		"sequence gap":    bytes.Join([][]byte{lines[0], lines[2]}, nil),
		"edited record":   bytes.Replace(data, []byte(`"limit_price_krw":70100`), []byte(`"limit_price_krw":70400`), 1),
		"edited digest":   bytes.Replace(data, []byte(`"state":"`), []byte(`"state":"0`), 1),
	} {
		if _, err := exec.ReplayJournal(bytes.NewReader(bad)); !errors.Is(err, exec.ErrCorrupt) {
			t.Errorf("%s: err %v, want ErrCorrupt", name, err)
		}
	}
}

func TestReplayRefusesTrailingDataOnLine(t *testing.T) {
	r := richRun(t)
	data, _ := os.ReadFile(r.path)
	if _, err := exec.ReplayJournal(bytes.NewReader(data)); err != nil {
		t.Fatalf("untouched journal: %v", err)
	}
	lines := bytes.SplitAfter(bytes.TrimSuffix(data, []byte("\n")), []byte("\n"))
	for name, idx := range map[string]int{"first line": 0, "last line": len(lines) - 1} {
		cp := make([][]byte, len(lines))
		copy(cp, lines)
		cp[idx] = append(bytes.TrimSuffix(cp[idx], []byte("\n")), '{', '}')
		if idx != len(lines)-1 {
			cp[idx] = append(cp[idx], '\n')
		}
		bad := append(bytes.Join(cp, nil), '\n')
		if _, err := exec.ReplayJournal(bytes.NewReader(bad)); !errors.Is(err, exec.ErrCorrupt) {
			t.Errorf("%s replay: err %v, want ErrCorrupt", name, err)
		}
		path := filepath.Join(t.TempDir(), "j.log")
		if err := os.WriteFile(path, bad, 0o640); err != nil {
			t.Fatal(err)
		}
		if _, _, err := exec.OpenJournal(path); !errors.Is(err, exec.ErrCorrupt) {
			t.Errorf("%s open: err %v, want ErrCorrupt", name, err)
		}
	}
}

// TestJournalRefusesSecondOpen: a second handle on a journal that is open
// would append at its own offset and overwrite the first one's records.
func TestJournalRefusesSecondOpen(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	if j, _, err := exec.OpenJournal(r.path); !errors.Is(err, exec.ErrJournalLocked) {
		if j != nil {
			j.Close()
		}
		t.Fatalf("second open: err %v, want ErrJournalLocked", err)
	}
	if st := r.restart(); !st.Started() {
		t.Fatal("reopen after close lost the start record")
	}
}

// acceptGW accepts every child and reports no working order.
type acceptGW struct{ sends []exec.Child }

func (g *acceptGW) Send(c exec.Child) (exec.Outcome, error) {
	g.sends = append(g.sends, c)
	return exec.Outcome{Key: c.Key, Status: exec.StatusAccepted}, nil
}

func (g *acceptGW) Query(string) (exec.View, error) { return exec.View{}, nil }

// TestLegacySellAboveNotionalReplays: a journal written before sells were
// held to max_notional_krw (testdata: a sell of 10 opened at the 70,400 ask,
// 704,000 KRW against a 700,000 cap) still opens, and the next step amends
// the order back within max_notional_krw instead of stepping from the ask.
func TestLegacySellAboveNotionalReplays(t *testing.T) {
	data, err := os.ReadFile("testdata/legacy_sell_above_notional.journal")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "exec.journal")
	if err := os.WriteFile(path, data, 0o640); err != nil {
		t.Fatal(err)
	}
	j, st, err := exec.OpenJournal(path)
	if err != nil {
		t.Fatalf("legacy journal: %v", err)
	}
	t.Cleanup(func() { j.Close() })
	if st.Order == nil || st.Order.Price != 70_400 || st.Intent.MaxNotionalKRW != 700_000 {
		t.Fatalf("replayed order %+v max notional %d", st.Order, st.Intent.MaxNotionalKRW)
	}
	g := &acceptGW{}
	e := exec.New(enabled(), j, st, g, &exec.KillSwitch{})
	e.OnBook(exec.Top{Bid: 69_800, Ask: 70_400, AtNS: at(5), Synced: true})
	if err := e.Tick(at(5)); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range g.sends {
		got = append(got, describe(c))
	}
	equal(t, got, []string{"amend@70000x10"})
}

// TestJournalOpensInUnreadableDir: an existing journal in a directory the
// process may write and search but not list still opens; only a new, empty
// journal needs its directory entry synced.
func TestJournalOpensInUnreadableDir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	r := newRig(t, fixtureIntent(t), enabled())
	r.j.Close()
	dir := filepath.Dir(r.path)
	if err := os.Chmod(dir, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o700) })
	j, st, err := exec.OpenJournal(r.path)
	if err != nil {
		t.Fatalf("existing journal: %v", err)
	}
	r.j = j
	if !st.Started() {
		t.Fatal("replay lost the start record")
	}
}
