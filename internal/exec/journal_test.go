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
