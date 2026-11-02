package exec_test

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/mgh3326/fillwire/internal/exec"
)

// Every test here ends with r.safe(): no key sent twice, no send before a
// lost answer was reconciled, no second live order, no price beyond the cap,
// nothing filled beyond the target.

func TestFaultDuplicateEvents(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	r.top = exec.Top{Bid: 70_000, Ask: 70_400}
	r.step(0)
	f := r.fill(4, 1)
	for range 3 {
		if err := r.e.OnFill(f); err != nil {
			t.Fatal(err)
		}
	}
	r.restart()
	r.e.OnFill(f) // the ledger redelivers after a restart
	if st := r.e.State(); st.Filled != 4 || len(st.Fills) != 1 {
		t.Fatalf("filled %d fills %d", st.Filled, len(st.Fills))
	}
	r.safe()
}

func TestFaultOutOfOrderEvents(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	r.top = exec.Top{Bid: 70_000, Ask: 70_400}
	r.step(0)
	early, _ := r.g.Fill(2, at(1)) // a fill on the submit's key ...
	r.step(5)                      // ... delivered only after the amend (sized 10) replaced that key
	late, _ := r.g.Fill(3, at(6))
	r.e.OnFill(late) // and the ledger delivers the second fill first
	r.e.OnFill(early)
	st := r.e.State()
	if st.Filled != 5 || st.Remaining() != 5 {
		t.Fatalf("filled %d remaining %d", st.Filled, st.Remaining())
	}
	// A fill on a key whose answer was lost counts before the reconcile.
	r.g.DropResponse = true
	r.step(10)
	inFlight, _ := r.g.Fill(1, at(10.5))
	r.e.OnFill(inFlight)
	if st := r.e.State(); st.InFlight == nil || st.InFlight.Key != inFlight.Key || st.Filled != 6 {
		t.Fatalf("in flight %+v filled %d", st.InFlight, st.Filled)
	}
	r.g.Down = false
	r.step(11)
	equal(t, r.actions(), []string{"submit@70000x10", "amend@70100x10", "amend@70200x5"})
	r.safe()
}

func TestFaultPartialFills(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	r.top = exec.Top{Bid: 70_000, Ask: 70_400}
	for i, s := range []float64{0, 5, 10, 15} {
		r.step(s)
		r.fill(int64(i+1), s+1) // 1+2+3+4 = 10
	}
	r.step(20)
	if st := r.e.State(); st.Filled != 10 || st.Done != exec.DoneFilled || r.g.Filled() != 10 {
		t.Fatalf("filled %d done %q", st.Filled, st.Done)
	}
	equal(t, r.actions(), []string{"submit@70000x10", "amend@70100x9", "amend@70200x7", "amend@70300x4", "cancel"})
	r.safe()
}

func TestFaultCancelFillRace(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	r.top = exec.Top{Bid: 70_000, Ask: 70_400}
	for _, s := range []float64{0, 5, 10, 15} {
		r.step(s)
	}
	f, _ := r.g.Fill(10, at(19)) // fills just before the cap cancel arrives
	r.step(20)                   // cancel answered uncertain; reconcile finds no order
	st := r.e.State()
	if st.Done != exec.DoneCap || st.Filled != 0 || st.Order != nil {
		t.Fatalf("done %q filled %d order %+v", st.Done, st.Filled, st.Order)
	}
	r.e.OnFill(f) // ledger evidence arrives after the finish and still counts
	if st := r.e.State(); st.Filled != 10 {
		t.Fatalf("filled %d", st.Filled)
	}
	equal(t, r.actions()[4:], []string{"cancel"})
	r.safe()
}

func TestFaultDisconnectReconnect(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	r.top = exec.Top{Bid: 70_000, Ask: 70_400}
	r.step(0)
	r.g.Down = true
	r.step(5) // amend never arrives
	missed, _ := r.g.Fill(4, at(6))
	_ = r.step(7)
	r.e.OnBook(exec.Top{Bid: 70_000, Ask: 70_400, AtNS: at(8), Synced: false}) // book resyncs after reconnect
	r.g.Down = false
	r.e.Tick(at(8)) // reconciles: the order is still at 70,000; nothing sent on an unsynced book
	equal(t, r.actions(), []string{"submit@70000x10"})
	r.e.OnFill(missed) // ledger replays what was missed, twice
	r.e.OnFill(missed)
	r.step(10) // amend from the reconciled order, new key
	equal(t, r.actions(), []string{"submit@70000x10", "amend@70100x6"})
	if st := r.e.State(); st.Filled != 4 {
		t.Fatalf("filled %d", st.Filled)
	}
	r.safe()
}

func TestFaultProcessKillAndRestart(t *testing.T) {
	cases := []struct {
		name      string
		arm       func(*rig)
		killAt    float64
		wantAfter []string
		wantPrice int64
	}{
		// E1 counts the journaled submit against the caps even though it
		// never reached the broker, so no second submit fits (exposure.go).
		{"kill before the submit reaches the broker", func(r *rig) { r.g.KillBefore = true }, 0,
			[]string{"submit@70000x10"}, 0},
		{"kill after the submit reached the broker", func(r *rig) { r.g.KillAfter = true }, 0,
			[]string{"submit@70000x10"}, 70_000},
		{"kill before the amend reaches the broker", func(r *rig) { r.step(0); r.g.KillBefore = true }, 5,
			[]string{"submit@70000x10", "amend@70100x10"}, 70_000},
		{"kill after the amend reached the broker", func(r *rig) { r.step(0); r.g.KillAfter = true }, 5,
			[]string{"submit@70000x10", "amend@70100x10"}, 70_100},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newRig(t, fixtureIntent(t), enabled())
			r.top = exec.Top{Bid: 70_000, Ask: 70_400}
			c.arm(r)
			if !r.tickRecover(c.killAt) {
				t.Fatal("the fake did not kill the process")
			}
			st := r.restart()
			if st.InFlight == nil {
				t.Fatal("the journaled send is not in flight after restart")
			}
			r.book(c.killAt+0.5, 70_000, 70_400)
			r.e.Tick(at(c.killAt + 0.5))
			st = r.e.State()
			switch {
			case st.InFlight != nil:
				t.Fatalf("after restart: in flight %+v", st.InFlight)
			case c.wantPrice == 0 && (st.Order != nil || st.Done != exec.DoneE1Caps):
				t.Fatalf("after restart: order %+v done %q, want no order and %q", st.Order, st.Done, exec.DoneE1Caps)
			case c.wantPrice != 0 && (st.Order == nil || st.Order.Price != c.wantPrice):
				t.Fatalf("after restart: order %+v", st.Order)
			}
			equal(t, r.actions(), c.wantAfter)
			r.safe()
		})
	}
}

// outcomeFailGW answers every send as uncertain and every query with the
// first child still working at the broker; it fails the first query and
// closes the journal inside the failAt-th send.
type outcomeFailGW struct {
	j      *exec.Journal
	first  exec.Child
	sends  int
	adds   int // submits and amends
	failAt int
	queryN int
}

func (g *outcomeFailGW) Send(c exec.Child) (exec.Outcome, error) {
	g.sends++
	if g.sends == 1 {
		g.first = c
	}
	if c.Action != exec.ActCancel {
		g.adds++
	}
	if g.sends == g.failAt {
		g.j.Close()
	}
	return exec.Outcome{Key: c.Key, Status: exec.StatusUncertain}, nil
}

func (g *outcomeFailGW) Query(string) (exec.View, error) {
	g.queryN++
	if g.queryN == 1 {
		return exec.View{}, errors.New("query down")
	}
	return exec.View{Order: &exec.Order{Key: g.first.Key, Price: g.first.Price, Qty: g.first.Qty}}, nil
}

// An outcome append that fails on the last step of a Tick must surface from
// that Tick, not only from the next one. The kill switch makes every step of
// the second Tick a cancel that stays uncertain and a reconcile that finds
// the order still working. Cancels drive the count because E1's caps allow
// no second submit after an uncertain one.
func TestFaultOutcomeJournalFailureOnLastStep(t *testing.T) {
	j, st, err := exec.OpenJournal(filepath.Join(t.TempDir(), "exec.journal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	gw := &outcomeFailGW{j: j, failAt: 5} // 1 send in the first Tick, the 4th of the second is the 5th overall
	kill := &exec.KillSwitch{}
	e := exec.New(enabled(), j, st, gw, kill)
	if err := e.Start(fixtureIntent(t), at(0)); err != nil {
		t.Fatal(err)
	}
	e.OnBook(exec.Top{Bid: 70_000, Ask: 70_400, AtNS: at(0), Synced: true})
	if err := e.Tick(at(0)); err == nil || gw.sends != 1 {
		t.Fatalf("first tick: err %v sends %d", err, gw.sends)
	}
	kill.Engage()
	err = e.Tick(at(0))
	if gw.sends != 5 {
		t.Fatalf("sends %d, want the 8th step to be the 5th send", gw.sends)
	}
	if !errors.Is(err, exec.ErrHalted) {
		t.Fatalf("tick err %v, want ErrHalted", err)
	}
	if gw.adds != 1 {
		t.Fatalf("%d submits or amends, want only the first", gw.adds)
	}
	if err := e.Tick(at(0)); !errors.Is(err, exec.ErrHalted) || gw.sends != 5 {
		t.Fatalf("after halt: err %v sends %d", err, gw.sends)
	}
}
