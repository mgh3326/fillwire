package exec_test

import (
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
