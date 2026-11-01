package exec_test

import (
	"testing"

	"github.com/mgh3326/fillwire/internal/exec"
)

func TestAmbiguousSendIsNotResentUntilReconciled(t *testing.T) {
	for _, applied := range []bool{true, false} {
		r := newRig(t, fixtureIntent(t), enabled())
		r.top = exec.Top{Bid: 70_000, Ask: 70_400}
		r.g.DropResponse, r.g.DropRequest = applied, !applied
		r.step(0)
		// The answer was lost and the link went down: every tick tries to
		// reconcile and sends nothing.
		for _, s := range []float64{1, 2, 3} {
			if err := r.step(s); err == nil {
				t.Fatalf("applied=%v: tick %v while down returned no error", applied, s)
			}
		}
		if st := r.e.State(); st.InFlight == nil || st.InFlightStatus != exec.StatusUncertain {
			t.Fatalf("applied=%v: in flight %+v status %q", applied, st.InFlight, st.InFlightStatus)
		}
		equal(t, r.actions(), []string{"submit@70000x10"})
		r.g.Down = false
		r.step(4)
		st := r.e.State()
		if applied {
			// The order exists: it is adopted, not submitted again.
			equal(t, r.actions(), []string{"submit@70000x10"})
			if st.Order == nil || st.Order.Price != 70_000 {
				t.Fatalf("order %+v", st.Order)
			}
		} else {
			// The order never reached the broker: a new decision, a new key.
			equal(t, r.actions(), []string{"submit@70000x10", "submit@70000x10"})
		}
		r.safe()
	}
}

func TestUncertainAnswerBlocksSends(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	r.top = exec.Top{Bid: 70_000, Ask: 70_400}
	r.g.Uncertain = true
	r.step(0) // submit answered uncertain, reconciled in the same tick
	r.step(5)
	equal(t, r.actions(), []string{"submit@70000x10", "amend@70100x10"})
	r.safe()
}

func TestStaleBookBlocksNewSends(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	r.book(0, 70_000, 70_400)
	r.e.Tick(at(2.5)) // book is 2.5 s old
	r.e.OnBook(exec.Top{Bid: 70_000, Ask: 70_400, AtNS: at(3), Synced: false})
	r.e.Tick(at(3)) // fresh but not resynced after a reconnect
	if len(r.g.Sends) != 0 {
		t.Fatalf("sent on a stale book: %v", r.actions())
	}
	r.book(4, 70_000, 70_400)
	r.e.Tick(at(4))
	r.e.Tick(at(9.5)) // step due, book 5.5 s old: no amend
	equal(t, r.actions(), []string{"submit@70000x10"})
	r.e.Tick(at(60)) // deadline: a cancel needs no fresh book
	equal(t, r.actions(), []string{"submit@70000x10", "cancel"})
	r.safe()
}

func TestRestartHasNoBook(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	r.book(0, 70_000, 70_400)
	r.restart()
	r.e.Tick(at(0.5))
	if len(r.g.Sends) != 0 {
		t.Fatal("sent after a restart before any new book")
	}
}

func TestNoFillWithoutLedgerEvidence(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	r.top = exec.Top{Bid: 70_000, Ask: 70_400}
	r.step(0)
	f, _ := r.g.Fill(10, at(1)) // the broker filled it; the ledger has not said so
	r.step(5)                   // the amend fails: the order is gone; reconcile
	st := r.e.State()
	if st.Filled != 0 || st.Done != "" || st.BrokerFilled != 10 || st.Order != nil {
		t.Fatalf("filled %d done %q broker %d order %+v", st.Filled, st.Done, st.BrokerFilled, st.Order)
	}
	for _, s := range []float64{10, 15, 20} {
		r.step(s) // nothing remains to send while the broker reports the fill
	}
	equal(t, r.actions(), []string{"submit@70000x10", "amend@70100x10"})
	if err := r.e.OnFill(exec.Fill{FillID: "x", Key: "someone-else:1", Qty: 10, Price: 1}); err != nil || r.e.State().Filled != 0 {
		t.Fatal("a fill for a foreign key counted")
	}
	r.e.OnFill(f)
	r.step(21)
	if st := r.e.State(); st.Filled != 10 || st.Done != exec.DoneFilled {
		t.Fatalf("filled %d done %q", st.Filled, st.Done)
	}
	r.safe()
}

func TestKillSwitchStopsSends(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	r.top = exec.Top{Bid: 70_000, Ask: 70_400}
	r.kill.Engage()
	r.step(0)
	if len(r.g.Sends) != 0 {
		t.Fatalf("sent while killed: %v", r.actions())
	}
	if st := r.e.State(); st.Done != exec.DoneKilled {
		t.Fatalf("done %q", st.Done)
	}

	r = newRig(t, fixtureIntent(t), enabled())
	r.top = exec.Top{Bid: 70_000, Ask: 70_400}
	r.step(0)
	r.kill.Engage()
	for _, s := range []float64{5, 10, 15} {
		r.step(s)
	}
	// Only the cancel of the working order goes out (E1: cancels stay
	// allowed while submissions are blocked).
	equal(t, r.actions(), []string{"submit@70000x10", "cancel"})
	if st := r.e.State(); st.Done != exec.DoneKilled {
		t.Fatalf("done %q", st.Done)
	}
	r.safe()

	if !(*exec.KillSwitch)(nil).Engaged() {
		t.Fatal("a nil kill switch must count as engaged")
	}
}
