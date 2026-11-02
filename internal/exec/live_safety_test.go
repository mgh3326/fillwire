package exec_test

import (
	"errors"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/exec"
)

func TestStaleBookCancelsAndStops(t *testing.T) {
	for _, restart := range []bool{false, true} {
		r := newRig(t, fixtureIntent(t), enabled())
		r.book(0, 70000, 70400)
		if err := r.e.Tick(at(0)); err != nil {
			t.Fatal(err)
		}
		if restart {
			r.restart()
		}
		if err := r.e.Tick(at(30)); err != nil {
			t.Fatal(err)
		}
		if len(r.g.Sends) != 1 {
			t.Fatal("stopped before book exceeded 30s")
		}
		if err := r.e.Tick(at(30.001)); err != nil {
			t.Fatal(err)
		}
		equal(t, r.actions(), []string{"submit@70000x10", "cancel"})
		if st := r.e.State(); st.Done != exec.DoneStaleBook || st.Order != nil {
			t.Fatalf("state %+v", st)
		}
		if err := r.step(31); err != nil {
			t.Fatal(err)
		}
		if len(r.g.Sends) != 2 {
			t.Fatal("resumed after stale stop")
		}
	}
}

func TestLedgerFillAttributedAfterRestart(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	r.book(0, 70000, 70400)
	if err := r.e.Tick(at(0)); err != nil {
		t.Fatal(err)
	}
	key := r.e.State().Order.Key
	r.restart()
	f := exec.Fill{FillID: "ledger-1", Qty: 2, Price: 70000, AtNS: at(1)}
	for range 2 {
		if err := r.e.OnLedgerFill("fake-"+key, f); err != nil {
			t.Fatal(err)
		}
	}
	if st := r.e.State(); st.Filled != 2 || st.Fills[f.FillID].Key != key {
		t.Fatalf("state %+v", st)
	}
}

func TestLedgerAttributionErrorsStopExecutor(t *testing.T) {
	for _, kind := range []string{"unknown_order", "missing_order", "unknown_key", "wrong_day", "changed_fill"} {
		t.Run(kind, func(t *testing.T) {
			r := newRig(t, fixtureIntent(t), enabled())
			r.book(0, 70000, 70400)
			if err := r.e.Tick(at(0)); err != nil {
				t.Fatal(err)
			}
			key := r.e.State().Order.Key
			id := "fake-" + key
			f := exec.Fill{FillID: "ledger-1", Qty: 1, Price: 70000, AtNS: at(1)}
			switch kind {
			case "unknown_order":
				id = "unknown"
			case "missing_order":
				id = ""
			case "unknown_key":
				f.Key = "unknown"
			case "wrong_day":
				f.AtNS = at(86401)
			case "changed_fill":
				if err := r.e.OnLedgerFill(id, f); err != nil {
					t.Fatal(err)
				}
				f.Qty++
			}
			if err := r.e.OnLedgerFill(id, f); !errors.Is(err, exec.ErrAttribution) {
				t.Fatalf("error %v", err)
			}
			if err := r.step(5); !errors.Is(err, exec.ErrAttribution) {
				t.Fatalf("tick: %v", err)
			}
			if len(r.g.Sends) != 1 {
				t.Fatal("sent after attribution failure")
			}
		})
	}
}

// A gateway can lose the broker number even though the broker accepted the
// order. A key alone must never turn that into attributed ledger evidence.
type withoutOrderID struct{ exec.Gateway }

func (g withoutOrderID) Send(c exec.Child) (exec.Outcome, error) {
	out, err := g.Gateway.Send(c)
	out.BrokerOrderID = ""
	return out, err
}

func TestLedgerFillRequiresJournalBrokerOrderID(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	st := r.e.State()
	r.e = exec.New(r.cfg, r.j, st, withoutOrderID{r.g}, r.kill)
	r.book(0, 70000, 70400)
	if err := r.e.Tick(at(0)); err != nil {
		t.Fatal(err)
	}
	key := r.e.State().Order.Key
	err := r.e.OnLedgerFill("fake-"+key, exec.Fill{Key: key, FillID: "ledger-1", Qty: 1, Price: 70000, AtNS: at(1)})
	if !errors.Is(err, exec.ErrAttribution) {
		t.Fatalf("error %v", err)
	}
	if r.e.State().Filled != 0 {
		t.Fatal("counted fill without journal broker identity")
	}
}

func TestLedgerFillOnSupersededOrder(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	r.book(0, 70000, 70400)
	if err := r.e.Tick(at(0)); err != nil {
		t.Fatal(err)
	}
	oldKey := r.e.State().Order.Key
	if err := r.step(5); err != nil {
		t.Fatal(err)
	}
	r.restart()
	if err := r.e.OnLedgerFill("fake-"+oldKey, exec.Fill{FillID: "late-fill", Qty: 1, Price: 70000, AtNS: at(1)}); err != nil {
		t.Fatal(err)
	}
	if st := r.e.State(); st.Filled != 1 || st.Fills["late-fill"].Key != oldKey {
		t.Fatalf("state %+v", st)
	}
}

type fixedOrderID struct{ exec.Gateway }

func (g fixedOrderID) Send(c exec.Child) (exec.Outcome, error) {
	out, err := g.Gateway.Send(c)
	out.BrokerOrderID = "000123"
	return out, err
}

func TestLedgerBrokerIdentityNormalizationAndAmbiguity(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	r.e = exec.New(r.cfg, r.j, r.e.State(), fixedOrderID{r.g}, r.kill)
	r.book(0, 70000, 70400)
	if err := r.e.Tick(at(0)); err != nil {
		t.Fatal(err)
	}
	f := exec.Fill{FillID: "ledger-1", Qty: 1, Price: 70000, AtNS: at(1)}
	if err := r.e.OnLedgerFill("123", f); err != nil {
		t.Fatal(err)
	}
	if r.e.State().Filled != 1 {
		t.Fatal("leading zeros prevented attribution")
	}
	if err := r.step(5); err != nil {
		t.Fatal(err)
	}
	f.FillID = "ledger-2"
	if err := r.e.OnLedgerFill("123", f); !errors.Is(err, exec.ErrAttribution) {
		t.Fatalf("ambiguous order: %v", err)
	}
}

func TestDeferredCompletionSurvivesRestart(t *testing.T) {
	cfg := enabled()
	cfg.RequireLedgerComplete = true
	r := newRig(t, fixtureIntent(t), cfg)
	r.book(0, 70000, 70400)
	if err := r.e.Tick(at(0)); err != nil {
		t.Fatal(err)
	}
	r.kill.Engage()
	if err := r.e.Tick(at(1)); err != nil {
		t.Fatal(err)
	}
	if r.e.State().Done != "" || r.e.State().Halt != exec.DoneKilled {
		t.Fatalf("premature completion %+v", r.e.State())
	}
	r.restart()
	r.e.SetLedgerComplete(true)
	if err := r.e.Tick(at(2)); err != nil {
		t.Fatal(err)
	}
	if r.e.State().Done != exec.DoneKilled {
		t.Fatalf("lost deferred finish after replay: %+v", r.e.State())
	}
}

func TestUnsyncedBooksCannotPostponeStaleStop(t *testing.T) {
	cfg := enabled()
	cfg.StepEvery = time.Minute
	r := newRig(t, fixtureIntent(t), cfg)
	r.book(0, 70000, 70400)
	if err := r.e.Tick(at(0)); err != nil {
		t.Fatal(err)
	}
	for _, sec := range []float64{10, 20, 30, 31} {
		r.e.OnBook(exec.Top{Bid: 70000, Ask: 70400, AtNS: at(sec), Synced: false})
		if err := r.e.Tick(at(sec)); err != nil {
			t.Fatal(err)
		}
	}
	if st := r.e.State(); st.Done != exec.DoneStaleBook || st.Order != nil {
		t.Fatalf("unsynced books postponed stale stop: done=%q working=%v; want stale_book with no working order", st.Done, st.Order != nil)
	}
	equal(t, r.actions(), []string{"submit@70000x10", "cancel"})
}
