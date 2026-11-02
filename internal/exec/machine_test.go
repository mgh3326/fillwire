package exec_test

import (
	"errors"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/exec"
)

func TestTransitions(t *testing.T) {
	cases := []struct {
		name       string
		intent     func(*testing.T) exec.Intent
		bid, ask   int64
		script     func(*rig)
		wantSends  []string
		wantDone   string
		wantFilled int64
	}{{
		name: "buy steps one tick every 5s, then cancels at the cap",
		bid:  70_000, ask: 70_400,
		script: func(r *rig) {
			for _, s := range []float64{0, 4.9, 5, 10, 15, 19.9, 20, 25} {
				r.step(s)
			}
		},
		wantSends: []string{"submit@70000x10", "amend@70100x10", "amend@70200x10", "amend@70300x10", "cancel"},
		wantDone:  exec.DoneCap,
	}, {
		name:   "sell steps down from the ask to its cap",
		intent: sellIntent,
		bid:    69_600, ask: 70_000,
		script: func(r *rig) {
			for _, s := range []float64{0, 5, 10, 15, 20} {
				r.step(s)
			}
		},
		wantSends: []string{"submit@70000x10", "amend@69900x10", "amend@69800x10", "amend@69700x10", "cancel"},
		wantDone:  exec.DoneCap,
	}, {
		name: "a touch beyond the cap starts at the cap",
		bid:  70_500, ask: 70_600,
		script: func(r *rig) {
			r.step(0)
			r.step(5)
		},
		wantSends: []string{"submit@70300x10", "cancel"},
		wantDone:  exec.DoneCap,
	}, {
		name: "steps cross the 50,000 tick band",
		intent: func(t *testing.T) exec.Intent {
			in := fixtureIntent(t)
			in.LimitPriceKRW = 50_200
			return in
		},
		bid: 49_900, ask: 50_300,
		script: func(r *rig) {
			for _, s := range []float64{0, 5, 10, 15} {
				r.step(s)
			}
		},
		wantSends: []string{"submit@49900x10", "amend@49950x10", "amend@50000x10", "amend@50100x10"},
	}, {
		name: "deadline cancels a working order",
		bid:  70_000, ask: 70_400,
		script: func(r *rig) {
			r.step(0)
			r.step(5)
			r.step(60)
			r.step(65)
		},
		wantSends: []string{"submit@70000x10", "amend@70100x10", "cancel"},
		wantDone:  exec.DoneDeadline,
	}, {
		name: "deadline with no order finishes without a send",
		bid:  70_000, ask: 70_400,
		script: func(r *rig) {
			r.book(0, 70_000, 70_400)
			r.top.Synced = false
			r.e.OnBook(r.top)
			r.e.Tick(at(0))
			r.e.Tick(at(60))
		},
		wantDone: exec.DoneDeadline,
	}, {
		name: "partial fills reduce the remaining quantity",
		bid:  70_000, ask: 70_400,
		script: func(r *rig) {
			r.step(0)
			r.fill(4, 1)
			r.step(5)
			r.fill(2, 6)
			r.step(10)
			r.fill(4, 11)
			r.step(15)
		},
		wantSends:  []string{"submit@70000x10", "amend@70100x6", "amend@70200x4", "cancel"},
		wantDone:   exec.DoneFilled,
		wantFilled: 10,
	}, {
		name: "a cancel after partial fills keeps them",
		bid:  70_000, ask: 70_400,
		script: func(r *rig) {
			r.step(0)
			r.fill(3, 1)
			for _, s := range []float64{5, 10, 15, 20} {
				r.step(s)
			}
		},
		wantSends:  []string{"submit@70000x10", "amend@70100x7", "amend@70200x7", "amend@70300x7", "cancel"},
		wantDone:   exec.DoneCap,
		wantFilled: 3,
	}, {
		name: "a refused submit finishes without retrying",
		bid:  70_000, ask: 70_400,
		script: func(r *rig) {
			r.g.RefuseNext = "submissions_killed"
			r.step(0)
			r.step(5)
		},
		wantSends: []string{"submit@70000x10"},
		wantDone:  exec.DoneRefused,
	}}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := fixtureIntent(t)
			if c.intent != nil {
				in = c.intent(t)
			}
			r := newRig(t, in, enabled())
			r.top = exec.Top{Bid: c.bid, Ask: c.ask}
			c.script(r)
			equal(t, r.actions(), c.wantSends)
			st := r.e.State()
			if st.Done != c.wantDone || st.Filled != c.wantFilled {
				t.Fatalf("done %q filled %d, want %q %d", st.Done, st.Filled, c.wantDone, c.wantFilled)
			}
			r.safe()
		})
	}
}

func TestDefaultConfigSendsNothing(t *testing.T) {
	cfg := exec.DefaultConfig()
	if cfg.SendEnabled || cfg.StepEvery != 5*time.Second || cfg.StaleAfter != 2*time.Second {
		t.Fatalf("default %+v", cfg)
	}
	r := newRig(t, fixtureIntent(t), cfg)
	r.top = exec.Top{Bid: 70_000, Ask: 70_400}
	for s := 0.0; s <= 70; s += 1 {
		if err := r.step(s); err != nil {
			t.Fatal(err)
		}
	}
	if len(r.g.Sends) != 0 {
		t.Fatalf("sends %v", r.actions())
	}
	if (exec.Config{}).SendEnabled {
		t.Fatal("zero config enables sending")
	}
}

// TestApplyRefuses checks the second line of defence: a journal or a decision
// that carries any of these cannot become state.
func TestApplyRefuses(t *testing.T) {
	in := fixtureIntent(t)
	st, err := exec.Apply(exec.State{}, exec.Record{Kind: exec.RecStart, Intent: &in})
	if err != nil {
		t.Fatal(err)
	}
	send := func(action string, qty, price int64) exec.Record {
		return exec.Record{Kind: exec.RecSend, Child: &exec.Child{Action: action, IntentID: in.IntentID, Key: in.IntentID + ":1", Qty: qty, Price: price}}
	}
	working, err := exec.Apply(st, send(exec.ActSubmit, 10, 70_000))
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		st  exec.State
		rec exec.Record
	}{
		"price beyond the cap":       {st, send(exec.ActSubmit, 10, 70_400)},
		"quantity beyond the target": {st, send(exec.ActSubmit, 11, 70_000)},
		"amend without an order":     {st, send(exec.ActAmend, 10, 70_000)},
		"send while one is in flight": {working, exec.Record{Kind: exec.RecSend, Child: &exec.Child{
			Action: exec.ActSubmit, IntentID: in.IntentID, Key: in.IntentID + ":2", Qty: 1, Price: 70_000}}},
		"fill for a key never sent": {st, exec.Record{Kind: exec.RecFill, Fill: &exec.Fill{FillID: "f", Key: "other:1", Qty: 1, Price: 1}}},
		"second start":              {st, exec.Record{Kind: exec.RecStart, Intent: &in}},
	} {
		if _, err := exec.Apply(c.st, c.rec); !errors.Is(err, exec.ErrTransition) {
			t.Errorf("%s: err %v, want ErrTransition", name, err)
		}
	}
}

func TestModelVersionIsNotShared(t *testing.T) {
	in := fixtureIntent(t)
	mv := "model-a"
	in.ModelVersion = &mv
	r := newRig(t, in, enabled())
	mv = "model-b"
	s := r.e.State()
	*s.Intent.ModelVersion = "model-c"
	r.book(1, 69_000, 69_100)
	if err := r.e.Tick(at(1)); err != nil {
		t.Fatal(err)
	}
	if got := *r.e.State().Intent.ModelVersion; got != "model-a" {
		t.Fatalf("live model_version = %q", got)
	}
	st := r.restart()
	if got := *st.Intent.ModelVersion; got != "model-a" {
		t.Fatalf("replayed model_version = %q", got)
	}
}

// TestSellOpenHeldToMaxNotional: a sell opens at the ask, which may sit above
// Cap. The opening price is held so the whole child stays within
// max_notional_krw (E1 refuses a child past it), and amends step on from
// there toward Cap.
func TestSellOpenHeldToMaxNotional(t *testing.T) {
	in := sellIntent(t)
	in.MaxNotionalKRW = 700_000 // 10 at the 69,700 cap is 697,000
	r := newRig(t, in, enabled())
	r.top = exec.Top{Bid: 69_800, Ask: 70_400} // 10 at the ask is 704,000
	r.step(0)
	r.step(5)
	equal(t, r.actions(), []string{"submit@70000x10", "amend@69900x10"})
	r.safe()
}

// TestFutureBookBlocksSends: a book stamped ahead of the clock is no
// fresher than a stale one; nothing is priced from it until the clock
// reaches its stamp.
func TestFutureBookBlocksSends(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	r.book(1, 70_000, 70_400)
	r.e.Tick(at(0))
	if len(r.g.Sends) != 0 {
		t.Fatalf("sent on a book from the future: %v", r.actions())
	}
	r.e.Tick(at(1))
	equal(t, r.actions(), []string{"submit@70000x10"})
	r.safe()
}

// TestCapCancelNeedsNoFreshBook: once the order rested a step at the cap it
// is cancelled even if the book went stale, as every other cancel is.
func TestCapCancelNeedsNoFreshBook(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	r.top = exec.Top{Bid: 70_000, Ask: 70_400}
	for _, s := range []float64{0, 5, 10, 15} {
		r.step(s)
	}
	r.e.Tick(at(20)) // last book at 15 s: stale
	equal(t, r.actions(), []string{"submit@70000x10", "amend@70100x10", "amend@70200x10", "amend@70300x10", "cancel"})
	if st := r.e.State(); st.Done != exec.DoneCap {
		t.Fatalf("done %q", st.Done)
	}
	r.safe()
}

// TestDisabledExecutorSendsNoCancel pins the SendEnabled contract: a
// disabled executor that resumes with a working order sends nothing, not
// even the cancel a kill or the deadline would send.
func TestDisabledExecutorSendsNoCancel(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	r.top = exec.Top{Bid: 70_000, Ask: 70_400}
	r.step(0)
	r.cfg = exec.DefaultConfig()
	r.restart()
	r.kill.Engage()
	for _, s := range []float64{5, 10, 61} { // killed, then past the deadline
		if err := r.step(s); err != nil {
			t.Fatal(err)
		}
	}
	equal(t, r.actions(), []string{"submit@70000x10"})
	if st := r.e.State(); st.Order == nil || st.Done != "" {
		t.Fatalf("order %+v done %q", st.Order, st.Done)
	}
}
