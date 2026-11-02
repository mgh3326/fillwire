package exec_test

import (
	"testing"

	"github.com/mgh3326/fillwire/internal/exec"
)

// wideIntent buys up to 20 shares within 1,000,000 KRW at a 70,300 cap, so the
// notional cap binds first: the target is 14.
func wideIntent(t *testing.T) exec.Intent {
	in := fixtureIntent(t)
	in.MaxQuantity = 20
	in.Inventory.MaxPositionAfter = 20
	return in
}

// A first order whose request was lost still counts in E1. The second order
// gets only the room E1 leaves: 20-14 = 6 shares by quantity, and
// 1,000,000-840,000 = 160,000 KRW by notional, so 2 shares at 60,000.
func TestSecondRootGetsOnlyE1Room(t *testing.T) {
	in := wideIntent(t)
	r := newRig(t, in, enabled())
	r.top = exec.Top{Bid: 60_000, Ask: 60_100}
	r.g.DropRequest = true
	r.step(0)
	r.g.Down = false
	r.step(1) // reconcile: no order at the broker; then the second order
	equal(t, r.actions(), []string{"submit@60000x14", "submit@60000x2"})
	for s := 6.0; s <= 120; s += 5 {
		r.step(s)
	}
	st := r.e.State()
	for _, c := range r.g.Sends[1:] {
		if c.Qty > 2 || (c.Action != exec.ActCancel && c.Qty*c.Price > 160_000) {
			t.Fatalf("second order child %s leaves E1's room", describe(c))
		}
	}
	if st.Done == "" {
		t.Fatalf("not done: %+v", st)
	}
	r.safe()
}

// When no room is left the executor finishes instead of sending a submit E1
// would refuse.
func TestNoSecondRootBeyondE1Caps(t *testing.T) {
	r := newRig(t, fixtureIntent(t), enabled())
	r.top = exec.Top{Bid: 70_000, Ask: 70_100}
	r.g.DropRequest = true
	r.step(0)
	r.g.Down = false
	r.step(1)
	equal(t, r.actions(), []string{"submit@70000x10"})
	if st := r.e.State(); st.Done != exec.DoneE1Caps || st.Order != nil {
		t.Fatalf("done %q order %+v", st.Done, st.Order)
	}
	r.safe()
}
