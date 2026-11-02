package exec

// E1 counts an intent's caps over everything the intent ever sent, not over
// what is open now (auto_trader app/services/execution_intent_gateway/
// decisions.py, _exposure_by_order and _caps_refusal):
//
//   - an order lineage is a submit and the amends that chain from it by
//     target key;
//   - each lineage counts at its high-water quantity and its high-water
//     notional (quantity × price) over its children, because an amend whose
//     outcome is unknown may or may not have applied;
//   - every child that was not refused counts, including cancelled, failed
//     and uncertain ones. A cancel frees nothing.
//
// The sums over lineages must stay within max_quantity and max_notional_krw,
// and the position projected from the summed quantity must stay inside the
// inventory range. The executor mirrors this so it never sends a child that
// E1 would refuse for caps: decide sizes a send to the room left, and the
// executor refuses to journal a send that does not fit (Executor.commit).
// Apply does not check it, so a journal written before the mirror replays.
//
// E1 skips refused children; the executor counts every child it sent. The
// two agree on every child the executor can still send: a refused submit or
// amend halts the executor, after which it sends only cancels, and a cancel
// carries no quantity.

// E1 refusal reasons for caps (decisions.py Refusal).
const (
	refusalQuantityCap  = "quantity_cap_exceeded"
	refusalNotionalCap  = "notional_cap_exceeded"
	refusalInventoryCap = "inventory_constraint_exceeded"
)

type exposure struct{ qty, notional int64 }

// rootOf follows target keys from key back to the submit that started its
// lineage. A key not sent by this intent is its own root.
func (s State) rootOf(key string) string {
	seen := map[string]bool{}
	for !seen[key] {
		seen[key] = true
		c, ok := s.sentKey(key)
		if !ok || c.TargetKey == "" {
			return key
		}
		key = c.TargetKey
	}
	return key
}

// e1Exposure is the high-water quantity and notional of every lineage.
func (s State) e1Exposure() map[string]exposure {
	out := map[string]exposure{}
	for _, c := range s.Sent {
		if c.Action != ActSubmit && c.Action != ActAmend {
			continue
		}
		r := s.rootOf(c.Key)
		e := out[r]
		out[r] = exposure{max(e.qty, c.Qty), max(e.notional, c.Qty*c.Price)}
	}
	return out
}

// e1Room is what a child in the lineage rooted at root may still carry: the
// largest quantity and the largest notional that keep the sums within E1's
// caps. The lineage's own high-water mark is not subtracted, because a child
// below it does not raise the sums.
func (s State) e1Room(root string) (qty, notional int64) {
	var others exposure
	for r, e := range s.e1Exposure() {
		if r != root {
			others.qty += e.qty
			others.notional += e.notional
		}
	}
	in := s.Intent
	inv := in.Inventory
	limit := in.MaxQuantity
	if in.Side == Buy {
		limit = min(limit, inv.MaxPositionAfter-inv.PositionAtAuthorization)
	} else {
		limit = min(limit, inv.PositionAtAuthorization-inv.MinPositionAfter)
	}
	return max(limit-others.qty, 0), max(in.MaxNotionalKRW-others.notional, 0)
}

// E1CapsRefusal is the reason E1 would refuse submit or amend c for caps, in
// E1's order of checks, or "" if c fits.
func (s State) E1CapsRefusal(c Child) string {
	exp := s.e1Exposure()
	root := c.Key
	if c.Action == ActAmend {
		root = s.rootOf(c.TargetKey)
	}
	e := exp[root]
	exp[root] = exposure{max(e.qty, c.Qty), max(e.notional, c.Qty*c.Price)}
	var total exposure
	for _, e := range exp {
		total.qty += e.qty
		total.notional += e.notional
	}
	in := s.Intent
	inv := in.Inventory
	pos := inv.PositionAtAuthorization + total.qty
	if in.Side == Sell {
		pos = inv.PositionAtAuthorization - total.qty
	}
	// E1 checks the locked single-order notional first. It equals
	// maxIntentNotionalKRW, and max_notional_krw never exceeds that, so the
	// notional check below already covers it.
	switch {
	case total.qty > in.MaxQuantity:
		return refusalQuantityCap
	case total.notional > in.MaxNotionalKRW:
		return refusalNotionalCap
	case pos < inv.MinPositionAfter || pos > inv.MaxPositionAfter:
		return refusalInventoryCap
	}
	return ""
}
