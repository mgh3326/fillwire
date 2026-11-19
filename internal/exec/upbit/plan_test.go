package upbit

import (
	"encoding/json"
	"testing"
	"time"
)

var testNow = time.Date(2026, 11, 19, 9, 0, 0, 0, time.UTC)

const testUUID = "2d3c4b5a-6e7f-4a1b-8c2d-3e4f5a6b7c8d"

func testIntent(t *testing.T, mutate func(*Intent)) Intent {
	t.Helper()
	in := Intent{
		SchemaVersion:  1,
		IntentID:       testUUID,
		Venue:          VenueUpbit,
		Account:        AccountShadow,
		Market:         "KRW-XRP",
		Side:           Buy,
		MaxQuantity:    Qty{units: 33 * qtyPerUnit},
		MaxNotionalKRW: 100_000,
		LimitPriceKRW:  3_000,
		MaxSlippageBps: 100,
		MaxRequotes:    2,
		ExpiresAt:      testNow.Add(3 * time.Second),
	}
	if mutate != nil {
		mutate(&in)
	}
	return in
}

func testBook(bid, ask int64, at time.Time) Book {
	return Book{
		Market: "KRW-XRP", BidPriceKRW: bid, AskPriceKRW: ask,
		BidQty: Qty{units: qtyPerUnit}, AskQty: Qty{units: qtyPerUnit},
		ExchangeMS: at.UnixMilli(), ReceivedAt: at,
	}
}

func TestIntentBandRoundsInward(t *testing.T) {
	in := testIntent(t, func(in *Intent) { in.MaxSlippageBps = 100 })
	low, high := in.Band()
	if low != 2_970 || high != 3_030 {
		t.Fatalf("band = [%d, %d]", low, high)
	}
	if in.Target().String() != "33" {
		t.Fatalf("target = %q", in.Target())
	}
}

func TestIntentRefusesWhatTheModelCannotWork(t *testing.T) {
	cases := map[string]func(*Intent){
		"wrong venue":        func(in *Intent) { in.Venue = "krx" },
		"wrong account":      func(in *Intent) { in.Account = "kis_mock" },
		"market outside":     func(in *Intent) { in.Market = "KRW-DOGE" },
		"off-grid limit":     func(in *Intent) { in.LimitPriceKRW = 5_002 },
		"sub-won limit":      func(in *Intent) { in.LimitPriceKRW = 99 },
		"zero quantity":      func(in *Intent) { in.MaxQuantity = Qty{} },
		"too much notional":  func(in *Intent) { in.MaxNotionalKRW = 5_000_001 },
		"too many requotes":  func(in *Intent) { in.MaxRequotes = 101 },
		"no expiry":          func(in *Intent) { in.ExpiresAt = time.Time{} },
		"below min order":    func(in *Intent) { in.MaxQuantity = Qty{units: qtyPerUnit} },
		"negative slippage":  func(in *Intent) { in.MaxSlippageBps = -1 },
		"slippage over cap":  func(in *Intent) { in.MaxSlippageBps = 501 },
		"not a lowercase id": func(in *Intent) { in.IntentID = "2D3C4B5A-6E7F-4A1B-8C2D-3E4F5A6B7C8D" },
	}
	for name, mutate := range cases {
		if _, err := NewPlan(testIntent(t, mutate), DefaultConfig()); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestParseIntentRefusesUnknownFields(t *testing.T) {
	data, err := json.Marshal(testIntent(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseIntent(data); err != nil {
		t.Fatalf("valid intent: %v", err)
	}
	var obj map[string]any
	if err := json.Unmarshal(data, &obj); err != nil {
		t.Fatal(err)
	}
	obj["quantity"] = 1 // E1's integer field name must not be silently ignored
	bad, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseIntent(bad); err == nil {
		t.Fatal("unknown field accepted")
	}
}

func TestPlanBuyTakersAtAskAndChasesWithinTheBand(t *testing.T) {
	p, err := NewPlan(testIntent(t, nil), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	check := func(d Decision, action, reason string) *Child {
		t.Helper()
		if d.Action != action || d.Reason != reason {
			t.Fatalf("decision = %s/%s, want %s/%s", d.Action, d.Reason, action, reason)
		}
		return d.Child
	}
	// Open at the best ask, sized to max_quantity.
	c := check(p.OnBook(testNow, testBook(2_997, 2_998, testNow)), ActSubmit, "open")
	if c.LimitPriceKRW != 2_998 || c.Quantity == nil || c.Quantity.String() != "33" || c.Key != testUUID+":1" {
		t.Fatalf("submit = %+v", c)
	}
	// The ask moves up: chase, at most MaxRequotes times.
	c = check(p.OnBook(testNow.Add(500*time.Millisecond), testBook(2_999, 3_000, testNow.Add(500*time.Millisecond))), ActAmend, "requote")
	if c.LimitPriceKRW != 3_000 || c.TargetKey != testUUID+":1" || c.Quantity.String() != "33" {
		t.Fatalf("first amend = %+v", c)
	}
	// Out of band: no child, and the chase budget is untouched.
	check(p.OnBook(testNow.Add(time.Second), testBook(3_039, 3_040, testNow.Add(time.Second))), ActHold, "band")
	c = check(p.OnBook(testNow.Add(1500*time.Millisecond), testBook(3_009, 3_010, testNow.Add(1500*time.Millisecond))), ActAmend, "requote")
	if c.LimitPriceKRW != 3_010 || c.TargetKey != testUUID+":2" {
		t.Fatalf("second amend = %+v", c)
	}
	if p.Requotes() != 2 {
		t.Fatalf("requotes = %d", p.Requotes())
	}
	// Budget spent: the next move is held, the order rests until expiry.
	check(p.OnBook(testNow.Add(2*time.Second), testBook(3_019, 3_020, testNow.Add(2*time.Second))), ActHold, "requote_limit")
}

func TestPlanSellTakersAtBid(t *testing.T) {
	in := testIntent(t, func(in *Intent) { in.Side = Sell })
	in.MaxQuantity = Qty{units: 2 * qtyPerUnit} // 2 XRP at ~3,000 KRW clears the 5,000 KRW minimum
	p, err := NewPlan(in, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	d := p.OnBook(testNow, testBook(3_000, 3_001, testNow))
	if d.Action != ActSubmit || d.Child.LimitPriceKRW != 3_000 {
		t.Fatalf("sell open = %+v", d.Child)
	}
	// A bid below the band's floor is not taken.
	in2 := testIntent(t, func(in *Intent) {
		in.Side = Sell
		in.MaxQuantity = Qty{units: 2 * qtyPerUnit}
	})
	p2, err := NewPlan(in2, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if d := p2.OnBook(testNow, testBook(2_960, 2_961, testNow)); d.Action != ActHold || d.Reason != "band" {
		t.Fatalf("sell below band = %+v", d)
	}
	// The touch moving down chases the sell; moving up leaves it marketable.
	if d := p2.OnBook(testNow.Add(time.Second), testBook(3_000, 3_001, testNow.Add(time.Second))); d.Action != ActSubmit {
		t.Fatalf("sell open at bid = %+v", d)
	}
	if d := p2.OnBook(testNow.Add(2*time.Second), testBook(2_990, 2_991, testNow.Add(2*time.Second))); d.Action != ActAmend || d.Reason != "requote" || d.Child.LimitPriceKRW != 2_990 {
		t.Fatalf("sell chase = %+v", d)
	}
}

func TestPlanRefusesStaleAndFutureBooks(t *testing.T) {
	p, err := NewPlan(testIntent(t, nil), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	old := testNow.Add(-3 * time.Second)
	if d := p.OnBook(testNow, testBook(2_999, 3_000, old)); d.Action != ActHold || d.Reason != "stale_book" {
		t.Fatalf("stale = %+v", d)
	}
	future := testNow.Add(time.Second)
	if d := p.OnBook(testNow, testBook(2_999, 3_000, future)); d.Action != ActHold || d.Reason != "bad_book_time" {
		t.Fatalf("future = %+v", d)
	}
	if d := p.OnBook(testNow, testBook(2_999, 3_000, testNow.Add(-2*time.Second))); d.Action != ActSubmit {
		t.Fatalf("book at the stale bound = %+v", d)
	}
	// A book for another market is not this intent's touch.
	other := testBook(2_999, 3_000, testNow)
	other.Market = "KRW-BTC"
	p2, _ := NewPlan(testIntent(t, nil), DefaultConfig())
	if d := p2.OnBook(testNow, other); d.Action != ActHold || d.Reason != "other_market" {
		t.Fatalf("other market = %+v", d)
	}
}

// A fill inside the model's bounds is recorded even when units*price alone
// would exceed 2^64: the notional is units*price/1e8, and 30 units at 1e10 KRW
// is 3e11 KRW.
func TestFillRecordsALargeValueInsideItsBounds(t *testing.T) {
	p, err := NewPlan(testIntent(t, nil), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Fill(Qty{units: 3_000_000_000}, maxLimitPriceKRW); err != nil {
		t.Fatalf("within-bounds fill refused: %v", err)
	}
	if p.filledNotionalKRW != 300_000_000_000 {
		t.Fatalf("notional = %d, want 300000000000", p.filledNotionalKRW)
	}
}

// The quantity accumulator wraps only after ~92,000 maximum-size fills, so the
// test starts the accumulator one unit from the int64 limit.
func TestFillGuardsTheCumulativeQuantity(t *testing.T) {
	p, err := NewPlan(testIntent(t, nil), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	p.filled = Qty{units: 1<<63 - 1}
	if err := p.Fill(Qty{units: 10}, 3_000); err == nil {
		t.Fatal("a fill that overflows the accumulated quantity was accepted")
	}
	if p.filled.units != 1<<63-1 {
		t.Fatalf("filled changed to %d after a refused fill", p.filled.units)
	}
}

func TestPlanExpiryCancelsThenFinishes(t *testing.T) {
	p, err := NewPlan(testIntent(t, nil), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	p.OnBook(testNow, testBook(2_999, 3_000, testNow))
	expired := testNow.Add(3 * time.Second)
	d := p.OnBook(expired, testBook(2_999, 3_000, expired))
	if d.Action != ActCancel || d.Reason != DoneDeadline || d.Child.TargetKey != testUUID+":1" {
		t.Fatalf("expiry cancel = %+v", d)
	}
	if p.WorkingOrder() != nil {
		t.Fatal("order still working after the cancel")
	}
	d = p.Tick(expired.Add(time.Second))
	if d.Action != ActFinish || d.Done != DoneDeadline || p.Done() != DoneDeadline {
		t.Fatalf("finish = %+v (done %q)", d, p.Done())
	}
	// Nothing follows a finish.
	if d := p.OnBook(expired.Add(time.Second), testBook(2_999, 3_000, expired)); d.Action != ActHold || d.Reason != "done" {
		t.Fatalf("after finish = %+v", d)
	}
}

func TestPlanTickCancelsAtExpiryWithoutABook(t *testing.T) {
	p, err := NewPlan(testIntent(t, nil), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	p.OnBook(testNow, testBook(2_999, 3_000, testNow))
	if d := p.Tick(testNow.Add(2 * time.Second)); d.Action != ActHold {
		t.Fatalf("before expiry = %+v", d)
	}
	if d := p.Tick(testNow.Add(3 * time.Second)); d.Action != ActCancel || d.Reason != DoneDeadline {
		t.Fatalf("silent expiry = %+v", d)
	}
}

func TestPlanFillsCutTheRemainingQuantity(t *testing.T) {
	p, err := NewPlan(testIntent(t, nil), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	p.OnBook(testNow, testBook(2_999, 3_000, testNow))
	if err := p.Fill(Qty{units: 30 * qtyPerUnit}, 3_000); err != nil {
		t.Fatal(err)
	}
	next := testNow.Add(time.Second)
	d := p.OnBook(next, testBook(3_009, 3_010, next))
	if d.Action != ActAmend || d.Child.Quantity == nil || d.Child.Quantity.String() != "3" {
		t.Fatalf("amend after a partial fill = %+v", d.Child)
	}
	if err := p.Fill(Qty{units: 3 * qtyPerUnit}, 3_010); err != nil {
		t.Fatal(err)
	}
	// Target reached with an order working: cancel it, then finish filled.
	d = p.Tick(next)
	if d.Action != ActCancel || d.Reason != DoneFilled {
		t.Fatalf("cancel on target = %+v", d)
	}
	if d = p.Tick(next); d.Action != ActFinish || d.Done != DoneFilled {
		t.Fatalf("finish on target = %+v", d)
	}
}

func TestPlanNotionalCapsTheOpeningQuantity(t *testing.T) {
	in := testIntent(t, func(in *Intent) {
		in.MaxQuantity = Qty{units: 100 * qtyPerUnit}
	})
	p, err := NewPlan(in, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	d := p.OnBook(testNow, testBook(2_999, 3_000, testNow))
	if d.Action != ActSubmit || d.Child.Quantity.String() != "33.00330033" {
		t.Fatalf("notional-capped submit = %+v", d.Child)
	}
	if !notionalFits(*d.Child.Quantity, d.Child.LimitPriceKRW, in.MaxNotionalKRW) {
		t.Fatal("child exceeds max_notional_krw")
	}
}

func TestPlanRefusesAFillThatIsNotOne(t *testing.T) {
	p, err := NewPlan(testIntent(t, nil), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []struct {
		q     Qty
		price int64
	}{
		{Qty{}, 3_000},
		{Qty{units: -1}, 3_000},
		{Qty{units: 1}, 0},
		{Qty{units: 1}, 99},
		// Outside the model's bounds: the intent never produces these, and a
		// fill must not wrap the notional it accumulates.
		{Qty{units: maxIntentUnits + 1}, 3_000},
		{Qty{units: 1}, maxLimitPriceKRW + 1},
	} {
		if err := p.Fill(f.q, f.price); err == nil {
			t.Errorf("Fill(%q, %d) accepted", f.q, f.price)
		}
	}
}
