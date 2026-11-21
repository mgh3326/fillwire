package upbit

import (
	"compress/gzip"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// recorder lines, as internal/upbit writes them.

func at(ms int64) time.Time { return testNow.Add(time.Duration(ms) * time.Millisecond) }

func iso(ms int64) string { return at(ms).Format(time.RFC3339Nano) }

type lvl struct {
	price int64
	qty   string
}

func orderbookLine(ms int64, asks, bids []lvl) string {
	var units []string
	for i := range asks {
		units = append(units, fmt.Sprintf(`{"ask_price":%d.0,"bid_price":%d.0,"ask_size":%s,"bid_size":%s}`,
			asks[i].price, bids[i].price, asks[i].qty, bids[i].qty))
	}
	raw := fmt.Sprintf(`{"type":"orderbook","code":"KRW-XRP","timestamp":%d,"orderbook_units":[%s],"stream_type":"REALTIME"}`,
		at(ms).UnixMilli(), strings.Join(units, ","))
	return fmt.Sprintf(`{"kind":"orderbook","market":"KRW-XRP","received_at":%q,"raw":%s}`, iso(ms), raw)
}

// top is a one-level book.
func top(ms, bid int64, ask int64) string {
	return orderbookLine(ms, []lvl{{ask, "1000"}}, []lvl{{bid, "1000"}})
}

func tradeLine(ms, price int64, vol string) string {
	raw := fmt.Sprintf(`{"type":"trade","code":"KRW-XRP","timestamp":%d,"trade_price":%d.0,"trade_volume":%s,"ask_bid":"BID","sequential_id":%d}`,
		at(ms).UnixMilli(), price, vol, 1_000_000+ms)
	return fmt.Sprintf(`{"kind":"trade","market":"KRW-XRP","received_at":%q,"raw":%s}`, iso(ms), raw)
}

// tradeSeq is a trade frame with an explicit stream type and sequential id.
func tradeSeq(ms, price int64, vol, stream string, seq int64) string {
	raw := fmt.Sprintf(`{"type":"trade","code":"KRW-XRP","timestamp":%d,"trade_price":%d.0,"trade_volume":%s,"ask_bid":"BID","sequential_id":%d,"stream_type":%q}`,
		at(ms).UnixMilli(), price, vol, seq, stream)
	return fmt.Sprintf(`{"kind":"trade","market":"KRW-XRP","received_at":%q,"sequential_id":%d,"raw":%s}`, iso(ms), seq, raw)
}

func markLine(kind string, ms int64) string {
	return fmt.Sprintf(`{"kind":%q,"market":"KRW-XRP","received_at":%q,"reason":"test"}`, kind, iso(ms))
}

func runReplay(t *testing.T, intents []ReplayIntent, lines ...string) []Report {
	t.Helper()
	r, err := NewReplay(intents, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range lines {
		ev, err := DecodeRecord([]byte(l))
		if err != nil {
			t.Fatalf("%v\n%s", err, l)
		}
		if err := r.Feed(ev); err != nil {
			t.Fatal(err)
		}
	}
	return r.Finish()
}

func buyIntent(t *testing.T, mutate func(*Intent)) ReplayIntent {
	t.Helper()
	return ReplayIntent{Intent: testIntent(t, func(in *Intent) {
		in.ExpiresAt = testNow.Add(time.Minute)
		if mutate != nil {
			mutate(in)
		}
	})}
}

// byRule picks the report of one rule (and trigger).
func byRule(t *testing.T, reports []Report, rule FillRule, trig Trigger) Report {
	t.Helper()
	for _, r := range reports {
		if r.Result.Rule == rule && r.Result.Trigger == trig {
			return r
		}
	}
	t.Fatalf("no report for %s/%q in %d reports", rule, trig, len(reports))
	return Report{}
}

func fillSummary(c ChildReport) string {
	var parts []string
	for _, f := range c.Fills {
		parts = append(parts, fmt.Sprintf("%s@%d/%s", f.Quantity, f.PriceKRW, f.Source))
	}
	return strings.Join(parts, " ")
}

// Rule 1: a child arrives at the first book at or after τ+1s, not before, and
// rule 2: it takes the displayed size of each level within its limit at the
// level's own price, the rest resting.
func TestRule1And2ChildArrivesAfterOneSecondAndSweepsDisplayedDepth(t *testing.T) {
	deep := orderbookLine(1_000,
		[]lvl{{2_995, "10"}, {3_000, "5"}, {3_001, "100"}},
		[]lvl{{2_994, "1"}, {2_993, "1"}, {2_992, "1"}})
	reports := runReplay(t, []ReplayIntent{buyIntent(t, nil)},
		top(0, 2_999, 3_000),
		// 500 ms: better ask, but the child cannot be there yet.
		orderbookLine(500, []lvl{{2_990, "50"}}, []lvl{{2_989, "1"}}),
		deep,
		// The next book still has an ask at the limit: the resting rest fills.
		top(1_500, 2_994, 3_000))

	through := byRule(t, reports, RuleThrough, TriggerNone)
	submit := through.Children[0]
	if submit.Action != ActSubmit || submit.LimitPriceKRW != 3_000 || !submit.DecidedAt.Equal(at(0)) {
		t.Fatalf("submit = %+v", submit)
	}
	if submit.ArrivedAt == nil || !submit.ArrivedAt.Equal(at(1_000)) || submit.Status != StatusActive {
		t.Fatalf("arrival = %v status %s; want the 1000 ms frame, not the 500 ms one", submit.ArrivedAt, submit.Status)
	}
	if got := fillSummary(submit); got != "10@2995/taker 5@3000/taker" {
		t.Fatalf("through fills = %q", got)
	}
	if through.Result.Filled.String() != "15" || through.Result.FillNotionalKRW != "44950" || through.Result.AvgPriceKRW != "2996.67" {
		t.Fatalf("through result = %+v", through.Result)
	}

	// Touch: the 15 taken on arrival; the other 18 fill at the limit only on
	// the next book, because the arrival frame's own touch was just swept.
	touch := byRule(t, reports, RuleTouch, TriggerNone)
	if got := fillSummary(touch.Children[0]); got != "10@2995/taker 5@3000/taker 18@3000/touch_book" {
		t.Fatalf("touch fills = %q", got)
	}
	if f := touch.Children[0].Fills[2]; !f.At.Equal(at(1_500)) {
		t.Fatalf("touch_book fill at %s, want the 1500 ms book", f.At)
	}
	if touch.Result.Filled.String() != "33" || touch.Result.Done != DoneFilled {
		t.Fatalf("touch result = %+v", touch.Result)
	}
}

// Rule 1 again: a frame inside the delay does not even count as arrival when
// the recording has no later frame; the child is reported as not arrived.
func TestRule1ChildNeverArrivesWithoutALaterFrame(t *testing.T) {
	reports := runReplay(t, []ReplayIntent{buyIntent(t, nil)}, top(0, 2_999, 3_000), top(900, 2_999, 2_990))
	c := byRule(t, reports, RuleThrough, TriggerNone).Children[0]
	if c.Status != StatusNotArrived || c.ArrivedAt != nil || len(c.Fills) != 0 {
		t.Fatalf("child = %+v", c)
	}
}

// Rule 3: Through fills only on a trade strictly through the limit, by that
// trade's size; Touch fills all on a trade at the limit. A trade before the
// arrival frame never reaches the order.
func TestRule3ThroughAndTouchRestingFills(t *testing.T) {
	lines := []string{
		top(0, 2_999, 3_000),
		tradeLine(500, 2_990, "100"), // before arrival: reaches nothing
		top(1_000, 2_999, 3_001),     // arrival; the ask is past the limit, so it rests whole
		tradeLine(1_100, 3_000, "5"), // at the limit: Touch only
		tradeLine(1_200, 2_999, "4"), // strictly through: Through takes 4
		tradeLine(1_300, 2_999, "100"),
	}
	reports := runReplay(t, []ReplayIntent{buyIntent(t, nil)}, lines...)

	through := byRule(t, reports, RuleThrough, TriggerNone)
	if got := fillSummary(through.Children[0]); got != "4@3000/trade 29@3000/trade" {
		t.Fatalf("through fills = %q", got)
	}
	if through.Result.Filled.String() != "33" {
		t.Fatalf("through filled %s", through.Result.Filled)
	}
	touch := byRule(t, reports, RuleTouch, TriggerNone)
	if got := fillSummary(touch.Children[0]); got != "33@3000/touch_trade" {
		t.Fatalf("touch fills = %q", got)
	}
	if f := touch.Children[0].Fills[0]; !f.At.Equal(at(1_100)) {
		t.Fatalf("touch fill at %s", f.At)
	}
}

// Rule 3 for a sell: a bid touch at or through the limit fills under Touch;
// Through needs a trade strictly above.
func TestRule3SellSide(t *testing.T) {
	sell := buyIntent(t, func(in *Intent) { in.Side = Sell })
	lines := []string{
		top(0, 3_000, 3_001),
		top(1_000, 2_999, 3_001), // arrives at 3000; bid 2999 is under the limit
		tradeLine(1_100, 3_000, "7"),
		tradeLine(1_200, 3_001, "3"),
		top(1_300, 3_000, 3_002), // bid touches the limit
	}
	reports := runReplay(t, []ReplayIntent{sell}, lines...)
	if got := fillSummary(byRule(t, reports, RuleThrough, TriggerNone).Children[0]); got != "3@3000/trade" {
		t.Fatalf("through fills = %q", got)
	}
	if got := fillSummary(byRule(t, reports, RuleTouch, TriggerNone).Children[0]); got != "33@3000/touch_trade" {
		t.Fatalf("touch fills = %q", got)
	}
}

// Rule 3: fills are priced at the order's own limit, never at the trade's.
func TestRule3FillsAtOwnLimit(t *testing.T) {
	reports := runReplay(t, []ReplayIntent{buyIntent(t, nil)},
		top(0, 2_999, 3_000), top(1_000, 2_999, 3_001), tradeLine(1_100, 2_950, "1"))
	for _, rep := range reports {
		for _, f := range rep.Children[0].Fills {
			if f.PriceKRW != 3_000 {
				t.Fatalf("%s fill at %d, want the limit 3000", rep.Result.Rule, f.PriceKRW)
			}
		}
	}
}

// Rule 4: nothing fills at or after expires_at, and the plan cancels.
func TestRule4ExpiryCutsFills(t *testing.T) {
	in := buyIntent(t, func(in *Intent) { in.ExpiresAt = testNow.Add(5 * time.Second) })
	reports := runReplay(t, []ReplayIntent{in},
		top(0, 2_999, 3_000),
		top(1_000, 2_999, 3_001),
		tradeLine(4_999, 2_999, "2"), // last instant before expiry
		tradeLine(5_000, 2_999, "9"), // at expires_at: refused
		top(5_100, 2_999, 3_001))
	through := byRule(t, reports, RuleThrough, TriggerNone)
	if got := fillSummary(through.Children[0]); got != "2@3000/trade" {
		t.Fatalf("fills = %q", got)
	}
	if through.Result.Done != DoneDeadline {
		t.Fatalf("done = %q", through.Result.Done)
	}
	last := through.Children[len(through.Children)-1]
	if last.Action != ActCancel || last.Status != StatusApplied {
		t.Fatalf("last child = %+v", last)
	}
}

// Rule 4: a child in flight at expiry does not arrive.
func TestRule4ChildInFlightAtExpiryNeverArrives(t *testing.T) {
	in := buyIntent(t, func(in *Intent) { in.ExpiresAt = testNow.Add(5 * time.Second) })
	reports := runReplay(t, []ReplayIntent{in},
		top(4_500, 2_999, 3_000),
		top(5_200, 2_999, 2_990))
	submit := byRule(t, reports, RuleTouch, TriggerNone).Children[0]
	if submit.Status != StatusCancelled || len(submit.Fills) != 0 || submit.ArrivedAt != nil {
		t.Fatalf("submit = %+v", submit)
	}
}

// Rule 5: a child in flight when a gap opens is void, however good the book
// after the gap looks, and no fill is recognised for it.
func TestRule5GapVoidsAChildInFlight(t *testing.T) {
	reports := runReplay(t, []ReplayIntent{buyIntent(t, nil)},
		top(0, 2_999, 3_000),
		markLine("gap", 500),
		markLine("connected", 2_000),
		top(2_500, 2_989, 2_990), // would be an arrival sweep without the gap
		tradeLine(2_600, 2_900, "100"))
	for _, rep := range reports {
		c := rep.Children[0]
		if c.Status != StatusVoidGap || len(c.Fills) != 0 {
			t.Fatalf("%s child = %+v", rep.Result.Rule, c)
		}
		if rep.Result.VoidChildren != 1 || rep.Result.Gaps != 1 || !rep.Result.Filled.IsZero() {
			t.Fatalf("%s result = %+v", rep.Result.Rule, rep.Result)
		}
	}
}

// Rule 5: a gap longer than 60 s while holding a position marks the run
// gap_exposed; 60 s exactly, or holding nothing, does not.
func TestRule5GapExposed(t *testing.T) {
	arrive := []string{top(0, 2_999, 3_000), orderbookLine(1_000, []lvl{{3_000, "10"}}, []lvl{{2_999, "1"}})}
	cases := []struct {
		name    string
		lines   []string
		exposed bool
	}{
		{"61 s holding", append(arrive[:2:2], markLine("gap", 2_000), markLine("connected", 63_000)), true},
		{"60 s holding", append(arrive[:2:2], markLine("gap", 2_000), markLine("connected", 62_000)), false},
		{"61 s flat", []string{top(0, 2_999, 3_000), markLine("gap", 100), markLine("connected", 61_100)}, false},
	}
	for _, c := range cases {
		reports := runReplay(t, []ReplayIntent{buyIntent(t, nil)}, c.lines...)
		res := byRule(t, reports, RuleThrough, TriggerNone).Result
		if res.GapExposed != c.exposed {
			t.Errorf("%s: gap_exposed = %v, want %v (%+v)", c.name, res.GapExposed, c.exposed, res)
		}
	}
}

// Rule 6: through and touch are separate runs and the plan sees only its own
// rule's fills, so the two can diverge in what they send.
func TestRule6RulesAreCountedSeparately(t *testing.T) {
	// No requotes, so the order keeps resting at 3000 while the ask is 3001.
	in := buyIntent(t, func(in *Intent) { in.MaxRequotes = 0 })
	reports := runReplay(t, []ReplayIntent{in},
		top(0, 2_999, 3_000), top(1_000, 2_999, 3_001), tradeLine(1_100, 3_000, "1"), top(2_000, 2_999, 3_001))
	if len(reports) != 2 || reports[0].Result.Rule != RuleThrough || reports[1].Result.Rule != RuleTouch {
		t.Fatalf("reports = %+v", reports)
	}
	through, touch := reports[0].Result, reports[1].Result
	if !through.Filled.IsZero() || touch.Filled.String() != "33" {
		t.Fatalf("through %s touch %s", through.Filled, touch.Filled)
	}
	if touch.Done != DoneFilled || through.Done != DoneUnfinished {
		t.Fatalf("done: through %q touch %q", through.Done, touch.Done)
	}
}

// Rule 7: a stop-loss sell starts only when its trigger fires. The slot value
// checks the last book at issued_at; the continuous value the first book at
// or after it.
func stopIntent(t *testing.T) ReplayIntent {
	t.Helper()
	return ReplayIntent{
		Intent: testIntent(t, func(in *Intent) {
			in.Side = Sell
			in.LimitPriceKRW = 2_900
			in.ExpiresAt = testNow.Add(time.Minute)
		}),
		IssuedAt:     at(10_000),
		StopPriceKRW: 2_900,
	}
}

func TestRule7SlotAndContinuousTriggers(t *testing.T) {
	reports := runReplay(t, []ReplayIntent{stopIntent(t)},
		top(5_000, 2_950, 2_951),
		top(9_500, 2_950, 2_951),  // the book at the slot: above the stop
		top(12_000, 2_890, 2_891), // under the stop, after the slot
		top(13_000, 2_890, 2_891))
	if len(reports) != 4 {
		t.Fatalf("%d reports, want 2 rules x 2 triggers", len(reports))
	}
	for _, rule := range FillRules {
		slot := byRule(t, reports, rule, TriggerSlot)
		if slot.Result.Done != DoneNotTriggered || len(slot.Children) != 0 {
			t.Fatalf("slot %s = %+v", rule, slot.Result)
		}
		cont := byRule(t, reports, rule, TriggerContinuous)
		if len(cont.Children) == 0 || !cont.Children[0].DecidedAt.Equal(at(12_000)) {
			t.Fatalf("continuous %s children = %+v", rule, cont.Children)
		}
		if a := cont.Children[0].ArrivedAt; a == nil || !a.Equal(at(13_000)) {
			t.Fatalf("continuous arrival = %v", a)
		}
		if got := fillSummary(cont.Children[0]); got != "33@2890/taker" {
			t.Fatalf("continuous %s fills = %q", rule, got)
		}
	}
}

func TestRule7SlotTriggerFiresAtTheSlotBook(t *testing.T) {
	reports := runReplay(t, []ReplayIntent{stopIntent(t)},
		top(9_500, 2_890, 2_891), // the last book at the slot: at or under the stop
		top(10_400, 2_950, 2_951),
		top(11_000, 2_895, 2_896))
	slot := byRule(t, reports, RuleThrough, TriggerSlot)
	c := slot.Children[0]
	// Priced from the book at the slot, decided at issued_at, arriving at the
	// first book 1 s later, whose bid is above the limit: taken at the bid.
	if !c.DecidedAt.Equal(at(10_000)) || c.LimitPriceKRW != 2_890 {
		t.Fatalf("child = %+v", c)
	}
	if c.ArrivedAt == nil || !c.ArrivedAt.Equal(at(11_000)) {
		t.Fatalf("arrival = %v", c.ArrivedAt)
	}
	if got := fillSummary(c); got != "33@2895/taker" {
		t.Fatalf("fills = %q", got)
	}
}

func TestRule7SlotBookTooOldDoesNotTrigger(t *testing.T) {
	reports := runReplay(t, []ReplayIntent{stopIntent(t)},
		top(7_000, 2_890, 2_891), // 3 s before the slot: older than the 2 s bound
		top(10_500, 2_950, 2_951))
	if r := byRule(t, reports, RuleThrough, TriggerSlot).Result; r.Done != DoneNotTriggered {
		t.Fatalf("done = %q", r.Done)
	}
}

func TestPlanSeesFillsAndFinishes(t *testing.T) {
	deep := orderbookLine(1_000, []lvl{{3_000, "40"}}, []lvl{{2_999, "1"}})
	reports := runReplay(t, []ReplayIntent{buyIntent(t, nil)}, top(0, 2_999, 3_000), deep, top(2_000, 2_999, 3_001))
	res := byRule(t, reports, RuleThrough, TriggerNone).Result
	if res.Done != DoneFilled || res.Filled.String() != "33" {
		t.Fatalf("result = %+v", res)
	}
}

func TestRefusedFramesAreCountedAndPriceNothing(t *testing.T) {
	bad := strings.Replace(top(500, 2_999, 3_000), `"ask_price":3000.0`, `"ask_price":2998.0`, 1) // crossed
	reports := runReplay(t, []ReplayIntent{buyIntent(t, nil)}, top(0, 2_999, 3_000), bad, top(1_000, 2_999, 3_001))
	if got := byRule(t, reports, RuleThrough, TriggerNone).Result.RefusedFrames; got != 1 {
		t.Fatalf("refused = %d", got)
	}
}

func TestFeedRefusesARecordThatGoesBackInTime(t *testing.T) {
	r, err := NewReplay([]ReplayIntent{buyIntent(t, nil)}, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	for i, ms := range []int64{1_000, 500} {
		ev, err := DecodeRecord([]byte(top(ms, 2_999, 3_000)))
		if err != nil {
			t.Fatal(err)
		}
		err = r.Feed(ev)
		if (i == 1) != (err != nil) {
			t.Fatalf("record %d: %v", i, err)
		}
	}
}

func TestResultIsDeterministic(t *testing.T) {
	lines := []string{top(0, 2_999, 3_000), top(1_000, 2_999, 3_001), tradeLine(1_100, 2_999, "4"), top(2_000, 2_990, 2_991)}
	a := runReplay(t, []ReplayIntent{buyIntent(t, nil)}, lines...)
	b := runReplay(t, []ReplayIntent{buyIntent(t, nil)}, lines...)
	if fmt.Sprintf("%+v", a) != fmt.Sprintf("%+v", b) {
		t.Fatalf("runs differ:\n%+v\n%+v", a, b)
	}
}

func TestOtherMarketRecordsAreIgnored(t *testing.T) {
	lines := []string{top(0, 2_999, 3_000), top(1_000, 2_999, 3_001), tradeLine(1_100, 2_999, "4")}
	other := strings.ReplaceAll(tradeLine(1_050, 1, "999"), "KRW-XRP", "KRW-BTC")
	with := runReplay(t, []ReplayIntent{buyIntent(t, nil)}, lines[0], lines[1], other, lines[2])
	without := runReplay(t, []ReplayIntent{buyIntent(t, nil)}, lines...)
	if fmt.Sprintf("%+v", with) != fmt.Sprintf("%+v", without) {
		t.Fatalf("a BTC record changed an XRP run:\n%+v\n%+v", with, without)
	}
}

func TestAvgPriceRoundsHalfUpFromExactValues(t *testing.T) {
	cases := []struct {
		qty      string
		notional string // decimal KRW
		want     string
	}{
		{"3", "9", "3.00"},
		{"3", "10", "3.33"},
		{"3", "11", "3.67"},
		{"8", "1", "0.13"}, // 0.125 rounds up
		{"1", "2998", "2998.00"},
	}
	for _, c := range cases {
		q, err := ParseQty(c.qty)
		if err != nil {
			t.Fatal(err)
		}
		n, ok := new(big.Int).SetString(c.notional, 10)
		if !ok {
			t.Fatal(c.notional)
		}
		n.Mul(n, big.NewInt(qtyPerUnit))
		if got := avgPrice(n, q); got != c.want {
			t.Errorf("avg(%s, %s) = %s, want %s", c.qty, c.notional, got, c.want)
		}
	}
}

func TestScaledPrintsShortestDecimal(t *testing.T) {
	for in, want := range map[string]string{"0": "0", "1": "0.00000001", "100000000": "1", "250000000": "2.5", "12345678901": "123.45678901"} {
		n, _ := new(big.Int).SetString(in, 10)
		if got := scaled(n, QtyScale); got != want {
			t.Errorf("scaled(%s) = %s, want %s", in, got, want)
		}
	}
}

func TestParseReplayIntent(t *testing.T) {
	base := `{"schema_version":1,"intent_id":"2d3c4b5a-6e7f-4a1b-8c2d-3e4f5a6b7c8d","venue":"upbit","account":"upbit_shadow","market":"KRW-XRP","side":"sell","max_quantity":"33","max_notional_krw":100000,"limit_price_krw":2900,"max_slippage_bps":100,"max_requotes":2,"expires_at":"2026-11-19T09:01:00Z"`
	if in, err := ParseReplayIntent([]byte(base + `,"issued_at":"2026-11-19T09:00:10Z","stop_price_krw":2900}`)); err != nil || in.StopPriceKRW != 2_900 || !in.IssuedAt.Equal(at(10_000)) {
		t.Fatalf("stop intent: %+v, %v", in, err)
	}
	if _, err := ParseReplayIntent([]byte(base + `}`)); err != nil {
		t.Fatalf("plain intent: %v", err)
	}
	for name, tail := range map[string]string{
		"stop without slot":   `,"stop_price_krw":2900}`,
		"stop on a buy":       `,"issued_at":"2026-11-19T09:00:10Z","stop_price_krw":2900,"side":"buy"}`,
		"off-grid stop":       `,"issued_at":"2026-11-19T09:00:10Z","stop_price_krw":5002}`,
		"issued after expiry": `,"issued_at":"2026-11-19T09:02:00Z"}`,
		"unknown field":       `,"surprise":1}`,
	} {
		if name == "stop on a buy" {
			// a duplicate key would overwrite side; build the buy directly
			buy := strings.Replace(base, `"side":"sell"`, `"side":"buy"`, 1)
			if _, err := ParseReplayIntent([]byte(buy + `,"issued_at":"2026-11-19T09:00:10Z","stop_price_krw":2900}`)); !errors.Is(err, ErrIntent) {
				t.Errorf("%s: error %v", name, err)
			}
			continue
		}
		if _, err := ParseReplayIntent([]byte(base + tail)); !errors.Is(err, ErrIntent) {
			t.Errorf("%s: error %v, want ErrIntent", name, err)
		}
	}
	// execupbit's strict Intent must keep refusing the replay-only fields.
	if _, err := ParseIntent([]byte(base + `,"stop_price_krw":2900}`)); err == nil {
		t.Error("ParseIntent accepted stop_price_krw")
	}
}

func TestParseDepthReadsEveryLevel(t *testing.T) {
	line := orderbookLine(0, []lvl{{3_000, "1.5"}, {3_001, "2"}, {3_003, "0.00000001"}}, []lvl{{2_999, "3"}, {2_998, "4"}, {2_996, "5"}})
	ev, err := DecodeRecord([]byte(line))
	if err != nil || ev.Kind != EventBook {
		t.Fatalf("%+v, %v", ev, err)
	}
	d := ev.Book
	if len(d.Asks) != 3 || len(d.Bids) != 3 || d.AskPriceKRW != 3_000 || d.BidPriceKRW != 2_999 {
		t.Fatalf("depth = %+v", d)
	}
	if d.Asks[2].PriceKRW != 3_003 || d.Asks[2].Qty.String() != "0.00000001" || d.Bids[2].PriceKRW != 2_996 || d.Bids[2].Qty.String() != "5" {
		t.Fatalf("levels = %+v / %+v", d.Asks, d.Bids)
	}
}

func TestParseDepthRefusesWhatItCannotFillAgainst(t *testing.T) {
	ok := []lvl{{3_000, "1"}, {3_001, "1"}}
	okBids := []lvl{{2_999, "1"}, {2_998, "1"}}
	cases := map[string][2][]lvl{
		"deeper ask not above the one before": {{{3_000, "1"}, {3_000, "1"}}, okBids},
		"deeper bid not below the one before": {ok, {{2_999, "1"}, {2_999, "1"}}},
		"empty deeper level":                  {{{3_000, "1"}, {3_001, "0"}}, okBids},
		"deeper level below the grid":         {ok, {{2_999, "1"}, {99, "1"}}},
		"crossed touch":                       {{{2_999, "1"}}, {{3_000, "1"}}},
	}
	for name, c := range cases {
		line := orderbookLine(0, c[0], c[1])
		ev, err := DecodeRecord([]byte(line))
		if err != nil || ev.Kind != EventRefused {
			t.Errorf("%s: event %+v, %v", name, ev, err)
		}
	}
	if _, err := ParseDepth([]byte(sampleFrame), testNow); err != nil {
		t.Fatalf("sample frame: %v", err)
	}
}

func TestParseTrade(t *testing.T) {
	good := `{"type":"trade","code":"KRW-XRP","timestamp":1795078800000,"trade_price":2998.0,"trade_volume":0.5,"ask_bid":"ASK"}`
	tr, err := ParseTrade([]byte(good), testNow)
	if err != nil || tr.PriceKRW != 2_998 || tr.Qty.String() != "0.5" || tr.Market != "KRW-XRP" {
		t.Fatalf("%+v, %v", tr, err)
	}
	for name, bad := range map[string]string{
		"orderbook":     `{"type":"orderbook","code":"KRW-XRP","timestamp":1,"trade_price":1.0,"trade_volume":1}`,
		"fractional":    `{"type":"trade","code":"KRW-XRP","timestamp":1,"trade_price":2998.5,"trade_volume":1}`,
		"zero volume":   `{"type":"trade","code":"KRW-XRP","timestamp":1,"trade_price":2998.0,"trade_volume":0}`,
		"nine decimals": `{"type":"trade","code":"KRW-XRP","timestamp":1,"trade_price":2998.0,"trade_volume":0.000000001}`,
		"no price":      `{"type":"trade","code":"KRW-XRP","timestamp":1,"trade_volume":1}`,
		"other market":  `{"type":"trade","code":"KRW-DOGE","timestamp":1,"trade_price":2998.0,"trade_volume":1}`,
	} {
		if _, err := ParseTrade([]byte(bad), testNow); !errors.Is(err, ErrTrade) {
			t.Errorf("%s: error %v, want ErrTrade", name, err)
		}
	}
}

func TestReadRecordsReadsPlainAndGzipDays(t *testing.T) {
	dir := t.TempDir()
	body := strings.Join([]string{top(0, 2_999, 3_000), markLine("gap", 100), tradeLine(200, 2_999, "1"), `{"kind":"heartbeat","market":"KRW-XRP","received_at":"` + iso(300) + `"}`}, "\n") + "\n"
	plain := filepath.Join(dir, "KRW-XRP.jsonl")
	if err := os.WriteFile(plain, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	gzPath := filepath.Join(dir, "day.jsonl.gz")
	f, err := os.Create(gzPath)
	if err != nil {
		t.Fatal(err)
	}
	zw := gzip.NewWriter(f)
	_, _ = zw.Write([]byte(body))
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	for _, path := range []string{plain, gzPath} {
		var kinds []string
		if err := ReadRecords(path, func(ev Event) error { kinds = append(kinds, ev.Kind); return nil }); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if got := strings.Join(kinds, ","); got != "book,gap,trade,other" {
			t.Fatalf("%s kinds = %s", path, got)
		}
	}
	if err := os.WriteFile(plain, []byte("not json\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ReadRecords(plain, func(Event) error { return nil }); err == nil || !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("bad line: %v", err)
	}
}

// Review finding: children that arrive on one frame share its displayed size.
func TestArrivalsOnOneFrameShareDisplayedDepth(t *testing.T) {
	reports := runReplay(t, []ReplayIntent{buyIntent(t, nil)},
		top(0, 2_999, 3_000),
		top(100, 2_999, 3_001), // the plan amends 3000 -> 3001
		orderbookLine(1_200, []lvl{{3_000, "10"}, {3_002, "100"}}, []lvl{{2_999, "1"}, {2_998, "1"}}))
	through := byRule(t, reports, RuleThrough, TriggerNone)
	if len(through.Children) < 2 || through.Children[1].Action != ActAmend {
		t.Fatalf("children = %+v", through.Children)
	}
	if through.Result.Filled.String() != "10" {
		t.Fatalf("filled %s from 10 displayed units (submit %q, amend %q)", through.Result.Filled,
			fillSummary(through.Children[0]), fillSummary(through.Children[1]))
	}
}

// Review finding: a trade snapshot after a reconnect is history, not a new
// execution, and a repeated sequential id is the same execution.
func TestTradeSnapshotAndRepeatedSequenceDoNotFillAgain(t *testing.T) {
	in := buyIntent(t, func(in *Intent) { in.MaxRequotes = 0 })
	reports := runReplay(t, []ReplayIntent{in},
		top(0, 2_999, 3_000),
		top(1_000, 2_999, 3_001),
		tradeSeq(1_100, 2_999, "10", "REALTIME", 7),
		tradeSeq(1_150, 2_999, "10", "REALTIME", 7), // the same execution again
		markLine("gap", 1_500),
		markLine("connected", 1_600),
		tradeSeq(1_650, 2_999, "10", "SNAPSHOT", 7), // replayed history
		tradeSeq(1_660, 2_999, "10", "SNAPSHOT", 8)) // history never seen live
	through := byRule(t, reports, RuleThrough, TriggerNone)
	if through.Result.Filled.String() != "10" {
		t.Fatalf("filled %s from one 10-unit execution; fills %q", through.Result.Filled, fillSummary(through.Children[0]))
	}
}

// Review finding: an intent issued inside a gap cannot be priced from the
// book from before the gap.
func TestIntentIssuedInsideAGapIsNotPricedFromAPreGapBook(t *testing.T) {
	in := buyIntent(t, func(in *Intent) {})
	in.IssuedAt = at(500)
	reports := runReplay(t, []ReplayIntent{in},
		top(0, 2_999, 3_000),
		markLine("gap", 100),
		markLine("connected", 1_500),
		top(1_600, 2_989, 2_990))
	for _, rep := range reports {
		if len(rep.Children) == 0 || !rep.Children[0].DecidedAt.Equal(at(1_600)) {
			t.Fatalf("%s: first child = %+v, want one decided at the 1600 ms book", rep.Result.Rule, rep.Children)
		}
	}
}

// Review finding: an amend that targets a child that never arrived must not
// be applied to the order that is working.
func TestAmendTargetingAVoidChildIsNotApplied(t *testing.T) {
	reports := runReplay(t, []ReplayIntent{buyIntent(t, nil)},
		top(0, 2_999, 3_000),
		top(1_000, 2_999, 3_001), // A arrives; the plan amends to B (3001)
		markLine("gap", 1_200),   // B is lost
		markLine("connected", 1_300),
		top(1_400, 2_999, 3_002), // the plan amends again, targeting B (C, 3002)
		top(2_500, 2_999, 3_002))
	through := byRule(t, reports, RuleThrough, TriggerNone)
	var c *ChildReport
	for i := range through.Children {
		if through.Children[i].Action == ActAmend && through.Children[i].LimitPriceKRW == 3_002 {
			c = &through.Children[i]
		}
	}
	if c == nil {
		t.Fatalf("no amend to 3002 in %+v", through.Children)
	}
	if c.Status != StatusTargetUnknown || len(c.Fills) != 0 || !through.Result.Filled.IsZero() {
		t.Fatalf("amend = %+v, filled %s", c, through.Result.Filled)
	}
}

// Review finding: the slot's book is the last one at or before issued_at,
// that includes a book received exactly at it, whether or not a marker with
// the same time comes first.
func TestSlotTriggerUsesTheBookReceivedExactlyAtTheSlot(t *testing.T) {
	for name, lines := range map[string][]string{
		"book at the slot":        {top(9_500, 2_950, 2_951), top(10_000, 2_890, 2_891), top(10_400, 2_950, 2_951)},
		"marker before that book": {top(9_500, 2_950, 2_951), markLine("connected", 10_000), top(10_000, 2_890, 2_891), top(10_400, 2_950, 2_951)},
	} {
		reports := runReplay(t, []ReplayIntent{stopIntent(t)}, lines...)
		slot := byRule(t, reports, RuleThrough, TriggerSlot)
		if slot.Result.Done == DoneNotTriggered || len(slot.Children) == 0 || !slot.Children[0].DecidedAt.Equal(at(10_000)) || slot.Children[0].LimitPriceKRW != 2_890 {
			t.Errorf("%s: slot run = %+v / %+v", name, slot.Result, slot.Children)
		}
	}
	reversed := runReplay(t, []ReplayIntent{stopIntent(t)}, top(9_500, 2_890, 2_891), top(10_000, 2_950, 2_951), top(10_400, 2_950, 2_951))
	if r := byRule(t, reversed, RuleThrough, TriggerSlot).Result; r.Done != DoneNotTriggered {
		t.Errorf("a bid above the stop at the slot triggered: %+v", r)
	}
}

// Review finding: the continuous trigger fires on a book at or after issued_at,
// not on the slot's look-back.
func TestContinuousTriggerIgnoresTheLookBack(t *testing.T) {
	reports := runReplay(t, []ReplayIntent{stopIntent(t)},
		top(9_500, 2_890, 2_891), // under the stop, before the slot
		top(10_400, 2_950, 2_951),
		top(11_000, 2_950, 2_951))
	if r := byRule(t, reports, RuleThrough, TriggerContinuous).Result; r.Done != DoneNotTriggered || r.Children != 0 {
		t.Fatalf("continuous run = %+v", r)
	}
	if r := byRule(t, reports, RuleThrough, TriggerSlot).Result; r.Children == 0 {
		t.Fatalf("slot run did not start: %+v", r)
	}
}

// Review finding: a stop must fire while the intent is valid.
func TestContinuousTriggerDoesNotFireAfterExpiry(t *testing.T) {
	reports := runReplay(t, []ReplayIntent{stopIntent(t)}, // expires at 60 s
		top(9_500, 2_950, 2_951), top(30_000, 2_950, 2_951), top(61_000, 2_890, 2_891), top(62_000, 2_890, 2_891))
	if r := byRule(t, reports, RuleThrough, TriggerContinuous).Result; r.Done != DoneNotTriggered || r.Children != 0 {
		t.Fatalf("continuous run = %+v", r)
	}
}

// Review finding: an outage that is known to exceed 60 s is exposed even if
// no reconnect follows.
func TestGapExposedWithoutAReconnect(t *testing.T) {
	arrive := []string{top(0, 2_999, 3_000), orderbookLine(1_000, []lvl{{3_000, "10"}}, []lvl{{2_999, "1"}})}
	long := runReplay(t, []ReplayIntent{buyIntent(t, nil)}, append(arrive[:2:2], markLine("gap", 2_000), markLine("gap", 63_000))...)
	if !byRule(t, long, RuleThrough, TriggerNone).Result.GapExposed {
		t.Error("gap at 2 s and a retry marker at 63 s is a 61 s outage while holding: want gap_exposed")
	}
	short := runReplay(t, []ReplayIntent{buyIntent(t, nil)}, append(arrive[:2:2], markLine("gap", 2_000), markLine("gap", 62_000))...)
	if byRule(t, short, RuleThrough, TriggerNone).Result.GapExposed {
		t.Error("60 s is not over 60 s")
	}
}

// Review finding: a heartbeat is written by another goroutine, so it may sit
// out of time order between market frames; it must not abort the replay or
// move the clock.
func TestHeartbeatOutOfOrderIsIgnored(t *testing.T) {
	hb := fmt.Sprintf(`{"kind":"heartbeat","market":"KRW-XRP","received_at":%q}`, iso(5_000))
	reports := runReplay(t, []ReplayIntent{buyIntent(t, nil)}, top(0, 2_999, 3_000), hb, top(4_900, 2_999, 3_000))
	if len(reports) != 2 {
		t.Fatalf("%d reports", len(reports))
	}
}

// Review finding: a level must sit on its own tick, not just above the grid.
func TestParseDepthRefusesAnOffTickLevel(t *testing.T) {
	for name, lines := range map[string][2][]lvl{
		"ask off its tick": {{{100_001, "1"}}, {{100_000, "1"}}},
		"bid off its tick": {{{100_100, "1"}}, {{100_050, "1"}}},
		"deeper off tick":  {{{100_100, "1"}, {100_250, "1"}}, {{100_000, "1"}, {99_900, "1"}}},
	} {
		ev, err := DecodeRecord([]byte(orderbookLine(0, lines[0], lines[1])))
		if err != nil || ev.Kind != EventRefused {
			t.Errorf("%s: event %+v, %v", name, ev, err)
		}
	}
}
