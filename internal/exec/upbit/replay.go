package upbit

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/jsonx"
)

// Replay fills the children of a Plan against a recording. It implements
// auto_trader docs/design/5852-upbit-krw-shadow-strategy.md §7.2 rules 1-7:
// the recorded book is the only authority, and nothing here sends or reads
// anything but the recording.
//
// How each rule is read (the design leaves the wording open at these points):
//
//  1. A child decided at τ (the receive time of the book that priced it)
//     arrives at the first orderbook frame received at or after τ+ArrivalDelay.
//     It is filled only against that frame and later records. The plan sees a
//     fill only from the next record on, never from the record it decides on.
//  2. On arrival the order takes the displayed size of every opposite level
//     within its limit, best first, each at the level's own price. Whatever
//     the 15 recorded levels do not cover rests at the limit.
//  3. A resting order fills at its own limit. Under Through only a trade
//     strictly through the limit fills it, by that trade's size. Under Touch a
//     trade at or through the limit, or an opposite touch at or through it,
//     fills all that is open. No queue position is modelled.
//  4. Nothing fills at or after expires_at. The plan cancels at the first
//     record at or after it, before that record could land a child, so no
//     child arrives then.
//  5. A gap record opens a window that the next connected record (or the next
//     frame) closes. A child still in flight when a gap opens is void, and no
//     fill is recognised inside the window. A gap longer than GapExposedAfter
//     that opens while the run holds a position (positionHeld) marks it
//     gap_exposed; the episode, which knows the position the run does not,
//     is the harness's to judge.
//  6. Every run is done once per fill rule, so through and touch are counted
//     separately; the plan decides from the fills of its own rule.
//  7. An intent with stop_price_krw is a stop-loss sell, dormant until
//     triggered. Slot trigger (the judged value): the best bid of the last
//     book at or before issued_at, if no older than the plan's stale bound,
//     is at or under the stop; if not, the intent never starts. Continuous
//     trigger (auxiliary): the first recorded book at or after issued_at with
//     a best bid at or under the stop starts it, priced from that book.
//
// An order whose submit was void (rule 5) stays "working" in the plan, which
// has no way to learn of the loss; the replay does not resubmit it. The run
// reports the void child, and its fill stays at what rule 5 allows.
const (
	// ArrivalDelay is rule 1's order round trip. It is a fixed value, not a
	// flag: the design sets it before the sealing and never changes it.
	ArrivalDelay = time.Second
	// GapExposedAfter is rule 5's longest unobserved stretch while holding a
	// position.
	GapExposedAfter = 60 * time.Second
)

// FillRule is rule 3's choice for a resting order.
type FillRule string

const (
	// RuleThrough is the conservative rule and the one a verdict uses.
	RuleThrough FillRule = "through"
	// RuleTouch is the optimistic bound.
	RuleTouch FillRule = "touch"
)

// FillRules is both rules, conservative first.
var FillRules = []FillRule{RuleThrough, RuleTouch}

// Trigger is rule 7's stop trigger; it is empty for an intent without a stop.
type Trigger string

const (
	TriggerNone       Trigger = ""
	TriggerSlot       Trigger = "slot"
	TriggerContinuous Trigger = "continuous"
)

// ReplayIntent is an Intent as replay reads it: the shadow intent plus the
// time the executor starts and, for a stop-loss sell, the stop. The extra
// fields exist only here, so cmd/execupbit, which decodes Intent strictly,
// refuses them rather than ignoring a stop.
type ReplayIntent struct {
	Intent
	// IssuedAt is when the intent is published (the slot time). Without it
	// the intent starts at the first record.
	IssuedAt time.Time `json:"issued_at,omitzero"`
	// StopPriceKRW makes the intent a stop-loss sell, dormant until the best
	// bid reaches it. It needs IssuedAt, the slot that checks it.
	StopPriceKRW int64 `json:"stop_price_krw,omitempty"`
}

// ParseReplayIntent decodes one replay intent and refuses unknown fields.
func ParseReplayIntent(data []byte) (ReplayIntent, error) {
	var in ReplayIntent
	if err := jsonx.DecodeStrict(data, &in); err != nil {
		return ReplayIntent{}, fmt.Errorf("%w: %v", ErrIntent, err)
	}
	if err := in.Validate(); err != nil {
		return ReplayIntent{}, err
	}
	return in, nil
}

// Validate checks the intent and the replay fields.
func (in ReplayIntent) Validate() error {
	if err := in.Intent.Validate(); err != nil {
		return err
	}
	bad := func(format string, a ...any) error {
		return fmt.Errorf("%w: "+format, append([]any{ErrIntent}, a...)...)
	}
	switch {
	case !in.IssuedAt.IsZero() && !in.IssuedAt.Before(in.ExpiresAt):
		return bad("issued_at %s is not before expires_at", in.IssuedAt.Format(time.RFC3339Nano))
	case in.StopPriceKRW < 0:
		return bad("stop_price_krw %d", in.StopPriceKRW)
	case in.StopPriceKRW > 0 && in.Side != Sell:
		return bad("stop_price_krw needs a sell, not %q", in.Side)
	case in.StopPriceKRW > 0 && (TickAt(in.StopPriceKRW) == 0 || in.StopPriceKRW%TickAt(in.StopPriceKRW) != 0):
		return bad("stop_price_krw %d is not on the KRW tick grid", in.StopPriceKRW)
	case in.StopPriceKRW > 0 && in.IssuedAt.IsZero():
		return bad("stop_price_krw needs issued_at, the slot that checks it")
	}
	return nil
}

// Fill is one execution of a child.
type Fill struct {
	At       time.Time `json:"at"`
	Quantity Qty       `json:"quantity"`
	PriceKRW int64     `json:"price_krw"`
	// Source is taker (rule 2), trade (Through, rule 3), touch_trade or
	// touch_book (Touch, rule 3).
	Source string `json:"source"`
}

// Child statuses.
const (
	StatusApplied    = "applied"          // a cancel
	StatusActive     = "active"           // arrived; Fills tells what it did
	StatusVoidGap    = "void_gap"         // in flight when a gap opened
	StatusNoOrder    = "no_working_order" // an amend with nothing left to amend
	StatusCancelled  = "cancelled"        // cancelled before it arrived
	StatusNotArrived = "not_arrived"      // the recording ended first
)

// Finish reasons a run adds to the plan's own (filled, deadline).
const (
	DoneNotTriggered = "not_triggered" // a stop that never reached its trigger
	DoneUnfinished   = "unfinished"    // the recording ended before expires_at
)

// Fill sources.
const (
	fillSourceTaker   = "taker"
	fillSourceTrade   = "trade"
	fillSourceTouchTr = "touch_trade"
	fillSourceTouchBk = "touch_book"
)

// Output line kinds.
const (
	resultKind = "result"
	childKind  = "child"
)

// ChildReport is one child the plan issued, with what the recording did to it.
type ChildReport struct {
	Kind          string     `json:"kind"`
	IntentID      string     `json:"intent_id"`
	Rule          FillRule   `json:"rule"`
	Trigger       Trigger    `json:"trigger,omitempty"`
	Key           string     `json:"idempotency_key"`
	TargetKey     string     `json:"target_idempotency_key,omitempty"`
	Action        string     `json:"action"`
	Reason        string     `json:"reason,omitempty"`
	Quantity      *Qty       `json:"quantity,omitempty"`
	LimitPriceKRW int64      `json:"limit_price_krw,omitempty"`
	DecidedAt     time.Time  `json:"decided_at"`
	ArrivedAt     *time.Time `json:"arrived_at,omitempty"`
	Status        string     `json:"status"`
	Fills         []Fill     `json:"fills,omitempty"`
}

// Result is one run: an intent under one fill rule (and trigger).
type Result struct {
	Kind     string   `json:"kind"`
	IntentID string   `json:"intent_id"`
	Market   string   `json:"market"`
	Side     Side     `json:"side"`
	Rule     FillRule `json:"rule"`
	Trigger  Trigger  `json:"trigger,omitempty"`
	Target   Qty      `json:"target_quantity"`
	Filled   Qty      `json:"filled_quantity"`
	// FillNotionalKRW is the exact sum of quantity x price over the fills, a
	// decimal string; AvgPriceKRW is that over the filled quantity rounded to
	// 2 decimals, half away from zero. Both are empty without fills.
	FillNotionalKRW string `json:"fill_notional_krw,omitempty"`
	AvgPriceKRW     string `json:"avg_price_krw,omitempty"`
	Children        int    `json:"children"`
	VoidChildren    int    `json:"void_children"`
	Done            string `json:"done"`
	Gaps            int    `json:"gaps"`
	GapExposed      bool   `json:"gap_exposed"`
	RefusedFrames   int    `json:"refused_frames"`
}

// Report is one run's children, in the order the plan issued them, and its
// result.
type Report struct {
	Children []ChildReport
	Result   Result
}

// Replay drives one run per (intent, rule, trigger) over a stream of records
// in a single pass: records are decoded once and fed to every run.
type Replay struct {
	runs   []*run
	lastAt map[string]time.Time
}

// NewReplay builds the runs: for each intent, both rules, and for a stop-loss
// intent both rules under the slot trigger and then under the continuous one.
func NewReplay(intents []ReplayIntent, cfg Config) (*Replay, error) {
	if len(intents) == 0 {
		return nil, errors.New("upbit: replay needs an intent")
	}
	r := &Replay{lastAt: map[string]time.Time{}}
	for _, in := range intents {
		triggers := []Trigger{TriggerNone}
		if in.StopPriceKRW > 0 {
			triggers = []Trigger{TriggerSlot, TriggerContinuous}
		}
		for _, trig := range triggers {
			for _, rule := range FillRules {
				plan, err := NewPlan(in.Intent, cfg)
				if err != nil {
					return nil, err
				}
				r.runs = append(r.runs, &run{
					in: in, rule: rule, trig: trig, cfg: cfg, plan: plan,
					notional: new(big.Int),
				})
			}
		}
	}
	return r, nil
}

// Feed gives every run one record. Records of a market must not go back in
// time; a recording that does is not evidence of an order.
func (r *Replay) Feed(ev Event) error {
	if last, ok := r.lastAt[ev.Market]; ok && ev.At.Before(last) {
		return fmt.Errorf("%s record at %s is before the one at %s", ev.Market, ev.At.Format(time.RFC3339Nano), last.Format(time.RFC3339Nano))
	}
	r.lastAt[ev.Market] = ev.At
	for _, run := range r.runs {
		if run.in.Market != ev.Market {
			continue
		}
		if err := run.feed(ev); err != nil {
			return fmt.Errorf("intent %s %s: %w", run.in.IntentID, run.rule, err)
		}
	}
	return nil
}

// Finish ends the recording and returns one Report per run, in run order.
func (r *Replay) Finish() []Report {
	out := make([]Report, 0, len(r.runs))
	for _, run := range r.runs {
		out = append(out, run.finish())
	}
	return out
}

// child is a ChildReport under construction, plus the time it arrives.
type child struct {
	ChildReport
	due   time.Time
	price int64
	qty   Qty
}

// rest is the one order working in the book.
type rest struct {
	c     *child
	price int64
	open  Qty
}

type run struct {
	in   ReplayIntent
	rule FillRule
	trig Trigger
	cfg  Config
	plan *Plan

	started, triggered, dead bool
	last                     *Depth
	lastAt                   time.Time

	pending  []*child
	rest     *rest
	children []*child

	filled   Qty
	notional *big.Int // sum of quantity units x price: KRW at 1e-8

	inGap   bool
	gapAt   time.Time
	gapHeld bool
	gaps    int
	exposed bool
	refused int
}

func (r *run) live() bool { return r.started && r.triggered && !r.dead }

func (r *run) feed(ev Event) error {
	r.lastAt = ev.At
	if !r.started && (r.in.IssuedAt.IsZero() || !ev.At.Before(r.in.IssuedAt)) {
		if err := r.start(); err != nil {
			return err
		}
	}
	switch ev.Kind {
	case EventBook:
		r.closeGap(ev.At)
		d := ev.Book
		if r.started && !r.triggered && !r.dead && r.trig == TriggerContinuous && d.BidPriceKRW <= r.in.StopPriceKRW {
			r.triggered = true
		}
		if r.live() {
			r.handle(r.plan.OnBook(ev.At, d.Book), ev.At)
			if err := r.arrive(ev.At, d); err != nil {
				return err
			}
			if err := r.touchBook(ev.At, d); err != nil {
				return err
			}
		}
		r.last = &d
	case EventTrade:
		r.closeGap(ev.At)
		if r.live() {
			r.handle(r.plan.Tick(ev.At), ev.At)
			if err := r.onTrade(ev.At, ev.Trade); err != nil {
				return err
			}
		}
	case EventGap:
		r.tick(ev.At)
		r.openGap(ev.At)
	case EventRefused:
		r.refused++
	default: // connected, other
		if ev.Kind == EventConnected {
			r.closeGap(ev.At)
		}
		r.tick(ev.At)
	}
	return nil
}

func (r *run) tick(at time.Time) {
	if r.live() {
		r.handle(r.plan.Tick(at), at)
	}
}

// start runs once, at the first record at or after issued_at (rule 7's slot).
func (r *run) start() error {
	r.started = true
	slot := r.in.IssuedAt
	if r.in.StopPriceKRW == 0 {
		r.triggered = true
	} else if r.last != nil && slot.Sub(r.last.ReceivedAt) <= r.cfg.StaleAfter && r.last.BidPriceKRW <= r.in.StopPriceKRW {
		r.triggered = true
	}
	switch {
	case r.triggered:
		// The slot prices from the last book it can see, which is older than
		// the record that starts the run.
		if r.last != nil && !slot.IsZero() {
			r.handle(r.plan.OnBook(slot, r.last.Book), slot)
		}
	case r.trig == TriggerSlot:
		r.dead = true
	}
	return nil
}

// handle records what the plan decided at at.
func (r *run) handle(d Decision, at time.Time) {
	c := d.Child
	if c == nil {
		return
	}
	ch := &child{
		ChildReport: ChildReport{
			Kind: childKind, IntentID: c.IntentID, Rule: r.rule, Trigger: r.trig,
			Key: c.Key, TargetKey: c.TargetKey, Action: c.Action, Reason: d.Reason,
			Quantity: c.Quantity, LimitPriceKRW: c.LimitPriceKRW, DecidedAt: at,
		},
		due: at.Add(ArrivalDelay), price: c.LimitPriceKRW,
	}
	if c.Quantity != nil {
		ch.qty = *c.Quantity
	}
	r.children = append(r.children, ch)
	if c.Action == ActCancel {
		ch.Status = StatusApplied
		for _, p := range r.pending {
			p.Status = StatusCancelled
		}
		r.pending, r.rest = nil, nil
		return
	}
	r.pending = append(r.pending, ch)
}

// arrive lands every child that is due at this book frame (rule 1) and takes
// the book's displayed size for it (rule 2).
func (r *run) arrive(at time.Time, d Depth) error {
	if len(r.pending) == 0 {
		return nil
	}
	var wait []*child
	for _, c := range r.pending {
		if at.Before(c.due) {
			wait = append(wait, c)
			continue
		}
		switch c.Action {
		case ActSubmit:
			r.rest = &rest{c: c, price: c.price, open: c.qty}
		case ActAmend:
			if r.rest == nil {
				c.Status = StatusNoOrder
				continue
			}
			// An amend never grows the order: it keeps what is still open.
			open := r.rest.open
			if c.qty.Cmp(open) < 0 {
				open = c.qty
			}
			r.rest = &rest{c: c, price: c.price, open: open}
		}
		c.Status = StatusActive
		arrived := at
		c.ArrivedAt = &arrived
		if err := r.sweep(at, d); err != nil {
			return err
		}
	}
	r.pending = wait
	return nil
}

// sweep is rule 2: the working order takes each opposite level within its
// limit, best first, up to the level's displayed size.
func (r *run) sweep(at time.Time, d Depth) error {
	levels, ok := d.Asks, func(p int64) bool { return p <= r.rest.price }
	if r.in.Side == Sell {
		levels, ok = d.Bids, func(p int64) bool { return p >= r.rest.price }
	}
	for _, l := range levels {
		if r.rest == nil || !ok(l.PriceKRW) {
			break
		}
		q := l.Qty
		if r.rest.open.Cmp(q) < 0 {
			q = r.rest.open
		}
		if err := r.fill(at, q, l.PriceKRW, fillSourceTaker); err != nil {
			return err
		}
	}
	return nil
}

// touchBook is rule 3 under Touch for a book frame: an opposite touch at or
// through the limit fills all that is open, at the limit.
func (r *run) touchBook(at time.Time, d Depth) error {
	if r.rule != RuleTouch || r.rest == nil || !at.Before(r.in.ExpiresAt) {
		return nil
	}
	if reaches(r.in.Side, oppositeTouch(r.in.Side, d.Book), r.rest.price) {
		return r.fill(at, r.rest.open, r.rest.price, fillSourceTouchBk)
	}
	return nil
}

// onTrade is rule 3 for a trade frame. The order is only working once it has
// arrived, so a trade before its arrival frame never reaches it.
func (r *run) onTrade(at time.Time, t Trade) error {
	if r.rest == nil || !at.Before(r.in.ExpiresAt) {
		return nil
	}
	switch {
	case r.rule == RuleThrough && through(r.in.Side, t.PriceKRW, r.rest.price):
		q := t.Qty
		if r.rest.open.Cmp(q) < 0 {
			q = r.rest.open
		}
		return r.fill(at, q, r.rest.price, fillSourceTrade)
	case r.rule == RuleTouch && reaches(r.in.Side, t.PriceKRW, r.rest.price):
		return r.fill(at, r.rest.open, r.rest.price, fillSourceTouchTr)
	}
	return nil
}

// fill books q at price against the working order. It is the one place a fill
// is recognised, so rules 4 and 5 are enforced here and nowhere else.
func (r *run) fill(at time.Time, q Qty, price int64, source string) error {
	if r.rest == nil || q.IsZero() || r.inGap || !at.Before(r.in.ExpiresAt) {
		return nil
	}
	rs := r.rest
	rs.open = rs.open.Sub(q)
	rs.c.Fills = append(rs.c.Fills, Fill{At: at, Quantity: q, PriceKRW: price, Source: source})
	r.filled = r.filled.Add(q)
	r.notional.Add(r.notional, new(big.Int).Mul(big.NewInt(q.units), big.NewInt(price)))
	if rs.open.IsZero() {
		r.rest = nil
	}
	return r.plan.Fill(q, price)
}

// openGap starts rule 5's window: a child in flight is lost, and a position
// held now is exposed for as long as the window lasts.
func (r *run) openGap(at time.Time) {
	if r.inGap {
		return
	}
	r.inGap, r.gapAt, r.gapHeld = true, at, r.positionHeld()
	r.gaps++
	for _, c := range r.pending {
		c.Status = StatusVoidGap
	}
	r.pending = nil
}

// positionHeld is whether this run has a position exposed to the market. A
// buy holds one from its first fill on, to the end of the recording. A sell
// holds the position it is selling from the run's start until it is done or
// filled; a stop that never triggered under the slot value sells nothing.
func (r *run) positionHeld() bool {
	if r.in.Side == Buy {
		return !r.filled.IsZero()
	}
	return r.started && !r.dead && r.plan.Done() == "" && r.filled.Cmp(r.in.Target()) < 0
}

func (r *run) closeGap(at time.Time) {
	if !r.inGap {
		return
	}
	r.inGap = false
	if r.gapHeld && at.Sub(r.gapAt) > GapExposedAfter {
		r.exposed = true
	}
}

func (r *run) finish() Report {
	// A plan that cancels needs one more step to finish (it has no cancel
	// outcome to wait for), so the recording's end steps it twice.
	r.tick(r.lastAt)
	r.tick(r.lastAt)
	for _, c := range r.pending {
		c.Status = StatusNotArrived
	}
	res := Result{
		Kind: resultKind, IntentID: r.in.IntentID, Market: r.in.Market, Side: r.in.Side,
		Rule: r.rule, Trigger: r.trig, Target: r.in.Target(), Filled: r.filled,
		Children: len(r.children), Gaps: r.gaps, GapExposed: r.exposed, RefusedFrames: r.refused,
	}
	switch {
	case !r.triggered:
		res.Done = DoneNotTriggered
	case r.plan.Done() != "":
		res.Done = r.plan.Done()
	default:
		res.Done = DoneUnfinished
	}
	out := make([]ChildReport, 0, len(r.children))
	for _, c := range r.children {
		if c.Status == StatusVoidGap {
			res.VoidChildren++
		}
		out = append(out, c.ChildReport)
	}
	if !r.filled.IsZero() {
		res.FillNotionalKRW = scaled(r.notional, QtyScale)
		res.AvgPriceKRW = avgPrice(r.notional, r.filled)
	}
	return Report{Children: out, Result: res}
}

func oppositeTouch(side Side, b Book) int64 {
	if side == Buy {
		return b.AskPriceKRW
	}
	return b.BidPriceKRW
}

// reaches reports whether a market price p is at or through order price o.
func reaches(side Side, p, o int64) bool {
	if side == Buy {
		return p <= o
	}
	return p >= o
}

// through reports whether a trade at p is strictly through order price o.
func through(side Side, p, o int64) bool {
	if side == Buy {
		return p < o
	}
	return p > o
}

// scaled writes v / 10^scale as the shortest plain decimal.
func scaled(v *big.Int, scale int) string {
	s := v.String()
	if len(s) <= scale {
		s = strings.Repeat("0", scale-len(s)+1) + s
	}
	whole, frac := s[:len(s)-scale], strings.TrimRight(s[len(s)-scale:], "0")
	if frac == "" {
		return whole
	}
	return whole + "." + frac
}

// avgPrice is notional over qty in KRW rounded to 2 decimals, half up (both
// are positive), from the exact values.
func avgPrice(notional *big.Int, qty Qty) string {
	n := new(big.Int).Mul(notional, big.NewInt(200))
	d := new(big.Int).Mul(big.NewInt(qty.units), big.NewInt(2))
	n.Add(n, big.NewInt(qty.units))
	n.Quo(n, d)
	s := n.String()
	if len(s) < 3 {
		s = strings.Repeat("0", 3-len(s)) + s
	}
	return s[:len(s)-2] + "." + s[len(s)-2:]
}
