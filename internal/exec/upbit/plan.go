package upbit

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

// Actions a decision can carry. submit opens the one order lineage, amend
// moves its price and never grows it, cancel closes it, and finish ends the
// intent. hold is "do nothing".
const (
	ActHold   = "hold"
	ActSubmit = "submit"
	ActAmend  = "amend"
	ActCancel = "cancel"
	ActFinish = "finish"
)

// Finish reasons.
const (
	DoneFilled   = "filled"
	DoneDeadline = "deadline"
)

// Child is one Upbit child request as this model describes it. It is printed
// by cmd/execupbit and never sent; the wire contract is auto_trader hk 5438's.
type Child struct {
	Action        string `json:"action"`
	IntentID      string `json:"intent_id"`
	Key           string `json:"idempotency_key"`
	TargetKey     string `json:"target_idempotency_key,omitempty"`
	Quantity      *Qty   `json:"quantity,omitempty"`
	LimitPriceKRW int64  `json:"limit_price_krw,omitempty"`
}

// Order is the plan's view of its one working order.
type Order struct {
	Key      string
	PriceKRW int64
	Qty      Qty
}

// Decision is what the plan would do at one instant. Action is always set;
// Child is set for submit, amend and cancel; Done for finish.
type Decision struct {
	Action string `json:"action"`
	Reason string `json:"reason,omitempty"`
	Child  *Child `json:"child,omitempty"`
	Done   string `json:"done,omitempty"`
}

// Config holds the plan's runtime knobs.
type Config struct {
	// StaleAfter is the oldest book a submit or amend may be priced from. A
	// re-quote computes the same way: no action reads a book older than this.
	StaleAfter time.Duration
}

// DefaultConfig refuses a book older than 2 s, the recorder's stale bound.
func DefaultConfig() Config { return Config{StaleAfter: 2 * time.Second} }

var (
	// ErrPlan marks a plan misuse: an invalid config, a fill that is not one,
	// or a decision asked of a finished plan.
	ErrPlan = errors.New("upbit: invalid plan use")
)

// Plan is the shadow timing policy for one intent: taker price at the touch,
// re-quote while the touch moves away, at most MaxRequotes times, cancel the
// remainder at expiry, and never exceed the intent's remaining quantity or
// notional.
//
// It holds no journal and no gateway. One Plan is driven from one goroutine.
type Plan struct {
	in  Intent
	cfg Config

	low, high int64

	filled            Qty
	filledNotionalKRW int64

	order    *Order
	requotes int
	// closing is the reason of the cancel already emitted; the next step
	// finishes with it, since a shadow run has no cancel outcome to wait for.
	closing string
	done    string
	sent    int
}

// NewPlan validates the intent and config and computes the price band.
func NewPlan(in Intent, cfg Config) (*Plan, error) {
	if cfg.StaleAfter <= 0 {
		return nil, fmt.Errorf("%w: stale_after %s", ErrPlan, cfg.StaleAfter)
	}
	if err := in.Validate(); err != nil {
		return nil, err
	}
	low, high := in.Band()
	return &Plan{in: in, cfg: cfg, low: low, high: high}, nil
}

// Intent returns the plan's intent.
func (p *Plan) Intent() Intent { return p.in }

// Done is the finish reason once the plan has finished, else "".
func (p *Plan) Done() string { return p.done }

// Requotes is how many re-quotes the plan has emitted.
func (p *Plan) Requotes() int { return p.requotes }

// WorkingOrder is the plan's working order, or nil.
func (p *Plan) WorkingOrder() *Order { return p.order }

// Fill records one execution of q at price. Fills are evidence: they are
// recorded whole even past the target, and unlimited sending never follows
// (remaining quantity clamps at zero). The shadow dry run has no fill source;
// this exists so the remaining-quantity rule is stated and testable.
func (p *Plan) Fill(q Qty, price int64) error {
	if q.IsNegative() || q.IsZero() || price < minPriceKRW || TickAt(price) == 0 {
		return fmt.Errorf("%w: fill %q at %d", ErrPlan, q, price)
	}
	n := notionalKRW(q, price)
	if p.filledNotionalKRW > (1<<63-1)-n {
		return fmt.Errorf("%w: fill notional overflow", ErrPlan)
	}
	p.filled = p.filled.Add(q)
	p.filledNotionalKRW += n
	return nil
}

// OnBook decides at now for the touch b. It is the only place a new submit or
// amend is priced.
func (p *Plan) OnBook(now time.Time, b Book) Decision {
	if d, ok := p.pre(now); !ok {
		return d
	}
	if b.Market != p.in.Market {
		return hold("other_market")
	}
	if b.ReceivedAt.IsZero() || now.Before(b.ReceivedAt) {
		return hold("bad_book_time")
	}
	if now.Sub(b.ReceivedAt) > p.cfg.StaleAfter {
		return hold("stale_book")
	}
	price, ok := p.takerPrice(b)
	if !ok {
		return hold("band")
	}
	if p.order == nil {
		qty := p.openQty(price)
		if !p.orderOK(qty, price) {
			return hold("min_order")
		}
		c := p.child(ActSubmit, &qty, price, "")
		p.order = &Order{Key: c.Key, PriceKRW: price, Qty: qty}
		return Decision{Action: ActSubmit, Reason: "open", Child: &c}
	}
	switch {
	case price == p.order.PriceKRW:
		return hold("at_touch")
	case !p.in.worse(price, p.order.PriceKRW):
		// The touch moved past our order: it is marketable as it stands, and
		// re-quoting would only churn.
		return hold("marketable")
	case p.requotes >= p.in.MaxRequotes:
		return hold("requote_limit")
	}
	qty := p.order.Qty
	if rem := p.remaining(); qty.Cmp(rem) > 0 {
		qty = rem
	}
	if room := qtyForBudget(p.remainingNotionalKRW(), price); qty.Cmp(room) > 0 {
		qty = room
	}
	if !p.orderOK(qty, price) {
		return hold("min_order")
	}
	c := p.child(ActAmend, &qty, price, p.order.Key)
	p.order = &Order{Key: c.Key, PriceKRW: price, Qty: qty}
	p.requotes++
	return Decision{Action: ActAmend, Reason: "requote", Child: &c}
}

// Tick decides at now with no new book: it only finishes and cancels the
// remainder at expiry, so a silent book still meets the deadline.
func (p *Plan) Tick(now time.Time) Decision {
	if d, ok := p.pre(now); !ok {
		return d
	}
	return hold("idle")
}

// pre handles everything that needs no book: a finished plan, a pending
// finish after an emitted cancel, a reached target, and expiry.
func (p *Plan) pre(now time.Time) (Decision, bool) {
	if p.done != "" {
		return hold("done"), false
	}
	if p.closing != "" {
		p.done = p.closing
		return Decision{Action: ActFinish, Reason: p.closing, Done: p.closing}, false
	}
	if p.filled.Cmp(p.in.Target()) >= 0 {
		return p.close(DoneFilled), false
	}
	if !now.Before(p.in.ExpiresAt) {
		return p.close(DoneDeadline), false
	}
	return Decision{}, true
}

// close cancels the working order and finishes; with no order it finishes.
func (p *Plan) close(reason string) Decision {
	if p.order != nil {
		c := p.child(ActCancel, nil, 0, p.order.Key)
		p.order = nil
		p.closing = reason
		return Decision{Action: ActCancel, Reason: reason, Child: &c}
	}
	p.done = reason
	return Decision{Action: ActFinish, Reason: reason, Done: reason}
}

// takerPrice is the price that crosses the touch on the intent's side, snapped
// onto the tick grid in the crossing direction and held inside the band.
func (p *Plan) takerPrice(b Book) (int64, bool) {
	var price int64
	if p.in.Side == Buy {
		price = Ceil(b.AskPriceKRW)
		if price > p.high {
			return 0, false
		}
	} else {
		price = Floor(b.BidPriceKRW)
		if price < p.low {
			return 0, false
		}
	}
	if TickAt(price) == 0 {
		return 0, false
	}
	return price, true
}

// openQty is the quantity a new order may carry at price: the remaining
// quantity, cut to the notional room left.
func (p *Plan) openQty(price int64) Qty {
	qty := p.remaining()
	if room := qtyForBudget(p.remainingNotionalKRW(), price); qty.Cmp(room) > 0 {
		qty = room
	}
	return qty
}

// orderOK reports whether qty at price may be sent: positive, at least the
// venue minimum, and within the intent's notional.
func (p *Plan) orderOK(qty Qty, price int64) bool {
	return !qty.IsNegative() && !qty.IsZero() &&
		notionalAtLeast(qty, price, MinOrderKRW) &&
		notionalFits(qty, price, p.in.MaxNotionalKRW) &&
		notionalFits(qty, price, p.remainingNotionalKRW())
}

// remainingNotionalKRW is the notional the intent may still commit, never
// below zero.
func (p *Plan) remainingNotionalKRW() int64 {
	r := p.in.MaxNotionalKRW - p.filledNotionalKRW
	if r < 0 {
		return 0
	}
	return r
}

// remaining is the intent's target less recorded fills, never below zero.
func (p *Plan) remaining() Qty {
	r := p.in.Target().Sub(p.filled)
	if r.IsNegative() {
		return Qty{}
	}
	return r
}

func (p *Plan) child(action string, qty *Qty, price int64, target string) Child {
	p.sent++
	return Child{
		Action:        action,
		IntentID:      p.in.IntentID,
		Key:           p.in.IntentID + ":" + strconv.Itoa(p.sent),
		TargetKey:     target,
		Quantity:      qty,
		LimitPriceKRW: price,
	}
}

func hold(reason string) Decision { return Decision{Action: ActHold, Reason: reason} }
