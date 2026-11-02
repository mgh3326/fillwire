package exec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"
)

// Child actions, as in E1's child request contract.
const (
	ActSubmit = "submit"
	ActAmend  = "amend"
	ActCancel = "cancel"
)

// E1 outcome statuses (docs/design/1440-intent-gateway-contract.md, Outcome).
const (
	StatusAccepted  = "accepted"
	StatusUncertain = "uncertain"
	StatusSending   = "sending"
	StatusRefused   = "refused"
	StatusConflict  = "conflict"
)

// Finish reasons.
const (
	DoneFilled    = "filled"
	DoneCap       = "cap"
	DoneDeadline  = "deadline"
	DoneKilled    = "killed"
	DoneRefused   = "refused"
	DoneStaleBook = "stale_book"
	// DoneE1Caps: no order is working and E1's cumulative caps leave no room
	// for a new one (exposure.go).
	DoneE1Caps = "e1_caps"
)

// Child is one child request. Targets are named by idempotency key, never by
// broker order id (E1 contract, Child request).
type Child struct {
	Action    string `json:"action"`
	IntentID  string `json:"intent_id"`
	Key       string `json:"idempotency_key"`
	TargetKey string `json:"target_idempotency_key,omitempty"`
	Qty       int64  `json:"quantity,omitempty"`
	Price     int64  `json:"limit_price_krw,omitempty"`
}

// Outcome is the gateway's answer to one child request.
type Outcome struct {
	Key           string `json:"key"`
	Status        string `json:"status"`
	Reason        string `json:"reason,omitempty"`
	BrokerOrderID string `json:"broker_order_id,omitempty"`
}

// Order is the executor's view of its one working order. Only one order
// lineage is ever live, so its open quantity is State.Remaining; Qty is the
// quantity last sent or reported and is not reduced by fills.
type Order struct {
	Key   string `json:"key"` // key of the submit or amend that set it
	Price int64  `json:"price"`
	Qty   int64  `json:"qty"`
}

// View is what a reconciling query returns: the intent's working order at the
// broker, if any, and the quantity the broker says has filled. FilledQty only
// holds back sends; it never counts as a fill.
type View struct {
	Order     *Order `json:"order,omitempty"`
	FilledQty int64  `json:"filled_qty"`
}

// Fill is one execution from ledger evidence: the only thing that makes the
// executor count a fill.
type Fill struct {
	FillID string `json:"fill_id"`
	Key    string `json:"key"`
	Qty    int64  `json:"qty"`
	Price  int64  `json:"price"`
	AtNS   int64  `json:"at_ns"`
}

// Record kinds. Every state change is one journaled record.
const (
	RecStart     = "start"
	RecSend      = "send"
	RecOutcome   = "outcome"
	RecReconcile = "reconcile"
	RecFill      = "fill"
	RecFinish    = "finish"
)

// Record is one transition. Apply is the only function that changes State,
// and it reads nothing but the state and the record, so replaying a journal
// rebuilds the state exactly.
type Record struct {
	Kind        string   `json:"kind"`
	DeferFinish bool     `json:"defer_finish,omitempty"`
	AtNS        int64    `json:"at_ns"`
	Intent      *Intent  `json:"intent,omitempty"`
	Child       *Child   `json:"child,omitempty"`
	Reason      string   `json:"reason,omitempty"`
	Outcome     *Outcome `json:"outcome,omitempty"`
	View        *View    `json:"view,omitempty"`
	Fill        *Fill    `json:"fill,omitempty"`
}

// State is everything the executor knows about one intent.
type State struct {
	Intent Intent `json:"intent"`
	Cap    int64  `json:"cap"`
	Target int64  `json:"target"`
	// Filled and FillNotional count ledger fills only.
	Filled       int64           `json:"filled"`
	FillNotional int64           `json:"fill_notional"`
	Fills        map[string]Fill `json:"fills"`
	// BrokerFilled is the largest filled quantity a reconcile reported. It
	// limits what may still be sent; it is not fill evidence.
	BrokerFilled int64   `json:"broker_filled"`
	Sent         []Child `json:"sent"`
	Order        *Order  `json:"order,omitempty"`
	// InFlight is the child whose outcome is not known. While it is set the
	// executor sends nothing and reconciles first.
	InFlight       *Child `json:"in_flight,omitempty"`
	InFlightStatus string `json:"in_flight_status,omitempty"`
	InFlightReason string `json:"in_flight_reason,omitempty"`
	LastMoveNS     int64  `json:"last_move_ns"`
	Halt           string `json:"halt,omitempty"`
	Done           string `json:"done,omitempty"`
}

// Started reports whether a start record was applied.
func (s State) Started() bool { return s.Intent.IntentID != "" }

// Remaining is what may still be sent: the target less the larger of ledger
// fills and broker-reported fills.
func (s State) Remaining() int64 { return s.Target - max(s.Filled, s.BrokerFilled) }

// Overfill is how far ledger fills exceed the target (0 if they do not).
// Nothing the executor sends can cause it (no send exceeds Remaining); a
// non-zero value is broker or ledger evidence for an operator to examine.
func (s State) Overfill() int64 { return max(s.Filled-s.Target, 0) }

// Digest is the SHA-256 of the state's JSON form; equal states have equal
// digests.
func (s State) Digest() string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err) // State holds only marshalable fields.
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (s State) sentKey(key string) (Child, bool) {
	for _, c := range s.Sent {
		if c.Key == key {
			return c, true
		}
	}
	return Child{}, false
}

func (s State) clone() State {
	c := s
	c.Intent = s.Intent.clone()
	c.Fills = maps.Clone(s.Fills)
	c.Sent = slices.Clone(s.Sent)
	if s.Order != nil {
		o := *s.Order
		c.Order = &o
	}
	if s.InFlight != nil {
		f := *s.InFlight
		c.InFlight = &f
	}
	return c
}

// ErrTransition marks a record that cannot apply to the state.
var ErrTransition = errors.New("exec: invalid transition")

// Apply returns the state after r. It refuses any record that would break an
// invariant: a send while another child is unresolved, a price beyond the
// cap, a quantity beyond what remains, a fill for a key this intent never
// sent.
func Apply(s State, r Record) (State, error) {
	bad := func(format string, a ...any) (State, error) {
		return s, fmt.Errorf("%w: %s: "+format, append([]any{ErrTransition, r.Kind}, a...)...)
	}
	if r.Kind != RecStart && !s.Started() {
		return bad("before start")
	}
	n := s.clone()
	switch r.Kind {
	case RecStart:
		if s.Started() || r.Intent == nil {
			return bad("repeated or empty start")
		}
		if err := r.Intent.Validate(); err != nil {
			return bad("%v", err)
		}
		n = State{Intent: r.Intent.clone(), Cap: r.Intent.Cap(), Target: r.Intent.TargetQty(), Fills: map[string]Fill{}}
	case RecSend:
		c := r.Child
		switch {
		case c == nil:
			return bad("no child")
		case s.Done != "" || s.InFlight != nil:
			return bad("send while done or unresolved")
		case c.IntentID != s.Intent.IntentID:
			return bad("intent %q", c.IntentID)
		case c.Key == "":
			return bad("empty key")
		}
		if _, used := s.sentKey(c.Key); used {
			return bad("key %q reused", c.Key)
		}
		switch c.Action {
		case ActSubmit, ActAmend:
			if s.Halt != "" {
				return bad("%s after halt", c.Action)
			}
			if c.Qty < 1 || c.Qty > s.Remaining() || c.Price < 1 || s.Intent.worse(c.Price, s.Cap) {
				return bad("%s qty %d price %d outside remaining %d cap %d", c.Action, c.Qty, c.Price, s.Remaining(), s.Cap)
			}
			if (c.Action == ActSubmit) != (s.Order == nil) {
				return bad("%s with order %v", c.Action, s.Order)
			}
			n.LastMoveNS = r.AtNS
		case ActCancel:
		default:
			return bad("action %q", c.Action)
		}
		if c.Action != ActSubmit && (s.Order == nil || c.TargetKey != s.Order.Key) {
			return bad("%s target %q is not the working order", c.Action, c.TargetKey)
		}
		n.Sent = append(n.Sent, *c)
		n.InFlight, n.InFlightStatus, n.InFlightReason = c, StatusSending, r.Reason
	case RecOutcome:
		o := r.Outcome
		if o == nil || s.InFlight == nil || o.Key != s.InFlight.Key {
			return bad("outcome does not match the unresolved child")
		}
		c := *s.InFlight
		switch {
		case o.Status == StatusAccepted && c.Action == ActCancel:
			n.Order = nil
			if r.DeferFinish {
				n.Halt = s.InFlightReason
			} else {
				n.Done = s.InFlightReason
			}
		case o.Status == StatusAccepted && c.Action == ActSubmit:
			n.Order = &Order{Key: c.Key, Price: c.Price, Qty: c.Qty}
		case o.Status == StatusAccepted:
			n.Order = &Order{Key: c.Key, Price: c.Price, Qty: c.Qty}
		case o.Status == StatusRefused && c.Action != ActCancel:
			n.Halt = DoneRefused
		default:
			// Uncertain, sending, conflict, a refused cancel, or a status this
			// executor does not know: the order may have moved. Keep the child
			// unresolved; only a reconcile clears it.
			n.InFlightStatus = StatusUncertain
			return n, nil
		}
		n.InFlight, n.InFlightStatus, n.InFlightReason = nil, "", ""
	case RecReconcile:
		v := r.View
		if v == nil {
			return bad("no view")
		}
		if v.Order != nil {
			if _, ok := s.sentKey(v.Order.Key); !ok {
				return bad("working order key %q was never sent", v.Order.Key)
			}
			if s.Intent.worse(v.Order.Price, s.Cap) {
				return bad("working order price %d beyond cap %d", v.Order.Price, s.Cap)
			}
			o := *v.Order
			n.Order = &o
		} else {
			n.Order = nil
		}
		n.BrokerFilled = max(s.BrokerFilled, v.FilledQty)
		if s.InFlight != nil && s.InFlight.Action == ActCancel && n.Order == nil {
			if r.DeferFinish {
				n.Halt = s.InFlightReason
			} else {
				n.Done = s.InFlightReason
			}
		}
		n.InFlight, n.InFlightStatus, n.InFlightReason = nil, "", ""
	case RecFill:
		f := r.Fill
		if f == nil || f.FillID == "" || f.Qty < 1 || f.Price < 1 {
			return bad("empty fill")
		}
		if _, dup := s.Fills[f.FillID]; dup {
			return bad("fill %q repeated", f.FillID)
		}
		if _, ok := s.sentKey(f.Key); !ok {
			return bad("fill for key %q this intent never sent", f.Key)
		}
		// A fill is evidence and is recorded whole, even past the target or
		// larger than the order it hit. Clamping or refusing it would make
		// the executor undercount and keep sending. Past the target,
		// Remaining is below 1, so decide sends nothing but a cancel of the
		// working order, then finishes; Overfill reports the excess.
		n.Fills[f.FillID] = *f
		n.Filled += f.Qty
		n.FillNotional += f.Qty * f.Price
	case RecFinish:
		if s.Done != "" || s.InFlight != nil || r.Reason == "" {
			return bad("finish while done or unresolved")
		}
		if s.Order != nil {
			return bad("finish %q with a working order", r.Reason)
		}
		n.Done = r.Reason
	default:
		return bad("unknown kind")
	}
	return n, nil
}

// Top is the touch of the latest book snapshot.
type Top struct {
	Bid    int64 `json:"bid"`
	Ask    int64 `json:"ask"`
	AtNS   int64 `json:"at_ns"`
	Synced bool  `json:"synced"`
}

// inputs is everything outside State that a decision reads.
type inputs struct {
	now    int64
	top    *Top
	cfg    Config
	killed bool
}

// decision is the next thing to do: nothing, a reconcile query, or a record.
type decision struct {
	query bool
	rec   *Record
}

// decide picks the next step. It is the executor's only decision point; Apply
// re-checks what a send may carry.
func decide(s State, in inputs) decision {
	if !s.Started() || s.Done != "" {
		return decision{}
	}
	finish := func(reason string) decision {
		return decision{rec: &Record{Kind: RecFinish, AtNS: in.now, Reason: reason}}
	}
	// Never resend while an outcome is unknown: reconcile first.
	if s.InFlight != nil {
		return decision{query: true}
	}
	if s.Filled >= s.Target && s.Order == nil {
		return finish(DoneFilled)
	}
	if !in.cfg.SendEnabled {
		return decision{}
	}
	send := func(c Child, reason string) decision {
		c.IntentID, c.Key = s.Intent.IntentID, fmt.Sprintf("%s:%d", s.Intent.IntentID, len(s.Sent)+1)
		if c.Action != ActSubmit {
			c.TargetKey = s.Order.Key
		}
		return decision{rec: &Record{Kind: RecSend, AtNS: in.now, Child: &c, Reason: reason}}
	}
	closing := s.Halt
	switch {
	case s.Filled >= s.Target:
		// The ledger shows the target filled, but an order may still rest
		// at the broker: cancel it before finishing.
		closing = DoneFilled
	case in.killed:
		closing = DoneKilled
	case in.now >= s.Intent.ExpiresAt.UnixNano():
		closing = DoneDeadline
	case s.Order != nil && bookSilentSince(s, in) < in.now-int64(30*time.Second):
		closing = DoneStaleBook
	}
	if closing != "" {
		if s.Order != nil {
			return send(Child{Action: ActCancel}, closing)
		}
		return finish(closing)
	}
	rem := s.Remaining()
	if rem < 1 {
		return decision{} // the broker reports fills the ledger has not shown yet
	}
	var step Child
	if s.Order != nil {
		if in.now-s.LastMoveNS < in.cfg.StepEvery.Nanoseconds() {
			return decision{}
		}
		// The order rested a full step. An amend never grows it and never
		// leaves E1's room (exposure.go). The cancels below need no fresh
		// book, like every cancel.
		q := rem
		if s.Order.Qty > 0 {
			q = min(q, s.Order.Qty)
		}
		qRoom, nRoom := s.e1Room(s.rootOf(s.Order.Key))
		q = min(q, qRoom)
		cp := s.Cap
		if s.Intent.Side == Buy && q > 0 {
			cp = min(cp, FloorTick(nRoom/q))
		}
		if q < 1 || !s.Intent.worse(cp, s.Order.Price) {
			return send(Child{Action: ActCancel}, DoneCap)
		}
		step = Child{Action: ActAmend, TargetKey: s.Order.Key, Qty: q, Price: stepToward(s.Intent.Side, s.Order.Price, cp)}
		// A lineage already past E1's caps cannot be amended at all: E1
		// counts its high-water mark. Only a journal written before the
		// executor mirrored E1 holds one (a sell opened above
		// max_notional_krw); it still replays, and its order is cancelled.
		if s.E1CapsRefusal(step) != "" {
			return send(Child{Action: ActCancel}, DoneE1Caps)
		}
	}
	t := in.top
	if t == nil || !t.Synced || t.AtNS > in.now || in.now-t.AtNS > in.cfg.StaleAfter.Nanoseconds() || t.Bid < 1 || t.Ask < 1 {
		return decision{} // stale or future-stamped book: no new send
	}
	if s.Order != nil {
		return send(Child{Action: ActAmend, Qty: step.Qty, Price: step.Price}, "step")
	}
	p := t.Bid
	if s.Intent.Side == Sell {
		p = t.Ask
	}
	if s.Intent.worse(p, s.Cap) {
		p = s.Cap
	}
	q, p := s.fitNewOrder(rem, p)
	if q < 1 {
		return finish(DoneE1Caps)
	}
	return send(Child{Action: ActSubmit, Qty: q, Price: p}, "open")
}

// A restart has no book. Count silence from the last order move until a
// snapshot arrives; a future timestamp cannot postpone the stop.
func bookSilentSince(s State, in inputs) int64 {
	if in.top != nil && in.top.AtNS > 0 && in.top.AtNS <= in.now {
		return in.top.AtNS
	}
	return s.LastMoveNS
}

// fitNewOrder sizes a new order lineage at price p to the room E1's caps
// leave (exposure.go). A buy keeps its price and loses quantity. A sell may
// open above Cap, so it first gives up price, down to Cap, to stay within
// max_notional_krw, then quantity. q < 1 means nothing fits.
func (s State) fitNewOrder(rem, p int64) (q, price int64) {
	qRoom, nRoom := s.e1Room("") // a new lineage has no high-water mark yet
	q = min(rem, qRoom)
	if q < 1 {
		return 0, p
	}
	if s.Intent.Side == Buy {
		return min(q, nRoom/p), p
	}
	if q*s.Cap > nRoom {
		q = nRoom / s.Cap
	}
	if q < 1 {
		return 0, p
	}
	// nRoom/q >= Cap and Cap is a grid price, so the floor stays at or above
	// Cap.
	return q, min(p, FloorTick(nRoom/q))
}
