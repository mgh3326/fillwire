// Package exectest is a fake E1 gateway in front of a fake broker, for
// executor tests. It never reaches a network. Faults are injected by setting
// fields before a call; Violations lists every safety rule an executor broke.
package exectest

import (
	"errors"
	"fmt"

	"github.com/mgh3326/fillwire/internal/exec"
)

// ErrDown is the transport error while the fake is disconnected.
var ErrDown = errors.New("exectest: disconnected")

// Killed is the panic value of a simulated process kill.
type Killed struct{}

type order struct {
	key    string
	price  int64
	open   int64
	filled int64
}

// Gateway is the fake E1 gateway and broker for one intent.
type Gateway struct {
	intent exec.Intent
	cap    int64
	target int64

	answers map[string]exec.Outcome // E1 idempotency: a key replays its first answer
	live    *order
	filled  int64
	fillSeq int

	// Sends lists every child request that reached the gateway, in order.
	Sends []exec.Child
	// Violations lists every broken rule: a reused key, a send before
	// reconciling an unanswered one, a second live order, a price beyond the
	// cap, or more filled than the target.
	Violations []string
	// unanswered is true while a send's answer was not delivered and no query
	// has followed.
	unanswered bool

	// Faults. Each one-shot fault clears itself when it fires.
	Down         bool   // Send and Query fail; nothing is applied
	DropRequest  bool   // next Send fails before it is applied, and the link goes Down
	DropResponse bool   // next Send is applied, then fails, and the link goes Down
	Uncertain    bool   // next Send is applied and answered "uncertain"
	RefuseNext   string // next Send is refused with this reason, not applied
	KillBefore   bool   // next Send panics with Killed before it is applied
	KillAfter    bool   // next Send is applied, then panics with Killed
}

// New returns a fake gateway holding one stored intent.
func New(in exec.Intent) *Gateway {
	return &Gateway{intent: in, cap: in.Cap(), target: in.TargetQty(), answers: map[string]exec.Outcome{}}
}

func (g *Gateway) violate(format string, a ...any) {
	g.Violations = append(g.Violations, fmt.Sprintf(format, a...))
}

// Send implements exec.Gateway.
func (g *Gateway) Send(c exec.Child) (exec.Outcome, error) {
	if g.Down {
		g.unanswered = true
		return exec.Outcome{}, ErrDown
	}
	g.Sends = append(g.Sends, c)
	if g.unanswered {
		g.violate("send %s before reconciling an unanswered send", c.Key)
	}
	if prev, ok := g.answers[c.Key]; ok {
		g.violate("key %s sent twice", c.Key)
		return prev, nil
	}
	if (c.Action == exec.ActSubmit || c.Action == exec.ActAmend) && worse(g.intent.Side, c.Price, g.cap) {
		g.violate("%s %s price %d beyond cap %d", c.Action, c.Key, c.Price, g.cap)
	}
	switch {
	case g.DropRequest:
		g.DropRequest, g.unanswered, g.Down = false, true, true
		return exec.Outcome{}, ErrDown
	case g.KillBefore:
		g.KillBefore, g.unanswered = false, true
		panic(Killed{})
	case g.RefuseNext != "":
		out := exec.Outcome{Key: c.Key, Status: exec.StatusRefused, Reason: g.RefuseNext}
		g.RefuseNext, g.answers[c.Key] = "", out
		return out, nil
	}
	out := g.apply(c)
	g.answers[c.Key] = out
	switch {
	case g.DropResponse:
		g.DropResponse, g.unanswered, g.Down = false, true, true
		return exec.Outcome{}, ErrDown
	case g.KillAfter:
		g.KillAfter, g.unanswered = false, true
		panic(Killed{})
	case g.Uncertain:
		g.Uncertain, g.unanswered = false, true
		return exec.Outcome{Key: c.Key, Status: exec.StatusUncertain}, nil
	}
	if out.Status == exec.StatusUncertain {
		g.unanswered = true
	}
	return out, nil
}

// apply runs c against the broker. A request the broker rejects ends
// uncertain, as E1 records a failed broker call.
func (g *Gateway) apply(c exec.Child) exec.Outcome {
	ok := exec.Outcome{Key: c.Key, Status: exec.StatusAccepted, BrokerOrderID: "fake-" + c.Key}
	switch c.Action {
	case exec.ActSubmit:
		if g.live != nil {
			g.violate("submit %s while order %s is live", c.Key, g.live.key)
		}
		g.live = &order{key: c.Key, price: c.Price, open: c.Qty}
		return ok
	case exec.ActAmend, exec.ActCancel:
		if g.live == nil || g.live.key != c.TargetKey {
			return exec.Outcome{Key: c.Key, Status: exec.StatusUncertain, Reason: "target not working"}
		}
		if c.Action == exec.ActCancel {
			g.live = nil
			return ok
		}
		g.live.key, g.live.price, g.live.open = c.Key, c.Price, min(c.Qty, g.live.open)
		return ok
	}
	return exec.Outcome{Key: c.Key, Status: exec.StatusRefused, Reason: "unknown action"}
}

// Query implements exec.Gateway.
func (g *Gateway) Query(intentID string) (exec.View, error) {
	if g.Down {
		return exec.View{}, ErrDown
	}
	g.unanswered = false
	v := exec.View{FilledQty: g.filled}
	if g.live != nil {
		v.Order = &exec.Order{Key: g.live.key, Price: g.live.price, Qty: g.live.open}
	}
	return v, nil
}

// Fill executes qty of the live order at its price and returns the ledger
// evidence. It fills nothing (and returns ok=false) without a live order.
func (g *Gateway) Fill(qty, atNS int64) (exec.Fill, bool) {
	if g.live == nil || qty < 1 {
		return exec.Fill{}, false
	}
	o := g.live
	qty = min(qty, o.open)
	o.open -= qty
	o.filled += qty
	g.filled += qty
	if g.filled > g.target {
		g.violate("filled %d beyond target %d", g.filled, g.target)
	}
	g.fillSeq++
	f := exec.Fill{FillID: fmt.Sprintf("fill-%d", g.fillSeq), Key: o.key, Qty: qty, Price: o.price, AtNS: atNS}
	if o.open == 0 {
		g.live = nil
	}
	return f, true
}

// Live reports the broker's working order key and price, if any.
func (g *Gateway) Live() (key string, price int64, ok bool) {
	if g.live == nil {
		return "", 0, false
	}
	return g.live.key, g.live.price, true
}

// Filled is the broker's filled quantity.
func (g *Gateway) Filled() int64 { return g.filled }

func worse(side exec.Side, a, b int64) bool {
	if side == exec.Buy {
		return a > b
	}
	return a < b
}
