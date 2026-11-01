// Package sim replays recorded U6 book evidence (internal/book) through the
// E2 executor against the fake gateway and reports, per intent, what would
// have filled under a fill rule. Nothing here reaches a broker.
package sim

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/mgh3326/fillwire/internal/book"
	"github.com/mgh3326/fillwire/internal/exec"
	"github.com/mgh3326/fillwire/internal/exec/exectest"
)

// Rule decides when a resting order fills during replay. Fills are always
// priced at the order's own limit.
type Rule string

const (
	// Through is the conservative rule: only a trade strictly through the
	// order's price fills it, and only up to that trade's quantity.
	Through Rule = "through"
	// Touch is the optimistic rule: a trade at the order's price, or an
	// opposite touch reaching it, fills the whole open quantity.
	Touch Rule = "touch"
)

// Rules is both rules, conservative first.
var Rules = []Rule{Through, Touch}

// Result is one intent's replay under one rule.
type Result struct {
	IntentID string    `json:"intent_id"`
	Symbol   string    `json:"symbol"`
	Side     exec.Side `json:"side"`
	Rule     Rule      `json:"rule"`
	Target   int64     `json:"target"`
	Filled   int64     `json:"filled"`
	// AvgPrice is the fill notional over the filled quantity, in KRW, rounded
	// to 2 decimals; 0 without fills.
	AvgPrice float64 `json:"avg_price"`
	// ArrivalMid is the mid of the first synced book after start.
	ArrivalMid float64 `json:"arrival_mid"`
	// SlippageBps is the cost of AvgPrice against ArrivalMid in basis points;
	// positive is worse (paid above mid on a buy, sold below on a sell).
	SlippageBps float64 `json:"slippage_bps"`
	Sends       int     `json:"sends"`
	Done        string  `json:"done"`
	Violations  int     `json:"violations"`
}

// Run replays evidence for each intent under rule. Each intent runs alone:
// its own executor, journal (in a temporary directory, removed after) and
// fake gateway.
func Run(evidence []byte, intents []exec.Intent, cfg exec.Config, rule Rule) ([]Result, error) {
	if rule != Through && rule != Touch {
		return nil, fmt.Errorf("sim: unknown rule %q", rule)
	}
	dir, err := os.MkdirTemp("", "execsim-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	out := make([]Result, 0, len(intents))
	for i, in := range intents {
		r, err := runOne(evidence, in, cfg, rule, filepath.Join(dir, fmt.Sprintf("%d.journal", i)))
		if err != nil {
			return nil, fmt.Errorf("sim: intent %s: %w", in.IntentID, err)
		}
		out = append(out, r)
	}
	return out, nil
}

func runOne(evidence []byte, in exec.Intent, cfg exec.Config, rule Rule, path string) (Result, error) {
	j, st, err := exec.OpenJournal(path)
	if err != nil {
		return Result{}, err
	}
	defer j.Close()
	g := exectest.New(in)
	e := exec.New(cfg, j, st, g, &exec.KillSwitch{})
	started := false
	var mid float64
	var top exec.Top
	deadline := in.ExpiresAt.UnixNano()
	fill := func(qty, atNS int64) error {
		// The executor cancels at the first tick at or after the deadline
		// (decide: now >= expires_at). That tick runs after this event's
		// fills, so without this check an event at or past the deadline
		// could fill an order that already counts as cancelled.
		if atNS >= deadline {
			return nil
		}
		f, ok := g.Fill(qty, atNS)
		if !ok {
			return nil
		}
		return e.OnFill(f)
	}
	_, err = book.Replay(bytes.NewReader(evidence), func(ev book.Event) error {
		if !started {
			if err := e.Start(in, ev.AtNS); err != nil {
				return err
			}
			started = true
		}
		switch {
		case ev.Type == book.EventReconnect || ev.Type == book.EventSession,
			ev.Type == book.EventStale && ev.TR == book.TRBook && ev.Symbol == in.Symbol:
			// A reconnect, a new session or a silent book stream: nothing is
			// priced from this book until the next snapshot.
			top.Synced = false
			e.OnBook(top)
		case ev.Type == book.EventBook && ev.Symbol == in.Symbol:
			b := ev.Book
			top = exec.Top{Bid: b.Bids[0].Price, Ask: b.Asks[0].Price, AtNS: ev.AtNS, Synced: b.Synced}
			e.OnBook(top)
			if mid == 0 && top.Synced && top.Bid > 0 && top.Ask > 0 {
				mid = float64(top.Bid+top.Ask) / 2
			}
			if _, price, live := g.Live(); live && rule == Touch && top.Synced && reaches(in.Side, opposite(in.Side, top), price) {
				if err := fill(in.TargetQty(), ev.AtNS); err != nil {
					return err
				}
			}
		case ev.Type == book.EventTrade && ev.Symbol == in.Symbol:
			if _, price, live := g.Live(); live {
				t := ev.Trade
				if rule == Through && through(in.Side, t.Price, price) {
					if err := fill(t.Qty, ev.AtNS); err != nil {
						return err
					}
				}
				if rule == Touch && reaches(in.Side, t.Price, price) {
					if err := fill(in.TargetQty(), ev.AtNS); err != nil {
						return err
					}
				}
			}
		}
		// Every event advances the executor's clock, whatever its symbol.
		return e.Tick(ev.AtNS)
	})
	if err != nil {
		return Result{}, err
	}
	s := e.State()
	r := Result{
		IntentID: in.IntentID, Symbol: in.Symbol, Side: in.Side, Rule: rule,
		Target: s.Target, Filled: s.Filled, ArrivalMid: mid,
		Sends: len(g.Sends), Done: s.Done, Violations: len(g.Violations),
	}
	if s.Filled > 0 {
		avg := float64(s.FillNotional) / float64(s.Filled)
		r.AvgPrice = round2(avg)
		if mid > 0 {
			bps := (avg - mid) / mid * 10_000
			if in.Side == exec.Sell {
				bps = -bps
			}
			r.SlippageBps = round2(bps)
		}
	}
	return r, nil
}

func opposite(side exec.Side, t exec.Top) int64 {
	if side == exec.Buy {
		return t.Ask
	}
	return t.Bid
}

// reaches reports whether a market price p is at or through order price o.
func reaches(side exec.Side, p, o int64) bool {
	if p < 1 {
		return false
	}
	if side == exec.Buy {
		return p <= o
	}
	return p >= o
}

// through reports whether a trade at p is strictly through order price o.
func through(side exec.Side, p, o int64) bool {
	if side == exec.Buy {
		return p < o
	}
	return p > o
}

func round2(v float64) float64 {
	if v < 0 {
		return -round2(-v)
	}
	return float64(int64(v*100+0.5)) / 100
}
