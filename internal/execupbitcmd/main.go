// Command execupbit is the shadow-only Upbit executor dry run. It reads one
// shadow intent, takes Upbit book events from a recorded JSONL stream or the
// live public websocket, and prints one JSON line per book event plus the
// child requests it would send.
//
//	execupbit -dry-run -intent FILE -books FILE [-stale 2s]
//	execupbit -dry-run -intent FILE -live -root DIR [-duration 1m] [-stale 2s]
//
// -dry-run is required and is the only mode. There is no send flag, no gateway
// or broker client is linked, and nothing is ordered: the child lines are a
// decision log. Sending is wired later against auto_trader hk 5438's Upbit
// wire contract.
package execupbitcmd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/mgh3326/fillwire/internal/exec/upbit"
	"github.com/mgh3326/fillwire/internal/exitcode"
)

const usage = "usage: execupbit -dry-run -intent FILE {-books FILE | -live -root DIR [-duration 1m]} [-stale 2s]\n"

// record is one line of the recorded stream: an internal/upbit reader record,
// whose Raw field holds the websocket frame as received.
type record struct {
	Kind              string          `json:"kind"`
	Market            string          `json:"market"`
	ExchangeTimestamp int64           `json:"exchange_timestamp"`
	ReceivedAt        time.Time       `json:"received_at"`
	Raw               json.RawMessage `json:"raw"`
	Reason            string          `json:"reason"`
}

// Output lines. Every line names its kind so a reader can tell a book event
// from a child request without guessing.
type bookLine struct {
	Kind        string    `json:"kind"`
	Market      string    `json:"market"`
	ReceivedAt  time.Time `json:"received_at"`
	ExchangeMS  int64     `json:"exchange_timestamp_ms"`
	BidPriceKRW int64     `json:"bid_price_krw"`
	AskPriceKRW int64     `json:"ask_price_krw"`
	BidQty      upbit.Qty `json:"bid_qty"`
	AskQty      upbit.Qty `json:"ask_qty"`
	Decision    string    `json:"decision"`
	Reason      string    `json:"reason,omitempty"`
}

type childLine struct {
	Kind           string `json:"kind"`
	Action         string `json:"action"`
	IntentID       string `json:"intent_id"`
	IdempotencyKey string `json:"idempotency_key"`
	TargetKey      string `json:"target_idempotency_key,omitempty"`
	Quantity       string `json:"quantity,omitempty"`
	LimitPriceKRW  int64  `json:"limit_price_krw,omitempty"`
}

type finishLine struct {
	Kind     string `json:"kind"`
	IntentID string `json:"intent_id"`
	Done     string `json:"done"`
	Reason   string `json:"reason,omitempty"`
}

type noticeLine struct {
	Kind       string    `json:"kind"`
	Market     string    `json:"market"`
	ReceivedAt time.Time `json:"received_at"`
	Reason     string    `json:"reason,omitempty"`
}

// emitter writes the decision log. It serializes lines because the live source
// emits on its own goroutine while the expiry ticker runs on another.
type emitter struct {
	mu   sync.Mutex
	enc  *json.Encoder
	plan *upbit.Plan
}

// book feeds one touch to the plan and prints the book line and any child.
func (e *emitter) book(now time.Time, b upbit.Book) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	d := e.plan.OnBook(now, b)
	if err := e.enc.Encode(bookLine{
		Kind: "book", Market: b.Market, ReceivedAt: b.ReceivedAt, ExchangeMS: b.ExchangeMS,
		BidPriceKRW: b.BidPriceKRW, AskPriceKRW: b.AskPriceKRW,
		BidQty: b.BidQty, AskQty: b.AskQty,
		Decision: d.Action, Reason: d.Reason,
	}); err != nil {
		return err
	}
	return e.decision(d)
}

// tick drives expiry without a book, so a silent stream still cancels.
func (e *emitter) tick(now time.Time) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	d := e.plan.Tick(now)
	if d.Action == upbit.ActHold {
		return nil
	}
	return e.decision(d)
}

// decision prints a child request and a finish, in that order.
func (e *emitter) decision(d upbit.Decision) error {
	if c := d.Child; c != nil {
		line := childLine{
			Kind: "child", Action: c.Action, IntentID: c.IntentID,
			IdempotencyKey: c.Key, TargetKey: c.TargetKey, LimitPriceKRW: c.LimitPriceKRW,
		}
		if c.Quantity != nil {
			line.Quantity = c.Quantity.String()
		}
		if err := e.enc.Encode(line); err != nil {
			return err
		}
	}
	if d.Action == upbit.ActFinish {
		return e.enc.Encode(finishLine{Kind: "finish", IntentID: e.plan.Intent().IntentID, Done: d.Done, Reason: d.Reason})
	}
	return nil
}

func (e *emitter) notice(kind string, at time.Time, market, reason string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.enc.Encode(noticeLine{Kind: kind, Market: market, ReceivedAt: at, Reason: reason})
}

func Run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("execupbit", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dryRun := fs.Bool("dry-run", false, "print each child request instead of sending it; required")
	intentPath := fs.String("intent", "", "one shadow intent JSON object")
	booksPath := fs.String("books", "", "recorded Upbit websocket records (JSONL) to replay")
	live := fs.Bool("live", false, "read the live public Upbit book")
	root := fs.String("root", "", "recording root for the live book source")
	duration := fs.Duration("duration", time.Minute, "how long a live shadow run listens")
	cfg := upbit.DefaultConfig()
	fs.DurationVar(&cfg.StaleAfter, "stale", cfg.StaleAfter, "oldest book a child request may be priced from")
	if err := fs.Parse(args); err != nil {
		return exitcode.Usage
	}
	if !*dryRun {
		fmt.Fprintln(stderr, "execupbit: shadow only: -dry-run is required and is the only mode")
		return exitcode.Usage
	}
	if *intentPath == "" || fs.NArg() > 0 || cfg.StaleAfter <= 0 {
		fmt.Fprint(stderr, usage)
		return exitcode.Usage
	}
	if (*booksPath == "") == !*live {
		fmt.Fprint(stderr, usage)
		return exitcode.Usage
	}
	if *live && (*root == "" || *duration <= 0) {
		fmt.Fprintln(stderr, "execupbit: -live needs -root and a positive -duration")
		return exitcode.Usage
	}
	data, err := os.ReadFile(*intentPath)
	if err != nil {
		return fail(stderr, err)
	}
	in, err := upbit.ParseIntent(data)
	if err != nil {
		return fail(stderr, err)
	}
	plan, err := upbit.NewPlan(in, cfg)
	if err != nil {
		return fail(stderr, err)
	}
	e := &emitter{enc: json.NewEncoder(stdout), plan: plan}
	if *booksPath != "" {
		if err := replay(*booksPath, e); err != nil {
			return fail(stderr, err)
		}
		return exitcode.OK
	}
	if err := liveRun(e, in.Market, *root, *duration); err != nil {
		return fail(stderr, err)
	}
	return exitcode.OK
}

// replay walks recorded records in file order. Each book event's clock is its
// own receive time, so the output is deterministic.
func replay(path string, e *emitter) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(bufio.NewReader(f))
	for {
		var rec record
		if err := dec.Decode(&rec); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("%s: %v", path, err)
		}
		switch rec.Kind {
		case "orderbook":
			b, err := upbit.ParseBook(rec.Raw, rec.ReceivedAt)
			if err != nil {
				return fmt.Errorf("%s: %v", path, err)
			}
			if err := e.book(rec.ReceivedAt, b); err != nil {
				return err
			}
		case "gap":
			if err := e.notice("gap", rec.ReceivedAt, rec.Market, rec.Reason); err != nil {
				return err
			}
			if err := e.tick(rec.ReceivedAt); err != nil {
				return err
			}
		}
	}
}

// liveRun listens to the public websocket for duration. Book ages are measured
// against the wall clock, and a one-second ticker meets the expiry deadline
// even when the stream is silent.
func liveRun(e *emitter, market, root string, duration time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	src, err := upbit.NewSource(upbit.SourceConfig{Market: market, Root: root}, func(ev upbit.SourceEvent) {
		switch ev.Kind {
		case upbit.EventBook:
			_ = e.book(time.Now(), ev.Book)
		case upbit.EventGap:
			_ = e.notice("gap", ev.At, market, ev.Reason)
		case upbit.EventDecodeError:
			_ = e.notice("decode_error", ev.At, market, ev.Reason)
		}
	})
	if err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				_ = e.tick(now)
			}
		}
	}()
	if err := src.Run(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func fail(stderr io.Writer, err error) int {
	fmt.Fprintln(stderr, "execupbit:", err)
	return exitcode.Failure
}
