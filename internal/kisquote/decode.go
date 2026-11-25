package kisquote

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/mgh3326/fillwire/internal/book"
	"github.com/mgh3326/fillwire/internal/bookfeed"
	"github.com/mgh3326/go-kis/kis/ws"
)

const (
	// venue is the only venue this lane subscribes. NXT and the combined
	// venue TRs are out of scope until their transaction IDs are confirmed.
	venue = "KRX"

	tradeRESTPath = "/uapi/domestic-stock/v1/quotations/inquire-ccnl"
	tradeRESTTR   = "FHKST01010300"
)

// tick is one outbound quote entry. kind is "trade" or "book"; source is "ws"
// or "rest"; feed is the transaction that carried it.
type tick struct {
	symbol   string
	kind     string
	source   string
	feed     string
	exchange time.Time
	received time.Time
	price    int64
	qty      int64
	acmlVol  int64
	side     string
	bids     [5]book.Level
	asks     [5]book.Level
}

// values renders the fixed field set. Every field is present on every entry;
// fields that do not apply to the tick's kind are empty, as in the Toss lane.
func (t tick) values() map[string]any {
	text := func(v int64, ok bool) string {
		if !ok {
			return ""
		}
		return strconv.FormatInt(v, 10)
	}
	v := map[string]any{
		"symbol":      t.symbol,
		"kind":        t.kind,
		"source":      t.source,
		"feed":        t.feed,
		"venue":       venue,
		"ts_exchange": t.exchange.Format(time.RFC3339Nano),
		"ts_received": t.received.Format(time.RFC3339Nano),
		"price":       text(t.price, t.kind == "trade"),
		"qty":         text(t.qty, t.kind == "trade"),
		"acml_vol":    text(t.acmlVol, t.kind == "trade"),
		"side":        t.side,
	}
	for i := range 5 {
		n := strconv.Itoa(i + 1)
		isBook := t.kind == "book"
		v["bid_price_"+n] = text(t.bids[i].Price, isBook)
		v["bid_qty_"+n] = text(t.bids[i].Qty, isBook)
		v["ask_price_"+n] = text(t.asks[i].Price, isBook)
		v["ask_qty_"+n] = text(t.asks[i].Qty, isBook)
	}
	return v
}

// ticksFromEvent parses one realtime frame and keeps the records whose symbol
// the lane holds. Other transactions and malformed frames yield no ticks.
func ticksFromEvent(ev ws.Event, owned func(string) bool) ([]tick, error) {
	if ev.TR != book.TRTrade && ev.TR != book.TRBook {
		return nil, nil
	}
	frame, err := book.ParseFrame(ev.Raw)
	if err != nil {
		return nil, err
	}
	received := ev.ReceivedAt
	if received.IsZero() {
		received = time.Now()
	}
	out := make([]tick, 0, len(frame.Trades)+len(frame.Books))
	for _, record := range frame.Trades {
		if !owned(record.Symbol) {
			continue
		}
		exchange, err := bookfeed.ExchangeTime(record.Hour, received)
		if err != nil {
			return nil, err
		}
		out = append(out, tick{
			symbol: record.Symbol, kind: "trade", source: "ws", feed: book.TRTrade,
			exchange: exchange, received: received,
			price: record.Price, qty: record.Qty, acmlVol: record.AcmlVol, side: record.Side,
		})
	}
	for _, record := range frame.Books {
		if !owned(record.Symbol) {
			continue
		}
		exchange, err := bookfeed.ExchangeTime(record.Hour, received)
		if err != nil {
			return nil, err
		}
		t := tick{
			symbol: record.Symbol, kind: "book", source: "ws", feed: book.TRBook,
			exchange: exchange, received: received,
		}
		copy(t.bids[:], record.Bids[:5])
		copy(t.asks[:], record.Asks[:5])
		if err := validateLevels(t.bids, t.asks); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

// pollTrade reads the read-only current-price trade quote (FHKST01010300).
func pollTrade(ctx context.Context, r RESTReader, symbol string, now func() time.Time) (tick, error) {
	var response struct {
		Output struct {
			Hour  string `json:"stck_cntg_hour"`
			Price string `json:"stck_prpr"`
			Qty   string `json:"cntg_vol"`
		} `json:"output"`
	}
	err := r.Read(ctx, tradeRESTPath, tradeRESTTR, map[string]string{
		"FID_COND_MRKT_DIV_CODE": "J",
		"FID_INPUT_ISCD":         symbol,
	}, &response)
	if err != nil {
		return tick{}, errors.New("kisquote: trade read failed")
	}
	received := now()
	exchange, err := bookfeed.ExchangeTime(response.Output.Hour, received)
	if err != nil {
		return tick{}, err
	}
	price, err := strconv.ParseInt(response.Output.Price, 10, 64)
	if err != nil || price <= 0 {
		return tick{}, errors.New("kisquote: invalid trade price")
	}
	qty, err := strconv.ParseInt(response.Output.Qty, 10, 64)
	if err != nil || qty < 0 {
		return tick{}, errors.New("kisquote: invalid trade quantity")
	}
	return tick{
		symbol: symbol, kind: "trade", source: "rest", feed: tradeRESTTR,
		exchange: exchange, received: received, price: price, qty: qty,
	}, nil
}

// tickFromSnapshot maps an asking-price REST snapshot to a book tick.
func tickFromSnapshot(s bookfeed.Snapshot) tick {
	t := tick{
		symbol: s.Symbol, kind: "book", source: "rest", feed: "FHKST01010200",
		exchange: s.Exchange, received: s.Received,
	}
	copy(t.bids[:], s.Bids[:5])
	copy(t.asks[:], s.Asks[:5])
	return t
}

// validateLevels mirrors the book lane: prices positive and ordered on each
// side, quantities nonnegative, best bid not above best ask.
func validateLevels(bids, asks [5]book.Level) error {
	for i := range 5 {
		b, a := bids[i], asks[i]
		if b.Price <= 0 || a.Price <= 0 || b.Qty < 0 || a.Qty < 0 {
			return errors.New("kisquote: invalid book levels")
		}
		if i > 0 && (b.Price > bids[i-1].Price || a.Price < asks[i-1].Price) {
			return errors.New("kisquote: unordered book levels")
		}
	}
	if bids[0].Price > asks[0].Price {
		return errors.New("kisquote: crossed book")
	}
	return nil
}
