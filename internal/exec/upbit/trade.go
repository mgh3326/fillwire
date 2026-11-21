package upbit

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrTrade marks an Upbit trade frame this package cannot use.
var ErrTrade = errors.New("upbit: invalid trade frame")

// Trade is one executed trade from Upbit's public trade stream.
type Trade struct {
	Market     string
	PriceKRW   int64
	Qty        Qty
	ExchangeMS int64
	ReceivedAt time.Time
	// StreamType is SNAPSHOT for the trades Upbit replays on subscribing (the
	// recorder does not ask for realtime only) and REALTIME for a new one.
	StreamType string
	// SequentialID identifies one execution within its market; 0 if absent.
	SequentialID int64
}

// wireTrade holds the fields of a DEFAULT-format websocket trade message that
// replay uses (https://docs.upbit.com/kr/reference/websocket-trade).
type wireTrade struct {
	Type      string      `json:"type"`
	Code      string      `json:"code"`
	Timestamp int64       `json:"timestamp"`
	Price     json.Number `json:"trade_price"`
	Volume    json.Number `json:"trade_volume"`
	Stream    string      `json:"stream_type"`
	Sequence  int64       `json:"sequential_id"`
}

// ParseTrade decodes one trade frame received at receivedAt. Like ParseBook it
// refuses a whole-won price it cannot read exactly and a quantity beyond
// QtyScale decimals instead of rounding them.
func ParseTrade(data []byte, receivedAt time.Time) (Trade, error) {
	var w wireTrade
	if err := json.Unmarshal(data, &w); err != nil {
		return Trade{}, fmt.Errorf("%w: %v", ErrTrade, err)
	}
	if w.Type != "trade" {
		return Trade{}, fmt.Errorf("%w: type %q", ErrTrade, w.Type)
	}
	if !ValidMarket(w.Code) {
		return Trade{}, fmt.Errorf("%w: market %q", ErrTrade, w.Code)
	}
	if w.Timestamp <= 0 {
		return Trade{}, fmt.Errorf("%w: timestamp %d", ErrTrade, w.Timestamp)
	}
	price, err := parseWon(w.Price)
	if err != nil {
		return Trade{}, fmt.Errorf("%w: %v", ErrTrade, err)
	}
	qty, err := parseQtyNumber(w.Volume)
	if err != nil {
		return Trade{}, fmt.Errorf("%w: %v", ErrTrade, err)
	}
	if price < 1 || qty.IsZero() {
		return Trade{}, fmt.Errorf("%w: price %d quantity %q", ErrTrade, price, qty)
	}
	return Trade{Market: w.Code, PriceKRW: price, Qty: qty, ExchangeMS: w.Timestamp, ReceivedAt: receivedAt, StreamType: w.Stream, SequentialID: w.Sequence}, nil
}
