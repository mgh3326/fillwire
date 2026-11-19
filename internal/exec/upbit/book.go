package upbit

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ErrBook marks an Upbit orderbook frame this package cannot use.
var ErrBook = errors.New("upbit: invalid orderbook frame")

// Book is the touch of one Upbit orderbook frame. Every orderbook message is
// a full snapshot of the subscribed depth, so the first level is the best bid
// and the best ask.
type Book struct {
	Market      string    `json:"market"`
	BidPriceKRW int64     `json:"bid_price_krw"`
	AskPriceKRW int64     `json:"ask_price_krw"`
	BidQty      Qty       `json:"bid_qty"`
	AskQty      Qty       `json:"ask_qty"`
	ExchangeMS  int64     `json:"exchange_timestamp_ms"`
	ReceivedAt  time.Time `json:"received_at"`
	StreamType  string    `json:"stream_type,omitempty"`
}

type wireBook struct {
	Type      string `json:"type"`
	Code      string `json:"code"`
	Timestamp int64  `json:"timestamp"`
	Stream    string `json:"stream_type"`
	Units     []struct {
		AskPrice json.Number `json:"ask_price"`
		BidPrice json.Number `json:"bid_price"`
		AskSize  json.Number `json:"ask_size"`
		BidSize  json.Number `json:"bid_size"`
	} `json:"orderbook_units"`
}

// ParseBook decodes one orderbook frame received at receivedAt. It refuses a
// frame that is not an orderbook, has no levels, prices below the whole-won
// tick grid, a non-integer won price, an empty level, or a crossed book: the
// shadow model never acts on a touch it cannot price exactly.
func ParseBook(data []byte, receivedAt time.Time) (Book, error) {
	var w wireBook
	if err := json.Unmarshal(data, &w); err != nil {
		return Book{}, fmt.Errorf("%w: %v", ErrBook, err)
	}
	if w.Type != "orderbook" {
		return Book{}, fmt.Errorf("%w: type %q", ErrBook, w.Type)
	}
	if !ValidMarket(w.Code) {
		return Book{}, fmt.Errorf("%w: market %q", ErrBook, w.Code)
	}
	if w.Timestamp <= 0 {
		return Book{}, fmt.Errorf("%w: timestamp %d", ErrBook, w.Timestamp)
	}
	if len(w.Units) == 0 {
		return Book{}, fmt.Errorf("%w: no orderbook_units", ErrBook)
	}
	ask, err := parseWon(w.Units[0].AskPrice)
	if err != nil {
		return Book{}, err
	}
	bid, err := parseWon(w.Units[0].BidPrice)
	if err != nil {
		return Book{}, err
	}
	askQty, err := parseQtyNumber(w.Units[0].AskSize)
	if err != nil {
		return Book{}, err
	}
	bidQty, err := parseQtyNumber(w.Units[0].BidSize)
	if err != nil {
		return Book{}, err
	}
	switch {
	case TickAt(ask) == 0 || TickAt(bid) == 0:
		return Book{}, fmt.Errorf("%w: price below the whole-won grid (bid %d ask %d)", ErrBook, bid, ask)
	case bid >= ask:
		return Book{}, fmt.Errorf("%w: crossed book (bid %d ask %d)", ErrBook, bid, ask)
	case askQty.IsZero() || bidQty.IsZero():
		return Book{}, fmt.Errorf("%w: empty touch", ErrBook)
	}
	return Book{
		Market:      w.Code,
		BidPriceKRW: bid,
		AskPriceKRW: ask,
		BidQty:      bidQty,
		AskQty:      askQty,
		ExchangeMS:  w.Timestamp,
		ReceivedAt:  receivedAt,
		StreamType:  w.Stream,
	}, nil
}

// parseWon reads a JSON price as whole KRW. Upbit's default wire format sends
// prices as doubles ("109950000.0"); a non-zero fraction would need a sub-won
// tick, which this whole-won model does not represent.
func parseWon(n json.Number) (int64, error) {
	s := n.String()
	if i := strings.IndexByte(s, '.'); i >= 0 {
		whole, frac := s[:i], s[i+1:]
		if whole == "" || frac == "" || strings.Trim(frac, "0") != "" {
			return 0, fmt.Errorf("%w: non-integer won price %q", ErrBook, s)
		}
		s = whole
	}
	if !allDigits(s) {
		return 0, fmt.Errorf("%w: won price %q", ErrBook, s)
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: won price %q", ErrBook, s)
	}
	return v, nil
}

func parseQtyNumber(n json.Number) (Qty, error) {
	q, err := ParseQty(n.String())
	if err != nil {
		return Qty{}, fmt.Errorf("%w: %v", ErrBook, err)
	}
	return q, nil
}

// MarketNames are the KRW markets this shadow model accepts (auto_trader
// hk 5438 fixes the wider list with the wire contract).
var MarketNames = []string{"KRW-BTC", "KRW-ETH", "KRW-SOL", "KRW-XRP"}

// ValidMarket reports whether m is one of MarketNames.
func ValidMarket(m string) bool {
	for _, v := range MarketNames {
		if m == v {
			return true
		}
	}
	return false
}
