package upbit

import (
	"encoding/json"
	"fmt"
	"time"
)

// Level is one price level of an orderbook side.
type Level struct {
	PriceKRW int64
	Qty      Qty
}

// Depth is a whole orderbook frame: the touch (Book) and every level the
// recorder subscribed to (15). Asks are ascending and Bids descending, so the
// first of each is the touch. Replay needs the depth because a taker fill
// walks it (design 5852 §7.2 rule 2); ParseBook, which execupbit uses, reads
// the touch only and is left as it is.
type Depth struct {
	Book
	Asks []Level
	Bids []Level
}

// ParseDepth decodes one orderbook frame received at receivedAt and refuses
// what ParseBook refuses (not an orderbook, unknown market, no levels,
// non-integer or sub-grid won price, empty or crossed touch, bad quantity),
// plus any deeper level that is empty, off the grid or out of price order: a
// frame whose depth cannot be read exactly is not a book to fill against.
func ParseDepth(data []byte, receivedAt time.Time) (Depth, error) {
	var w wireBook
	if err := json.Unmarshal(data, &w); err != nil {
		return Depth{}, fmt.Errorf("%w: %v", ErrBook, err)
	}
	if w.Type != "orderbook" {
		return Depth{}, fmt.Errorf("%w: type %q", ErrBook, w.Type)
	}
	if !ValidMarket(w.Code) {
		return Depth{}, fmt.Errorf("%w: market %q", ErrBook, w.Code)
	}
	if w.Timestamp <= 0 {
		return Depth{}, fmt.Errorf("%w: timestamp %d", ErrBook, w.Timestamp)
	}
	if len(w.Units) == 0 {
		return Depth{}, fmt.Errorf("%w: no orderbook_units", ErrBook)
	}
	d := Depth{Asks: make([]Level, 0, len(w.Units)), Bids: make([]Level, 0, len(w.Units))}
	for i, u := range w.Units {
		ask, err := parseWon(u.AskPrice)
		if err != nil {
			return Depth{}, err
		}
		bid, err := parseWon(u.BidPrice)
		if err != nil {
			return Depth{}, err
		}
		askQty, err := parseQtyNumber(u.AskSize)
		if err != nil {
			return Depth{}, err
		}
		bidQty, err := parseQtyNumber(u.BidSize)
		if err != nil {
			return Depth{}, err
		}
		switch {
		case TickAt(ask) == 0 || TickAt(bid) == 0:
			return Depth{}, fmt.Errorf("%w: level %d price below the whole-won grid (bid %d ask %d)", ErrBook, i, bid, ask)
		case askQty.IsZero() || bidQty.IsZero():
			return Depth{}, fmt.Errorf("%w: level %d is empty", ErrBook, i)
		case i == 0 && bid >= ask:
			return Depth{}, fmt.Errorf("%w: crossed book (bid %d ask %d)", ErrBook, bid, ask)
		case i > 0 && (ask <= d.Asks[i-1].PriceKRW || bid >= d.Bids[i-1].PriceKRW):
			return Depth{}, fmt.Errorf("%w: level %d is out of price order", ErrBook, i)
		}
		d.Asks = append(d.Asks, Level{PriceKRW: ask, Qty: askQty})
		d.Bids = append(d.Bids, Level{PriceKRW: bid, Qty: bidQty})
	}
	d.Book = Book{
		Market:      w.Code,
		BidPriceKRW: d.Bids[0].PriceKRW,
		AskPriceKRW: d.Asks[0].PriceKRW,
		BidQty:      d.Bids[0].Qty,
		AskQty:      d.Asks[0].Qty,
		ExchangeMS:  w.Timestamp,
		ReceivedAt:  receivedAt,
		StreamType:  w.Stream,
	}
	return d, nil
}
