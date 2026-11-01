// Package book records KIS domestic order-book (H0STASP0) and trade
// (H0STCNT0) websocket frames into append-only evidence files and replays
// those files deterministically into book state and a trade tape.
//
// The package has no network client and no order capability. It reads frames
// it is handed and writes evidence to the io.Writer it is given.
package book

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// KIS transaction IDs this package records.
const (
	TRBook  = "H0STASP0"
	TRTrade = "H0STCNT0"
)

// Depth is the number of price levels on each side of an H0STASP0 record.
const Depth = 10

// Field positions in one H0STASP0 record. Only the fields the book needs are
// named; the record carries more (expected-auction fields and deltas).
const (
	bookSymbol   = 0
	bookHour     = 1
	bookAsk1     = 3
	bookBid1     = bookAsk1 + Depth
	bookAskQty1  = bookBid1 + Depth
	bookBidQty1  = bookAskQty1 + Depth
	bookTotalAsk = bookBidQty1 + Depth
	bookTotalBid = bookTotalAsk + 1
	bookMinWidth = bookTotalBid + 1
)

// Field positions in one H0STCNT0 record.
const (
	tradeSymbol   = 0
	tradeHour     = 1
	tradePrice    = 2
	tradeQty      = 12
	tradeAcmlVol  = 13
	tradeSideCode = 21
	tradeMinWidth = tradeSideCode + 1
)

var (
	// ErrNotRealtime marks a JSON system frame (subscribe ACK, PINGPONG).
	ErrNotRealtime = errors.New("book: not a realtime data frame")
	// ErrUnsupportedTR marks a realtime frame for a transaction this package
	// does not record, such as an execution notice.
	ErrUnsupportedTR = errors.New("book: unsupported transaction")
	// ErrEncrypted marks an encrypted frame. Book and trade frames arrive in
	// plain text, so an encrypted one is recorded as evidence and not applied.
	ErrEncrypted = errors.New("book: encrypted frame")
	// ErrMalformed marks a book or trade frame whose records cannot be parsed.
	ErrMalformed = errors.New("book: malformed frame")
)

// Level is one price level.
type Level struct {
	Price int64 `json:"price"`
	Qty   int64 `json:"qty"`
}

// BookRecord is one H0STASP0 record: a full ten-level snapshot.
type BookRecord struct {
	Symbol   string
	Hour     string // exchange time, HHMMSS (KST)
	Asks     [Depth]Level
	Bids     [Depth]Level
	TotalAsk int64
	TotalBid int64
	text     string // the record's own fields, for duplicate detection
}

// TradeRecord is one H0STCNT0 record: one execution on the exchange.
type TradeRecord struct {
	Symbol  string
	Hour    string // exchange time, HHMMSS (KST)
	Price   int64
	Qty     int64  // CNTG_VOL, this trade's volume
	AcmlVol int64  // ACML_VOL, the day's cumulative volume including this trade
	Side    string // "buy", "sell" or "auction" (CCLD_DVSN 1, 5, 3)
	text    string
}

// Frame is one parsed realtime frame. Exactly one of Books and Trades is set.
type Frame struct {
	TR     string
	Books  []BookRecord
	Trades []TradeRecord
}

// ParseFrame parses one raw KIS realtime frame,
//
//	<encrypted flag>|<tr_id>|<record count>|<payload>
//
// whose payload holds count records of equal width separated by '^'.
func ParseFrame(raw []byte) (Frame, error) {
	text := strings.TrimRight(string(raw), "\r\n")
	if strings.HasPrefix(strings.TrimLeft(text, " \t\r\n"), "{") {
		return Frame{}, ErrNotRealtime
	}
	parts := strings.SplitN(text, "|", 4)
	if len(parts) < 4 || (parts[0] != "0" && parts[0] != "1") || parts[1] == "" {
		// A damaged header that still names a recorded transaction is a
		// malformed frame, kept as evidence, not noise to ignore.
		if len(parts) > 1 && (parts[1] == TRBook || parts[1] == TRTrade) {
			return Frame{TR: parts[1]}, fmt.Errorf("%w: header %q", ErrMalformed, text)
		}
		return Frame{}, ErrNotRealtime
	}
	tr := parts[1]
	if tr != TRBook && tr != TRTrade {
		return Frame{TR: tr}, ErrUnsupportedTR
	}
	if parts[0] == "1" {
		return Frame{TR: tr}, ErrEncrypted
	}
	count, err := strconv.Atoi(parts[2])
	if err != nil || count <= 0 {
		return Frame{TR: tr}, fmt.Errorf("%w: record count %q", ErrMalformed, parts[2])
	}
	fields := strings.Split(parts[3], "^")
	if len(fields)%count != 0 {
		return Frame{TR: tr}, fmt.Errorf("%w: %d fields do not split into %d records", ErrMalformed, len(fields), count)
	}
	width := len(fields) / count
	frame := Frame{TR: tr}
	for i := 0; i < count; i++ {
		record := fields[i*width : (i+1)*width]
		if tr == TRBook {
			parsed, err := parseBook(record)
			if err != nil {
				return Frame{TR: tr}, fmt.Errorf("%w: record %d: %v", ErrMalformed, i, err)
			}
			frame.Books = append(frame.Books, parsed)
			continue
		}
		parsed, err := parseTrade(record)
		if err != nil {
			return Frame{TR: tr}, fmt.Errorf("%w: record %d: %v", ErrMalformed, i, err)
		}
		frame.Trades = append(frame.Trades, parsed)
	}
	return frame, nil
}

func parseBook(f []string) (BookRecord, error) {
	if len(f) < bookMinWidth {
		return BookRecord{}, fmt.Errorf("%d fields, want at least %d", len(f), bookMinWidth)
	}
	p := numberParser{fields: f}
	r := BookRecord{Symbol: f[bookSymbol], Hour: f[bookHour], text: strings.Join(f, "^")}
	for i := 0; i < Depth; i++ {
		r.Asks[i] = Level{Price: p.int(bookAsk1 + i), Qty: p.int(bookAskQty1 + i)}
		r.Bids[i] = Level{Price: p.int(bookBid1 + i), Qty: p.int(bookBidQty1 + i)}
	}
	r.TotalAsk = p.int(bookTotalAsk)
	r.TotalBid = p.int(bookTotalBid)
	if err := validateHeader(r.Symbol, r.Hour); err != nil {
		return BookRecord{}, err
	}
	return r, p.err
}

func parseTrade(f []string) (TradeRecord, error) {
	if len(f) < tradeMinWidth {
		return TradeRecord{}, fmt.Errorf("%d fields, want at least %d", len(f), tradeMinWidth)
	}
	p := numberParser{fields: f}
	r := TradeRecord{
		Symbol:  f[tradeSymbol],
		Hour:    f[tradeHour],
		Price:   p.int(tradePrice),
		Qty:     p.int(tradeQty),
		AcmlVol: p.int(tradeAcmlVol),
		text:    strings.Join(f, "^"),
	}
	switch code := f[tradeSideCode]; code {
	case "1":
		r.Side = "buy"
	case "5":
		r.Side = "sell"
	case "3":
		r.Side = "auction"
	default:
		return TradeRecord{}, fmt.Errorf("CCLD_DVSN %q", code)
	}
	if err := validateHeader(r.Symbol, r.Hour); err != nil {
		return TradeRecord{}, err
	}
	if p.err == nil && (r.Qty <= 0 || r.AcmlVol < r.Qty) {
		return TradeRecord{}, fmt.Errorf("volume %d with cumulative %d", r.Qty, r.AcmlVol)
	}
	if p.err == nil && r.Price <= 0 {
		return TradeRecord{}, fmt.Errorf("price %d", r.Price)
	}
	return r, p.err
}

func validateHeader(symbol, hour string) error {
	if symbol == "" {
		return errors.New("empty symbol")
	}
	if len(hour) != 6 {
		return fmt.Errorf("hour %q", hour)
	}
	for _, c := range hour {
		if c < '0' || c > '9' {
			return fmt.Errorf("hour %q", hour)
		}
	}
	// An out-of-range time would become the book's ordering baseline and
	// make every later snapshot look like a regression.
	if hour[0:2] > "23" || hour[2:4] > "59" || hour[4:6] > "59" {
		return fmt.Errorf("hour %q out of range", hour)
	}
	return nil
}

// numberParser keeps the first parse error so a record parse reads as a list
// of fields instead of a ladder of error checks.
type numberParser struct {
	fields []string
	err    error
}

func (p *numberParser) int(index int) int64 {
	value := p.fields[index]
	if value == "" {
		return 0
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil && p.err == nil {
		p.err = fmt.Errorf("field %d: %q is not an integer", index, value)
	}
	return n
}
