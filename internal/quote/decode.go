package quote

import (
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/go-kis/kis/ws"
)

// StreamFields is the complete, ordered field set of every quotes stream
// entry. No other field is ever written.
var StreamFields = []string{"symbol", "ts", "price", "bid1", "ask1", "bid_qty", "ask_qty", "session"}

// Tick is one stream entry. Every field is always written; a value the
// source transaction does not carry is the empty string:
//
//   - H0STCNT0 (trade) fills price; bid1, ask1, bid_qty, and ask_qty are "".
//   - H0STASP0 (order book) fills bid1, ask1, bid_qty, and ask_qty; price is "".
//
// Values are never carried over from an earlier frame, so an entry never
// mixes observations taken at different times.
type Tick struct {
	Symbol  string
	TS      string // RFC 3339 in KST: receipt date plus the frame's HHMMSS
	Price   string
	Bid1    string
	Ask1    string
	BidQty  string
	AskQty  string
	Session string // SessionRegular or SessionAfterHours
}

// values returns the XADD field/value list in StreamFields order.
func (t Tick) values() []interface{} {
	return []interface{}{
		"symbol", t.Symbol,
		"ts", t.TS,
		"price", t.Price,
		"bid1", t.Bid1,
		"ask1", t.Ask1,
		"bid_qty", t.BidQty,
		"ask_qty", t.AskQty,
		"session", t.Session,
	}
}

// Field positions are exactly the maps in auto_trader
// app/services/brokers/kis/mock_scalping_ws/quote_protocol.py (TRADE_FIELDS,
// ORDERBOOK_FIELDS), the only layout source in the repositories. The one
// addition is the order-book time at index 1 (BSOP_HOUR), which sits between
// the symbol and the HOUR_CLS_CODE that precedes ASKP1 at index 3. The trade
// TR's own best-level quote positions are not in that source, so a trade tick
// leaves bid1 and ask1 empty rather than guess them.
//
// A record is partial when it ends before the last position read here.
const (
	tradeSymbol = 0
	tradeTime   = 1 // STCK_CNTG_HOUR, HHMMSS
	tradePrice  = 2 // STCK_PRPR
	tradeMinLen = tradePrice + 1

	bookSymbol    = 0
	bookTime      = 1  // BSOP_HOUR, HHMMSS
	bookAsk1      = 3  // ASKP1
	bookBid1      = 13 // BIDP1
	bookAskQty1   = 23 // ASKP_RSQN1
	bookBidQty1   = 33 // BIDP_RSQN1
	bookMinLen    = bookBidQty1 + 1
	maxFrameCount = 100
)

// DropReason names why a frame, or one record in it, produced no tick.
type DropReason string

const (
	DropMalformed    DropReason = "malformed"
	DropPartial      DropReason = "partial"
	DropUnknownTR    DropReason = "unknown_tr"
	DropUnknownSym   DropReason = "unknown_symbol"
	DropOutOfSession DropReason = "out_of_session"
)

// Decoder turns KIS quote events into ticks for a fixed symbol set.
type Decoder struct {
	symbols map[string]struct{}
}

// NewDecoder accepts ticks only for symbols.
func NewDecoder(symbols []string) *Decoder {
	set := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		set[symbol] = struct{}{}
	}
	return &Decoder{symbols: set}
}

// Decode returns the ticks in one event and a reason for each record that was
// dropped. It never panics on short or malformed input.
func (d *Decoder) Decode(event ws.Event) ([]Tick, []DropReason) {
	var minLen int
	switch event.TR {
	case ws.TRQuotePrice:
		minLen = tradeMinLen
	case ws.TRQuoteBook:
		minLen = bookMinLen
	default:
		return nil, []DropReason{DropUnknownTR}
	}
	count, ok := recordCount(event.Raw, event.TR)
	if !ok || event.ReceivedAt.IsZero() {
		return nil, []DropReason{DropMalformed}
	}
	fields := event.Fields
	if len(fields)%count != 0 {
		return nil, []DropReason{DropMalformed}
	}
	size := len(fields) / count
	var ticks []Tick
	var drops []DropReason
	for index := 0; index < count; index++ {
		record := fields[index*size : (index+1)*size]
		if len(record) < minLen {
			drops = append(drops, DropPartial)
			continue
		}
		tick, reason := d.decodeRecord(event.TR, record, event.ReceivedAt)
		if reason != "" {
			drops = append(drops, reason)
			continue
		}
		ticks = append(ticks, tick)
	}
	return ticks, drops
}

func (d *Decoder) decodeRecord(tr string, record []string, receivedAt time.Time) (Tick, DropReason) {
	var tick Tick
	var second int
	var ok bool
	switch tr {
	case ws.TRQuotePrice:
		tick.Symbol = record[tradeSymbol]
		tick.TS, second, ok = stamp(receivedAt, record[tradeTime])
		if !ok {
			return Tick{}, DropMalformed
		}
		if tick.Price, ok = positive(record[tradePrice]); !ok {
			return Tick{}, DropMalformed
		}
	case ws.TRQuoteBook:
		tick.Symbol = record[bookSymbol]
		tick.TS, second, ok = stamp(receivedAt, record[bookTime])
		if !ok {
			return Tick{}, DropMalformed
		}
		if tick.Ask1, ok = unsigned(record[bookAsk1]); !ok {
			return Tick{}, DropMalformed
		}
		if tick.Bid1, ok = unsigned(record[bookBid1]); !ok {
			return Tick{}, DropMalformed
		}
		if tick.AskQty, ok = unsigned(record[bookAskQty1]); !ok {
			return Tick{}, DropMalformed
		}
		if tick.BidQty, ok = unsigned(record[bookBidQty1]); !ok {
			return Tick{}, DropMalformed
		}
	}
	if !ValidSymbol(tick.Symbol) {
		return Tick{}, DropMalformed
	}
	if _, ok := d.symbols[tick.Symbol]; !ok {
		return Tick{}, DropUnknownSym
	}
	if tick.Session, ok = sessionForSecond(second); !ok {
		return Tick{}, DropOutOfSession
	}
	return tick, ""
}

// recordCount reads the record count from the raw frame header
// "<flag>|<tr>|<count>|<payload>". The header must name tr.
func recordCount(raw []byte, tr string) (int, bool) {
	parts := strings.SplitN(string(raw), "|", 4)
	if len(parts) < 4 || (parts[0] != "0" && parts[0] != "1") || parts[1] != tr {
		return 0, false
	}
	count, err := strconv.Atoi(parts[2])
	if err != nil || count < 1 || count > maxFrameCount {
		return 0, false
	}
	return count, true
}

// stamp combines the KST receipt date with the frame's HHMMSS and also
// returns that time of day in seconds.
func stamp(receivedAt time.Time, hhmmss string) (string, int, bool) {
	second, ok := hhmmssSeconds(hhmmss)
	if !ok {
		return "", 0, false
	}
	return kstMidnight(receivedAt).Add(time.Duration(second) * time.Second).Format(time.RFC3339), second, true
}

func hhmmssSeconds(value string) (int, bool) {
	if len(value) != 6 {
		return 0, false
	}
	for index := 0; index < 6; index++ {
		if value[index] < '0' || value[index] > '9' {
			return 0, false
		}
	}
	hour, _ := strconv.Atoi(value[0:2])
	minute, _ := strconv.Atoi(value[2:4])
	second, _ := strconv.Atoi(value[4:6])
	if hour > 23 || minute > 59 || second > 59 {
		return 0, false
	}
	return hour*3600 + minute*60 + second, true
}

// unsigned canonicalises a non-negative decimal integer; signs, spaces,
// decimals, and empty values are rejected.
func unsigned(value string) (string, bool) {
	if value == "" {
		return "", false
	}
	for index := 0; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return "", false
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return "", false
	}
	return strconv.FormatUint(parsed, 10), true
}

func positive(value string) (string, bool) {
	canonical, ok := unsigned(value)
	if !ok || canonical == "0" {
		return "", false
	}
	return canonical, true
}
