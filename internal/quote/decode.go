package quote

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"
)

// StreamFields is the complete, ordered field set of every quotes stream
// entry. No other field is ever written.
var StreamFields = []string{"symbol", "ts", "price", "bid1", "ask1", "bid_qty", "ask_qty", "session"}

// Tick is one stream entry. Every field is always written; a value the
// source channel does not carry is the empty string:
//
//   - trade frames fill price; bid1, ask1, bid_qty, and ask_qty are "".
//   - orderbook frames fill bid1 and bid_qty from bids[0] and ask1 and ask_qty
//     from asks[0]; price is "". An empty side leaves its two fields "".
//
// Values are never carried over from an earlier frame. Toss trade and
// orderbook frames carry no sequence number and may be dropped by the server
// (the latest state wins), so the stream is lossy by design.
type Tick struct {
	Symbol  string
	TS      string // the frame's own timestamp, RFC 3339 with milliseconds
	Price   string
	Bid1    string
	Ask1    string
	BidQty  string
	AskQty  string
	Session string // one of the Session* labels
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

const tsLayout = "2006-01-02T15:04:05.000Z07:00"

// maxDecimal is the AsyncAPI maxLength of every decimal string.
const maxDecimal = 30

// DropReason names why a data frame produced no tick.
type DropReason string

const (
	DropMalformed    DropReason = "malformed"
	DropUnknownTopic DropReason = "unknown_topic"
	DropUnknownSym   DropReason = "unknown_symbol"
	DropNoTimestamp  DropReason = "no_timestamp"
	DropOutOfSession DropReason = "out_of_session"
)

// FrameKind is the top-level frame type from the Toss AsyncAPI connection
// channel.
type FrameKind int

const (
	FrameUnknown FrameKind = iota
	FrameMessage
	FrameSubscriptions
	FrameError
	FramePong
)

// Frame is one decoded server frame.
type Frame struct {
	Kind FrameKind
	// FrameMessage
	Tick Tick
	Drop DropReason // non-empty when a message frame produced no tick
	// FrameSubscriptions
	Subscribed []string
	Rejected   []Rejection
	// FrameError
	ErrorCode string
}

// Rejection is one rejected subscription target from an ack.
type Rejection struct {
	Target string `json:"target"`
	Code   string `json:"code"`
}

type wireFrame struct {
	Type       string          `json:"type"`
	Topic      string          `json:"topic"`
	Data       json.RawMessage `json:"data"`
	Subscribed []string        `json:"subscribed"`
	Rejected   []Rejection     `json:"rejected"`
	Error      *struct {
		Code string `json:"code"`
	} `json:"error"`
}

type tradeData struct {
	Price     *string `json:"price"`
	Timestamp *string `json:"timestamp"`
}

type level struct {
	Price  *string `json:"price"`
	Volume *string `json:"volume"`
}

type orderbookData struct {
	Timestamp *string `json:"timestamp"`
	Asks      []level `json:"asks"`
	Bids      []level `json:"bids"`
}

// Decoder turns Toss frames into ticks for a fixed symbol set.
type Decoder struct {
	symbols map[Symbol]struct{}
}

// NewDecoder accepts ticks only for symbols.
func NewDecoder(symbols []Symbol) *Decoder {
	set := make(map[Symbol]struct{}, len(symbols))
	for _, symbol := range symbols {
		set[symbol] = struct{}{}
	}
	return &Decoder{symbols: set}
}

// Decode classifies one text frame. It never panics on malformed input.
func (d *Decoder) Decode(raw []byte) Frame {
	var wire wireFrame
	if err := json.Unmarshal(raw, &wire); err != nil {
		return Frame{Kind: FrameUnknown}
	}
	switch wire.Type {
	case "message":
		tick, drop := d.decodeMessage(wire)
		return Frame{Kind: FrameMessage, Tick: tick, Drop: drop}
	case "subscriptions":
		return Frame{Kind: FrameSubscriptions, Subscribed: wire.Subscribed, Rejected: wire.Rejected}
	case "error":
		code := ""
		if wire.Error != nil {
			code = wire.Error.Code
		}
		return Frame{Kind: FrameError, ErrorCode: code}
	case "pong":
		return Frame{Kind: FramePong}
	default:
		return Frame{Kind: FrameUnknown}
	}
}

func (d *Decoder) decodeMessage(wire wireFrame) (Tick, DropReason) {
	parts := strings.Split(wire.Topic, ":")
	if len(parts) != 3 || (parts[0] != "trade" && parts[0] != "orderbook") {
		return Tick{}, DropUnknownTopic
	}
	symbol := Symbol{Market: parts[1], Code: parts[2]}
	if !ValidSymbol(symbol) {
		return Tick{}, DropUnknownTopic
	}
	if _, ok := d.symbols[symbol]; !ok {
		return Tick{}, DropUnknownSym
	}
	if len(wire.Data) == 0 || bytes.Equal(wire.Data, []byte("null")) {
		return Tick{}, DropMalformed
	}
	tick := Tick{Symbol: symbol.Code}
	var stamp *string
	switch parts[0] {
	case "trade":
		var data tradeData
		if json.Unmarshal(wire.Data, &data) != nil || data.Price == nil || !decimal(*data.Price) || isZero(*data.Price) {
			return Tick{}, DropMalformed
		}
		tick.Price = *data.Price
		stamp = data.Timestamp
		if stamp == nil {
			return Tick{}, DropMalformed
		}
	case "orderbook":
		var data orderbookData
		if json.Unmarshal(wire.Data, &data) != nil {
			return Tick{}, DropMalformed
		}
		var ok bool
		if tick.Ask1, tick.AskQty, ok = best(data.Asks); !ok {
			return Tick{}, DropMalformed
		}
		if tick.Bid1, tick.BidQty, ok = best(data.Bids); !ok {
			return Tick{}, DropMalformed
		}
		if data.Timestamp == nil {
			// AsyncAPI: the orderbook timestamp is null when no data is
			// provided. A tick without its own time is not written.
			return Tick{}, DropNoTimestamp
		}
		stamp = data.Timestamp
	}
	at, err := time.Parse(time.RFC3339Nano, *stamp)
	if err != nil {
		return Tick{}, DropMalformed
	}
	tick.TS = at.Format(tsLayout)
	session, ok := SessionLabel(symbol.Market, at)
	if !ok {
		return Tick{}, DropOutOfSession
	}
	tick.Session = session
	return tick, ""
}

// best returns the first level of one side. An empty side is valid and
// yields empty values; a present level must carry two decimals.
func best(levels []level) (price, volume string, ok bool) {
	if len(levels) == 0 {
		return "", "", true
	}
	first := levels[0]
	if first.Price == nil || first.Volume == nil || !decimal(*first.Price) || !decimal(*first.Volume) {
		return "", "", false
	}
	return *first.Price, *first.Volume, true
}

// decimal accepts the AsyncAPI decimal strings: unsigned digits with an
// optional fractional part, at most 30 characters.
func decimal(value string) bool {
	if value == "" || len(value) > maxDecimal {
		return false
	}
	dot := false
	for index := 0; index < len(value); index++ {
		c := value[index]
		switch {
		case c >= '0' && c <= '9':
		case c == '.' && !dot && index > 0 && index < len(value)-1:
			dot = true
		default:
			return false
		}
	}
	return true
}

func isZero(value string) bool {
	return strings.Trim(value, "0.") == ""
}
