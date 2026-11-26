// Package upbitpriv records private Upbit WebSocket fills.
package upbitpriv

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"
)

var (
	marketCode   = regexp.MustCompile(`^[A-Z0-9]+-[A-Z0-9]+$`)
	errMalformed = errors.New("Upbit WS malformed myOrder frame")
	errRejected  = errors.New("Upbit WS subscription rejected")
)

type Record struct {
	ReceivedAt    time.Time `json:"received_at"`
	Broker        string    `json:"broker"`
	Venue         string    `json:"venue"`
	Instrument    string    `json:"instrument_type"`
	Symbol        string    `json:"symbol"`
	RawSymbol     string    `json:"raw_symbol"`
	Side          string    `json:"side"`
	BrokerOrderID string    `json:"broker_order_id"`
	TradeUUID     string    `json:"trade_uuid"`
	Identifier    string    `json:"identifier,omitempty"`
	FilledPrice   float64   `json:"filled_price"`
	FilledQty     float64   `json:"filled_qty"`
	FilledAt      time.Time `json:"filled_at"`
	Currency      string    `json:"currency"`
}

// decodeFrame consumes the documented DEFAULT format. A nil record with no
// error is a known status frame or order update without an individual trade.
func decodeFrame(raw []byte, receivedAt time.Time) (*Record, error) {
	var frame struct {
		Type    string          `json:"type"`
		Status  string          `json:"status"`
		Error   json.RawMessage `json:"error"`
		Market  string          `json:"code"`
		OrderID string          `json:"uuid"`
		Side    string          `json:"ask_bid"`
		State   string          `json:"state"`
		TradeID string          `json:"trade_uuid"`
		ID      string          `json:"identifier"`
		Price   float64         `json:"price"`
		Volume  float64         `json:"volume"`
		TradeMS int64           `json:"trade_timestamp"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil {
		return nil, errMalformed
	}
	if frame.Error != nil && string(frame.Error) != "null" {
		return nil, errRejected
	}
	if frame.Type == "" && frame.Status == "UP" {
		return nil, nil
	}
	if frame.Type != "myOrder" || !marketCode.MatchString(frame.Market) || strings.TrimSpace(frame.OrderID) == "" {
		return nil, errMalformed
	}
	var side string
	switch frame.Side {
	case "BID":
		side = "buy"
	case "ASK":
		side = "sell"
	default:
		return nil, errMalformed
	}
	switch frame.State {
	case "wait", "watch", "done", "cancel", "prevented":
		return nil, nil
	case "trade":
	default:
		return nil, errMalformed
	}
	// Keep the trade time within the range supported by RFC 3339 JSON.
	if strings.TrimSpace(frame.TradeID) == "" || frame.Price <= 0 || frame.Volume <= 0 || frame.TradeMS <= 0 || frame.TradeMS >= 253402300800000 {
		return nil, errMalformed
	}
	currency, asset, _ := strings.Cut(frame.Market, "-")
	symbol := frame.Market
	if currency == "KRW" || currency == "USDT" {
		symbol = asset
	}
	return &Record{
		ReceivedAt: receivedAt.UTC(), Broker: "upbit", Venue: "upbit_" + strings.ToLower(currency),
		Instrument: "crypto", Symbol: symbol, RawSymbol: frame.Market, Side: side,
		BrokerOrderID: frame.OrderID, TradeUUID: frame.TradeID, Identifier: frame.ID,
		FilledPrice: frame.Price, FilledQty: frame.Volume, FilledAt: time.UnixMilli(frame.TradeMS).UTC(),
		Currency: currency,
	}, nil
}
