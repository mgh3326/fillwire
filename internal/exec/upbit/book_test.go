package upbit

import (
	"errors"
	"testing"
	"time"
)

const sampleFrame = `{"type":"orderbook","code":"KRW-XRP","timestamp":1795078800000,` +
	`"total_ask_size":1250.5,"total_bid_size":2010.25,` +
	`"orderbook_units":[{"ask_price":3000.0,"bid_price":2999.0,"ask_size":0.00000001,"bid_size":33.5},` +
	`{"ask_price":3001.0,"bid_price":2998.0,"ask_size":10.0,"bid_size":20.0}],` +
	`"level":0.0,"stream_type":"REALTIME"}`

func TestParseBookReadsTheTouch(t *testing.T) {
	at := time.Date(2026, 11, 19, 9, 0, 0, 0, time.UTC)
	b, err := ParseBook([]byte(sampleFrame), at)
	if err != nil {
		t.Fatal(err)
	}
	if b.Market != "KRW-XRP" || b.BidPriceKRW != 2999 || b.AskPriceKRW != 3000 {
		t.Fatalf("book = %+v", b)
	}
	if b.BidQty.String() != "33.5" || b.AskQty.String() != "0.00000001" {
		t.Fatalf("quantities = %q / %q", b.BidQty, b.AskQty)
	}
	if b.ExchangeMS != 1795078800000 || !b.ReceivedAt.Equal(at) || b.StreamType != "REALTIME" {
		t.Fatalf("timestamps = %+v", b)
	}
}

func TestParseBookRefusesWhatItCannotPrice(t *testing.T) {
	at := time.Date(2026, 11, 19, 9, 0, 0, 0, time.UTC)
	cases := map[string]string{
		"not an orderbook":    `{"type":"trade","code":"KRW-XRP","timestamp":1}`,
		"unknown market":      `{"type":"orderbook","code":"BTC-XRP","timestamp":1,"orderbook_units":[{"ask_price":3000.0,"bid_price":2999.0,"ask_size":1,"bid_size":1}]}`,
		"no timestamp":        `{"type":"orderbook","code":"KRW-XRP","orderbook_units":[{"ask_price":3000.0,"bid_price":2999.0,"ask_size":1,"bid_size":1}]}`,
		"no levels":           `{"type":"orderbook","code":"KRW-XRP","timestamp":1,"orderbook_units":[]}`,
		"crossed book":        `{"type":"orderbook","code":"KRW-XRP","timestamp":1,"orderbook_units":[{"ask_price":2999.0,"bid_price":3000.0,"ask_size":1,"bid_size":1}]}`,
		"equal touch":         `{"type":"orderbook","code":"KRW-XRP","timestamp":1,"orderbook_units":[{"ask_price":3000.0,"bid_price":3000.0,"ask_size":1,"bid_size":1}]}`,
		"empty level":         `{"type":"orderbook","code":"KRW-XRP","timestamp":1,"orderbook_units":[{"ask_price":3000.0,"bid_price":2999.0,"ask_size":0.0,"bid_size":1}]}`,
		"fractional won":      `{"type":"orderbook","code":"KRW-XRP","timestamp":1,"orderbook_units":[{"ask_price":3000.5,"bid_price":2999.0,"ask_size":1,"bid_size":1}]}`,
		"below the grid":      `{"type":"orderbook","code":"KRW-XRP","timestamp":1,"orderbook_units":[{"ask_price":99.0,"bid_price":98.0,"ask_size":1,"bid_size":1}]}`,
		"exponent price":      `{"type":"orderbook","code":"KRW-XRP","timestamp":1,"orderbook_units":[{"ask_price":3e3,"bid_price":2.999e3,"ask_size":1,"bid_size":1}]}`,
		"too many qty digits": `{"type":"orderbook","code":"KRW-XRP","timestamp":1,"orderbook_units":[{"ask_price":3000.0,"bid_price":2999.0,"ask_size":0.000000001,"bid_size":1}]}`,
	}
	for name, frame := range cases {
		if b, err := ParseBook([]byte(frame), at); err == nil {
			t.Errorf("%s: accepted %+v", name, b)
		} else if !errors.Is(err, ErrBook) {
			t.Errorf("%s: error %v is not ErrBook", name, err)
		}
	}
}
