package book

import (
	"errors"
	"testing"
)

func TestParseBookFrame(t *testing.T) {
	f, err := ParseFrame(append(bookFrame("005930", "090001", 71100, 71000, 10), '\r', '\n'))
	if err != nil {
		t.Fatal(err)
	}
	if f.TR != TRBook || len(f.Books) != 1 || len(f.Trades) != 0 {
		t.Fatalf("frame = %+v", f)
	}
	b := f.Books[0]
	if b.Symbol != "005930" || b.Hour != "090001" {
		t.Fatalf("header = %s %s", b.Symbol, b.Hour)
	}
	if b.Asks[0] != (Level{71100, 10}) || b.Asks[9] != (Level{72000, 19}) {
		t.Fatalf("asks = %+v", b.Asks)
	}
	if b.Bids[0] != (Level{71000, 20}) || b.Bids[9] != (Level{70100, 29}) {
		t.Fatalf("bids = %+v", b.Bids)
	}
	if b.TotalAsk != 100 || b.TotalBid != 101 {
		t.Fatalf("totals = %d %d", b.TotalAsk, b.TotalBid)
	}
}

func TestParseTradeFrame(t *testing.T) {
	f, err := ParseFrame(frame(TRTrade,
		tradeRecord("005930", "090001", 71100, 7, 1007, "5"),
		tradeRecord("005930", "090001", 71200, 3, 1010, "1")))
	if err != nil {
		t.Fatal(err)
	}
	want := []TradeRecord{
		{Symbol: "005930", Hour: "090001", Price: 71100, Qty: 7, AcmlVol: 1007, Side: "sell"},
		{Symbol: "005930", Hour: "090001", Price: 71200, Qty: 3, AcmlVol: 1010, Side: "buy"},
	}
	for i := range want {
		got := f.Trades[i]
		got.text = ""
		if got != want[i] {
			t.Fatalf("trade %d = %+v, want %+v", i, got, want[i])
		}
	}
}

func TestParseFrameRejects(t *testing.T) {
	cases := map[string]struct {
		raw  string
		want error
	}{
		"ack":            {`{"header":{"tr_id":"H0STASP0"},"body":{"rt_cd":"0"}}`, ErrNotRealtime},
		"not kis":        {"hello", ErrNotRealtime},
		"too few parts":  {"0|H0STASP0|001", ErrMalformed},
		"bad flag":       {"2|H0STCNT0|001|x", ErrMalformed},
		"hour range":     {string(tradeFrame("005930", "259999", 1, 1, 1)), ErrMalformed},
		"minute range":   {string(bookFrame("005930", "096000", 2, 1, 1)), ErrMalformed},
		"zero price":     {string(tradeFrame("005930", "090000", 0, 1, 1)), ErrMalformed},
		"negative price": {string(tradeFrame("005930", "090000", -5, 1, 1)), ErrMalformed},
		"execution":      {"0|H0STCNI0|001|a^b", ErrUnsupportedTR},
		"encrypted":      {"1|H0STCNT0|001|xyz", ErrEncrypted},
		"zero count":     {"0|H0STCNT0|000|a", ErrMalformed},
		"short record":   {"0|H0STCNT0|001|005930^090000^71000", ErrMalformed},
		"uneven records": {"0|H0STCNT0|002|" + tradeRecord("005930", "090000", 1, 1, 1, "1") + "^x", ErrMalformed},
		"bad price":      {"0|H0STASP0|001|" + bookRecord("005930", "090000", 1, 1, 1)[:20] + "x" + bookRecord("005930", "090000", 1, 1, 1)[20:], ErrMalformed},
		"bad hour":       {string(tradeFrame("005930", "9:00", 1, 1, 1)), ErrMalformed},
		"bad side":       {string(frame(TRTrade, tradeRecord("005930", "090000", 1, 1, 1, "9"))), ErrMalformed},
		"zero volume":    {string(tradeFrame("005930", "090000", 1, 0, 1)), ErrMalformed},
	}
	for name, c := range cases {
		if _, err := ParseFrame([]byte(c.raw)); !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", name, err, c.want)
		}
	}
}
