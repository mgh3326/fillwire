package quote

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/go-kis/kis/ws"
)

// receivedAt is 13:15:03 KST on a Wednesday, expressed in UTC so the decoder
// must convert to KST itself.
var receivedAt = time.Date(2026, time.September, 30, 4, 15, 3, 0, time.UTC)

func readFrame(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimRight(string(raw), "\n")
}

// eventFromFrame builds the ws.Event go-kis publishes for a plaintext frame:
// TR from the header and Fields from the '^'-split payload.
func eventFromFrame(frame string) ws.Event {
	parts := strings.SplitN(frame, "|", 4)
	event := ws.Event{Raw: []byte(frame), ReceivedAt: receivedAt}
	if len(parts) > 1 {
		event.TR = parts[1]
	}
	if len(parts) == 4 {
		event.Fields = strings.Split(parts[3], "^")
		event.Key = event.Fields[0]
	}
	return event
}

func testDecoder() *Decoder { return NewDecoder([]string{"005930", "000660"}) }

func TestDecodeTradeFixtureFrame(t *testing.T) {
	ticks, drops := testDecoder().Decode(eventFromFrame(readFrame(t, "h0stcnt0.frame")))
	if len(drops) != 0 {
		t.Fatalf("drops = %v, want none", drops)
	}
	want := []Tick{{Symbol: "005930", TS: "2026-09-30T13:15:02+09:00", Price: "70500", Session: SessionRegular}}
	if !reflect.DeepEqual(ticks, want) {
		t.Fatalf("ticks = %+v, want %+v", ticks, want)
	}
}

func TestDecodeOrderBookFixtureFrame(t *testing.T) {
	ticks, drops := testDecoder().Decode(eventFromFrame(readFrame(t, "h0stasp0.frame")))
	if len(drops) != 0 {
		t.Fatalf("drops = %v, want none", drops)
	}
	want := []Tick{{Symbol: "005930", TS: "2026-09-30T13:15:02+09:00", Bid1: "70500", Ask1: "70600", BidQty: "2300", AskQty: "1200", Session: SessionRegular}}
	if !reflect.DeepEqual(ticks, want) {
		t.Fatalf("ticks = %+v, want %+v", ticks, want)
	}
}

func TestDecodeMultiRecordTradeFrame(t *testing.T) {
	ticks, drops := testDecoder().Decode(eventFromFrame(readFrame(t, "h0stcnt0_multi.frame")))
	if len(drops) != 0 || len(ticks) != 2 {
		t.Fatalf("ticks = %+v drops = %v, want two ticks", ticks, drops)
	}
	if ticks[0].Price != "70500" || ticks[1].Price != "70600" || ticks[1].TS != "2026-09-30T13:15:03+09:00" {
		t.Fatalf("multi-record ticks = %+v", ticks)
	}
}

func TestDecodeGoKISUpstreamMarketDataFixture(t *testing.T) {
	// go-kis internal/testutil/fixtures/ws-market-data.json raw_body: a short
	// trade record that still carries every position the decoder reads.
	ticks, drops := testDecoder().Decode(eventFromFrame(readFrame(t, "gokis_ws_market_data.frame")))
	want := []Tick{{Symbol: "005930", TS: "2026-09-30T09:10:00+09:00", Price: "71000", Session: SessionRegular}}
	if len(drops) != 0 || !reflect.DeepEqual(ticks, want) {
		t.Fatalf("ticks = %+v drops = %v, want %+v", ticks, drops, want)
	}
}

func TestDecodeAfterHoursSessionLabel(t *testing.T) {
	frame := strings.Replace(readFrame(t, "h0stcnt0.frame"), "^131502^", "^171502^", 1)
	ticks, _ := testDecoder().Decode(eventFromFrame(frame))
	if len(ticks) != 1 || ticks[0].Session != SessionAfterHours || ticks[0].TS != "2026-09-30T17:15:02+09:00" {
		t.Fatalf("after-hours ticks = %+v", ticks)
	}
}

func TestDecodeMalformedAndPartialFrames(t *testing.T) {
	trade := readFrame(t, "h0stcnt0.frame")
	book := readFrame(t, "h0stasp0.frame")
	bookFields := strings.Split(strings.SplitN(book, "|", 4)[3], "^")
	replaceField := func(frame string, index int, value string) string {
		parts := strings.SplitN(frame, "|", 4)
		fields := strings.Split(parts[3], "^")
		fields[index] = value
		return strings.Join(parts[:3], "|") + "|" + strings.Join(fields, "^")
	}
	tests := []struct {
		name  string
		event ws.Event
		want  []DropReason
	}{
		{"trade partial: two fields", eventFromFrame("0|H0STCNT0|001|005930^131502"), []DropReason{DropPartial}},
		{"trade partial: symbol only", eventFromFrame("0|H0STCNT0|001|005930"), []DropReason{DropPartial}},
		{"trade partial: empty payload", eventFromFrame("0|H0STCNT0|001|"), []DropReason{DropPartial}},
		{"book partial: truncated before BIDP_RSQN1", eventFromFrame("0|H0STASP0|001|" + strings.Join(bookFields[:33], "^")), []DropReason{DropPartial}},
		{"book partial: header only", eventFromFrame("0|H0STASP0|001|005930^131502^0"), []DropReason{DropPartial}},
		{"header: missing payload segment", ws.Event{TR: ws.TRQuotePrice, Raw: []byte("0|H0STCNT0|001"), Fields: []string{"005930", "131502", "70500"}, ReceivedAt: receivedAt}, []DropReason{DropMalformed}},
		{"header: count zero", eventFromFrame(strings.Replace(trade, "|001|", "|000|", 1)), []DropReason{DropMalformed}},
		{"header: count not numeric", eventFromFrame(strings.Replace(trade, "|001|", "|0x1|", 1)), []DropReason{DropMalformed}},
		{"header: count exceeds records", eventFromFrame(strings.Replace(trade, "|001|", "|003|", 1)), []DropReason{DropMalformed}},
		{"header: TR differs from event", ws.Event{TR: ws.TRQuoteBook, Raw: []byte(trade), Fields: eventFromFrame(trade).Fields, ReceivedAt: receivedAt}, []DropReason{DropMalformed}},
		{"header: bad encryption flag", eventFromFrame(strings.Replace(trade, "0|", "2|", 1)), []DropReason{DropMalformed}},
		{"missing receipt time", ws.Event{TR: ws.TRQuotePrice, Raw: []byte(trade), Fields: eventFromFrame(trade).Fields}, []DropReason{DropMalformed}},
		{"trade: bad time", eventFromFrame(replaceField(trade, 1, "1315")), []DropReason{DropMalformed}},
		{"trade: hour out of range", eventFromFrame(replaceField(trade, 1, "251502")), []DropReason{DropMalformed}},
		{"trade: negative price", eventFromFrame(replaceField(trade, 2, "-70500")), []DropReason{DropMalformed}},
		{"trade: signed price", eventFromFrame(replaceField(trade, 2, "+70500")), []DropReason{DropMalformed}},
		{"trade: zero price", eventFromFrame(replaceField(trade, 2, "0")), []DropReason{DropMalformed}},
		{"trade: decimal price", eventFromFrame(replaceField(trade, 2, "70500.5")), []DropReason{DropMalformed}},
		{"trade: padded price", eventFromFrame(replaceField(trade, 2, " 70500")), []DropReason{DropMalformed}},
		{"trade: empty price", eventFromFrame(replaceField(trade, 2, "")), []DropReason{DropMalformed}},
		{"trade: lower-case symbol", eventFromFrame(replaceField(trade, 0, "00593a")), []DropReason{DropMalformed}},
		{"trade: short symbol", eventFromFrame(replaceField(trade, 0, "5930")), []DropReason{DropMalformed}},
		{"book: comma quantity", eventFromFrame(replaceField(book, 23, "1,200")), []DropReason{DropMalformed}},
		{"book: empty best bid", eventFromFrame(replaceField(book, 13, "")), []DropReason{DropMalformed}},
		{"book: bad time", eventFromFrame(replaceField(book, 1, "13150x")), []DropReason{DropMalformed}},
		{"unconfigured symbol", eventFromFrame(replaceField(trade, 0, "035420")), []DropReason{DropUnknownSym}},
		{"pre-open time", eventFromFrame(replaceField(trade, 1, "085959")), []DropReason{DropOutOfSession}},
		{"between sessions", eventFromFrame(replaceField(book, 1, "154500")), []DropReason{DropOutOfSession}},
		{"after after-hours", eventFromFrame(replaceField(trade, 1, "200100")), []DropReason{DropOutOfSession}},
		{"execution TR is not a quote", eventFromFrame("0|H0STCNI0|001|HTS^00000000^A1"), []DropReason{DropUnknownTR}},
		{"garbage", ws.Event{TR: "", Raw: []byte("garbage")}, []DropReason{DropUnknownTR}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ticks, drops := testDecoder().Decode(test.event)
			if len(ticks) != 0 {
				t.Fatalf("ticks = %+v, want none", ticks)
			}
			if !reflect.DeepEqual(drops, test.want) {
				t.Fatalf("drops = %v, want %v", drops, test.want)
			}
		})
	}
}

func TestDecodeMultiRecordFrameDropsOnlyTheBadRecord(t *testing.T) {
	frame := readFrame(t, "h0stcnt0_multi.frame")
	parts := strings.SplitN(frame, "|", 4)
	fields := strings.Split(parts[3], "^")
	fields[46+2] = "bad"
	ticks, drops := testDecoder().Decode(eventFromFrame(strings.Join(parts[:3], "|") + "|" + strings.Join(fields, "^")))
	if len(ticks) != 1 || ticks[0].Price != "70500" || !reflect.DeepEqual(drops, []DropReason{DropMalformed}) {
		t.Fatalf("ticks = %+v drops = %v, want first record only", ticks, drops)
	}
}

func TestTickValuesAreExactlyTheStreamFieldSet(t *testing.T) {
	want := []string{"symbol", "ts", "price", "bid1", "ask1", "bid_qty", "ask_qty", "session"}
	if !reflect.DeepEqual(StreamFields, want) {
		t.Fatalf("StreamFields = %v, want %v", StreamFields, want)
	}
	values := Tick{Symbol: "s", TS: "t", Price: "p", Bid1: "b", Ask1: "a", BidQty: "bq", AskQty: "aq", Session: "x"}.values()
	if len(values) != 2*len(want) {
		t.Fatalf("values length = %d, want %d", len(values), 2*len(want))
	}
	for index, name := range want {
		if values[2*index] != name {
			t.Fatalf("field %d = %v, want %s", index, values[2*index], name)
		}
	}
	if got := reflect.TypeFor[Tick]().NumField(); got != len(want) {
		t.Fatalf("Tick has %d fields, want %d", got, len(want))
	}
}
