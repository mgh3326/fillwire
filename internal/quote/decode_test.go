package quote

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return []byte(strings.TrimSpace(string(raw)))
}

var testSymbols = []Symbol{{MarketKR, "005930"}, {MarketKR, "000660"}, {MarketUS, "AAPL"}}

func testDecoder() *Decoder { return NewDecoder(testSymbols) }

func TestDecodeTradeFrames(t *testing.T) {
	for name, want := range map[string]Tick{
		"trade_kr.json": {Symbol: "005930", TS: "2026-09-30T13:15:02.123+09:00", Price: "72000", Session: SessionKRXRegular},
		// asyncapi example: 23:30 KST on 18 June is 10:30 EDT.
		"trade_us_asyncapi_example.json": {Symbol: "AAPL", TS: "2026-06-18T23:30:00.000+09:00", Price: "243.26", Session: SessionUSRegular},
	} {
		frame := testDecoder().Decode(fixture(t, name))
		if frame.Kind != FrameMessage || frame.Drop != "" || !reflect.DeepEqual(frame.Tick, want) {
			t.Fatalf("%s: frame = %+v, want tick %+v", name, frame, want)
		}
	}
}

func TestDecodeOrderbookFrames(t *testing.T) {
	for name, want := range map[string]Tick{
		"orderbook_kr.json": {Symbol: "005930", TS: "2026-09-30T13:15:02.456+09:00", Bid1: "72000", Ask1: "72100", BidQty: "12000", AskQty: "8500", Session: SessionKRXRegular},
		"orderbook_us.json": {Symbol: "AAPL", TS: "2026-09-30T23:30:00.000+09:00", Bid1: "243.25", Ask1: "243.30", BidQty: "150", AskQty: "200", Session: SessionUSRegular},
	} {
		frame := testDecoder().Decode(fixture(t, name))
		if frame.Kind != FrameMessage || frame.Drop != "" || !reflect.DeepEqual(frame.Tick, want) {
			t.Fatalf("%s: frame = %+v, want tick %+v", name, frame, want)
		}
	}
	// The asyncapi KR orderbook example is stamped 23:30 KST: no KR session.
	if frame := testDecoder().Decode(fixture(t, "orderbook_kr_asyncapi_example.json")); frame.Drop != DropOutOfSession {
		t.Fatalf("23:30 KST orderbook = %+v, want out_of_session", frame)
	}
}

func TestDecodeOrderbookEmptySideKeepsOtherSide(t *testing.T) {
	raw := `{"type":"message","topic":"orderbook:kr:005930","data":{"timestamp":"2026-09-30T13:15:02.000+09:00","currency":"KRW","asks":[],"bids":[{"price":"72000","volume":"5"}]}}`
	frame := testDecoder().Decode([]byte(raw))
	want := Tick{Symbol: "005930", TS: "2026-09-30T13:15:02.000+09:00", Bid1: "72000", BidQty: "5", Session: SessionKRXRegular}
	if frame.Drop != "" || !reflect.DeepEqual(frame.Tick, want) {
		t.Fatalf("frame = %+v, want %+v", frame, want)
	}
}

func TestDecodeControlFrames(t *testing.T) {
	ack := testDecoder().Decode(fixture(t, "subscriptions_ack_asyncapi_example.json"))
	if ack.Kind != FrameSubscriptions || len(ack.Subscribed) != 4 || len(ack.Rejected) != 0 {
		t.Fatalf("ack = %+v", ack)
	}
	partial := testDecoder().Decode(fixture(t, "subscriptions_partial_reject_asyncapi_example.json"))
	if partial.Kind != FrameSubscriptions || len(partial.Subscribed) != 1 || !reflect.DeepEqual(partial.Rejected, []Rejection{{Target: "trade:kr:999999", Code: "stock-not-found"}}) {
		t.Fatalf("partial ack = %+v", partial)
	}
	for name, code := range map[string]string{"error_rate_limit.json": "rate-limit-exceeded", "error_server_shutdown.json": "server-shutdown"} {
		if frame := testDecoder().Decode(fixture(t, name)); frame.Kind != FrameError || frame.ErrorCode != code {
			t.Fatalf("%s = %+v", name, frame)
		}
	}
	if frame := testDecoder().Decode([]byte(`{"type":"pong"}`)); frame.Kind != FramePong {
		t.Fatalf("pong = %+v", frame)
	}
}

func TestDecodeMalformedAndPartialFrames(t *testing.T) {
	trade := func(data string) string {
		return `{"type":"message","topic":"trade:kr:005930","data":` + data + `}`
	}
	book := func(data string) string {
		return `{"type":"message","topic":"orderbook:kr:005930","data":` + data + `}`
	}
	const ts = `"timestamp":"2026-09-30T13:15:02.000+09:00"`
	tests := []struct {
		name string
		raw  string
		kind FrameKind
		drop DropReason
	}{
		{"not json", `garbage`, FrameUnknown, ""},
		{"truncated json", `{"type":"message","topic":"trade:kr:0059`, FrameUnknown, ""},
		{"json array", `[1,2]`, FrameUnknown, ""},
		{"no type", `{"topic":"trade:kr:005930"}`, FrameUnknown, ""},
		{"unknown type", `{"type":"surprise"}`, FrameUnknown, ""},
		{"message without topic", `{"type":"message","data":{}}`, FrameMessage, DropUnknownTopic},
		{"personal order topic", `{"type":"message","topic":"personal:order:3","data":{}}`, FrameMessage, DropUnknownTopic},
		{"unknown channel", `{"type":"message","topic":"candle:kr:005930","data":{}}`, FrameMessage, DropUnknownTopic},
		{"unknown market", `{"type":"message","topic":"trade:jp:7203","data":{}}`, FrameMessage, DropUnknownTopic},
		{"topic extra part", `{"type":"message","topic":"trade:kr:005930:x","data":{}}`, FrameMessage, DropUnknownTopic},
		{"lower-case ticker", `{"type":"message","topic":"trade:us:aapl","data":{}}`, FrameMessage, DropUnknownTopic},
		{"unconfigured symbol", `{"type":"message","topic":"trade:kr:035420","data":{"price":"1",` + ts + `}}`, FrameMessage, DropUnknownSym},
		{"market mismatch", `{"type":"message","topic":"trade:us:005930","data":{}}`, FrameMessage, DropUnknownTopic},
		{"no data", `{"type":"message","topic":"trade:kr:005930"}`, FrameMessage, DropMalformed},
		{"null data", trade(`null`), FrameMessage, DropMalformed},
		{"data not object", trade(`"x"`), FrameMessage, DropMalformed},
		{"trade no price", trade(`{` + ts + `}`), FrameMessage, DropMalformed},
		{"trade numeric price", trade(`{"price":72000,` + ts + `}`), FrameMessage, DropMalformed},
		{"trade negative price", trade(`{"price":"-72000",` + ts + `}`), FrameMessage, DropMalformed},
		{"trade signed price", trade(`{"price":"+72000",` + ts + `}`), FrameMessage, DropMalformed},
		{"trade zero price", trade(`{"price":"0.00",` + ts + `}`), FrameMessage, DropMalformed},
		{"trade exponent price", trade(`{"price":"7.2e4",` + ts + `}`), FrameMessage, DropMalformed},
		{"trade comma price", trade(`{"price":"72,000",` + ts + `}`), FrameMessage, DropMalformed},
		{"trade trailing dot", trade(`{"price":"72000.",` + ts + `}`), FrameMessage, DropMalformed},
		{"trade price too long", trade(`{"price":"` + strings.Repeat("9", 31) + `",` + ts + `}`), FrameMessage, DropMalformed},
		{"trade no timestamp", trade(`{"price":"72000"}`), FrameMessage, DropMalformed},
		{"trade null timestamp", trade(`{"price":"72000","timestamp":null}`), FrameMessage, DropMalformed},
		{"trade bad timestamp", trade(`{"price":"72000","timestamp":"13:15:02"}`), FrameMessage, DropMalformed},
		{"trade timestamp no zone", trade(`{"price":"72000","timestamp":"2026-09-30T13:15:02"}`), FrameMessage, DropMalformed},
		{"book null timestamp", book(`{"timestamp":null,"asks":[],"bids":[]}`), FrameMessage, DropNoTimestamp},
		{"book no timestamp", book(`{"asks":[],"bids":[]}`), FrameMessage, DropNoTimestamp},
		{"book asks not array", book(`{` + ts + `,"asks":{},"bids":[]}`), FrameMessage, DropMalformed},
		{"book level without volume", book(`{` + ts + `,"asks":[{"price":"1"}],"bids":[]}`), FrameMessage, DropMalformed},
		{"book level bad price", book(`{` + ts + `,"asks":[],"bids":[{"price":"x","volume":"1"}]}`), FrameMessage, DropMalformed},
		{"pre-market KR 07:59", trade(`{"price":"72000","timestamp":"2026-09-30T07:59:59.000+09:00"}`), FrameMessage, DropOutOfSession},
		{"KR gap 15:40", trade(`{"price":"72000","timestamp":"2026-09-30T15:40:00.000+09:00"}`), FrameMessage, DropOutOfSession},
		{"KR saturday", trade(`{"price":"72000","timestamp":"2026-10-03T10:00:00.000+09:00"}`), FrameMessage, DropOutOfSession},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frame := testDecoder().Decode([]byte(test.raw))
			if frame.Kind != test.kind || frame.Drop != test.drop || frame.Tick != (Tick{}) {
				t.Fatalf("frame = %+v, want kind %d drop %q and no tick", frame, test.kind, test.drop)
			}
		})
	}
}

func TestDecodeNeverPanicsOnMutatedFrames(t *testing.T) {
	seeds := [][]byte{fixture(t, "trade_kr.json"), fixture(t, "orderbook_kr.json"), fixture(t, "orderbook_us.json"), fixture(t, "subscriptions_partial_reject_asyncapi_example.json")}
	decoder := testDecoder()
	for _, seed := range seeds {
		for cut := 0; cut <= len(seed); cut++ {
			frame := decoder.Decode(seed[:cut])
			assertTickShape(t, frame)
		}
		for index := range seed {
			for _, b := range []byte{'"', '{', '}', '0', '-', ':', 0} {
				mutated := append([]byte(nil), seed...)
				mutated[index] = b
				assertTickShape(t, decoder.Decode(mutated))
			}
		}
	}
}

func assertTickShape(t *testing.T, frame Frame) {
	t.Helper()
	if frame.Kind == FrameMessage && frame.Drop == "" {
		if frame.Tick.Symbol == "" || frame.Tick.TS == "" || frame.Tick.Session == "" {
			t.Fatalf("emitted tick missing a required field: %+v", frame.Tick)
		}
		if (frame.Tick.Price == "") == (frame.Tick.Bid1 == "" && frame.Tick.Ask1 == "" && frame.Tick.BidQty == "" && frame.Tick.AskQty == "") {
			t.Fatalf("tick mixes or lacks trade and book values: %+v", frame.Tick)
		}
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
