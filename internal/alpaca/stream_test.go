package alpaca

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type readResult struct {
	kind websocket.MessageType
	raw  []byte
	err  error
}
type fakeSocket struct {
	reads  []readResult
	writes [][]byte
}

func (s *fakeSocket) Read(context.Context) (websocket.MessageType, []byte, error) {
	if len(s.reads) == 0 {
		return websocket.MessageText, nil, errors.New("closed")
	}
	r := s.reads[0]
	s.reads = s.reads[1:]
	return r.kind, r.raw, r.err
}
func (s *fakeSocket) Write(_ context.Context, _ websocket.MessageType, raw []byte) error {
	s.writes = append(s.writes, append([]byte(nil), raw...))
	return nil
}
func (*fakeSocket) CloseNow() error { return nil }

func TestLiveTradingEndpointRefusedBeforeDial(t *testing.T) {
	module := newTestModule(t, nil)
	var dials int
	module.cfg.Dial = func(context.Context, string, http.Header) (Socket, error) { dials++; return nil, nil }
	err := module.session(context.Background(), "trade_updates", "wss://api.alpaca.markets/stream", "key", "secret")
	if !errors.Is(err, ErrLiveTradingEndpoint) {
		t.Fatalf("session error = %v, want ErrLiveTradingEndpoint", err)
	}
	if dials != 0 {
		t.Fatalf("dial count = %d, want 0", dials)
	}
}

func TestTradeFillAndMarketFramesAreRecordedWithReceivedAt(t *testing.T) {
	fixtures := func(name string) []byte {
		b, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	at := time.Date(2026, 11, 26, 4, 0, 0, 123, time.FixedZone("KST", 9*60*60))
	trade := &fakeSocket{reads: []readResult{
		{websocket.MessageBinary, []byte(`{"stream":"authorization","data":{"status":"authorized","action":"authenticate"}}`), nil},
		{websocket.MessageBinary, []byte(`{"stream":"listening","data":{"streams":["trade_updates"]}}`), nil},
		{websocket.MessageBinary, []byte(`not-json`), nil},
		{websocket.MessageBinary, fixtures("trade_fill.json"), nil},
		{websocket.MessageBinary, fixtures("trade_partial_fill.json"), nil},
	}}
	market := &fakeSocket{reads: []readResult{
		{websocket.MessageText, []byte(`[{"T":"success","msg":"authenticated"}]`), nil},
		{websocket.MessageText, []byte(`[{"T":"subscription","trades":["AAPL"],"quotes":["AAPL"],"bars":["AAPL"]}]`), nil},
		{websocket.MessageText, fixtures("market_trade.json"), nil},
		{websocket.MessageText, fixtures("market_quote.json"), nil},
		{websocket.MessageText, fixtures("market_bar.json"), nil},
	}}
	module := newTestModule(t, func(_ context.Context, endpoint string, _ http.Header) (Socket, error) {
		if endpoint == PaperTradingEndpoint {
			return trade, nil
		}
		return market, nil
	})
	module.cfg.Now = func() time.Time { return at }
	for _, tc := range []struct {
		stream, endpoint string
		socket           *fakeSocket
	}{
		{"trade_updates", PaperTradingEndpoint, trade},
		{"market_data", IEXEndpoint, market},
	} {
		if err := module.session(context.Background(), tc.stream, tc.endpoint, "key", "secret"); err == nil {
			t.Fatalf("%s session error = nil, want disconnect", tc.stream)
		}
		if tc.stream == "trade_updates" && len(tc.socket.writes) != 2 {
			t.Fatalf("trade writes = %d, want auth and listen", len(tc.socket.writes))
		}
	}
	tradeRecords := readRecords(t, filepath.Join(module.cfg.Root, TradeStream, at.UTC().Format("2006-01-02"), "trade_updates.jsonl"))
	if len(tradeRecords) != 2 || tradeRecords[0].Kind != "fill" || tradeRecords[1].Kind != "partial_fill" {
		t.Fatalf("trade records = %#v", tradeRecords)
	}
	marketRecords := readRecords(t, filepath.Join(module.cfg.Root, MarketStream, at.UTC().Format("2006-01-02"), "market_data.jsonl"))
	if len(marketRecords) != 3 || marketRecords[0].Kind != "trade" || marketRecords[1].Kind != "quote" || marketRecords[2].Kind != "bar" {
		t.Fatalf("market records = %#v", marketRecords)
	}
	if got := module.malformed.Load(); got != 1 {
		t.Fatalf("malformed frames = %d, want 1", got)
	}
	for _, record := range append(tradeRecords, marketRecords...) {
		if !record.ReceivedAt.Equal(at.UTC()) {
			t.Errorf("received_at = %s, want %s", record.ReceivedAt, at.UTC())
		}
	}
}

func newTestModule(t *testing.T, dial Dialer) *Module {
	t.Helper()
	t.Setenv("ALPACA_PAPER_API_KEY", "paper-key-for-test")
	t.Setenv("ALPACA_PAPER_API_SECRET", "paper-secret-for-test")
	t.Setenv("ALPACA_DATA_API_KEY_ID", "data-key-for-test")
	t.Setenv("ALPACA_DATA_API_SECRET_KEY", "data-secret-for-test")
	module, err := New(Config{
		PaperKeyEnv: "ALPACA_PAPER_API_KEY", PaperSecretEnv: "ALPACA_PAPER_API_SECRET",
		DataKeyEnv: "ALPACA_DATA_API_KEY_ID", DataSecretEnv: "ALPACA_DATA_API_SECRET_KEY",
		Symbols: []string{"AAPL"}, Root: t.TempDir(), Dial: dial,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(module.closeWriters)
	return module
}

func readRecords(t *testing.T, path string) []Record {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var records []Record
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var record Record
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	return records
}
