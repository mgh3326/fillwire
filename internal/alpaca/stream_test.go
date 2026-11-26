package alpaca

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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
func (*fakeSocket) Ping(context.Context) error { return nil }
func (*fakeSocket) CloseNow() error            { return nil }

type blockingSocket struct {
	*fakeSocket
	pingErr error
}

func (s *blockingSocket) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	if len(s.reads) != 0 {
		return s.fakeSocket.Read(ctx)
	}
	<-ctx.Done()
	return websocket.MessageText, nil, ctx.Err()
}
func (s *blockingSocket) Ping(context.Context) error { return s.pingErr }

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
		{websocket.MessageText, fixtures("market_batch.json"), nil},
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
	wantKinds := []string{"trade", "quote", "bar", "trade", "quote", "bar", "correction", "cancel_error"}
	if len(marketRecords) != len(wantKinds) {
		t.Fatalf("market records = %#v", marketRecords)
	}
	for i, record := range marketRecords {
		if record.Kind != wantKinds[i] {
			t.Errorf("market record %d kind = %q, want %q", i, record.Kind, wantKinds[i])
		}
	}
	if got := module.malformed.Load(); got != 2 {
		t.Fatalf("malformed frames = %d, want 2", got)
	}
	for _, record := range append(tradeRecords, marketRecords...) {
		if !record.ReceivedAt.Equal(at.UTC()) {
			t.Errorf("received_at = %s, want %s", record.ReceivedAt, at.UTC())
		}
	}
}

func TestReconnectDelayResetsAfterStableConnection(t *testing.T) {
	minDelay, maxDelay := time.Second, 30*time.Second
	if got := nextReconnectDelay(16*time.Second, time.Minute, time.Minute, minDelay, maxDelay); got != minDelay {
		t.Fatalf("stable session retry delay = %s, want %s", got, minDelay)
	}
	if got := nextReconnectDelay(16*time.Second, time.Minute-time.Nanosecond, time.Minute, minDelay, maxDelay); got != maxDelay {
		t.Fatalf("short session retry delay = %s, want %s", got, maxDelay)
	}
}

func TestMixedMarketBatchDoesNotDropValidSiblings(t *testing.T) {
	batch, err := os.ReadFile(filepath.Join("testdata", "market_batch.json"))
	if err != nil {
		t.Fatal(err)
	}
	socket := &fakeSocket{reads: []readResult{
		{websocket.MessageText, []byte(`[{"T":"success","msg":"authenticated"}]`), nil},
		{websocket.MessageText, []byte(`[{"T":"subscription","trades":["AAPL"]}]`), nil},
		{websocket.MessageText, batch, nil},
	}}
	module := newTestModule(t, func(context.Context, string, http.Header) (Socket, error) { return socket, nil })
	_ = module.session(context.Background(), "market_data", IEXEndpoint, "key", "secret")
	path := filepath.Join(module.cfg.Root, MarketStream, module.cfg.Now().UTC().Format("2006-01-02"), "market_data.jsonl")
	records := readRecords(t, path)
	want := []string{"trade", "quote", "bar", "correction", "cancel_error"}
	if len(records) != len(want) {
		t.Fatalf("recorded batch elements = %d, want %d valid siblings", len(records), len(want))
	}
	for i := range want {
		if records[i].Kind != want[i] {
			t.Errorf("record %d kind = %q, want %q", i, records[i].Kind, want[i])
		}
		if records[i].ReceivedAt.IsZero() {
			t.Errorf("record %d has zero received_at", i)
		}
	}
	if got := module.malformed.Load(); got != 1 {
		t.Fatalf("malformed elements = %d, want 1", got)
	}
}

func TestPingFailureClosesSilentSession(t *testing.T) {
	socket := &blockingSocket{fakeSocket: &fakeSocket{reads: []readResult{
		{websocket.MessageText, []byte(`[{"T":"success","msg":"authenticated"}]`), nil},
		{websocket.MessageText, []byte(`[{"T":"subscription","trades":["AAPL"],"quotes":["AAPL"],"bars":["AAPL"]}]`), nil},
	}}, pingErr: errors.New("peer stopped responding")}
	module := newTestModule(t, func(context.Context, string, http.Header) (Socket, error) { return socket, nil })
	module.cfg.PingInterval = time.Millisecond
	module.cfg.PingTimeout = time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	_ = module.session(ctx, "market_data", IEXEndpoint, "key", "secret")
	if elapsed := time.Since(started); elapsed >= 100*time.Millisecond {
		t.Fatalf("silent session took %s to close after ping failure", elapsed)
	}
}

func TestRecordingFailureReturnsCauseAndStopsBothStreams(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, MarketStream), []byte("file blocks stream directory"), 0600); err != nil {
		t.Fatal(err)
	}
	trade := &blockingSocket{fakeSocket: &fakeSocket{reads: []readResult{
		{websocket.MessageBinary, []byte(`{"stream":"authorization","data":{"status":"authorized"}}`), nil},
		{websocket.MessageBinary, []byte(`{"stream":"listening","data":{"streams":["trade_updates"]}}`), nil},
	}}}
	market := &fakeSocket{reads: []readResult{
		{websocket.MessageText, []byte(`[{"T":"success","msg":"authenticated"}]`), nil},
		{websocket.MessageText, []byte(`[{"T":"subscription","trades":["AAPL"]}]`), nil},
		{websocket.MessageText, []byte(`[ {"T":"t","S":"AAPL","p":100,"s":1,"t":"2024-01-01T00:00:00Z"} ]`), nil},
	}}
	module := newTestModuleAt(t, root, func(_ context.Context, endpoint string, _ http.Header) (Socket, error) {
		if endpoint == IEXEndpoint {
			return market, nil
		}
		return trade, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := module.Run(ctx)
	if !errors.Is(err, ErrRecordingAppend) || !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("Run error = %v, want recording append error with ENOTDIR cause", err)
	}
	if module.Health() {
		t.Fatal("module remained healthy after recording failure")
	}
}

func TestDialDoesNotFollowWebSocketRedirect(t *testing.T) {
	var destinationRequests int
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { destinationRequests++ }))
	defer destination.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "ws"+strings.TrimPrefix(destination.URL, "http"), http.StatusFound)
	}))
	defer source.Close()
	_, err := dial(context.Background(), "ws"+strings.TrimPrefix(source.URL, "http"), nil)
	if err == nil {
		t.Fatal("websocket redirect was followed")
	}
	if destinationRequests != 0 {
		t.Fatalf("redirect destination requests = %d, want 0", destinationRequests)
	}
}

func newTestModule(t *testing.T, dial Dialer) *Module {
	t.Helper()
	return newTestModuleAt(t, t.TempDir(), dial)
}

func newTestModuleAt(t *testing.T, root string, dial Dialer) *Module {
	t.Helper()
	t.Setenv("ALPACA_PAPER_API_KEY", "paper-key-for-test")
	t.Setenv("ALPACA_PAPER_API_SECRET", "paper-secret-for-test")
	t.Setenv("ALPACA_DATA_API_KEY_ID", "data-key-for-test")
	t.Setenv("ALPACA_DATA_API_SECRET_KEY", "data-secret-for-test")
	module, err := New(Config{
		PaperKeyEnv: "ALPACA_PAPER_API_KEY", PaperSecretEnv: "ALPACA_PAPER_API_SECRET",
		DataKeyEnv: "ALPACA_DATA_API_KEY_ID", DataSecretEnv: "ALPACA_DATA_API_SECRET_KEY",
		Symbols: []string{"AAPL"}, Root: root, Dial: dial,
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
