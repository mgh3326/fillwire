package binancedemo

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestValidateURLOnlyAllowsSpotDemoEndpoints(t *testing.T) {
	allowed := []struct{ kind, raw string }{
		{"market", "wss://demo-stream.binance.com/stream?streams=btcusdt@depth20"},
		{"market", "wss://demo-stream.binance.com:9443/ws"},
		{"user", "wss://demo-ws-api.binance.com:443/ws-api/v3"},
	}
	for _, tc := range allowed {
		if err := ValidateURL(tc.raw, tc.kind); err != nil {
			t.Errorf("ValidateURL(%q, %q): %v", tc.raw, tc.kind, err)
		}
	}
	denied := []struct{ kind, raw string }{
		{"market", "wss://stream.binance.com/stream"},
		{"market", "wss://stream.testnet.binance.vision/stream"},
		{"market", "wss://demo-fstream.binance.com/stream"},
		{"market", "wss://evil.demo-stream.binance.com/stream"},
		{"market", "ws://demo-stream.binance.com/stream"},
		{"market", "wss://demo-stream.binance.com:8443/stream"},
		{"market", "wss://user@demo-stream.binance.com/stream"},
		{"market", "wss://demo-stream.binance.com/other"},
		{"user", "wss://demo-ws-api.binance.com/stream"},
	}
	for _, tc := range denied {
		if err := ValidateURL(tc.raw, tc.kind); err == nil {
			t.Errorf("ValidateURL(%q, %q) unexpectedly succeeded", tc.raw, tc.kind)
		}
	}
}

func TestNewRejectsNonDemoHostBeforeDial(t *testing.T) {
	var calls atomic.Int64
	_, err := New(Config{
		Symbols: []string{"BTCUSDT"}, RecordingRoot: t.TempDir(),
		MarketURL: "wss://stream.binance.com/stream",
		Dial: func(context.Context, string, string) (Socket, error) {
			calls.Add(1)
			return nil, errors.New("unexpected dial")
		},
	})
	if err == nil {
		t.Fatal("New accepted a non-demo market host")
	}
	if calls.Load() != 0 {
		t.Fatalf("dial called %d times before host refusal", calls.Load())
	}
}

func TestDocumentedFramesValidate(t *testing.T) {
	for _, name := range []string{"depth20.json", "bookTicker.json", "aggTrade.json"} {
		raw := fixture(t, name)
		if _, _, err := validateMarketFrame(raw, []string{"BTCUSDT"}); err != nil {
			t.Errorf("market fixture %s rejected: %v", name, err)
		}
	}
	for _, name := range []string{"executionReport.json", "outboundAccountPosition.json", "balanceUpdate.json"} {
		if _, err := validateUserFrame(fixture(t, name)); err != nil {
			t.Errorf("user fixture %s rejected: %v", name, err)
		}
	}
	if _, _, err := validateMarketFrame([]byte(`{"stream":"btcusdt@aggTrade","data":{}}`), []string{"BTCUSDT"}); err == nil {
		t.Fatal("malformed market frame was accepted")
	}
}

func TestRecordsReceivedAtAndSubscriptionRejectionLeavesMarketRunning(t *testing.T) {
	marketFrame := fixture(t, "bookTicker.json")
	var mu sync.Mutex
	var records []Record
	appendRecord := func(record Record) error {
		mu.Lock()
		defer mu.Unlock()
		records = append(records, record)
		return nil
	}
	clock := time.Date(2026, 11, 26, 8, 7, 6, 123000000, time.UTC)
	var marketDialed atomic.Bool
	var userDialed atomic.Bool
	r, err := New(Config{
		Symbols: []string{"BTCUSDT"}, EnableUserStream: true, APIKey: "test-key", APISecret: "test-secret",
		Append: appendRecord, Now: func() time.Time { return clock },
		Sleep: func(ctx context.Context, _ time.Duration) error { <-ctx.Done(); return ctx.Err() },
		Dial: func(_ context.Context, endpoint, kind string) (Socket, error) {
			switch kind {
			case "market":
				marketDialed.Store(true)
				return &fakeSocket{reads: [][]byte{marketFrame}}, nil
			case "user":
				userDialed.Store(true)
				return &rejectSocket{}, nil
			default:
				return nil, errors.New("unexpected stream")
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if marketDialed.Load() && userDialed.Load() && r.Health().MarketFrames == 1 && r.Health().SubscribeRejects == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	health := r.Health()
	if !marketDialed.Load() || !userDialed.Load() || health.MarketFrames != 1 || health.SubscribeRejects != 1 {
		cancel()
		<-done
		t.Fatalf("market/user did not make progress after rejection: health=%+v", health)
	}
	if !health.MarketConnected {
		cancel()
		<-done
		t.Fatalf("market stream stopped after user rejection: %+v", health)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context cancellation", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(records) != 2 {
		t.Fatalf("recorded %d events, want market and rejection: %+v", len(records), records)
	}
	var marketRecord, rejectedRecord *Record
	for i := range records {
		switch records[i].Kind {
		case "market_frame":
			marketRecord = &records[i]
		case "subscribe_rejected":
			rejectedRecord = &records[i]
		}
	}
	if marketRecord == nil || !marketRecord.ReceivedAt.Equal(clock) {
		t.Errorf("market record missing receive time: %+v", records)
	}
	if rejectedRecord == nil || rejectedRecord.Status != http.StatusForbidden || rejectedRecord.Code != -2015 || !rejectedRecord.ReceivedAt.Equal(clock) {
		t.Errorf("subscription rejection not recorded safely: %+v", records)
	}
	encoded, _ := json.Marshal(records)
	if strings.Contains(string(encoded), "test-secret") || strings.Contains(string(encoded), "test-key") {
		t.Fatal("credentials appeared in recordings")
	}
}

func TestSuccessfulUserSubscriptionRecordsEventWithReceivedAt(t *testing.T) {
	var mu sync.Mutex
	var records []Record
	clock := time.Date(2026, 11, 26, 8, 9, 10, 0, time.UTC)
	socket := &successfulUserSocket{event: fixture(t, "executionReport.json")}
	r, err := New(Config{
		Symbols: []string{"BTCUSDT"}, EnableUserStream: true, APIKey: "test-key", APISecret: "test-secret",
		Now: func() time.Time { return clock },
		Append: func(record Record) error {
			mu.Lock()
			defer mu.Unlock()
			records = append(records, record)
			return nil
		},
		Dial: func(context.Context, string, string) (Socket, error) { return socket, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.session(ctx, userStream) }()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && r.Health().UserEvents == 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("session returned %v after cancellation", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(records) != 1 {
		t.Fatalf("recorded %d user events, want one: %+v", len(records), records)
	}
	got := records[0]
	if got.Kind != "user_event" || got.EventType != "executionReport" || !got.ReceivedAt.Equal(clock) || len(got.Payload) == 0 {
		t.Fatalf("successful user stream record = %+v", got)
	}
}

func TestRecordingFailureStopsRunAndCountsOnce(t *testing.T) {
	marketFrame := fixture(t, "bookTicker.json")
	r, err := New(Config{
		Symbols: []string{"BTCUSDT"}, EnableUserStream: false,
		Append: func(Record) error { return errors.New("disk full") },
		Dial: func(context.Context, string, string) (Socket, error) {
			return &fakeSocket{reads: [][]byte{marketFrame}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	select {
	case err := <-done:
		if err == nil || errors.Is(err, context.Canceled) {
			t.Fatalf("record append failure returned %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		t.Fatal("reader did not stop promptly after failed append")
	}
	if got := r.Health().RecordErrors; got != 1 {
		t.Fatalf("record errors = %d, want one", got)
	}
}

func TestSessionDialUsesBoundedContext(t *testing.T) {
	var hasDeadline atomic.Bool
	r, err := New(Config{
		Symbols: []string{"BTCUSDT"}, EnableUserStream: false, Append: func(Record) error { return nil },
		Dial: func(ctx context.Context, _, _ string) (Socket, error) {
			_, ok := ctx.Deadline()
			hasDeadline.Store(ok)
			return nil, errors.New("stop after observing dial context")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = r.session(context.Background(), marketStream)
	if !hasDeadline.Load() {
		t.Fatal("dialer received a context without a deadline")
	}
}

func TestFailedPingCancelsBlockedReadAndReconnects(t *testing.T) {
	var pings atomic.Int64
	r, err := New(Config{
		Symbols: []string{"BTCUSDT"}, EnableUserStream: false, Append: func(Record) error { return nil },
		Sleep: func(ctx context.Context, _ time.Duration) error {
			timer := time.NewTimer(time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		},
		Dial: func(context.Context, string, string) (Socket, error) {
			return &pingFailSocket{pings: &pings}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) && (pings.Load() == 0 || r.Health().Reconnects == 0) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v after cancellation", err)
	}
	if pings.Load() == 0 || r.Health().Reconnects == 0 {
		t.Fatalf("failed ping did not reconnect: pings=%d health=%+v", pings.Load(), r.Health())
	}
}

func TestRunReturnsParentCancellationWhenSupervisorStops(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(oldProcs) })
	for attempt := 0; attempt < 12; attempt++ {
		parent, cancel := context.WithCancel(context.Background())
		r, err := New(Config{
			Symbols: []string{"BTCUSDT"}, EnableUserStream: false, Logger: slog.New(slog.DiscardHandler),
			Append: func(Record) error { return nil },
			Dial:   func(context.Context, string, string) (Socket, error) { return nil, errors.New("simulated disconnect") },
			Sleep: func(context.Context, time.Duration) error {
				cancel()
				return errors.New("supervisor stopped")
			},
		})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if err := r.Run(parent); !errors.Is(err, context.Canceled) {
			cancel()
			t.Fatalf("attempt %d: Run returned %v after parent cancellation", attempt, err)
		}
		cancel()
	}
}

func TestCredentialsUseOnlyDocumentedNamesAndFallback(t *testing.T) {
	key, secret, ok := Credentials(map[string]string{APIKeyEnv: "primary", APISecretEnv: "secret"})
	if !ok || key != "primary" || secret != "secret" {
		t.Fatalf("primary credentials = %q, %q, %v", key, secret, ok)
	}
	key, secret, ok = Credentials(map[string]string{FallbackKeyEnv: "fallback", FallbackSecret: "fallback-secret"})
	if !ok || key != "fallback" || secret != "fallback-secret" {
		t.Fatalf("fallback credentials = %q, %q, %v", key, secret, ok)
	}
	key, secret, ok = Credentials(map[string]string{
		APIKeyEnv: "spot-key", FallbackKeyEnv: "shared-key", FallbackSecret: "shared-secret",
	})
	if ok || key != "spot-key" || secret != "" {
		t.Fatalf("incomplete product override was mixed with fallback: %q, %q, %v", key, secret, ok)
	}
}

func TestSignUsesSortedWebSocketAPIParams(t *testing.T) {
	got := Sign(map[string]any{"timestamp": 1, "apiKey": "k", "recvWindow": 5000}, "s")
	if got != "573bf36323a35879b9269be94bfe50622b48ff4baf10ce6c1b3a62e852d35ad8" {
		t.Fatalf("signature = %s", got)
	}
}

func TestRecordingSinkWritesDailyRecord(t *testing.T) {
	root := t.TempDir()
	r, err := New(Config{Symbols: []string{"BTCUSDT"}, RecordingRoot: root, EnableUserStream: false})
	if err != nil {
		t.Fatal(err)
	}
	r.handleMarket(fixture(t, "bookTicker.json"))
	if err := r.sink.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "binance-demo", "2026-11-26", "BTCUSDT.jsonl")
	if _, err := os.Stat(path); err == nil {
		// The default clock is current host time. Verify the file without
		// assuming the test host's UTC day.
		return
	}
	matches, err := filepath.Glob(filepath.Join(root, "binance-demo", "*", "BTCUSDT.jsonl"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("recording file not found: matches=%v err=%v", matches, err)
	}
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

type fakeSocket struct {
	mu    sync.Mutex
	reads [][]byte
}

func (s *fakeSocket) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	s.mu.Lock()
	if len(s.reads) > 0 {
		data := s.reads[0]
		s.reads = s.reads[1:]
		s.mu.Unlock()
		return websocket.MessageText, data, nil
	}
	s.mu.Unlock()
	<-ctx.Done()
	return websocket.MessageText, nil, ctx.Err()
}
func (*fakeSocket) Write(context.Context, websocket.MessageType, []byte) error { return nil }
func (*fakeSocket) Ping(context.Context) error                                 { return nil }
func (*fakeSocket) CloseNow() error                                            { return nil }

type rejectSocket struct{ fakeSocket }

type successfulUserSocket struct {
	fakeSocket
	event []byte
}

func (s *successfulUserSocket) Write(_ context.Context, _ websocket.MessageType, payload []byte) error {
	var req struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return err
	}
	s.reads = [][]byte{
		[]byte(`{"id":"` + req.ID + `","status":200,"result":{"subscriptionId":0}}`),
		s.event,
	}
	return nil
}

type pingFailSocket struct {
	fakeSocket
	pings *atomic.Int64
}

func (s *pingFailSocket) Ping(context.Context) error {
	s.pings.Add(1)
	return errors.New("simulated half-open connection")
}

func (s *rejectSocket) Write(_ context.Context, _ websocket.MessageType, payload []byte) error {
	var req struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(payload, &req); err != nil {
		return err
	}
	s.reads = [][]byte{[]byte(`{"id":"` + req.ID + `","status":403,"error":{"code":-2015,"msg":"redacted"}}`)}
	return nil
}
