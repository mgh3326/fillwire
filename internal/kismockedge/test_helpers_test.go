package kismockedge

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	"github.com/mgh3326/broker-edge/internal/kismockread"
)

func testCommand(commandID string) executioncontracts.ExecutionCommandV1 {
	return executioncontracts.ExecutionCommandV1{
		SchemaVersion: executioncontracts.ExecutionCommandV1SchemaVersion,
		CommandID:     commandID,
		AccountScope:  executioncontracts.AccountScopeKISMock,
		Side:          "buy",
		StockCode:     "005930",
		Quantity:      "1",
		Price:         "70000",
		OrderType:     "limit",
		IssuedAt:      "2026-09-01T12:00:00Z",
	}
}

func testBrokerConfig() kismockread.Config {
	return kismockread.Config{
		BaseURL:   kismockread.MockBaseURL,
		AppKey:    "app-key-for-test",
		AppSecret: "app-secret-for-test",
		AccountNo: "12345678-01",
		RedisURL:  "redis://127.0.0.1:1/0",
		Timeout:   time.Second,
	}
}

func openTestStore(t *testing.T, path string) *Store {
	t.Helper()
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

type fakeTokenLoader struct {
	token string
	code  string
	calls atomic.Int64
}

func (loader *fakeTokenLoader) Load(context.Context, kismockread.Config) (string, string) {
	loader.calls.Add(1)
	return loader.token, loader.code
}

type fakeBroker struct {
	result BrokerResult
	code   string

	prepared atomic.Int64
	sends    atomic.Int64

	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (broker *fakeBroker) Prepare(context.Context, kismockread.Config, executioncontracts.ExecutionCommandV1, string) (PreparedBroker, string) {
	broker.prepared.Add(1)
	if broker.code != "" {
		return nil, broker.code
	}
	return fakePreparedBroker{broker: broker}, ""
}

type fakePreparedBroker struct {
	broker *fakeBroker
}

func (prepared fakePreparedBroker) Send(ctx context.Context) BrokerResult {
	prepared.broker.sends.Add(1)
	if prepared.broker.entered != nil {
		prepared.broker.once.Do(func() { close(prepared.broker.entered) })
	}
	if prepared.broker.release != nil {
		select {
		case <-prepared.broker.release:
		case <-ctx.Done():
			return BrokerResult{ErrorCode: ErrorBrokerUnknown}
		}
	}
	return prepared.broker.result
}

func newTestService(store *Store, broker Broker, enabled bool) *Service {
	return &Service{
		Store:        store,
		PlaceEnabled: enabled,
		LoadConfig: func() (kismockread.Config, string) {
			return testBrokerConfig(), ""
		},
		Tokens: &fakeTokenLoader{token: "cached-token-for-test"},
		Broker: broker,
		Now:    func() time.Time { return time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC) },
	}
}

type countingTransport struct {
	calls   atomic.Int64
	respond func(*http.Request) (*http.Response, error)
}

func (transport *countingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.calls.Add(1)
	if transport.respond == nil {
		return nil, io.ErrUnexpectedEOF
	}
	return transport.respond(request)
}

func testHTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
