package kismockedge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
)

func TestAccountScopeRoutesKISAndAlpacaToDifferentBackends(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	kis := &fakeBroker{result: BrokerResult{Accepted: true, BrokerOrderID: "kis-order"}}
	alpaca := &fakeBroker{result: BrokerResult{Accepted: true, BrokerOrderID: "alpaca-order"}}
	service := newTestServiceWithBrokers(store, map[string]Broker{
		executioncontracts.AccountScopeKISMock:           kis,
		executioncontracts.AccountScopeAlpacaPaperCrypto: alpaca,
	}, true)

	kisReceipt, err := service.Process(context.Background(), testCommand("route-kis"))
	if err != nil {
		t.Fatal(err)
	}
	if kisReceipt.BrokerOrderID != "kis-order" {
		t.Fatalf("KIS receipt = %+v", kisReceipt)
	}
	alpacaReceipt, err := service.Process(context.Background(), testAlpacaCommand("route-alpaca"))
	if err != nil {
		t.Fatal(err)
	}
	if alpacaReceipt.BrokerOrderID != "alpaca-order" {
		t.Fatalf("Alpaca receipt = %+v", alpacaReceipt)
	}
	if got := kis.sends.Load(); got != 1 {
		t.Fatalf("KIS sends = %d, want 1", got)
	}
	if got := alpaca.sends.Load(); got != 1 {
		t.Fatalf("Alpaca sends = %d, want 1", got)
	}
}

func TestAlpacaScopeDoesNotLoadKISConfigOrToken(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	transport := &countingTransport{respond: func(*http.Request) (*http.Response, error) {
		return testHTTPResponse(http.StatusOK, `{"id":"alpaca-no-redis","status":"accepted"}`), nil
	}}
	service := NewEnvironmentService(store, func(name string) string {
		switch name {
		case "BROKER_EDGE_MOCK_PLACE_ENABLED":
			return "true"
		case "ALPACA_PAPER_CRYPTO_API_KEY":
			return "alpaca-paper-key-for-test"
		case "ALPACA_PAPER_CRYPTO_API_SECRET":
			return "alpaca-paper-secret-for-test"
		case "KIS_MOCK_APP_KEY", "KIS_MOCK_APP_SECRET", "KIS_MOCK_ACCOUNT_NO", "REDIS_URL":
			t.Fatalf("Alpaca scope loaded KIS-only setting %q", name)
		}
		return ""
	}, transport)
	receipt, err := service.Process(context.Background(), testAlpacaCommand("alpaca-no-kis-dependencies"))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Disposition != executioncontracts.DispositionAccepted || receipt.BrokerOrderID != "alpaca-no-redis" {
		t.Fatalf("receipt = %+v", receipt)
	}
	if got := transport.calls.Load(); got != 1 {
		t.Fatalf("Alpaca transport calls = %d, want 1", got)
	}
}

func TestAlpacaDuplicate100HTTPPostsSendExactlyOnce(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	transport := &countingTransport{respond: func(*http.Request) (*http.Response, error) {
		return testHTTPResponse(http.StatusOK, `{"id":"alpaca-order-1","status":"accepted"}`), nil
	}}
	server := httptest.NewServer(NewHandler(newTestServiceWithBrokers(store, map[string]Broker{
		executioncontracts.AccountScopeAlpacaPaperCrypto: testAlpacaPaperCryptoBroker(transport),
	}, true)))
	defer server.Close()

	command := testAlpacaCommand("alpaca-duplicate-100")
	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	requestErrors := make(chan error, 100)
	var group sync.WaitGroup
	for range 100 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			response, requestErr := http.Post(server.URL+"/v1/commands", "application/json", bytes.NewReader(payload))
			if requestErr != nil {
				requestErrors <- requestErr
				return
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				requestErrors <- &unexpectedStatus{got: response.StatusCode}
				return
			}
			var receipt executioncontracts.ExecutionReceiptV1
			if err := json.NewDecoder(response.Body).Decode(&receipt); err != nil {
				requestErrors <- err
				return
			}
			if receipt.CommandID != command.CommandID || !receipt.Disposition.Valid() {
				requestErrors <- &invalidReceipt{}
			}
		}()
	}
	close(start)
	group.Wait()
	close(requestErrors)
	for err := range requestErrors {
		t.Error(err)
	}
	if got := transport.calls.Load(); got != 1 {
		t.Fatalf("Alpaca POST calls = %d, want exactly 1", got)
	}

	response, err := http.Post(server.URL+"/v1/commands", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var receipt executioncontracts.ExecutionReceiptV1
	if err := json.NewDecoder(response.Body).Decode(&receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Disposition != executioncontracts.DispositionAccepted || receipt.BrokerOrderID != "alpaca-order-1" {
		t.Fatalf("post-final duplicate receipt = %+v", receipt)
	}
	if got := transport.calls.Load(); got != 1 {
		t.Fatalf("Alpaca re-sent after durable receipt: %d calls", got)
	}
}

func TestAlpacaRestartAfterSendBoundaryKeepsUnknownAndNeverResends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "edge.sqlite")
	store := openTestStore(t, path)
	release := make(chan struct{})
	entered := make(chan struct{})
	firstBroker := &fakeBroker{
		result:  BrokerResult{Accepted: true, BrokerOrderID: "should-not-persist"},
		entered: entered,
		release: release,
	}
	firstService := newTestServiceWithBrokers(store, map[string]Broker{
		executioncontracts.AccountScopeAlpacaPaperCrypto: firstBroker,
	}, true)
	command := testAlpacaCommand("alpaca-killed-after-send")
	finished := make(chan struct{})
	go func() {
		_, _ = firstService.Process(context.Background(), command)
		close(finished)
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("Alpaca Send did not begin")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	restartedStore, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer restartedStore.Close()
	secondBroker := &fakeBroker{result: BrokerResult{Accepted: true, BrokerOrderID: "must-not-send"}}
	restartedService := newTestServiceWithBrokers(restartedStore, map[string]Broker{
		executioncontracts.AccountScopeAlpacaPaperCrypto: secondBroker,
	}, true)
	receipt, err := restartedService.Process(context.Background(), command)
	if err != nil {
		t.Fatalf("restart process: %v", err)
	}
	if receipt.Disposition != executioncontracts.DispositionUnknown || receipt.ErrorCode != ErrorSendPending {
		t.Fatalf("restart receipt = %+v, want durable pending UNKNOWN", receipt)
	}
	if got := secondBroker.sends.Load(); got != 0 {
		t.Fatalf("restart Alpaca POST calls = %d, want 0", got)
	}
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("simulated killed sender did not finish")
	}
}

func TestAlpacaValidationUsesCryptoRulesWithoutKRXTicks(t *testing.T) {
	command := testAlpacaCommand("alpaca-decimals")
	if code := ValidateCommand(command); code != "" {
		t.Fatalf("valid decimal crypto command rejected: %q", code)
	}
	if code := ValidateOrderCaps(command); code != "" {
		t.Fatalf("valid crypto cap rejected: %q", code)
	}

	tests := []struct {
		name string
		edit func(*executioncontracts.ExecutionCommandV1)
		want string
	}{
		{
			name: "sell is outside smoke contract",
			edit: func(command *executioncontracts.ExecutionCommandV1) { command.Side = "sell" },
			want: ErrorInvalidCommand,
		},
		{
			name: "symbol is allowlisted",
			edit: func(command *executioncontracts.ExecutionCommandV1) { command.StockCode = "ETH/USD" },
			want: ErrorInvalidCommand,
		},
		{
			name: "fraction needs both sides of decimal point",
			edit: func(command *executioncontracts.ExecutionCommandV1) { command.Quantity = ".5" },
			want: ErrorInvalidCommand,
		},
		{
			name: "zero cannot be accepted",
			edit: func(command *executioncontracts.ExecutionCommandV1) { command.Price = "0.000" },
			want: ErrorInvalidCommand,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := command
			test.edit(&candidate)
			if code := ValidateCommand(candidate); code != test.want {
				t.Fatalf("ValidateCommand = %q, want %q", code, test.want)
			}
		})
	}

	command = testAlpacaCommand("alpaca-exact-cap")
	command.Quantity = "0.25"
	command.Price = "40.0"
	if code := ValidateOrderCaps(command); code != "" {
		t.Fatalf("exact $10 cap rejected: %q", code)
	}
	command.Price = "40.004"
	if code := ValidateOrderCaps(command); code != ErrorOrderLimitExceeded {
		t.Fatalf("over $10 cap code = %q", code)
	}
}

func TestAlpacaBrokerUsesPaperPinAndPreservesDecimalStrings(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	transport := &countingTransport{respond: func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || request.URL.Scheme != "https" || request.URL.Host != AlpacaPaperCryptoHost {
			t.Fatalf("unexpected Alpaca request %s %s", request.Method, request.URL)
		}
		if request.URL.Path != AlpacaPaperCryptoOrderPath {
			t.Fatalf("unexpected Alpaca order route: %s", request.URL.Path)
		}
		if request.Header.Get("APCA-API-KEY-ID") != "alpaca-paper-key-for-test" ||
			request.Header.Get("APCA-API-SECRET-KEY") != "alpaca-paper-secret-for-test" {
			t.Fatal("paper API authentication headers were not set")
		}
		var body map[string]string
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		want := map[string]string{
			"symbol":        "BTC/USD",
			"qty":           "0.5",
			"side":          "buy",
			"type":          "limit",
			"limit_price":   "10.01",
			"time_in_force": "gtc",
		}
		for key, value := range want {
			if body[key] != value {
				t.Fatalf("body[%q] = %q, want %q", key, body[key], value)
			}
		}
		return testHTTPResponse(http.StatusOK, `{"id":"alpaca-42","status":"accepted"}`), nil
	}}
	service := newTestServiceWithBrokers(store, map[string]Broker{
		executioncontracts.AccountScopeAlpacaPaperCrypto: testAlpacaPaperCryptoBroker(transport),
	}, true)
	receipt, err := service.Process(context.Background(), testAlpacaCommand("alpaca-original-decimals"))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Disposition != executioncontracts.DispositionAccepted || receipt.BrokerOrderID != "alpaca-42" {
		t.Fatalf("receipt = %+v", receipt)
	}
}

func TestAlpacaTransportRevalidatesPinnedHostImmediatelyBeforeSend(t *testing.T) {
	transport := &countingTransport{respond: func(*http.Request) (*http.Response, error) {
		return nil, errors.New("mutated request must not reach transport")
	}}
	prepared, code := testAlpacaPaperCryptoBroker(transport).Prepare(context.Background(), testAlpacaCommand("alpaca-final-pin"))
	if code != "" || prepared == nil {
		t.Fatalf("prepare = %v, code = %q", prepared, code)
	}
	request, ok := prepared.(*preparedAlpacaPaperCryptoBroker)
	if !ok {
		t.Fatalf("prepared type = %T", prepared)
	}
	request.request.URL.Host = AlpacaLiveTradingHost
	result := request.Send(context.Background())
	if result.Accepted || result.ErrorCode != ErrorBrokerUnknown {
		t.Fatalf("Send result = %+v", result)
	}
	if got := transport.calls.Load(); got != 0 {
		t.Fatalf("mutated live request reached transport %d times", got)
	}
}

func TestAlpacaRejectsCapsSymbolsAndLiveHostBeforeTransport(t *testing.T) {
	tests := []struct {
		name    string
		command func() executioncontracts.ExecutionCommandV1
		broker  func(*countingTransport) Broker
		want    string
	}{
		{
			name: "notional cap",
			command: func() executioncontracts.ExecutionCommandV1 {
				command := testAlpacaCommand("alpaca-over-cap")
				command.Quantity = "1"
				command.Price = "10.01"
				return command
			},
			broker: func(transport *countingTransport) Broker { return testAlpacaPaperCryptoBroker(transport) },
			want:   ErrorOrderLimitExceeded,
		},
		{
			name: "unknown symbol",
			command: func() executioncontracts.ExecutionCommandV1 {
				command := testAlpacaCommand("alpaca-unknown-symbol")
				command.StockCode = "ETH/USD"
				return command
			},
			broker: func(transport *countingTransport) Broker { return testAlpacaPaperCryptoBroker(transport) },
			want:   ErrorInvalidCommand,
		},
		{
			name: "live host",
			command: func() executioncontracts.ExecutionCommandV1 {
				return testAlpacaCommand("alpaca-live-host")
			},
			broker: func(transport *countingTransport) Broker {
				broker := testAlpacaPaperCryptoBroker(transport)
				broker.LoadConfig = func() (AlpacaPaperCryptoConfig, string) {
					return AlpacaPaperCryptoConfig{
						BaseURL:   AlpacaLiveTradingBaseURL,
						APIKey:    "alpaca-paper-key-for-test",
						APISecret: "alpaca-paper-secret-for-test",
						Timeout:   time.Second,
					}, ""
				}
				return broker
			},
			want: ErrorInvalidCommand,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := &countingTransport{respond: func(*http.Request) (*http.Response, error) {
				return nil, errors.New("must not send")
			}}
			store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
			service := newTestServiceWithBrokers(store, map[string]Broker{
				executioncontracts.AccountScopeAlpacaPaperCrypto: test.broker(transport),
			}, true)
			receipt, err := service.Process(context.Background(), test.command())
			if err != nil {
				t.Fatal(err)
			}
			if receipt.Disposition != executioncontracts.DispositionNotCreated || receipt.ErrorCode != test.want {
				t.Fatalf("receipt = %+v, want NOT_CREATED/%s", receipt, test.want)
			}
			if got := transport.calls.Load(); got != 0 {
				t.Fatalf("rejected command reached transport %d times", got)
			}
		})
	}
}

func TestAlpaca5xxTimeoutAndRedirectRemainUnknown(t *testing.T) {
	tests := []struct {
		name      string
		respond   func(*http.Request) (*http.Response, error)
		wantError string
	}{
		{
			name: "5xx",
			respond: func(*http.Request) (*http.Response, error) {
				return testHTTPResponse(http.StatusBadGateway, `{"message":"unavailable"}`), nil
			},
			wantError: ErrorBroker5xx,
		},
		{
			name: "timeout",
			respond: func(*http.Request) (*http.Response, error) {
				return nil, timeoutError{}
			},
			wantError: ErrorBrokerTimeout,
		},
		{
			name: "redirect",
			respond: func(*http.Request) (*http.Response, error) {
				response := testHTTPResponse(http.StatusFound, "")
				response.Header.Set("Location", AlpacaLiveTradingBaseURL+AlpacaPaperCryptoOrderPath)
				return response, nil
			},
			wantError: ErrorBrokerUnknown,
		},
		{
			name: "missing-status",
			respond: func(*http.Request) (*http.Response, error) {
				return testHTTPResponse(http.StatusOK, `{"id":"alpaca-no-status"}`), nil
			},
			wantError: ErrorBrokerUnknown,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := &countingTransport{respond: test.respond}
			store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
			service := newTestServiceWithBrokers(store, map[string]Broker{
				executioncontracts.AccountScopeAlpacaPaperCrypto: testAlpacaPaperCryptoBroker(transport),
			}, true)
			receipt, err := service.Process(context.Background(), testAlpacaCommand("alpaca-unknown-"+test.name))
			if err != nil {
				t.Fatal(err)
			}
			if receipt.Disposition != executioncontracts.DispositionUnknown || receipt.ErrorCode != test.wantError {
				t.Fatalf("receipt = %+v, want UNKNOWN/%s", receipt, test.wantError)
			}
			if got := transport.calls.Load(); got != 1 {
				t.Fatalf("Alpaca transport calls = %d, want 1", got)
			}
		})
	}
}

func TestAlpacaSecretValuesNeverEnterReceipts(t *testing.T) {
	const secret = "alpaca-paper-secret-redaction-test"
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	broker := testAlpacaPaperCryptoBroker(nil)
	broker.LoadConfig = func() (AlpacaPaperCryptoConfig, string) {
		return AlpacaPaperCryptoConfig{
			BaseURL:   AlpacaPaperCryptoBaseURL,
			APIKey:    "alpaca-paper-key-for-redaction-test",
			APISecret: "bad\r\n" + secret,
			Timeout:   time.Second,
		}, ""
	}
	service := newTestServiceWithBrokers(store, map[string]Broker{
		executioncontracts.AccountScopeAlpacaPaperCrypto: broker,
	}, true)
	receipt, err := service.Process(context.Background(), testAlpacaCommand("alpaca-secret-redaction"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), secret) {
		t.Fatal("receipt exposed Alpaca secret value")
	}
	if receipt.Disposition != executioncontracts.DispositionNotCreated || receipt.ErrorCode != ErrorInvalidCommand {
		t.Fatalf("receipt = %+v", receipt)
	}
}
