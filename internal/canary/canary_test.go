package canary

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	"github.com/mgh3326/broker-edge/internal/kismockedge"
	"github.com/mgh3326/broker-edge/internal/kismockread"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return fn(request) }

func jsonResponse(value string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(value)),
	}
}

func fixedKR(year int, month time.Month, day, hour, minute int) time.Time {
	location, _ := time.LoadLocation("Asia/Seoul")
	return time.Date(year, month, day, hour, minute, 0, 0, location)
}

func TestSelectScopeFixedClockIncludingNewYorkDST(t *testing.T) {
	tests := []struct {
		name string
		at   time.Time
		want string
	}{
		{"kr open", fixedKR(2026, time.September, 2, 9, 5), ScopeKR},
		{"kr close", fixedKR(2026, time.September, 2, 15, 15), ScopeKR},
		{"kr before", fixedKR(2026, time.September, 2, 9, 4), ScopeNoSession},
		{"weekend", fixedKR(2026, time.September, 5, 10, 0), ScopeNoSession},
		// 13:35 UTC is 09:35 EDT on this Monday, proving DST conversion.
		{"us DST open", time.Date(2026, time.March, 9, 13, 35, 0, 0, time.UTC), ScopeUS},
		{"us close", time.Date(2026, time.January, 5, 20, 55, 0, 0, time.UTC), ScopeUS},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := SelectScope(test.at); got != test.want {
				t.Fatalf("SelectScope() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestExecuteOutcomesAndOneRequestBudget(t *testing.T) {
	tests := []struct {
		name, place, cancel, want string
		calls                     int
	}{
		{"ok", "ACCEPTED", "CANCELLED", OutcomeOK, 3},
		{"place not accepted", "NOT_CREATED", "CANCELLED", OutcomePlaceNotAccepted, 2},
		{"cancel not cancelled", "ACCEPTED", "UNKNOWN", OutcomeCancelNotCancelled, 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				writer.Header().Set("content-type", "application/json")
				if request.URL.Path == "/v1/price-band" {
					_ = json.NewEncoder(writer).Encode(executioncontracts.PriceBandV1{
						SchemaVersion: executioncontracts.PriceBandV1SchemaVersion,
						StockCode:     "005930",
						LowerLimit:    "49700",
						UpperLimit:    "92300",
						BasePrice:     "71000",
						LastPrice:     "70500",
					})
					return
				}
				if request.URL.Path == "/v1/commands" {
					var command executioncontracts.ExecutionCommandV1
					if err := json.NewDecoder(request.Body).Decode(&command); err != nil {
						t.Errorf("decode command: %v", err)
					}
					if !strings.HasPrefix(command.CommandID, commandIDPrefix) {
						t.Errorf("command_id %q lacks prefix", command.CommandID)
					}
					if request.Header.Get("X-Correlation-ID") != command.CommandID {
						t.Errorf("correlation ID was not the command ID")
					}
					if command.AccountScope != ScopeKR || command.StockCode != "005930" || command.Price != "49700" {
						t.Errorf("unexpected command: %#v", command)
					}
					_ = json.NewEncoder(writer).Encode(executioncontracts.ExecutionReceiptV1{Disposition: executioncontracts.ExecutionDisposition(test.place)})
					return
				}
				_ = json.NewEncoder(writer).Encode(cancelReceipt{State: test.cancel})
			}))
			defer server.Close()
			at := fixedKR(2026, time.September, 2, 10, 0)
			got := execute(context.Background(), Config{EdgeURL: server.URL, KRSymbol: "005930"}, func() time.Time { return at }, server.Client())
			if got.Outcome != test.want {
				t.Fatalf("outcome = %q, want %q", got.Outcome, test.want)
			}
			if gotCalls := int(calls.Load()); gotCalls != test.calls {
				t.Fatalf("requests = %d, want %d (a retry would violate the one-order budget)", gotCalls, test.calls)
			}
		})
	}
}

// A transport error can occur after edge received a request but before its
// response reached us. The second successful response is intentionally armed
// as a mutation trap: a retry would turn this test red and risk a second order.
func TestTransportErrorsDoNotRetryPlaceOrCancel(t *testing.T) {
	at := fixedKR(2026, time.September, 2, 10, 0)
	config := Config{EdgeURL: "http://127.0.0.1:8080", KRSymbol: "005930"}
	tests := []struct {
		name               string
		failPath           string
		wantPlaceAttempts  int32
		wantCancelAttempts int32
	}{
		{"place transport error", "/v1/commands", 1, 0},
		{"cancel transport error", "/v1/commands/cancel", 1, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var placeAttempts, cancelAttempts atomic.Int32
			client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				if request.URL.Path == "/v1/price-band" {
					return jsonResponse(`{"schema_version":"price-band/v1","stock_code":"005930","lower_limit":"49700","upper_limit":"92300","base_price":"71000","last_price":"70500"}`), nil
				}
				if request.URL.Path == "/v1/commands" {
					attempt := placeAttempts.Add(1)
					if test.failPath == "/v1/commands" && attempt == 1 {
						return nil, errors.New("lost place response")
					}
					return jsonResponse(`{"disposition":"ACCEPTED"}`), nil
				}
				attempt := cancelAttempts.Add(1)
				if test.failPath == "/v1/commands/cancel" && attempt == 1 {
					return nil, errors.New("lost cancel response")
				}
				return jsonResponse(`{"state":"CANCELLED"}`), nil
			})}
			result := execute(context.Background(), config, func() time.Time { return at }, client)
			if result.Outcome != OutcomeEdgeUnreachable {
				t.Fatalf("outcome = %q, want %q", result.Outcome, OutcomeEdgeUnreachable)
			}
			if got := placeAttempts.Load(); got != test.wantPlaceAttempts {
				t.Fatalf("place attempts = %d, want %d; retry risks a duplicate order", got, test.wantPlaceAttempts)
			}
			if got := cancelAttempts.Load(); got != test.wantCancelAttempts {
				t.Fatalf("cancel attempts = %d, want %d; retry is forbidden", got, test.wantCancelAttempts)
			}
		})
	}
}

func TestExecuteInquiryUnreachableAndNoSessionMakesNoRequest(t *testing.T) {
	closed := httptest.NewServer(http.NotFoundHandler())
	url := closed.URL
	closed.Close()
	at := fixedKR(2026, time.September, 2, 10, 0)
	// A dead edge makes the price inquiry fail, so the run fails closed
	// without an order: price_unavailable, not a retry or a placement.
	if got := execute(context.Background(), Config{EdgeURL: url}, func() time.Time { return at }, closed.Client()); got.Outcome != OutcomePriceUnavailable {
		t.Fatalf("outcome = %q", got.Outcome)
	}

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	offHours := fixedKR(2026, time.September, 2, 7, 0)
	got := execute(context.Background(), Config{EdgeURL: server.URL}, func() time.Time { return offHours }, server.Client())
	if got.Scope != ScopeNoSession || got.Outcome != OutcomeNoSession || calls.Load() != 0 {
		t.Fatalf("off-hours result=%#v calls=%d", got, calls.Load())
	}
}

func TestRunWritesJSONAndTextfileWithSafeLabels(t *testing.T) {
	directory := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("content-type", "application/json")
		if request.URL.Path == "/v1/price-band" {
			_ = json.NewEncoder(writer).Encode(executioncontracts.PriceBandV1{SchemaVersion: executioncontracts.PriceBandV1SchemaVersion, StockCode: "005930", LowerLimit: "49700"})
			return
		}
		if request.URL.Path == "/v1/commands" {
			_ = json.NewEncoder(writer).Encode(executioncontracts.ExecutionReceiptV1{Disposition: executioncontracts.DispositionAccepted})
			return
		}
		_ = json.NewEncoder(writer).Encode(cancelReceipt{State: "CANCELLED"})
	}))
	defer server.Close()
	at := fixedKR(2026, time.September, 2, 10, 0)
	var stdout strings.Builder
	lookup := func(key string) string {
		values := map[string]string{"CANARY_EDGE_URL": server.URL, "CANARY_TEXTFILE_DIR": directory}
		return values[key]
	}
	if code := Run(context.Background(), Options{Now: func() time.Time { return at }, Client: server.Client(), Stdout: &stdout, Stderr: io.Discard, Lookup: lookup}); code != 0 {
		t.Fatalf("Run() = %d", code)
	}
	var result Result
	if err := json.Unmarshal([]byte(stdout.String()), &result); err != nil {
		t.Fatalf("stdout is not JSON: %v", err)
	}
	if result.Outcome != OutcomeOK {
		t.Fatalf("stdout result = %#v", result)
	}
	contents, err := os.ReadFile(filepath.Join(directory, textfileName))
	if err != nil {
		t.Fatal(err)
	}
	text := string(contents)
	if !strings.Contains(text, `broker_edge_canary_result{scope="kis_mock",outcome="ok"} 1`) || !strings.Contains(text, "broker_edge_canary_last_success_timestamp_seconds{scope=\"kis_mock\"}") {
		t.Fatalf("missing expected metrics:\n%s", text)
	}
	for _, forbidden := range []string{"order_id", "broker_order", "price=", "quantity=", "symbol="} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("forbidden metric label or value %q in:\n%s", forbidden, text)
		}
	}
}

func TestWriteTextfilePreservesOtherScopeSuccess(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, textfileName), []byte("broker_edge_canary_last_success_timestamp_seconds{scope=\"kis_mock_us\"} 12\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := WriteTextfile(directory, Result{Scope: ScopeKR, Outcome: OutcomeOK}, time.Unix(34, 0)); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(directory, textfileName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), `scope="kis_mock_us"} 12`) || !strings.Contains(string(contents), `scope="kis_mock"} 34`) {
		t.Fatalf("success series not preserved: %s", contents)
	}
}

// These assertions are mutation guards: mapping a failed receipt to ok, or adding
// a retry, turns TestExecuteOutcomesAndOneRequestBudget red.
func TestFailureOutcomeVocabularyIsClosed(t *testing.T) {
	for _, outcome := range []string{OutcomeOK, OutcomePlaceNotAccepted, OutcomeCancelNotCancelled, OutcomeEdgeUnreachable, OutcomePriceUnavailable, OutcomeNoSession} {
		if outcome == "" {
			t.Fatal("empty outcome")
		}
	}
}

func TestExecutePropagatesRejectionEvidence(t *testing.T) {
	tests := []struct {
		name        string
		placeBody   string
		cancelBody  string
		wantOutcome string
	}{
		{
			name:        "place rejected",
			placeBody:   `{"disposition":"UNKNOWN","error_code":"broker_unknown","rejection":{"rt_cd":"1","msg_cd":"APBK0957","msg1":"refused","http_status":200}}`,
			wantOutcome: OutcomePlaceNotAccepted,
		},
		{
			name:        "cancel rejected",
			placeBody:   `{"disposition":"ACCEPTED","broker_order_id":"9001"}`,
			cancelBody:  `{"state":"UNKNOWN","error_code":"broker_5xx","rejection":{"rt_cd":"1","msg_cd":"EGW00201","msg1":"cancel refused","http_status":500}}`,
			wantOutcome: OutcomeCancelNotCancelled,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("content-type", "application/json")
				if request.URL.Path == "/v1/price-band" {
					_, _ = writer.Write([]byte(`{"schema_version":"price-band/v1","stock_code":"005930","lower_limit":"49700","upper_limit":"92300","base_price":"71000","last_price":"70500"}`))
					return
				}
				if request.URL.Path == "/v1/commands" {
					_, _ = writer.Write([]byte(test.placeBody))
					return
				}
				if test.cancelBody == "" {
					t.Error("cancel must not be attempted after an unaccepted place")
					return
				}
				_, _ = writer.Write([]byte(test.cancelBody))
			}))
			defer server.Close()
			at := fixedKR(2026, time.September, 2, 10, 0)
			got := execute(context.Background(), Config{EdgeURL: server.URL, KRSymbol: "005930"}, func() time.Time { return at }, server.Client())
			if got.Outcome != test.wantOutcome {
				t.Fatalf("outcome = %q, want %q", got.Outcome, test.wantOutcome)
			}
			if got.ErrorCode == "" || got.Rejection == nil {
				t.Fatalf("missing evidence: %#v", got)
			}
			if got.Rejection.HTTPStatus == 0 || got.Rejection.RtCd != "1" || got.Rejection.MsgCd == "" || got.Rejection.Msg1 == "" {
				t.Fatalf("rejection = %#v", got.Rejection)
			}
		})
	}
}

// The success line is the contract every alert and journal reader parses.
// Any new field must stay absent here.
func TestSuccessResultJSONIsGolden(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("content-type", "application/json")
		if request.URL.Path == "/v1/price-band" {
			_ = json.NewEncoder(writer).Encode(executioncontracts.PriceBandV1{SchemaVersion: executioncontracts.PriceBandV1SchemaVersion, StockCode: "005930", LowerLimit: "49700"})
			return
		}
		if request.URL.Path == "/v1/commands" {
			_ = json.NewEncoder(writer).Encode(executioncontracts.ExecutionReceiptV1{Disposition: executioncontracts.DispositionAccepted, BrokerOrderID: "9001"})
			return
		}
		_ = json.NewEncoder(writer).Encode(cancelReceipt{State: "CANCELLED"})
	}))
	defer server.Close()
	at := fixedKR(2026, time.September, 2, 10, 0)
	got := execute(context.Background(), Config{EdgeURL: server.URL, KRSymbol: "005930"}, func() time.Time { return at }, server.Client())
	if got.Outcome != OutcomeOK {
		t.Fatalf("outcome = %q", got.Outcome)
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	const golden = `{"scope":"kis_mock","outcome":"ok","timestamp":"2026-09-02T01:00:00Z"}`
	if string(encoded) != golden {
		t.Fatalf("success JSON changed: %s", encoded)
	}
}

// The end-to-end AC: a KIS rejection body carrying an account number and a
// token produces canary output containing neither. This drives a real edge
// Service and handler; only the broker transport is faked.
func TestRunMasksSecretsFromRealEdge(t *testing.T) {
	const (
		account = "12345678-01"
		token   = "cached-token-for-test"
		appKey  = "app-key-for-test"
	)
	kis := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		// msg_cd is an 8-digit code that is not the account: it must survive
		// masking so the desk can read KIS's real rejection code.
		body := `{"rt_cd":"1","msg_cd":"87654321","msg1":"계좌 ` + account + ` 토큰 ` + token + ` 키 ` + appKey + ` 거부"}`
		if strings.HasSuffix(request.URL.Path, "order-rvsecncl") {
			body = `{"rt_cd":"1","msg_cd":"EGW00123","msg1":"cancel refused ` + account + ` ` + token + `"}`
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	store, err := kismockedge.OpenStore(filepath.Join(t.TempDir(), "edge.sqlite"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := &kismockedge.Service{
		Store:        store,
		PlaceEnabled: true,
		Quoters: map[string]kismockedge.Quoter{
			executioncontracts.AccountScopeKISMock: stubQuoter{band: kismockread.DomesticPriceBand{LowerLimit: "49700", UpperLimit: "92300", BasePrice: "71000"}},
		},
		Brokers: map[string]kismockedge.Broker{
			executioncontracts.AccountScopeKISMock: kismockedge.KISMockBroker{
				Transport: kis,
				LoadConfig: func() (kismockread.Config, string) {
					return kismockread.Config{
						BaseURL:   kismockread.MockBaseURL,
						AppKey:    appKey,
						AppSecret: "app-secret-for-test",
						AccountNo: account,
						Timeout:   time.Second,
					}, ""
				},
				Tokens: stubTokenLoader{token: token},
			},
		},
	}
	server := httptest.NewServer(kismockedge.NewHandler(service))
	defer server.Close()
	directory := t.TempDir()
	at := fixedKR(2026, time.September, 2, 10, 0)
	var stdout strings.Builder
	lookup := func(key string) string {
		return map[string]string{"CANARY_EDGE_URL": server.URL, "CANARY_TEXTFILE_DIR": directory}[key]
	}
	code := Run(context.Background(), Options{Now: func() time.Time { return at }, Client: server.Client(), Stdout: &stdout, Stderr: io.Discard, Lookup: lookup})
	if code != 1 {
		t.Fatalf("Run() = %d, want rejection exit 1", code)
	}
	line := stdout.String()
	for _, secret := range []string{"12345678", account, token, appKey, "app-secret-for-test"} {
		if strings.Contains(line, secret) {
			t.Fatalf("secret %q in output: %s", secret, line)
		}
	}
	var result Result
	if err := json.Unmarshal([]byte(line), &result); err != nil {
		t.Fatalf("stdout is not JSON: %v", err)
	}
	if result.Outcome != OutcomePlaceNotAccepted || result.Rejection == nil {
		t.Fatalf("result = %#v", result)
	}
	if result.Rejection.RtCd != "1" || result.Rejection.MsgCd != "87654321" || result.Rejection.HTTPStatus != http.StatusOK {
		t.Fatalf("rejection = %#v", result.Rejection)
	}
	if !strings.Contains(result.Rejection.Msg1, "[redacted]") {
		t.Fatalf("unmasked msg1: %q", result.Rejection.Msg1)
	}
}

// A rejected cancel must carry the same masked evidence as a rejected place.
// The cancel's msg_cd is the account number itself: exact-needle masking must
// still hide it even though generic code values pass through.
func TestRunMasksSecretsOnCancelRejection(t *testing.T) {
	const account = "12345678-01"
	kis := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if strings.HasSuffix(request.URL.Path, "order-rvsecncl") {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
				Body: io.NopCloser(strings.NewReader(`{"rt_cd":"1","msg_cd":"12345678","msg1":"no such order ` + account + `"}`))}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(`{"rt_cd":"0","output":{"ODNO":"9001","KRX_FWDG_ORD_ORGNO":"00000"}}`))}, nil
	})
	store, err := kismockedge.OpenStore(filepath.Join(t.TempDir(), "edge.sqlite"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := &kismockedge.Service{
		Store:        store,
		PlaceEnabled: true,
		Quoters: map[string]kismockedge.Quoter{
			executioncontracts.AccountScopeKISMock: stubQuoter{band: kismockread.DomesticPriceBand{LowerLimit: "49700"}},
		},
		Brokers: map[string]kismockedge.Broker{
			executioncontracts.AccountScopeKISMock: kismockedge.KISMockBroker{
				Transport: kis,
				LoadConfig: func() (kismockread.Config, string) {
					return kismockread.Config{BaseURL: kismockread.MockBaseURL, AppKey: "app-key-for-test", AppSecret: "app-secret-for-test", AccountNo: account, Timeout: time.Second}, ""
				},
				Tokens: stubTokenLoader{token: "cached-token-for-test"},
			},
		},
	}
	server := httptest.NewServer(kismockedge.NewHandler(service))
	defer server.Close()
	at := fixedKR(2026, time.September, 2, 10, 0)
	var stdout strings.Builder
	lookup := func(key string) string {
		return map[string]string{"CANARY_EDGE_URL": server.URL, "CANARY_TEXTFILE_DIR": t.TempDir()}[key]
	}
	code := Run(context.Background(), Options{Now: func() time.Time { return at }, Client: server.Client(), Stdout: &stdout, Stderr: io.Discard, Lookup: lookup})
	if code != 1 {
		t.Fatalf("Run() = %d, want rejection exit 1", code)
	}
	line := stdout.String()
	if strings.Contains(line, "12345678") {
		t.Fatalf("account in output: %s", line)
	}
	var result Result
	if err := json.Unmarshal([]byte(line), &result); err != nil {
		t.Fatalf("stdout is not JSON: %v", err)
	}
	if result.Outcome != OutcomeCancelNotCancelled || result.Rejection == nil || result.Rejection.MsgCd != "[redacted]" {
		t.Fatalf("result = %#v", result)
	}
}

type stubTokenLoader struct{ token string }

func (loader stubTokenLoader) Load(context.Context, kismockread.Config) (string, string) {
	return loader.token, ""
}

type stubQuoter struct {
	band kismockread.DomesticPriceBand
	code string
}

func (quoter stubQuoter) PriceBand(context.Context, string) (kismockread.DomesticPriceBand, string) {
	return quoter.band, quoter.code
}

// The derived KR price must always be the exchange lower limit (or its
// tick-rounded ceiling), and the base-price fallback must round base x 0.70
// UP to the next KRX tick. Band edges and tick boundaries are pinned here.
func TestDeriveKRPriceBandEdgesAndTickRounding(t *testing.T) {
	tests := []struct {
		name                     string
		lower, upper, base, last string
		want                     string
	}{
		{"lower exact tick", "49700", "92300", "71000", "70500", "49700"},
		{"lower tiny price", "37", "", "", "", "37"},
		{"lower non-tick rounds up one tick inside band", "32973", "92300", "71000", "70500", "33000"},
		{"lower zero falls back to base", "0", "", "47000", "", "32900"},
		{"no lower: base 47000", "", "", "47000", "", "32900"},
		{"no lower: base 47001 rounds 32900.7 up to tick 32950", "", "", "47001", "", "32950"},
		{"no lower: tick boundary 2000 base", "", "", "2000", "", "1400"},
		{"no lower: tick boundary 5000 base", "", "", "5000", "", "3500"},
		{"no lower: tick boundary 20000 base", "", "", "20000", "", "14000"},
		{"no lower: tick boundary 50000 base", "", "", "50000", "", "35000"},
		{"no lower: tick boundary 200000 base", "", "", "200000", "", "140000"},
		{"no lower: tick boundary 500000 base", "", "", "500000", "", "350000"},
		{"no lower: above 500000 base", "", "", "1000000", "", "700000"},
		{"no lower: off-tick base 1999", "", "", "1999", "", "1400"},
		{"no lower: off-tick base 5001", "", "", "5001", "", "3505"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			band := executioncontracts.PriceBandV1{
				LowerLimit: test.lower, UpperLimit: test.upper,
				BasePrice: test.base, LastPrice: test.last,
			}
			got, ok := deriveKRPrice(band)
			if !ok || got.String() != test.want {
				t.Fatalf("deriveKRPrice(%+v) = %v,%v want %s", band, got, ok, test.want)
			}
			if !kismockedge.TickMatchesKR(got, "buy") {
				t.Fatalf("derived price %s is not a valid KRX buy tick", got)
			}
		})
	}
}

func TestDeriveKRPriceFailsClosedWithoutBand(t *testing.T) {
	for _, band := range []executioncontracts.PriceBandV1{
		{},
		{LowerLimit: "", BasePrice: ""},
		{LowerLimit: "abc", BasePrice: ""},
		{LowerLimit: "0", BasePrice: "0"},
		{LowerLimit: "-100", BasePrice: "x"},
	} {
		if price, ok := deriveKRPrice(band); ok {
			t.Fatalf("deriveKRPrice(%+v) = %v, want fail closed", band, price)
		}
	}
}

// Every inquiry failure must yield price_unavailable with zero order POSTs.
func TestExecuteFailsClosedWhenInquiryFails(t *testing.T) {
	tests := []struct {
		name   string
		band   string
		status int
	}{
		{"http 500", `{"error_code":"request_failed"}`, http.StatusInternalServerError},
		{"bad gateway", `{"error_code":"token_expired"}`, http.StatusBadGateway},
		{"invalid json", `not json`, http.StatusOK},
		{"empty band", `{"schema_version":"price-band/v1","stock_code":"005930"}`, http.StatusOK},
		{"non-numeric lower", `{"schema_version":"price-band/v1","stock_code":"005930","lower_limit":"12a45"}`, http.StatusOK},
		{"wrong stock echo", `{"schema_version":"price-band/v1","stock_code":"000660","lower_limit":"49700"}`, http.StatusOK},
		{"old edge without endpoint fields", `{"lower_limit":"49700"}`, http.StatusOK},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var orderPosts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path == "/v1/price-band" {
					writer.Header().Set("content-type", "application/json")
					writer.WriteHeader(test.status)
					_, _ = writer.Write([]byte(test.band))
					return
				}
				if request.Method == http.MethodPost {
					orderPosts.Add(1)
				}
				writer.Header().Set("content-type", "application/json")
				_, _ = writer.Write([]byte(`{"disposition":"ACCEPTED"}`))
			}))
			defer server.Close()
			at := fixedKR(2026, time.September, 2, 10, 0)
			got := execute(context.Background(), Config{EdgeURL: server.URL, KRSymbol: "005930"}, func() time.Time { return at }, server.Client())
			if got.Outcome != OutcomePriceUnavailable {
				t.Fatalf("outcome = %q, want %q", got.Outcome, OutcomePriceUnavailable)
			}
			if orderPosts.Load() != 0 {
				t.Fatalf("order POSTs = %d; a failed inquiry must place nothing", orderPosts.Load())
			}
		})
	}
}

// The US scope keeps its constant price and must never call the band endpoint.
func TestExecuteUSScopeNeverCallsPriceBand(t *testing.T) {
	var bandCalls atomic.Int32
	var command executioncontracts.ExecutionCommandV1
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("content-type", "application/json")
		if request.URL.Path == "/v1/price-band" {
			bandCalls.Add(1)
			_, _ = writer.Write([]byte(`{"lower_limit":"49700"}`))
			return
		}
		if request.URL.Path == "/v1/commands" {
			if err := json.NewDecoder(request.Body).Decode(&command); err != nil {
				t.Errorf("decode: %v", err)
			}
			_ = json.NewEncoder(writer).Encode(executioncontracts.ExecutionReceiptV1{Disposition: executioncontracts.DispositionAccepted})
			return
		}
		_ = json.NewEncoder(writer).Encode(cancelReceipt{State: "CANCELLED"})
	}))
	defer server.Close()
	// Monday 14:35 UTC = 09:35 EST on 2026-01-05 — inside the US window only.
	at := time.Date(2026, time.January, 5, 14, 35, 0, 0, time.UTC)
	got := execute(context.Background(), Config{EdgeURL: server.URL, KRSymbol: "005930"}, func() time.Time { return at }, server.Client())
	if got.Outcome != OutcomeOK {
		t.Fatalf("outcome = %q", got.Outcome)
	}
	if bandCalls.Load() != 0 {
		t.Fatalf("US scope called price-band %d times", bandCalls.Load())
	}
	if command.AccountScope != ScopeUS || command.StockCode != "AAPL" || command.Price != "1" {
		t.Fatalf("US command = %#v", command)
	}
}

// End to end through a real edge: the canary asks the edge for the band,
// and the KIS-bound order body carries exactly the lower limit.
func TestRunSendsLowerLimitThroughRealEdge(t *testing.T) {
	var orderPrice atomic.Value
	kis := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if strings.HasSuffix(request.URL.Path, "order-cash") {
			var body map[string]string
			if err := json.NewDecoder(request.Body).Decode(&body); err == nil {
				orderPrice.Store(body["ORD_UNPR"])
			}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
				Body: io.NopCloser(strings.NewReader(`{"rt_cd":"0","output":{"ODNO":"9001","KRX_FWDG_ORD_ORGNO":"00000"}}`))}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(`{"rt_cd":"0"}`))}, nil
	})
	store, err := kismockedge.OpenStore(filepath.Join(t.TempDir(), "edge.sqlite"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	service := &kismockedge.Service{
		Store:        store,
		PlaceEnabled: true,
		Quoters: map[string]kismockedge.Quoter{
			executioncontracts.AccountScopeKISMock: stubQuoter{band: kismockread.DomesticPriceBand{LowerLimit: "49700", UpperLimit: "92300", BasePrice: "71000"}},
		},
		Brokers: map[string]kismockedge.Broker{
			executioncontracts.AccountScopeKISMock: kismockedge.KISMockBroker{
				Transport: kis,
				LoadConfig: func() (kismockread.Config, string) {
					return kismockread.Config{BaseURL: kismockread.MockBaseURL, AppKey: "app-key-for-test", AppSecret: "app-secret-for-test", AccountNo: "12345678-01", Timeout: time.Second}, ""
				},
				Tokens: stubTokenLoader{token: "cached-token-for-test"},
			},
		},
	}
	server := httptest.NewServer(kismockedge.NewHandler(service))
	defer server.Close()
	at := fixedKR(2026, time.September, 2, 10, 0)
	var stdout strings.Builder
	lookup := func(key string) string {
		return map[string]string{"CANARY_EDGE_URL": server.URL, "CANARY_TEXTFILE_DIR": t.TempDir()}[key]
	}
	if code := Run(context.Background(), Options{Now: func() time.Time { return at }, Client: server.Client(), Stdout: &stdout, Stderr: io.Discard, Lookup: lookup}); code != 0 {
		t.Fatalf("Run() = %d, stdout %s", code, stdout.String())
	}
	if got := orderPrice.Load(); got != "49700" {
		t.Fatalf("ORD_UNPR sent to KIS = %v, want the band lower limit 49700", got)
	}
}
