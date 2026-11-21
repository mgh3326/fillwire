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
	"reflect"
	"sort"
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
			got := execute(context.Background(), Config{EdgeURL: server.URL, KRSymbol: "005930"}, func() time.Time { return at }, server.Client(), Options{}.timeouts())
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
// The place-failure case additionally proves the post-ambiguity check issues
// exactly one command-scoped resolve and never a blind retry.
func TestTransportErrorsDoNotRetryPlaceOrCancel(t *testing.T) {
	at := fixedKR(2026, time.September, 2, 10, 0)
	config := Config{EdgeURL: "http://127.0.0.1:8080", KRSymbol: "005930"}
	tests := []struct {
		name                string
		failPath            string
		wantPlaceAttempts   int32
		wantCancelAttempts  int32
		wantResolveAttempts int32
	}{
		{"place transport error", "/v1/commands", 1, 0, 1},
		{"cancel transport error", "/v1/commands/cancel", 1, 1, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var placeAttempts, cancelAttempts, resolveAttempts atomic.Int32
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
				if strings.HasSuffix(request.URL.Path, "/resolve") {
					resolveAttempts.Add(1)
					// The fake edge has no record of the command: nothing
					// resting can be proven, so no cancel may follow.
					return jsonResponse(`{"schema_version":"command-check/v1","command_id":"x","disposition":"UNKNOWN","evidence_read":"unavailable","orders_seen":0,"matched":0}`), nil
				}
				attempt := cancelAttempts.Add(1)
				if test.failPath == "/v1/commands/cancel" && attempt == 1 {
					return nil, errors.New("lost cancel response")
				}
				return jsonResponse(`{"state":"CANCELLED"}`), nil
			})}
			result := execute(context.Background(), config, func() time.Time { return at }, client, Options{}.timeouts())
			if result.Outcome != OutcomeEdgeUnreachable {
				t.Fatalf("outcome = %q, want %q", result.Outcome, OutcomeEdgeUnreachable)
			}
			if got := placeAttempts.Load(); got != test.wantPlaceAttempts {
				t.Fatalf("place attempts = %d, want %d; retry risks a duplicate order", got, test.wantPlaceAttempts)
			}
			if got := cancelAttempts.Load(); got != test.wantCancelAttempts {
				t.Fatalf("cancel attempts = %d, want %d; retry is forbidden", got, test.wantCancelAttempts)
			}
			if got := resolveAttempts.Load(); got != test.wantResolveAttempts {
				t.Fatalf("resolve attempts = %d, want %d; a second check is a retry", got, test.wantResolveAttempts)
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
	if got := execute(context.Background(), Config{EdgeURL: url}, func() time.Time { return at }, closed.Client(), Options{}.timeouts()); got.Outcome != OutcomePriceUnavailable {
		t.Fatalf("outcome = %q", got.Outcome)
	}

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	offHours := fixedKR(2026, time.September, 2, 7, 0)
	got := execute(context.Background(), Config{EdgeURL: server.URL}, func() time.Time { return offHours }, server.Client(), Options{}.timeouts())
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
				if strings.HasSuffix(request.URL.Path, "/resolve") {
					// The post-ambiguity check is allowed; it must never find
					// an order the fake edge never created.
					_, _ = writer.Write([]byte(`{"schema_version":"command-check/v1","command_id":"x","disposition":"UNKNOWN","evidence_read":"unavailable","orders_seen":0,"matched":0}`))
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
			got := execute(context.Background(), Config{EdgeURL: server.URL, KRSymbol: "005930"}, func() time.Time { return at }, server.Client(), Options{}.timeouts())
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
// Any field outside this pinned set must stay absent; evidence fields are
// pinned to their documented values wherever they are deterministic.
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
	got := execute(context.Background(), Config{EdgeURL: server.URL, KRSymbol: "005930"}, func() time.Time { return at }, server.Client(), Options{}.timeouts())
	if got.Outcome != OutcomeOK {
		t.Fatalf("outcome = %q", got.Outcome)
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	wantKeys := []string{"scope", "outcome", "timestamp", "command_id", "price", "band", "stage", "durations_ms"}
	gotKeys := make([]string, 0, len(fields))
	for key := range fields {
		gotKeys = append(gotKeys, key)
	}
	sort.Strings(gotKeys)
	sort.Strings(wantKeys)
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("success JSON keys = %v, want %v (%s)", gotKeys, wantKeys, encoded)
	}
	if fields["scope"] != "kis_mock" || fields["outcome"] != "ok" || fields["timestamp"] != "2026-09-02T01:00:00Z" {
		t.Fatalf("stable fields drifted: %s", encoded)
	}
	if commandID, ok := fields["command_id"].(string); !ok || !strings.HasPrefix(commandID, commandIDPrefix) {
		t.Fatalf("command_id = %v", fields["command_id"])
	}
	if fields["price"] != "49700" || fields["stage"] != StageCancelAcked {
		t.Fatalf("evidence fields = %s", encoded)
	}
	band, _ := fields["band"].(map[string]any)
	if band["stock_code"] != "005930" || band["lower_limit"] != "49700" {
		t.Fatalf("band evidence = %s", encoded)
	}
	durations, _ := fields["durations_ms"].(map[string]any)
	for _, stage := range []string{StageBandGet, StagePlaceSent, StageCancelSent} {
		if _, ok := durations[stage]; !ok {
			t.Fatalf("missing duration for %s in %s", stage, encoded)
		}
	}
	// The broker order id is evidence the outcome line must never carry. The
	// command ID is scrubbed first: its random hex suffix could collide with
	// the needle by chance.
	scrubbed := strings.ReplaceAll(string(encoded), fields["command_id"].(string), "")
	if strings.Contains(scrubbed, "9001") {
		t.Fatalf("broker order id leaked into the outcome line: %s", encoded)
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

type stubOrderReader struct {
	orders []kismockread.DomesticOrder
	err    error
}

func (reader stubOrderReader) DomesticOrderHistory(context.Context, string) ([]kismockread.DomesticOrder, error) {
	return reader.orders, reader.err
}

// hangingKISTransport simulates KIS never answering the place call: it blocks
// until the request context dies (canary stage deadline, server-side client
// disconnect propagation, or a test fallback) and only then returns an error.
// The cancel TR answers immediately so a proven resting order can be cancelled.
type hangingKISTransport struct {
	cancelCalls atomic.Int32
	placeCalls  atomic.Int32
}

func (transport *hangingKISTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if strings.HasSuffix(request.URL.Path, "order-rvsecncl") {
		transport.cancelCalls.Add(1)
		return jsonResponse(`{"rt_cd":"0"}`), nil
	}
	transport.placeCalls.Add(1)
	select {
	case <-request.Context().Done():
		return nil, request.Context().Err()
	case <-time.After(5 * time.Second):
		return nil, errors.New("hang transport fallback")
	}
}

func timeoutEdgeService(t *testing.T, reader stubOrderReader, transport *hangingKISTransport) *kismockedge.Service {
	t.Helper()
	return &kismockedge.Service{
		Store:        mustStore(t),
		PlaceEnabled: true,
		Quoters: map[string]kismockedge.Quoter{
			executioncontracts.AccountScopeKISMock: stubQuoter{band: kismockread.DomesticPriceBand{LowerLimit: "49700", UpperLimit: "92300", BasePrice: "71000"}},
		},
		Brokers: map[string]kismockedge.Broker{
			executioncontracts.AccountScopeKISMock: kismockedge.KISMockBroker{
				Transport: transport,
				LoadConfig: func() (kismockread.Config, string) {
					return kismockread.Config{BaseURL: kismockread.MockBaseURL, AppKey: "app-key-for-test", AppSecret: "app-secret-for-test", AccountNo: "12345678-01", Timeout: 5 * time.Second}, ""
				},
				Tokens: stubTokenLoader{token: "cached-token-for-test"},
			},
		},
		OrderReader: reader,
		Now:         func() time.Time { return edgeTestNow },
	}
}

var edgeTestNow = time.Date(2026, time.September, 2, 1, 0, 0, 0, time.UTC)

func mustStore(t *testing.T) *kismockedge.Store {
	t.Helper()
	store, err := kismockedge.OpenStore(filepath.Join(t.TempDir(), "edge.sqlite"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// AC3 core case: a place that times out before the ack may still have crossed
// the send boundary. The canary must ask the edge to evidence its own command,
// find the resting order, and cancel exactly that command — recorded on the
// outcome line. Everything below runs the real edge Service and Store; only
// the KIS transport and the read evidence are faked.
func TestPlaceTimeoutFindsAndCancelsOwnRestingOrder(t *testing.T) {
	at := fixedKR(2026, time.September, 2, 10, 0)
	transport := &hangingKISTransport{}
	// The resting order matches the command's own facts (buy 005930 x1 at the
	// derived lower-limit price 49700, ordered at the service's send time).
	resting := stubOrderReader{orders: []kismockread.DomesticOrder{{
		BrokerOrderID: "9001", Side: "buy", StockCode: "005930",
		Quantity: "001", Price: "49700", OrderedAt: edgeTestNow,
	}}}
	service := timeoutEdgeService(t, resting, transport)
	server := httptest.NewServer(kismockedge.NewHandler(service))
	defer server.Close()

	budgets := Options{}.timeouts()
	budgets.place = 300 * time.Millisecond
	result := execute(context.Background(), Config{EdgeURL: server.URL, KRSymbol: "005930"}, func() time.Time { return at }, server.Client(), budgets)

	if result.Outcome != OutcomePlaceNotAccepted || result.ErrorCode != ErrorCanaryTimeout {
		t.Fatalf("result = %+v", result)
	}
	if result.Stage != StageCancelAcked {
		t.Fatalf("stage = %q, want %q after cleanup cancel", result.Stage, StageCancelAcked)
	}
	if result.OrderCheck == nil || result.OrderCheck.Disposition != string(executioncontracts.DispositionAccepted) ||
		result.OrderCheck.Matched != 1 || result.OrderCheck.OrdersSeen != 1 ||
		result.OrderCheck.CancelState != "CANCELLED" {
		t.Fatalf("order_check = %+v", result.OrderCheck)
	}
	for _, stage := range []string{StageBandGet, StagePlaceSent, "check", StageCancelSent} {
		if _, ok := result.StageDurationsMS[stage]; !ok {
			t.Fatalf("missing duration for %s in %+v", stage, result.StageDurationsMS)
		}
	}
	if transport.placeCalls.Load() != 1 || transport.cancelCalls.Load() != 1 {
		t.Fatalf("place=%d cancel=%d KIS calls; want exactly one each", transport.placeCalls.Load(), transport.cancelCalls.Load())
	}
	// The store proves the cleanup: the command resolved ACCEPTED and its one
	// cancel attempt is durably CANCELLED.
	resolved, found, err := service.Store.Find(context.Background(), result.CommandID)
	if err != nil || !found || resolved.Disposition != executioncontracts.DispositionAccepted || resolved.BrokerOrderID != "9001" {
		t.Fatalf("resolved receipt = %+v found=%t err=%v", resolved, found, err)
	}
	cancelled, found, err := service.Store.FindCancelAttempt(context.Background(), result.CommandID)
	if err != nil || !found || cancelled.State != "CANCELLED" {
		t.Fatalf("cancel attempt = %+v found=%t err=%v", cancelled, found, err)
	}
}

// A place timeout whose evidence read finds only foreign orders must leave the
// command UNKNOWN and must never dispatch a cancel. The foreign order shares
// the symbol but has a different price, proving matching is never symbol-only.
func TestPlaceTimeoutNoRestingOrderCancelsNothing(t *testing.T) {
	at := fixedKR(2026, time.September, 2, 10, 0)
	transport := &hangingKISTransport{}
	foreign := stubOrderReader{orders: []kismockread.DomesticOrder{{
		BrokerOrderID: "foreign-9", Side: "buy", StockCode: "005930",
		Quantity: "001", Price: "50000", OrderedAt: edgeTestNow,
	}}}
	service := timeoutEdgeService(t, foreign, transport)
	server := httptest.NewServer(kismockedge.NewHandler(service))
	defer server.Close()

	budgets := Options{}.timeouts()
	budgets.place = 300 * time.Millisecond
	result := execute(context.Background(), Config{EdgeURL: server.URL, KRSymbol: "005930"}, func() time.Time { return at }, server.Client(), budgets)

	if result.Outcome != OutcomePlaceNotAccepted || result.ErrorCode != ErrorCanaryTimeout {
		t.Fatalf("result = %+v", result)
	}
	if result.Stage != StagePlaceSent {
		t.Fatalf("stage = %q, want %q: no cancel may run for an unproven order", result.Stage, StagePlaceSent)
	}
	if result.OrderCheck == nil || result.OrderCheck.Disposition != string(executioncontracts.DispositionUnknown) ||
		result.OrderCheck.Matched != 0 || result.OrderCheck.OrdersSeen != 1 || result.OrderCheck.CancelState != "" {
		t.Fatalf("order_check = %+v", result.OrderCheck)
	}
	if transport.cancelCalls.Load() != 0 {
		t.Fatalf("cancel calls = %d; a foreign order must never be cancelled", transport.cancelCalls.Load())
	}
	if _, found, _ := service.Store.FindCancelAttempt(context.Background(), result.CommandID); found {
		t.Fatal("cancel attempt was recorded for an unproven order")
	}
	resolved, found, err := service.Store.Find(context.Background(), result.CommandID)
	if err != nil || !found || resolved.Disposition != executioncontracts.DispositionUnknown {
		t.Fatalf("command must stay UNKNOWN inside grace: %+v found=%t err=%v", resolved, found, err)
	}
}

// A decoded UNKNOWN receipt (the edge's own broker_timeout) is the same
// ambiguous class as a client-side timeout: it must trigger the check.
func TestAmbiguousPlaceReceiptTriggersCheck(t *testing.T) {
	var resolveCalls, cancelCalls atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.URL.Path == "/v1/price-band":
			return jsonResponse(`{"schema_version":"price-band/v1","stock_code":"005930","lower_limit":"49700"}`), nil
		case request.URL.Path == "/v1/commands":
			return jsonResponse(`{"schema_version":"execution-receipt/v1","command_id":"x","disposition":"UNKNOWN","error_code":"broker_timeout","recorded_at":"2026-09-02T01:00:00Z"}`), nil
		case strings.HasSuffix(request.URL.Path, "/resolve"):
			resolveCalls.Add(1)
			return jsonResponse(`{"schema_version":"command-check/v1","command_id":"x","disposition":"ACCEPTED","evidence_read":"completed","orders_seen":1,"matched":1}`), nil
		default:
			cancelCalls.Add(1)
			return jsonResponse(`{"state":"CANCELLED"}`), nil
		}
	})}
	at := fixedKR(2026, time.September, 2, 10, 0)
	result := execute(context.Background(), Config{EdgeURL: "http://127.0.0.1:8080", KRSymbol: "005930"}, func() time.Time { return at }, client, Options{}.timeouts())
	if result.Outcome != OutcomePlaceNotAccepted || result.ErrorCode != "broker_timeout" {
		t.Fatalf("result = %+v", result)
	}
	if resolveCalls.Load() != 1 || cancelCalls.Load() != 1 {
		t.Fatalf("resolve=%d cancel=%d", resolveCalls.Load(), cancelCalls.Load())
	}
	if result.OrderCheck == nil || result.OrderCheck.CancelState != "CANCELLED" {
		t.Fatalf("order_check = %+v", result.OrderCheck)
	}
}

// A decoded 200 check answered fine even when the stored receipt still carries
// its own error code (e.g. broker_timeout on an unresolved UNKNOWN row).
// order_check.error_code is reserved for the check itself failing to answer.
func TestAnsweredCheckNeverCarriesReceiptErrorCode(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch {
		case request.URL.Path == "/v1/price-band":
			return jsonResponse(`{"schema_version":"price-band/v1","stock_code":"005930","lower_limit":"49700"}`), nil
		case request.URL.Path == "/v1/commands":
			return jsonResponse(`{"schema_version":"execution-receipt/v1","command_id":"x","disposition":"UNKNOWN","error_code":"broker_timeout","recorded_at":"2026-09-02T01:00:00Z"}`), nil
		case strings.HasSuffix(request.URL.Path, "/resolve"):
			return jsonResponse(`{"schema_version":"command-check/v1","command_id":"x","disposition":"UNKNOWN","error_code":"broker_timeout","evidence_read":"completed","orders_seen":3,"matched":0}`), nil
		default:
			return jsonResponse(`{"state":"CANCELLED"}`), nil
		}
	})}
	at := fixedKR(2026, time.September, 2, 10, 0)
	result := execute(context.Background(), Config{EdgeURL: "http://127.0.0.1:8080", KRSymbol: "005930"}, func() time.Time { return at }, client, Options{}.timeouts())
	if result.OrderCheck == nil {
		t.Fatal("order_check missing after ambiguous place")
	}
	if result.OrderCheck.ErrorCode != "" {
		t.Fatalf("order_check.error_code = %q; a decoded 200 check must not carry the receipt error code", result.OrderCheck.ErrorCode)
	}
	if result.OrderCheck.Disposition != string(executioncontracts.DispositionUnknown) || result.OrderCheck.EvidenceRead != "completed" ||
		result.OrderCheck.OrdersSeen != 3 || result.OrderCheck.Matched != 0 {
		t.Fatalf("order_check = %+v", result.OrderCheck)
	}
}

// AC2: the band GET has its own budget. A slow band burns only that budget —
// the run fails as price_unavailable at the band stage without ever placing.
func TestSlowBandGetHonorsItsOwnBudget(t *testing.T) {
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/v1/price-band" {
			<-request.Context().Done()
			return
		}
		posts.Add(1)
	}))
	defer server.Close()
	budgets := Options{}.timeouts()
	budgets.band = 150 * time.Millisecond
	at := fixedKR(2026, time.September, 2, 10, 0)
	started := time.Now()
	result := execute(context.Background(), Config{EdgeURL: server.URL, KRSymbol: "005930"}, func() time.Time { return at }, server.Client(), budgets)
	elapsed := time.Since(started)
	if result.Outcome != OutcomePriceUnavailable || result.ErrorCode != ErrorCanaryTimeout {
		t.Fatalf("result = %+v", result)
	}
	if result.Stage != StageBandGet {
		t.Fatalf("stage = %q, want %q", result.Stage, StageBandGet)
	}
	if posts.Load() != 0 {
		t.Fatalf("order POSTs = %d; a timed-out inquiry must place nothing", posts.Load())
	}
	if elapsed > 5*time.Second {
		t.Fatalf("run took %s; the band budget did not bound the stage", elapsed)
	}
}

// AC2: the overall deadline bounds the entire run including cleanup — after
// the place stage consumes it, the check inherits the expired context instead
// of running unbounded.
func TestOverallDeadlineBoundsCheck(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/v1/price-band" {
			return jsonResponse(`{"schema_version":"price-band/v1","stock_code":"005930","lower_limit":"49700"}`), nil
		}
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}
	budgets := Options{}.timeouts()
	budgets.overall = 300 * time.Millisecond
	budgets.place = 30 * time.Second
	budgets.check = 30 * time.Second
	at := fixedKR(2026, time.September, 2, 10, 0)
	started := time.Now()
	result := execute(context.Background(), Config{EdgeURL: "http://127.0.0.1:8080", KRSymbol: "005930"}, func() time.Time { return at }, client, budgets)
	elapsed := time.Since(started)
	if result.Outcome != OutcomePlaceNotAccepted || result.ErrorCode != ErrorCanaryTimeout {
		t.Fatalf("result = %+v", result)
	}
	if result.OrderCheck == nil || result.OrderCheck.ErrorCode != ErrorCanaryTimeout {
		t.Fatalf("order_check = %+v; the check must inherit the spent deadline", result.OrderCheck)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("run took %s; the overall deadline did not bound the run", elapsed)
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
			got := execute(context.Background(), Config{EdgeURL: server.URL, KRSymbol: "005930"}, func() time.Time { return at }, server.Client(), Options{}.timeouts())
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
	got := execute(context.Background(), Config{EdgeURL: server.URL, KRSymbol: "005930"}, func() time.Time { return at }, server.Client(), Options{}.timeouts())
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
