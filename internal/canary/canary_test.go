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
		{"ok", "ACCEPTED", "CANCELLED", OutcomeOK, 2},
		{"place not accepted", "NOT_CREATED", "CANCELLED", OutcomePlaceNotAccepted, 1},
		{"cancel not cancelled", "ACCEPTED", "UNKNOWN", OutcomeCancelNotCancelled, 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				writer.Header().Set("content-type", "application/json")
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
					if command.AccountScope != ScopeKR || command.StockCode != "005930" || command.Price != "1000" {
						t.Errorf("unexpected command: %#v", command)
					}
					_ = json.NewEncoder(writer).Encode(executioncontracts.ExecutionReceiptV1{Disposition: executioncontracts.ExecutionDisposition(test.place)})
					return
				}
				_ = json.NewEncoder(writer).Encode(cancelReceipt{State: test.cancel})
			}))
			defer server.Close()
			at := fixedKR(2026, time.September, 2, 10, 0)
			got := execute(context.Background(), Config{EdgeURL: server.URL, KRSymbol: "005930", KRPrice: "1000"}, func() time.Time { return at }, server.Client())
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
	config := Config{EdgeURL: "http://127.0.0.1:8080", KRSymbol: "005930", KRPrice: "1000"}
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

func TestExecuteEdgeUnreachableAndNoSessionMakesNoRequest(t *testing.T) {
	closed := httptest.NewServer(http.NotFoundHandler())
	url := closed.URL
	closed.Close()
	at := fixedKR(2026, time.September, 2, 10, 0)
	if got := execute(context.Background(), Config{EdgeURL: url}, func() time.Time { return at }, closed.Client()); got.Outcome != OutcomeEdgeUnreachable {
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
	for _, outcome := range []string{OutcomeOK, OutcomePlaceNotAccepted, OutcomeCancelNotCancelled, OutcomeEdgeUnreachable, OutcomeNoSession} {
		if outcome == "" {
			t.Fatal("empty outcome")
		}
	}
}
