package canary

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	executioncontracts "github.com/mgh3326/fillwire/execution_contracts"
)

// callLog records edge calls in arrival order. The stub server and the injected
// sleep both append to it, so the test can assert the sequence directly.
type callLog struct {
	mu     sync.Mutex
	events []string
}

func (log *callLog) add(event string) {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.events = append(log.events, event)
}

func (log *callLog) snapshot() []string {
	log.mu.Lock()
	defer log.mu.Unlock()
	return append([]string(nil), log.events...)
}

// pacingEdge is a loopback edge stub. lookupStatus and lookupBody answer the
// carry-over listing; cancelState answers every cancel dispatch.
func pacingEdge(t *testing.T, log *callLog, lookupStatus int, lookupBody, cancelState string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("content-type", "application/json")
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/v1/cancel-carryover":
			query := request.URL.Query()
			if query.Get("scope") != "kis_mock" || query.Get("command_prefix") != commandIDPrefix || query.Get("placed_from") == "" || query.Get("placed_before") == "" {
				t.Errorf("lookup query = %q", request.URL.RawQuery)
			}
			log.add("lookup")
			writer.WriteHeader(lookupStatus)
			_, _ = io.WriteString(writer, lookupBody)
		case request.Method == http.MethodGet && request.URL.Path == "/v1/price-band":
			log.add("band")
			_ = json.NewEncoder(writer).Encode(executioncontracts.PriceBandV1{SchemaVersion: executioncontracts.PriceBandV1SchemaVersion, StockCode: "005930", LowerLimit: "49700"})
		case request.Method == http.MethodPost && request.URL.Path == "/v1/commands":
			log.add("place")
			_ = json.NewEncoder(writer).Encode(executioncontracts.ExecutionReceiptV1{Disposition: executioncontracts.DispositionAccepted, BrokerOrderID: "9001"})
		case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/cancel"):
			log.add("cancel")
			_ = json.NewEncoder(writer).Encode(cancelReceipt{State: cancelState})
		default:
			t.Errorf("unexpected request %s %s", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
}

func recordingSleep(log *callLog) func(time.Duration) {
	return func(duration time.Duration) {
		log.add("sleep:" + duration.String())
	}
}

func TestCancelIsPacedAfterPlaceAck(t *testing.T) {
	log := &callLog{}
	server := pacingEdge(t, log, http.StatusOK, `{"candidates":[]}`, "CANCELLED")
	defer server.Close()
	at := fixedKR(2026, time.September, 2, 10, 0)
	budgets := Options{Sleep: recordingSleep(log)}.timeouts()

	got := execute(context.Background(), Config{EdgeURL: server.URL, KRSymbol: "005930"}, func() time.Time { return at }, server.Client(), budgets)

	if got.Outcome != OutcomeOK {
		t.Fatalf("outcome = %q, want %q", got.Outcome, OutcomeOK)
	}
	// The fixed clock leaves no elapsed time, so the full fallback gap is waited
	// between the place ack and the single cancel.
	want := []string{"band", "place", "sleep:1.1s", "cancel"}
	if events := log.snapshot(); !reflect.DeepEqual(events, want) {
		t.Fatalf("call order = %v, want %v", events, want)
	}
}

func TestCancelPacerWaitsOnlyTheRemainingGap(t *testing.T) {
	current := time.Date(2026, time.September, 2, 1, 0, 0, 0, time.UTC)
	var slept []time.Duration
	pacer := cancelPacer{
		now:   func() time.Time { return current },
		sleep: func(duration time.Duration) { slept = append(slept, duration) },
		gap:   1100 * time.Millisecond,
	}

	pacer.wait()
	if len(slept) != 0 {
		t.Fatalf("waited %v before any edge response", slept)
	}
	pacer.mark()
	current = current.Add(400 * time.Millisecond)
	pacer.wait()
	current = current.Add(time.Second)
	pacer.wait()

	if want := []time.Duration{700 * time.Millisecond}; !reflect.DeepEqual(slept, want) {
		t.Fatalf("slept = %v, want %v", slept, want)
	}
}

func TestCarryOverCancelsOnceAndPlacesNothing(t *testing.T) {
	log := &callLog{}
	server := pacingEdge(t, log, http.StatusOK, `{"candidates":[{"command_id":"broker-edge-canary:old-1","account_scope":"kis_mock","cancel_state":"","actionable":true}]}`, "UNKNOWN")
	defer server.Close()
	at := fixedKR(2026, time.September, 2, 10, 0)

	got, carried := carryOverRun(context.Background(), Config{EdgeURL: server.URL}, func() time.Time { return at }, server.Client(), Options{Sleep: recordingSleep(log)}.timeouts())

	if !carried {
		t.Fatal("carry-over pending must end the run before placement")
	}
	if got.Outcome != OutcomeCarryOverPending {
		t.Fatalf("outcome = %q, want %q", got.Outcome, OutcomeCarryOverPending)
	}
	if got.CarryOver == nil || got.CarryOver.Pending != 1 || got.CarryOver.CommandID != "broker-edge-canary:old-1" || got.CarryOver.CancelState != "UNKNOWN" {
		t.Fatalf("carry_over = %+v", got.CarryOver)
	}
	// The carry-over cancel is paced after the lookup, and nothing else runs.
	if want := []string{"lookup", "sleep:1.1s", "cancel"}; !reflect.DeepEqual(log.snapshot(), want) {
		t.Fatalf("edge calls = %v, want %v (no band, no place)", log.snapshot(), want)
	}
}

func TestRunPendingCarryOverExitsNonZeroWithoutPlacement(t *testing.T) {
	log := &callLog{}
	server := pacingEdge(t, log, http.StatusOK, `{"candidates":[{"command_id":"broker-edge-canary:old-1","account_scope":"kis_mock","cancel_state":"","actionable":true}]}`, "UNKNOWN")
	defer server.Close()
	directory := t.TempDir()
	at := fixedKR(2026, time.September, 2, 10, 0)
	var stdout strings.Builder
	lookup := func(key string) string {
		return map[string]string{"CANARY_EDGE_URL": server.URL, "CANARY_TEXTFILE_DIR": directory}[key]
	}

	code := Run(context.Background(), Options{Now: func() time.Time { return at }, Client: server.Client(), Stdout: &stdout, Stderr: io.Discard, Lookup: lookup, Sleep: recordingSleep(log)})

	if code != 1 {
		t.Fatalf("Run() = %d, want 1: a carried-over cancel is not a proof of placement", code)
	}
	if !strings.Contains(stdout.String(), `"outcome":"carry_over_pending"`) {
		t.Fatalf("stdout = %s", stdout.String())
	}
	if events := log.snapshot(); strings.Contains(strings.Join(events, ","), "place") {
		t.Fatalf("a run with a pending carry-over placed an order: %v", events)
	}
	text, err := os.ReadFile(filepath.Join(directory, textfileName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(text), `broker_edge_canary_result{scope="kis_mock",outcome="carry_over_pending"} 1`) {
		t.Fatalf("textfile = %s", text)
	}
}

func TestRunLooksUpBeforePlacingWhenNothingIsPending(t *testing.T) {
	log := &callLog{}
	server := pacingEdge(t, log, http.StatusOK, `{"candidates":[]}`, "CANCELLED")
	defer server.Close()
	directory := t.TempDir()
	at := fixedKR(2026, time.September, 2, 10, 0)
	var stdout strings.Builder
	lookup := func(key string) string {
		return map[string]string{"CANARY_EDGE_URL": server.URL, "CANARY_TEXTFILE_DIR": directory}[key]
	}

	code := Run(context.Background(), Options{Now: func() time.Time { return at }, Client: server.Client(), Stdout: &stdout, Stderr: io.Discard, Lookup: lookup, Sleep: recordingSleep(log)})

	if code != 0 {
		t.Fatalf("Run() = %d, stdout %s", code, stdout.String())
	}
	want := []string{"lookup", "band", "place", "sleep:1.1s", "cancel"}
	if events := log.snapshot(); !reflect.DeepEqual(events, want) {
		t.Fatalf("call order = %v, want %v", events, want)
	}
}

func TestCarryOverLookupFailureFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{"edge error", http.StatusInternalServerError, `{"error_code":"storage_failure"}`},
		{"missing key is malformed", http.StatusOK, `{}`},
		{"foreign command ID", http.StatusOK, `{"candidates":[{"command_id":"order-123","account_scope":"kis_mock","cancel_state":""}]}`},
		{"bare prefix is not a command", http.StatusOK, `{"candidates":[{"command_id":"broker-edge-canary:","account_scope":"kis_mock","cancel_state":""}]}`},
		{"other scope", http.StatusOK, `{"candidates":[{"command_id":"broker-edge-canary:x","account_scope":"kis_mock_us","cancel_state":""}]}`},
		{"missing actionable flag", http.StatusOK, `{"candidates":[{"command_id":"broker-edge-canary:x","account_scope":"kis_mock","cancel_state":""}]}`},
		{"unexpected cancel state", http.StatusOK, `{"candidates":[{"command_id":"broker-edge-canary:x","account_scope":"kis_mock","cancel_state":"CANCELLED"}]}`},
		{"not JSON", http.StatusOK, `<html>`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			log := &callLog{}
			server := pacingEdge(t, log, test.status, test.body, "CANCELLED")
			defer server.Close()
			at := fixedKR(2026, time.September, 2, 10, 0)

			got, carried := carryOverRun(context.Background(), Config{EdgeURL: server.URL}, func() time.Time { return at }, server.Client(), Options{Sleep: recordingSleep(log)}.timeouts())

			if !carried || got.Outcome != OutcomeCarryOverCheckFailed {
				t.Fatalf("carried=%t outcome=%q, want fail-closed %q", carried, got.Outcome, OutcomeCarryOverCheckFailed)
			}
			if want := []string{"lookup"}; !reflect.DeepEqual(log.snapshot(), want) {
				t.Fatalf("edge calls = %v, want only the lookup", log.snapshot())
			}
		})
	}
}

func TestCarryOverNoSessionMakesNoRequest(t *testing.T) {
	log := &callLog{}
	server := pacingEdge(t, log, http.StatusOK, `{"candidates":[]}`, "CANCELLED")
	defer server.Close()
	// 20:00 KST is after the KR window and 07:00 New York is before the US one.
	offHours := fixedKR(2026, time.September, 2, 20, 0)

	if _, carried := carryOverRun(context.Background(), Config{EdgeURL: server.URL}, func() time.Time { return offHours }, server.Client(), Options{}.timeouts()); carried {
		t.Fatal("no_session must not run the carry-over lookup")
	}
	if events := log.snapshot(); len(events) != 0 {
		t.Fatalf("no_session made edge calls: %v", events)
	}
}

func TestCarryOverWithEmptyListingProceedsNormally(t *testing.T) {
	log := &callLog{}
	server := pacingEdge(t, log, http.StatusOK, `{"candidates":[]}`, "CANCELLED")
	defer server.Close()
	at := fixedKR(2026, time.September, 2, 10, 0)

	_, carried := carryOverRun(context.Background(), Config{EdgeURL: server.URL}, func() time.Time { return at }, server.Client(), Options{Sleep: recordingSleep(log)}.timeouts())

	if carried {
		t.Fatal("an empty listing must not end the run")
	}
	if want := []string{"lookup"}; !reflect.DeepEqual(log.snapshot(), want) {
		t.Fatalf("edge calls = %v, want only the lookup", log.snapshot())
	}
}

func TestCarryOverLookupUsesTheScopeTradingDay(t *testing.T) {
	var seen []url.Values
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/v1/cancel-carryover" {
			seen = append(seen, request.URL.Query())
		}
		_, _ = io.WriteString(writer, `{"candidates":[]}`)
	}))
	defer server.Close()
	// 2026-03-09 13:35 UTC is 09:35 EDT: the US window of that New York day.
	usOpen := time.Date(2026, time.March, 9, 13, 35, 0, 0, time.UTC)
	// 2026-09-02 10:00 KST: the KR window of that Seoul day.
	krOpen := fixedKR(2026, time.September, 2, 10, 0)

	if _, carried := carryOverRun(context.Background(), Config{EdgeURL: server.URL}, func() time.Time { return usOpen }, server.Client(), Options{}.timeouts()); carried {
		t.Fatal("empty listing must not end the run")
	}
	if _, carried := carryOverRun(context.Background(), Config{EdgeURL: server.URL}, func() time.Time { return krOpen }, server.Client(), Options{}.timeouts()); carried {
		t.Fatal("empty listing must not end the run")
	}
	if len(seen) != 2 {
		t.Fatalf("lookups = %d", len(seen))
	}
	us, kr := seen[0], seen[1]
	if us.Get("scope") != "kis_mock_us" || us.Get("placed_from") != "2026-03-09T00:00:00-04:00" || us.Get("placed_before") != "2026-03-10T00:00:00-04:00" {
		t.Fatalf("US window = %v", us)
	}
	if kr.Get("scope") != "kis_mock" || kr.Get("placed_from") != "2026-09-02T00:00:00+09:00" || kr.Get("placed_before") != "2026-09-03T00:00:00+09:00" {
		t.Fatalf("KR window = %v", kr)
	}
}

func TestCarryOverLookupStopsAtTheRunOverallDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// Hold the lookup until the caller gives up.
		<-request.Context().Done()
	}))
	defer server.Close()
	directory := t.TempDir()
	at := fixedKR(2026, time.September, 2, 10, 0)
	var stdout strings.Builder
	lookup := func(key string) string {
		return map[string]string{"CANARY_EDGE_URL": server.URL, "CANARY_TEXTFILE_DIR": directory}[key]
	}

	started := time.Now()
	code := Run(context.Background(), Options{Now: func() time.Time { return at }, Client: server.Client(), Stdout: &stdout, Stderr: io.Discard, Lookup: lookup, OverallTimeout: 200 * time.Millisecond, Sleep: func(time.Duration) {}})
	elapsed := time.Since(started)

	if code != 1 || elapsed > 5*time.Second {
		t.Fatalf("Run() = %d after %s; the overall deadline must bound the lookup", code, elapsed)
	}
	if !strings.Contains(stdout.String(), `"outcome":"carry_over_check_failed"`) {
		t.Fatalf("stdout = %s", stdout.String())
	}
}

func TestCancelStillReachesEdgeWhenRunDeadlineExpiresDuringPacing(t *testing.T) {
	log := &callLog{}
	server := pacingEdge(t, log, http.StatusOK, `{"candidates":[]}`, "CANCELLED")
	defer server.Close()
	at := fixedKR(2026, time.September, 2, 10, 0)
	// The overall deadline (200 ms) expires during the 1 s pacing wait. The
	// acknowledged order must still get its cancel, bounded by the cancel budget.
	budgets := Options{OverallTimeout: 200 * time.Millisecond, CancelGap: time.Second, Sleep: func(d time.Duration) {
		log.add("sleep")
		time.Sleep(d)
	}}.timeouts()

	got := execute(context.Background(), Config{EdgeURL: server.URL, KRSymbol: "005930"}, func() time.Time { return at }, server.Client(), budgets)

	if got.Outcome != OutcomeOK {
		t.Fatalf("outcome = %q (error %q): the cancel of an acknowledged order was lost to the expired run deadline", got.Outcome, got.ErrorCode)
	}
	if want := []string{"band", "place", "sleep", "cancel"}; !reflect.DeepEqual(log.snapshot(), want) {
		t.Fatalf("call order = %v, want %v", log.snapshot(), want)
	}
}

func TestCarryOverAttemptsOnlyAnActionableCandidate(t *testing.T) {
	log := &callLog{}
	// The head is UNKNOWN, which the edge answers from stored state without sending.
	// The second candidate is never-sent and is the one the edge can act on.
	server := pacingEdge(t, log, http.StatusOK, `{"candidates":[{"command_id":"broker-edge-canary:head-1","account_scope":"kis_mock","cancel_state":"UNKNOWN","actionable":false},{"command_id":"broker-edge-canary:next-2","account_scope":"kis_mock","cancel_state":"","actionable":true}]}`, "CANCELLED")
	defer server.Close()
	at := fixedKR(2026, time.September, 2, 10, 0)

	got, carried := carryOverRun(context.Background(), Config{EdgeURL: server.URL}, func() time.Time { return at }, server.Client(), Options{Sleep: recordingSleep(log)}.timeouts())

	if !carried || got.Outcome != OutcomeCarryOverPending {
		t.Fatalf("carried=%t outcome=%q", carried, got.Outcome)
	}
	if got.CarryOver == nil || got.CarryOver.Pending != 2 || got.CarryOver.CommandID != "broker-edge-canary:next-2" {
		t.Fatalf("carry_over = %+v", got.CarryOver)
	}
	if want := []string{"lookup", "sleep:1.1s", "cancel"}; !reflect.DeepEqual(log.snapshot(), want) {
		t.Fatalf("edge calls = %v, want %v", log.snapshot(), want)
	}
}

func TestCarryOverNeverSendsWhenNoCandidateIsActionable(t *testing.T) {
	log := &callLog{}
	server := pacingEdge(t, log, http.StatusOK, `{"candidates":[{"command_id":"broker-edge-canary:stuck-1","account_scope":"kis_mock","cancel_state":"UNKNOWN","actionable":false}]}`, "CANCELLED")
	defer server.Close()
	at := fixedKR(2026, time.September, 2, 10, 0)

	got, carried := carryOverRun(context.Background(), Config{EdgeURL: server.URL}, func() time.Time { return at }, server.Client(), Options{Sleep: recordingSleep(log)}.timeouts())

	if !carried || got.Outcome != OutcomeCarryOverPending || got.CarryOver == nil || got.CarryOver.Pending != 1 {
		t.Fatalf("carried=%t result=%+v", carried, got)
	}
	if want := []string{"lookup"}; !reflect.DeepEqual(log.snapshot(), want) {
		t.Fatalf("edge calls = %v, want only the lookup", log.snapshot())
	}
}
