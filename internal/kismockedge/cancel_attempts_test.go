package kismockedge

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	executioncontracts "github.com/mgh3326/fillwire/execution_contracts"
)

// kisCancelScript answers the nth KIS mock cancel POST with status and body.
// It also counts every broker call that reaches the transport.
type kisCancelScript struct {
	mu      sync.Mutex
	cancels int
	answer  func(call int) (int, string)
}

const (
	cancelBodyCancelled = `{"rt_cd":"0"}`
	cancelBodyThrottled = `{"rt_cd":"1","msg_cd":"EGW00201","msg1":"초당 거래건수를 초과하였습니다."}`
	cancelBodyOther5xx  = `{"rt_cd":"1","msg_cd":"EGW00999","msg1":"unrelated server error"}`
)

// throttleEdge builds a KIS mock service whose cancel answers come from script.
// Placement always succeeds; the transport fails the test on any other request.
func throttleEdge(t *testing.T, script *kisCancelScript) (*Service, *countingTransport, *[]time.Duration) {
	t.Helper()
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	var placed atomic.Int64
	transport := &countingTransport{respond: func(request *http.Request) (*http.Response, error) {
		switch {
		case request.Method == http.MethodPost && request.URL.Path == mockOrderPath:
			return testHTTPResponse(http.StatusOK, fmt.Sprintf(`{"rt_cd":"0","output":{"ODNO":"kis-%d","ORD_GNO_BRNO":"06010"}}`, placed.Add(1))), nil
		case request.Method == http.MethodPost && request.URL.Path == mockCancelPath:
			script.mu.Lock()
			script.cancels++
			call := script.cancels
			script.mu.Unlock()
			status, body := script.answer(call)
			return testHTTPResponse(status, body), nil
		case request.Method == http.MethodPost:
			// Alpaca paper placement, used only by the scope-isolation case.
			return testHTTPResponse(http.StatusOK, fmt.Sprintf(`{"id":"alpaca-%d","status":"accepted"}`, placed.Add(1))), nil
		}
		t.Errorf("unexpected broker request %s %s", request.Method, request.URL.Path)
		return testHTTPResponse(http.StatusInternalServerError, ""), nil
	}}
	service := newTestServiceWithBrokers(store, map[string]Broker{
		executioncontracts.AccountScopeKISMock:           testKISMockBroker(transport),
		executioncontracts.AccountScopeAlpacaPaperCrypto: testAlpacaPaperCryptoBroker(transport),
	}, true)
	var sleeps []time.Duration
	service.Sleep = func(duration time.Duration) { sleeps = append(sleeps, duration) }
	return service, transport, &sleeps
}

func (script *kisCancelScript) count() int {
	script.mu.Lock()
	defer script.mu.Unlock()
	return script.cancels
}

func answerAlways(status int, body string) func(int) (int, string) {
	return func(int) (int, string) { return status, body }
}

func answerSequence(answers ...func(int) (int, string)) func(int) (int, string) {
	return func(call int) (int, string) {
		index := call - 1
		if index >= len(answers) {
			index = len(answers) - 1
		}
		return answers[index](call)
	}
}

func status(code int, body string) func(int) (int, string) {
	return answerAlways(code, body)
}

func placeAlpaca(t *testing.T, service *Service, commandID string) {
	t.Helper()
	receipt, err := service.Process(context.Background(), testAlpacaCommand(commandID))
	if err != nil || receipt.Disposition != executioncontracts.DispositionAccepted {
		t.Fatalf("place alpaca %s receipt=%+v err=%v", commandID, receipt, err)
	}
}

func placeKISMock(t *testing.T, service *Service, commandID string) {
	t.Helper()
	receipt, err := service.Process(context.Background(), testCommand(commandID))
	if err != nil || receipt.Disposition != executioncontracts.DispositionAccepted {
		t.Fatalf("place %s receipt=%+v err=%v", commandID, receipt, err)
	}
}

func TestEGW00201CancelIsRecordedAsRejectedThrottledAndResentOnce(t *testing.T) {
	script := &kisCancelScript{answer: answerSequence(status(http.StatusInternalServerError, cancelBodyThrottled), status(http.StatusOK, cancelBodyCancelled))}
	service, _, sleeps := throttleEdge(t, script)
	placeKISMock(t, service, "broker-edge-canary:throttle-1")

	first, err := service.Cancel(context.Background(), "broker-edge-canary:throttle-1")
	if err != nil || first.State != CancelStateRejectedThrottled || first.ErrorCode != kisThrottleMsgCode {
		t.Fatalf("first cancel = %+v err=%v, want REJECTED_THROTTLED/EGW00201", first, err)
	}
	if script.count() != 1 {
		t.Fatalf("broker cancels after first = %d", script.count())
	}

	second, err := service.Cancel(context.Background(), "broker-edge-canary:throttle-1")
	if err != nil || second.State != CancelStateCancelled {
		t.Fatalf("re-send cancel = %+v err=%v, want CANCELLED", second, err)
	}
	if script.count() != 2 {
		t.Fatalf("broker cancels after re-send = %d, want 2", script.count())
	}
	if want := []time.Duration{cancelResendGap}; !reflect.DeepEqual(*sleeps, want) {
		t.Fatalf("re-send pacing = %v, want %v", *sleeps, want)
	}

	// A confirmed CANCELLED is replayed and never sent again.
	third, err := service.Cancel(context.Background(), "broker-edge-canary:throttle-1")
	if err != nil || third.State != CancelStateCancelled || script.count() != 2 {
		t.Fatalf("replay = %+v err=%v cancels=%d", third, err, script.count())
	}
}

func TestOtherCancelFailureStaysUnknownAndIsNeverResent(t *testing.T) {
	script := &kisCancelScript{answer: status(http.StatusInternalServerError, cancelBodyOther5xx)}
	service, _, sleeps := throttleEdge(t, script)
	placeKISMock(t, service, "broker-edge-canary:unknown-1")

	first, err := service.Cancel(context.Background(), "broker-edge-canary:unknown-1")
	if err != nil || first.State != CancelStateUnknown || first.ErrorCode != ErrorBroker5xx {
		t.Fatalf("first cancel = %+v err=%v, want UNKNOWN/broker_5xx", first, err)
	}
	again, err := service.Cancel(context.Background(), "broker-edge-canary:unknown-1")
	if err != nil || again.State != first.State || again.ErrorCode != first.ErrorCode || again.RecordedAt != first.RecordedAt {
		t.Fatalf("replay = %+v err=%v, want the stored UNKNOWN unchanged", again, err)
	}
	if script.count() != 1 || len(*sleeps) != 0 {
		t.Fatalf("UNKNOWN was re-sent: cancels=%d sleeps=%v", script.count(), *sleeps)
	}
}

func TestCancelAttemptCapStopsResendsAtThree(t *testing.T) {
	script := &kisCancelScript{answer: status(http.StatusInternalServerError, cancelBodyThrottled)}
	service, _, _ := throttleEdge(t, script)
	placeKISMock(t, service, "broker-edge-canary:cap-1")

	for attempt := 1; attempt <= maxCancelAttempts; attempt++ {
		got, err := service.Cancel(context.Background(), "broker-edge-canary:cap-1")
		if err != nil || got.State != CancelStateRejectedThrottled {
			t.Fatalf("attempt %d = %+v err=%v", attempt, got, err)
		}
	}
	if script.count() != maxCancelAttempts {
		t.Fatalf("broker cancels = %d, want %d", script.count(), maxCancelAttempts)
	}
	after, err := service.Cancel(context.Background(), "broker-edge-canary:cap-1")
	if err != nil || after.State != CancelStateRejectedThrottled || script.count() != maxCancelAttempts {
		t.Fatalf("over cap: %+v err=%v cancels=%d, want no further send", after, err, script.count())
	}
	latest, found, err := service.Store.FindLatestCancelAttempt(context.Background(), "broker-edge-canary:cap-1")
	if err != nil || !found || latest.AttemptNo != maxCancelAttempts {
		t.Fatalf("latest attempt = %+v found=%t err=%v", latest, found, err)
	}
}

func TestCarryOverCandidatesListUnconfirmedCanaryCancelsInWindowAndScope(t *testing.T) {
	script := &kisCancelScript{answer: answerSequence(
		status(http.StatusInternalServerError, cancelBodyOther5xx),
		status(http.StatusInternalServerError, cancelBodyThrottled),
		status(http.StatusOK, cancelBodyCancelled),
	)}
	service, _, _ := throttleEdge(t, script)
	day := time.Date(2026, time.September, 1, 12, 0, 0, 0, time.UTC)
	service.Now = func() time.Time { return day }
	ctx := context.Background()

	// never-sent: placed, cancel never reached the edge.
	placeKISMock(t, service, "broker-edge-canary:unsent-a")
	// UNKNOWN after a non-throttle failure.
	placeKISMock(t, service, "broker-edge-canary:unknown-b")
	if _, err := service.Cancel(ctx, "broker-edge-canary:unknown-b"); err != nil {
		t.Fatal(err)
	}
	// REJECTED_THROTTLED after a throttle.
	placeKISMock(t, service, "broker-edge-canary:throttled-d")
	if _, err := service.Cancel(ctx, "broker-edge-canary:throttled-d"); err != nil {
		t.Fatal(err)
	}
	// Confirmed CANCELLED: never listed.
	placeKISMock(t, service, "broker-edge-canary:done-c")
	if _, err := service.Cancel(ctx, "broker-edge-canary:done-c"); err != nil {
		t.Fatal(err)
	}
	// Not a canary command.
	placeKISMock(t, service, "manual-e")
	// Canary prefix on the Alpaca scope: a different market, never listed for KIS mock.
	placeAlpaca(t, service, "broker-edge-canary:alpaca-f")
	// Canary, KIS mock, placed on the previous day, never sent: outside the window.
	service.Now = func() time.Time { return day.Add(-24 * time.Hour) }
	placeKISMock(t, service, "broker-edge-canary:old-g")
	service.Now = func() time.Time { return day }

	from := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	before := from.Add(24 * time.Hour)
	got, err := service.CancelCarryOverCandidates(ctx, executioncontracts.AccountScopeKISMock, "broker-edge-canary:", from, before)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, candidate := range got {
		ids = append(ids, candidate.CommandID)
	}
	// Unsent first; the rest in placement order, which ties on this fixed clock
	// and so falls back to command ID.
	want := []string{"broker-edge-canary:unsent-a", "broker-edge-canary:throttled-d", "broker-edge-canary:unknown-b"}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("candidates = %v, want %v", ids, want)
	}
	if got[0].Attempts != 0 || got[1].LatestState != string(CancelStateRejectedThrottled) || got[2].LatestState != string(CancelStateUnknown) {
		t.Fatalf("candidate evidence = %+v", got)
	}
	if !got[0].Actionable() || !got[1].Actionable() || got[2].Actionable() {
		t.Fatalf("actionable = %t %t %t, want true true false", got[0].Actionable(), got[1].Actionable(), got[2].Actionable())
	}
}

func TestCancelCarryOverEndpointValidatesAndStaysReadOnly(t *testing.T) {
	script := &kisCancelScript{answer: status(http.StatusInternalServerError, cancelBodyOther5xx)}
	service, transport, _ := throttleEdge(t, script)
	placeKISMock(t, service, "broker-edge-canary:listed-1")
	if _, err := service.Cancel(context.Background(), "broker-edge-canary:listed-1"); err != nil {
		t.Fatal(err)
	}
	callsBefore := transport.calls.Load()
	server := httptest.NewServer(NewHandler(service))
	defer server.Close()

	window := "scope=kis_mock&placed_from=2026-09-01T00:00:00Z&placed_before=2026-09-02T00:00:00Z&command_prefix=broker-edge-canary%3A"
	for _, query := range []string{
		"",
		strings.Replace(window, "scope=kis_mock", "scope=kis_live", 1),
		strings.Replace(window, "placed_from=2026-09-01T00:00:00Z", "placed_from=yesterday", 1),
		strings.Replace(window, "placed_before=2026-09-02T00:00:00Z", "placed_before=2026-09-01T00:00:00Z", 1),
		strings.Replace(window, "command_prefix=broker-edge-canary%3A", "command_prefix=", 1),
		window + "&state=UNKNOWN",
		"scope=kis_mock&placed_from=2026-09-01T00:00:00Z&placed_before=2026-09-02T00:00:00Z",
	} {
		response, err := http.Get(server.URL + "/v1/cancel-carryover?" + query)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("query %q status = %d, want 400", query, response.StatusCode)
		}
	}

	response, err := http.Get(server.URL + "/v1/cancel-carryover?" + window)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var body struct {
		Candidates []struct {
			CommandID   string `json:"command_id"`
			CancelState string `json:"cancel_state"`
			Attempts    int    `json:"attempts"`
		} `json:"candidates"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Candidates) != 1 || body.Candidates[0].CommandID != "broker-edge-canary:listed-1" || body.Candidates[0].CancelState != "UNKNOWN" || body.Candidates[0].Attempts != 1 {
		t.Fatalf("candidates = %+v", body.Candidates)
	}

	empty, err := http.Get(server.URL + "/v1/cancel-carryover?" + strings.Replace(window, "broker-edge-canary%3A", "nothing-here-", 1))
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Body.Close()
	raw, _ := io.ReadAll(empty.Body)
	if !strings.Contains(string(raw), `"candidates":[]`) {
		t.Fatalf("empty listing must be an array, got %s", raw)
	}
	if got := transport.calls.Load(); got != callsBefore {
		t.Fatalf("listing reached the broker: calls %d -> %d", callsBefore, got)
	}
}

func TestLegacyCancelAttemptTableMigratesMarkerToAttemptOne(t *testing.T) {
	path := filepath.Join(t.TempDir(), "edge.sqlite")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	// Recreate the pre-migration table shape and one legacy UNKNOWN marker.
	for _, statement := range []string{
		`DROP TABLE cancel_attempts`,
		`CREATE TABLE cancel_attempts (
			command_id TEXT PRIMARY KEY REFERENCES commands(command_id),
			state TEXT NOT NULL CHECK (state IN ('CANCELLED', 'NOT_FOUND', 'UNKNOWN')),
			error_code TEXT,
			recorded_at TEXT NOT NULL
		)`,
		`INSERT INTO cancel_attempts (command_id, state, error_code, recorded_at) VALUES ('broker-edge-canary:legacy-1', 'UNKNOWN', 'broker_5xx', '2026-11-27T00:13:00Z')`,
	} {
		if _, err := store.db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reopen after legacy table: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	latest, found, err := reopened.FindLatestCancelAttempt(context.Background(), "broker-edge-canary:legacy-1")
	if err != nil || !found || latest.AttemptNo != 1 || latest.State != CancelStateUnknown || latest.ErrorCode != ErrorBroker5xx {
		t.Fatalf("migrated marker = %+v found=%t err=%v", latest, found, err)
	}
	// A reopen after migration is a no-op: the attempt row is still the only one.
	var rows int
	if err := reopened.db.QueryRow(`SELECT COUNT(*) FROM cancel_attempts`).Scan(&rows); err != nil && err != sql.ErrNoRows {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("migrated rows = %d, want 1", rows)
	}
}

// An ambiguous place that a command-scoped resolution later proved ACCEPTED is
// still a canary order resting on the account. It must be listed so the run
// blocks placement, but the edge never cancels it, so it is never actionable.
func TestEvidenceResolvedMockOrderIsListedAndNeverCancelled(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	var cancels atomic.Int64
	transport := &countingTransport{respond: func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPost && request.URL.Path == mockOrderPath {
			return testHTTPResponse(http.StatusInternalServerError, cancelBodyOther5xx), nil
		}
		if request.URL.Path == mockCancelPath {
			cancels.Add(1)
		}
		return testHTTPResponse(http.StatusInternalServerError, ""), nil
	}}
	service := newTestService(store, testKISMockBroker(transport), true)
	day := time.Date(2026, time.September, 1, 12, 0, 0, 0, time.UTC)
	service.Now = func() time.Time { return day }
	ctx := context.Background()
	const commandID = "broker-edge-canary:resolved-1"

	receipt, err := service.Process(ctx, testCommand(commandID))
	if err != nil || receipt.Disposition != executioncontracts.DispositionUnknown {
		t.Fatalf("ambiguous place = %+v err=%v, want UNKNOWN", receipt, err)
	}
	if _, err := store.ResolveUnknown(ctx, commandID, executioncontracts.DispositionAccepted, "kis-resolved-9", "", day); err != nil {
		t.Fatal(err)
	}

	cancel, _ := service.Cancel(ctx, commandID)
	if cancel.State != CancelStateUnknown || cancel.ErrorCode != ErrorCancelNotEligible || cancels.Load() != 0 {
		t.Fatalf("cancel = %+v broker cancels=%d, want not eligible with no send", cancel, cancels.Load())
	}

	from := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	got, err := service.CancelCarryOverCandidates(ctx, executioncontracts.AccountScopeKISMock, "broker-edge-canary:", from, from.Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].CommandID != commandID || !got[0].ResolvedByEvidence || got[0].Actionable() {
		t.Fatalf("candidates = %+v, want the resolved order listed and not actionable", got)
	}
}
