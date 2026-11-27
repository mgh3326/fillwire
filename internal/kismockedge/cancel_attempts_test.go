package kismockedge

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	executioncontracts "github.com/mgh3326/fillwire/execution_contracts"
)

// seededCancelService places one accepted KIS mock order per canary-style
// command and one accepted Alpaca order, so the listing can be tested against
// markers on several scopes. The transport counts every broker call.
func seededCancelService(t *testing.T) (*Service, *countingTransport) {
	t.Helper()
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	var sequence atomic.Int64
	transport := &countingTransport{respond: func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPost && request.URL.Path == mockOrderPath {
			return testHTTPResponse(http.StatusOK, fmt.Sprintf(`{"rt_cd":"0","output":{"ODNO":"kis-%d","ORD_GNO_BRNO":"06010"}}`, sequence.Add(1))), nil
		}
		if request.Method == http.MethodPost {
			return testHTTPResponse(http.StatusOK, fmt.Sprintf(`{"id":"alpaca-%d","status":"accepted"}`, sequence.Add(1))), nil
		}
		t.Errorf("unexpected broker request %s %s", request.Method, request.URL.Path)
		return testHTTPResponse(http.StatusInternalServerError, ""), nil
	}}
	return newTestServiceWithBrokers(store, map[string]Broker{
		executioncontracts.AccountScopeKISMock:           testKISMockBroker(transport),
		executioncontracts.AccountScopeAlpacaPaperCrypto: testAlpacaPaperCryptoBroker(transport),
	}, true), transport
}

func placeAccepted(t *testing.T, service *Service, command executioncontracts.ExecutionCommandV1) {
	t.Helper()
	receipt, err := service.Process(context.Background(), command)
	if err != nil || receipt.Disposition != executioncontracts.DispositionAccepted {
		t.Fatalf("place %s receipt=%+v err=%v", command.CommandID, receipt, err)
	}
}

func markCancelUnknown(t *testing.T, service *Service, commandID string) {
	t.Helper()
	pending := service.cancelReceipt(commandID, CancelStateUnknown, ErrorBroker5xx)
	if _, reserved, err := service.Store.ReserveCancel(context.Background(), pending); err != nil || !reserved {
		t.Fatalf("reserve %s reserved=%t err=%v", commandID, reserved, err)
	}
}

func TestUnknownCancelAttemptsListsOnlyMockMarkersWithThePrefix(t *testing.T) {
	service, _ := seededCancelService(t)
	ctx := context.Background()

	// Canary, KIS mock, UNKNOWN: the one that must be listed.
	placeAccepted(t, service, testCommand("broker-edge-canary:unknown-1"))
	markCancelUnknown(t, service, "broker-edge-canary:unknown-1")

	// Canary, KIS mock, cancel later confirmed: a finalized marker is not UNKNOWN.
	placeAccepted(t, service, testCommand("broker-edge-canary:done-2"))
	markCancelUnknown(t, service, "broker-edge-canary:done-2")
	if _, err := service.Store.FinalizeCancel(ctx, service.cancelReceipt("broker-edge-canary:done-2", CancelStateCancelled, "")); err != nil {
		t.Fatal(err)
	}

	// Not a canary: same scope and state, different prefix.
	placeAccepted(t, service, testCommand("manual-3"))
	markCancelUnknown(t, service, "manual-3")

	// Canary prefix on a non-mock scope: must never be offered for carry-over.
	placeAccepted(t, service, testAlpacaCommand("broker-edge-canary:alpaca-4"))
	markCancelUnknown(t, service, "broker-edge-canary:alpaca-4")

	canary, err := service.UnknownCancelAttempts(ctx, "broker-edge-canary:")
	if err != nil {
		t.Fatal(err)
	}
	var canaryIDs []string
	for _, receipt := range canary {
		canaryIDs = append(canaryIDs, receipt.CommandID)
		if receipt.State != CancelStateUnknown {
			t.Fatalf("listed receipt state = %q", receipt.State)
		}
	}
	if want := []string{"broker-edge-canary:unknown-1"}; !reflect.DeepEqual(canaryIDs, want) {
		t.Fatalf("canary listing = %v, want %v", canaryIDs, want)
	}

	manual, err := service.UnknownCancelAttempts(ctx, "manual-")
	if err != nil || len(manual) != 1 || manual[0].CommandID != "manual-3" {
		t.Fatalf("manual listing = %+v err=%v", manual, err)
	}
}

func TestUnknownCancelAttemptsEndpointValidatesAndStaysReadOnly(t *testing.T) {
	service, transport := seededCancelService(t)
	placeAccepted(t, service, testCommand("broker-edge-canary:listed-1"))
	markCancelUnknown(t, service, "broker-edge-canary:listed-1")
	callsBefore := transport.calls.Load()
	server := httptest.NewServer(NewHandler(service))
	defer server.Close()

	for _, query := range []string{
		"",
		"state=CANCELLED&command_prefix=broker-edge-canary%3A",
		"state=UNKNOWN",
		"state=UNKNOWN&command_prefix=",
		"state=UNKNOWN&command_prefix=broker%20edge",
		"state=UNKNOWN&command_prefix=broker-edge-canary%3A&scope=kis_mock",
	} {
		response, err := http.Get(server.URL + "/v1/cancel-attempts?" + query)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("query %q status = %d, want 400", query, response.StatusCode)
		}
	}

	response, err := http.Get(server.URL + "/v1/cancel-attempts?state=UNKNOWN&command_prefix=broker-edge-canary%3A")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	var body struct {
		CancelAttempts []CancelReceipt `json:"cancel_attempts"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.CancelAttempts) != 1 || body.CancelAttempts[0].CommandID != "broker-edge-canary:listed-1" || body.CancelAttempts[0].ErrorCode != ErrorBroker5xx {
		t.Fatalf("cancel_attempts = %+v", body.CancelAttempts)
	}

	empty, err := http.Get(server.URL + "/v1/cancel-attempts?state=UNKNOWN&command_prefix=nothing-here-")
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Body.Close()
	raw, _ := io.ReadAll(empty.Body)
	if !strings.Contains(string(raw), `"cancel_attempts":[]`) {
		t.Fatalf("empty listing must be an array, got %s", raw)
	}

	if got := transport.calls.Load(); got != callsBefore {
		t.Fatalf("listing reached the broker: calls %d -> %d", callsBefore, got)
	}
}
