package kismockedge

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	"github.com/mgh3326/broker-edge/internal/kismockread"
)

var resolveTestNow = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

// resolveTestService wires a Service whose Send always lands UNKNOWN
// (broker_timeout) so the command stays pending until the resolve endpoint
// evidences it.
func resolveTestService(t *testing.T, store *Store, reader OrderHistoryReader) *Service {
	t.Helper()
	return &Service{
		Store:        store,
		PlaceEnabled: true,
		Brokers: map[string]Broker{
			executioncontracts.AccountScopeKISMock: &fakeBroker{result: BrokerResult{ErrorCode: ErrorBrokerTimeout}},
		},
		OrderReader: reader,
		Now:         func() time.Time { return resolveTestNow },
	}
}

func postResolve(t *testing.T, server *httptest.Server, commandID string) (int, executioncontracts.CommandCheckV1) {
	t.Helper()
	response, err := http.Post(server.URL+"/v1/commands/"+commandID+"/resolve", "application/json", nil)
	if err != nil {
		t.Fatalf("resolve request: %v", err)
	}
	defer response.Body.Close()
	var check executioncontracts.CommandCheckV1
	// Non-2xx bodies are a shadow error object; decoding stays best-effort.
	_ = json.NewDecoder(response.Body).Decode(&check)
	return response.StatusCode, check
}

func TestResolveEndpointFindsRestingOrder(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	service := resolveTestService(t, store, fakeOrderHistoryReader{orders: []kismockread.DomesticOrder{{
		BrokerOrderID: "9001", Side: "buy", StockCode: "005930",
		Quantity: "001", Price: "070000",
		OrderedAt: resolveTestNow.Add(30 * time.Second),
	}}})
	server := httptest.NewServer(NewHandler(service))
	defer server.Close()
	receipt, err := service.Process(context.Background(), testCommand("resolve-resting"))
	if err != nil || receipt.Disposition != executioncontracts.DispositionUnknown {
		t.Fatalf("initial receipt = %+v err=%v", receipt, err)
	}
	status, check := postResolve(t, server, "resolve-resting")
	if status != http.StatusOK {
		t.Fatalf("resolve status = %d, check = %+v", status, check)
	}
	if check.Disposition != executioncontracts.DispositionAccepted || check.BrokerOrderID != "9001" ||
		check.EvidenceRead != "completed" || check.OrdersSeen != 1 || check.Matched != 1 {
		t.Fatalf("check = %+v", check)
	}
	// The now-ACCEPTED command is a valid cancel target for the same command id.
	target, found, err := store.FindCancelTarget(context.Background(), "resolve-resting")
	if err != nil || !found || target.BrokerOrderID != "9001" {
		t.Fatalf("cancel target = %+v found=%t err=%v", target, found, err)
	}
}

func TestResolveEndpointForeignOrderNeverMatches(t *testing.T) {
	// A same-symbol foreign order with a different price must never evidence
	// this command: matching is by the command's own stored facts, never by
	// symbol alone. The command stays UNKNOWN and uncancellable.
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	service := resolveTestService(t, store, fakeOrderHistoryReader{orders: []kismockread.DomesticOrder{{
		BrokerOrderID: "foreign-1", Side: "buy", StockCode: "005930",
		Quantity: "001", Price: "071000",
		OrderedAt: resolveTestNow.Add(30 * time.Second),
	}}})
	server := httptest.NewServer(NewHandler(service))
	defer server.Close()
	receipt, err := service.Process(context.Background(), testCommand("resolve-foreign"))
	if err != nil || receipt.Disposition != executioncontracts.DispositionUnknown {
		t.Fatalf("initial receipt = %+v err=%v", receipt, err)
	}
	status, check := postResolve(t, server, "resolve-foreign")
	if status != http.StatusOK {
		t.Fatalf("resolve status = %d, check = %+v", status, check)
	}
	if check.Disposition != executioncontracts.DispositionUnknown || check.OrdersSeen != 1 || check.Matched != 0 {
		t.Fatalf("foreign order must not match: %+v", check)
	}
	if _, found, err := store.FindCancelTarget(context.Background(), "resolve-foreign"); err != nil || found {
		t.Fatalf("unresolved command must never be a cancel target: found=%t err=%v", found, err)
	}
}

func TestResolveEndpointAmbiguousMatchesStayUnknown(t *testing.T) {
	order := kismockread.DomesticOrder{
		Side: "buy", StockCode: "005930", Quantity: "001", Price: "070000",
		OrderedAt: resolveTestNow.Add(30 * time.Second),
	}
	first, second := order, order
	first.BrokerOrderID, second.BrokerOrderID = "9001", "9002"
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	service := resolveTestService(t, store, fakeOrderHistoryReader{orders: []kismockread.DomesticOrder{first, second}})
	server := httptest.NewServer(NewHandler(service))
	defer server.Close()
	if _, err := service.Process(context.Background(), testCommand("resolve-ambiguous")); err != nil {
		t.Fatal(err)
	}
	status, check := postResolve(t, server, "resolve-ambiguous")
	if status != http.StatusOK {
		t.Fatalf("resolve status = %d, check = %+v", status, check)
	}
	if check.Disposition != executioncontracts.DispositionUnknown || check.Matched != 2 {
		t.Fatalf("ambiguous evidence must stay UNKNOWN: %+v", check)
	}
	if _, found, _ := store.FindCancelTarget(context.Background(), "resolve-ambiguous"); found {
		t.Fatal("ambiguous command became a cancel target")
	}
}

func TestResolveEndpointEmptyDayWithinGraceStaysUnknown(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	service := resolveTestService(t, store, fakeOrderHistoryReader{})
	server := httptest.NewServer(NewHandler(service))
	defer server.Close()
	if _, err := service.Process(context.Background(), testCommand("resolve-empty-day")); err != nil {
		t.Fatal(err)
	}
	status, check := postResolve(t, server, "resolve-empty-day")
	if status != http.StatusOK {
		t.Fatalf("resolve status = %d, check = %+v", status, check)
	}
	// Grace is unexpired (Now == sent_at), so even an empty proven day cannot
	// conclude absent yet: the honest answer is UNKNOWN with matched=0.
	if check.Disposition != executioncontracts.DispositionUnknown || check.EvidenceRead != "completed" ||
		check.OrdersSeen != 0 || check.Matched != 0 {
		t.Fatalf("check = %+v", check)
	}
}

func TestResolveEndpointReadFailureAnswersCode(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	service := resolveTestService(t, store, fakeOrderHistoryReader{err: &kismockread.SafeError{Code: kismockread.CodeRequestFailed}})
	server := httptest.NewServer(NewHandler(service))
	defer server.Close()
	if _, err := service.Process(context.Background(), testCommand("resolve-read-fails")); err != nil {
		t.Fatal(err)
	}
	status, _ := postResolve(t, server, "resolve-read-fails")
	if status != http.StatusBadGateway {
		t.Fatalf("read failure status = %d, want 502", status)
	}
	got, found, _ := store.Find(context.Background(), "resolve-read-fails")
	if !found || got.Disposition != executioncontracts.DispositionUnknown {
		t.Fatalf("failed read must leave UNKNOWN: %+v found=%t", got, found)
	}
}

func TestResolveEndpointReadFailureGenericCode(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	service := resolveTestService(t, store, fakeOrderHistoryReader{err: errors.New("no evidence")})
	server := httptest.NewServer(NewHandler(service))
	defer server.Close()
	if _, err := service.Process(context.Background(), testCommand("resolve-read-generic")); err != nil {
		t.Fatal(err)
	}
	status, _ := postResolve(t, server, "resolve-read-generic")
	if status != http.StatusBadGateway {
		t.Fatalf("generic read failure status = %d, want 502", status)
	}
}

func TestResolveEndpointAlreadyConclusiveAnswersNotNeeded(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	service := &Service{
		Store:        store,
		PlaceEnabled: true,
		Brokers: map[string]Broker{
			executioncontracts.AccountScopeKISMock: &fakeBroker{result: BrokerResult{Accepted: true, BrokerOrderID: "9001"}},
		},
		Now: func() time.Time { return resolveTestNow },
	}
	server := httptest.NewServer(NewHandler(service))
	defer server.Close()
	if _, err := service.Process(context.Background(), testCommand("resolve-accepted")); err != nil {
		t.Fatal(err)
	}
	status, check := postResolve(t, server, "resolve-accepted")
	if status != http.StatusOK || check.Disposition != executioncontracts.DispositionAccepted ||
		check.EvidenceRead != "not_needed" || check.BrokerOrderID != "9001" {
		t.Fatalf("status = %d check = %+v", status, check)
	}
}

func TestResolveEndpointUnknownCommandAndBadID(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	service := resolveTestService(t, store, fakeOrderHistoryReader{})
	server := httptest.NewServer(NewHandler(service))
	defer server.Close()
	if status, _ := postResolve(t, server, "never-seen"); status != http.StatusNotFound {
		t.Fatalf("unknown command status = %d, want 404", status)
	}
	if status, _ := postResolve(t, server, "bad%20id"); status != http.StatusBadRequest {
		t.Fatalf("invalid command status = %d, want 400", status)
	}
}
