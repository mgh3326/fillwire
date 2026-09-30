package kismockedge

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	"github.com/mgh3326/broker-edge/internal/kismockread"
)

// Command 254's stored row shape: command_id broker-edge-canary:20260930T050055.73257
// (':' and '.'), UNKNOWN/broker_timeout, no broker_order_id, one context row.
// These tests pin that resolve answers any existing commands row — whatever
// client wrote it — through the raw and URL-encoded path forms, and that only
// a truly absent row returns command_not_found.

// seedCommandsRow inserts a commands row (and optionally its context) exactly
// as an earlier edge release would have persisted it, without going through
// today's Service write path.
func seedCommandsRow(t *testing.T, store *Store, commandID string, disposition executioncontracts.ExecutionDisposition, errorCode string, withContext bool) {
	t.Helper()
	if _, err := store.db.ExecContext(context.Background(), `
		INSERT INTO commands (command_id, schema_version, disposition, broker_order_id, error_code, recorded_at, phase, account_scope)
		VALUES (?, 'execution-receipt/v1', ?, NULL, NULLIF(?, ''), ?, 'final', 'kis_mock')
	`, commandID, string(disposition), errorCode, resolveTestNow.UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("seed commands row: %v", err)
	}
	if !withContext {
		return
	}
	if _, err := store.db.ExecContext(context.Background(), `
		INSERT INTO command_contexts (command_id, side, stock_code, quantity, price, sent_at, client_order_id)
		VALUES (?, 'buy', '005930', '1', '70000', ?, NULL)
	`, commandID, resolveTestNow.UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("seed command context: %v", err)
	}
}

// The desk-reported row must resolve through every path form the desk tried:
// the literal command id and the percent-encodings a client produces.
func TestResolveEndpointFindsSpecialCommandIDEveryEncoding(t *testing.T) {
	commandID := "broker-edge-canary:20260930T050055.73257"
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	seedCommandsRow(t, store, commandID, executioncontracts.DispositionUnknown, "broker_timeout", true)
	service := resolveTestService(t, store, fakeOrderHistoryReader{orders: []kismockread.DomesticOrder{{
		BrokerOrderID: "9001", Side: "buy", StockCode: "005930",
		Quantity: "001", Price: "070000",
		OrderedAt: resolveTestNow.Add(30 * time.Second),
	}}})
	server := httptest.NewServer(NewHandler(service))
	defer server.Close()

	for index, form := range []string{
		commandID, // literal ':' and '.'
		"broker-edge-canary%3A20260930T050055.73257",   // quote()/url.PathEscape: ':' encoded
		"broker-edge-canary%3A20260930T050055%2E73257", // additionally encoded '.'
	} {
		status, check := postResolve(t, server, form)
		if status != http.StatusOK {
			t.Fatalf("form[%d]=%q status = %d, want 200 for an existing row", index, form, status)
		}
		if check.CommandID != commandID {
			t.Fatalf("form[%d]=%q decoded command_id = %q, want %q", index, form, check.CommandID, commandID)
		}
		if index > 0 {
			// The first call resolves the pending row; later calls see the
			// already-conclusive receipt, still proving the row was found.
			continue
		}
		if check.Disposition != executioncontracts.DispositionAccepted || check.BrokerOrderID != "9001" ||
			check.Matched != 1 || check.EvidenceRead != "completed" {
			t.Fatalf("form[%d]=%q check = %+v", index, form, check)
		}
	}
}

// A row persisted before the resolve endpoint existed — here the legacy shape
// with no command_contexts entry — must be found by the same commands lookup.
func TestResolveEndpointFindsPreCheckCommandRow(t *testing.T) {
	commandID := "broker-edge-canary:20260930T050055.73257"
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	seedCommandsRow(t, store, commandID, executioncontracts.DispositionUnknown, "broker_timeout", false)
	// Empty successful day read plus expired grace resolves the legacy row
	// absent; a context-less row can never match an order by facts.
	service := resolveTestService(t, store, fakeOrderHistoryReader{})
	service.Now = func() time.Time { return resolveTestNow.Add(DefaultResolutionGrace + time.Minute) }
	server := httptest.NewServer(NewHandler(service))
	defer server.Close()

	status, check := postResolve(t, server, "broker-edge-canary%3A20260930T050055.73257")
	if status != http.StatusOK {
		t.Fatalf("legacy row resolve status = %d, want 200", status)
	}
	if check.Disposition != executioncontracts.DispositionNotCreated || check.ErrorCode != ErrorResolvedAbsent ||
		check.EvidenceRead != "completed" {
		t.Fatalf("legacy row check = %+v", check)
	}
}

// The lookup is keyed on commands.command_id alone; an id that was never
// stored still answers 404 command_not_found in every encoding.
func TestResolveEndpointMissingSpecialIDStillNotFound(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	service := resolveTestService(t, store, fakeOrderHistoryReader{})
	server := httptest.NewServer(NewHandler(service))
	defer server.Close()

	for _, form := range []string{
		"broker-edge-canary:20260930T050055.73257",
		"broker-edge-canary%3A20260930T050055.73257",
	} {
		status, _ := postResolve(t, server, form)
		if status != http.StatusNotFound {
			t.Fatalf("form=%q status = %d, want 404 command_not_found", form, status)
		}
	}
}
