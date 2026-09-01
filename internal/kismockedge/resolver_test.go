package kismockedge

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	"github.com/mgh3326/broker-edge/internal/kismockread"
)

func TestResolverEvidenceBranches(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 20, 0, 0, time.UTC)
	tests := []struct {
		name            string
		orders          []kismockread.DomesticOrder
		readErr         error
		wantDisposition executioncontracts.ExecutionDisposition
		wantError       string
		wantOrderID     string
	}{
		{
			name: "matching read evidence accepts",
			orders: []kismockread.DomesticOrder{{
				BrokerOrderID: "12345", Side: "buy", StockCode: "005930", Quantity: "001", Price: "070000",
				OrderedAt: time.Date(2026, 9, 1, 12, 1, 0, 0, time.UTC),
			}},
			wantDisposition: executioncontracts.DispositionAccepted,
			wantOrderID:     "12345",
		},
		{
			name:            "complete read without match resolves absent after grace",
			orders:          []kismockread.DomesticOrder{},
			wantDisposition: executioncontracts.DispositionNotCreated,
			wantError:       ErrorResolvedAbsent,
		},
		{
			name:            "read failure remains unknown",
			readErr:         errors.New("read unavailable"),
			wantDisposition: executioncontracts.DispositionUnknown,
			wantError:       ErrorBrokerTimeout,
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
			service := newTestService(store, &fakeBroker{result: BrokerResult{ErrorCode: ErrorBrokerTimeout}}, true)
			receipt, err := service.Process(context.Background(), testCommand("resolve-branch-"+string(rune('a'+index))))
			if err != nil {
				t.Fatal(err)
			}
			if receipt.Disposition != executioncontracts.DispositionUnknown {
				t.Fatalf("initial receipt = %+v", receipt)
			}
			reader := fakeOrderHistoryReader{orders: test.orders, err: test.readErr}
			result, err := (Resolver{Store: store, Reader: reader, Now: func() time.Time { return now }}).Resolve(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			got, found, err := store.Find(context.Background(), receipt.CommandID)
			if err != nil || !found {
				t.Fatalf("find resolved receipt: found=%t err=%v", found, err)
			}
			if got.Disposition != test.wantDisposition || got.ErrorCode != test.wantError || got.BrokerOrderID != test.wantOrderID {
				t.Fatalf("resolved receipt = %+v", got)
			}
			if test.readErr != nil {
				if result.ReadFailures != 1 || len(result.Resolved) != 0 || result.Unresolved != 1 {
					t.Fatalf("failure result = %+v; an evidence failure must not create NOT_CREATED", result)
				}
				return
			}
			if len(result.Resolved) != 1 {
				t.Fatalf("resolution result = %+v", result)
			}
		})
	}
}

func TestResolverIsIdempotentAndDoesNotOverwriteOriginalReceipt(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	service := newTestService(store, &fakeBroker{result: BrokerResult{ErrorCode: ErrorBroker5xx}}, true)
	initial, err := service.Process(context.Background(), testCommand("resolve-idempotent"))
	if err != nil {
		t.Fatal(err)
	}
	resolver := Resolver{
		Store:  store,
		Reader: fakeOrderHistoryReader{},
		Now:    func() time.Time { return time.Date(2026, 9, 1, 12, 20, 0, 0, time.UTC) },
	}
	first, err := resolver.Resolve(context.Background())
	if err != nil || len(first.Resolved) != 1 {
		t.Fatalf("first resolution = %+v err=%v", first, err)
	}
	second, err := resolver.Resolve(context.Background())
	if err != nil || len(second.Resolved) != 0 || second.Unresolved != 0 {
		t.Fatalf("second resolution = %+v err=%v", second, err)
	}
	var disposition, errorCode, recordedAt string
	if err := store.db.QueryRow(`SELECT disposition, error_code, recorded_at FROM commands WHERE command_id = ?`, initial.CommandID).Scan(&disposition, &errorCode, &recordedAt); err != nil {
		t.Fatal(err)
	}
	if disposition != string(executioncontracts.DispositionUnknown) || errorCode != ErrorBroker5xx || recordedAt != initial.RecordedAt {
		t.Fatalf("original receipt row was overwritten: disposition=%s error=%s recorded_at=%s", disposition, errorCode, recordedAt)
	}
}

func TestResolverMigratesLegacyReceiptAndRequiresEmptyDayForAbsence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.sqlite")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Exec(`CREATE TABLE commands (
		id INTEGER PRIMARY KEY, command_id TEXT NOT NULL UNIQUE, schema_version TEXT NOT NULL,
		disposition TEXT NOT NULL, broker_order_id TEXT, error_code TEXT, recorded_at TEXT NOT NULL, phase TEXT NOT NULL
	)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Exec(`INSERT INTO commands (command_id, schema_version, disposition, error_code, recorded_at, phase)
		VALUES ('legacy-unknown', 'execution-receipt/v1', 'UNKNOWN', 'broker_timeout', '2026-09-01T12:00:00Z', 'final')`)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	store := openTestStore(t, path)
	result, err := (Resolver{
		Store:  store,
		Reader: fakeOrderHistoryReader{},
		Now:    func() time.Time { return time.Date(2026, 9, 1, 12, 20, 0, 0, time.UTC) },
	}).Resolve(context.Background())
	if err != nil || len(result.Resolved) != 1 {
		t.Fatalf("legacy resolution = %+v err=%v", result, err)
	}
	got, found, err := store.Find(context.Background(), "legacy-unknown")
	if err != nil || !found || got.Disposition != executioncontracts.DispositionNotCreated || got.ErrorCode != ErrorResolvedAbsent {
		t.Fatalf("legacy receipt = %+v found=%t err=%v", got, found, err)
	}
}

type fakeOrderHistoryReader struct {
	orders []kismockread.DomesticOrder
	err    error
}

func (reader fakeOrderHistoryReader) DomesticOrderHistory(context.Context, string) ([]kismockread.DomesticOrder, error) {
	return reader.orders, reader.err
}

func TestResolverKeepsUnknownBeforeGraceAndOnAmbiguousNonemptyDay(t *testing.T) {
	// Mutant witness (2026-09-01): replacing the resolution predicate with
	// bare len(matches)==0 survived the original table — neither the
	// unexpired-grace hold nor the legacy nonempty-day hold was pinned.

	// 1) Context-present command, empty day, but grace has NOT elapsed:
	//    must stay UNKNOWN.
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	service := newTestService(store, &fakeBroker{result: BrokerResult{ErrorCode: ErrorBrokerTimeout}}, true)
	receipt, err := service.Process(context.Background(), testCommand("resolve-grace-hold"))
	if err != nil {
		t.Fatal(err)
	}
	sentAt, err := time.Parse(time.RFC3339Nano, receipt.RecordedAt)
	if err != nil {
		t.Fatal(err)
	}
	result, err := (Resolver{Store: store, Reader: fakeOrderHistoryReader{}, Now: func() time.Time { return sentAt.Add(time.Minute) }}).Resolve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Resolved) != 0 || result.Unresolved == 0 {
		t.Fatalf("unexpired grace must hold UNKNOWN: %+v", result)
	}

	// 2) Legacy receipt (no immutable command context) on a NONEMPTY day:
	//    correspondence cannot be proven either way — must stay UNKNOWN.
	path := filepath.Join(t.TempDir(), "legacy2.sqlite")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = database.Exec(`CREATE TABLE commands (
		id INTEGER PRIMARY KEY, command_id TEXT NOT NULL UNIQUE, schema_version TEXT NOT NULL,
		disposition TEXT NOT NULL, broker_order_id TEXT, error_code TEXT, recorded_at TEXT NOT NULL, phase TEXT NOT NULL
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err = database.Exec(`INSERT INTO commands (command_id, schema_version, disposition, error_code, recorded_at, phase)
		VALUES ('legacy-ambiguous', 'execution-receipt/v1', 'UNKNOWN', 'broker_timeout', '2026-09-01T12:00:00Z', 'final')`); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	legacyStore := openTestStore(t, path)
	nonempty := fakeOrderHistoryReader{orders: []kismockread.DomesticOrder{{
		BrokerOrderID: "77777", Side: "buy", StockCode: "005930", Quantity: "001", Price: "070000",
		OrderedAt: time.Date(2026, 9, 1, 12, 1, 0, 0, time.UTC),
	}}}
	result2, err := (Resolver{Store: legacyStore, Reader: nonempty, Now: func() time.Time { return time.Date(2026, 9, 1, 12, 20, 0, 0, time.UTC) }}).Resolve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := legacyStore.Find(context.Background(), "legacy-ambiguous")
	if err != nil || !found {
		t.Fatalf("legacy receipt lookup: %+v found=%t err=%v", got, found, err)
	}
	if got.Disposition != executioncontracts.DispositionUnknown {
		t.Fatalf("legacy nonempty ambiguous day must stay UNKNOWN: %+v (result=%+v)", got, result2)
	}
}
