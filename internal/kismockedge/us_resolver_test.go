package kismockedge

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	"github.com/mgh3326/broker-edge/internal/kismockread"
)

type fakeUSHistoryReader struct {
	orders []kismockread.OverseasOrder
	err    error
}

func (reader fakeUSHistoryReader) DomesticOrderHistory(context.Context, string) ([]kismockread.DomesticOrder, error) {
	return nil, nil
}

func (reader fakeUSHistoryReader) OverseasOrderHistory(context.Context, string) ([]kismockread.OverseasOrder, error) {
	return reader.orders, reader.err
}

func TestKISMockUSResolverEvidenceBranches(t *testing.T) {
	now := time.Date(2026, 9, 1, 12, 20, 0, 0, time.UTC)
	tests := []struct {
		name            string
		orders          []kismockread.OverseasOrder
		readErr         error
		wantDisposition executioncontracts.ExecutionDisposition
		wantError       string
		wantOrderID     string
	}{
		{
			name: "matching mock daily-history evidence accepts",
			orders: []kismockread.OverseasOrder{{
				BrokerOrderID: "us-12345", Side: "buy", StockCode: "AAPL", Quantity: "001", Price: "1.00",
				OrderedAt: time.Date(2026, 9, 1, 12, 1, 0, 0, time.UTC),
			}},
			wantDisposition: executioncontracts.DispositionAccepted,
			wantOrderID:     "us-12345",
		},
		{
			name:            "complete mock daily-history absence resolves after grace",
			orders:          []kismockread.OverseasOrder{},
			wantDisposition: executioncontracts.DispositionNotCreated,
			wantError:       ErrorResolvedAbsent,
		},
		{
			name:            "mock daily-history failure stays unknown",
			readErr:         errors.New("read unavailable"),
			wantDisposition: executioncontracts.DispositionUnknown,
			wantError:       ErrorBrokerTimeout,
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
			service := newTestServiceWithBrokers(store, map[string]Broker{
				executioncontracts.AccountScopeKISMockUS: &fakeBroker{result: BrokerResult{ErrorCode: ErrorBrokerTimeout}},
			}, true)
			command := testKISMockUSCommand("resolve-us-branch-" + string(rune('a'+index)))
			command.Price = "1.00"
			receipt, err := service.Process(context.Background(), command)
			if err != nil || receipt.Disposition != executioncontracts.DispositionUnknown {
				t.Fatalf("initial receipt=%+v err=%v", receipt, err)
			}
			result, err := (Resolver{Store: store, Reader: fakeUSHistoryReader{orders: test.orders, err: test.readErr}, Now: func() time.Time { return now }}).Resolve(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			got, found, err := store.Find(context.Background(), receipt.CommandID)
			if err != nil || !found {
				t.Fatalf("find resolved receipt: found=%t err=%v", found, err)
			}
			if got.Disposition != test.wantDisposition || got.ErrorCode != test.wantError || got.BrokerOrderID != test.wantOrderID {
				t.Fatalf("resolved receipt=%+v", got)
			}
			if test.readErr != nil {
				if result.ReadFailures != 1 || len(result.Resolved) != 0 || result.Unresolved != 1 {
					t.Fatalf("failure result=%+v; a read failure must not create a conclusion", result)
				}
				return
			}
			if len(result.Resolved) != 1 {
				t.Fatalf("resolution result=%+v", result)
			}
		})
	}
}

func TestKISMockUSResolverRejectsDomesticOnlyEvidence(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	service := newTestServiceWithBrokers(store, map[string]Broker{
		executioncontracts.AccountScopeKISMockUS: &fakeBroker{result: BrokerResult{ErrorCode: ErrorBrokerTimeout}},
	}, true)
	command := testKISMockUSCommand("resolve-us-no-evidence")
	if _, err := service.Process(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	result, err := (Resolver{
		Store:  store,
		Reader: fakeOrderHistoryReader{},
		Now:    func() time.Time { return time.Date(2026, 9, 1, 12, 20, 0, 0, time.UTC) },
	}).Resolve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := store.Find(context.Background(), command.CommandID)
	if err != nil || !found {
		t.Fatalf("find receipt: found=%t err=%v", found, err)
	}
	if got.Disposition != executioncontracts.DispositionUnknown || result.ReadFailures != 1 || result.Unresolved != 1 {
		t.Fatalf("US receipt must remain UNKNOWN without overseas evidence: receipt=%+v result=%+v", got, result)
	}
}
