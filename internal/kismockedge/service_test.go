package kismockedge

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	"github.com/mgh3326/broker-edge/internal/kismockread"
)

func TestDuplicate100HTTPPostsSendBrokerExactlyOnce(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	broker := &fakeBroker{result: BrokerResult{Accepted: true, BrokerOrderID: "mock-order-1"}}
	server := httptest.NewServer(NewHandler(newTestService(store, broker, true)))
	defer server.Close()

	command := testCommand("duplicate-100")
	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errors := make(chan error, 100)
	var group sync.WaitGroup
	for range 100 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			response, requestErr := http.Post(server.URL+"/v1/commands", "application/json", bytes.NewReader(payload))
			if requestErr != nil {
				errors <- requestErr
				return
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				errors <- &unexpectedStatus{got: response.StatusCode}
				return
			}
			var receipt executioncontracts.ExecutionReceiptV1
			if err := json.NewDecoder(response.Body).Decode(&receipt); err != nil {
				errors <- err
				return
			}
			if receipt.CommandID != command.CommandID || !receipt.Disposition.Valid() {
				errors <- &invalidReceipt{}
			}
		}()
	}
	close(start)
	group.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	if calls := broker.sends.Load(); calls != 1 {
		t.Fatalf("broker POST calls = %d, want exactly 1", calls)
	}

	response, err := http.Post(server.URL+"/v1/commands", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var receipt executioncontracts.ExecutionReceiptV1
	if err := json.NewDecoder(response.Body).Decode(&receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Disposition != executioncontracts.DispositionAccepted || receipt.BrokerOrderID != "mock-order-1" {
		t.Fatalf("post-final duplicate receipt = %+v", receipt)
	}
	if calls := broker.sends.Load(); calls != 1 {
		t.Fatalf("broker re-sent after durable receipt: %d calls", calls)
	}
}

func TestRestartAfterSendBoundaryKeepsUnknownAndNeverResends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "edge.sqlite")
	store := openTestStore(t, path)
	release := make(chan struct{})
	entered := make(chan struct{})
	firstBroker := &fakeBroker{
		result:  BrokerResult{Accepted: true, BrokerOrderID: "should-not-persist"},
		entered: entered,
		release: release,
	}
	firstService := newTestService(store, firstBroker, true)
	command := testCommand("killed-after-send")
	finished := make(chan struct{})
	go func() {
		_, _ = firstService.Process(context.Background(), command)
		close(finished)
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("broker Send did not begin")
	}
	// Send has begun only after ReservePending returned. Closing this handle
	// models process death before it can finalize the broker outcome.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	restartedStore, err := OpenStore(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer restartedStore.Close()
	secondBroker := &fakeBroker{result: BrokerResult{Accepted: true, BrokerOrderID: "must-not-send"}}
	restartedService := newTestService(restartedStore, secondBroker, true)
	receipt, err := restartedService.Process(context.Background(), command)
	if err != nil {
		t.Fatalf("restart process: %v", err)
	}
	if receipt.Disposition != executioncontracts.DispositionUnknown || receipt.ErrorCode != ErrorSendPending {
		t.Fatalf("restart receipt = %+v, want durable pending UNKNOWN", receipt)
	}
	if calls := secondBroker.sends.Load(); calls != 0 {
		t.Fatalf("restart broker POST calls = %d, want 0", calls)
	}
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("simulated killed sender did not finish")
	}
}

func TestGateOffCapsAndTickMismatchDoNotReachBroker(t *testing.T) {
	t.Run("gate off is zero network", func(t *testing.T) {
		store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
		transport := &countingTransport{respond: func(*http.Request) (*http.Response, error) {
			return testHTTPResponse(http.StatusOK, `{"rt_cd":"0","output":{"ODNO":"1"}}`), nil
		}}
		loader := &fakeTokenLoader{token: "cached-token-for-test"}
		service := &Service{
			Store:        store,
			PlaceEnabled: false,
			LoadConfig: func() (config kismockread.Config, code string) {
				t.Fatal("disabled gate loaded broker configuration")
				return config, code
			},
			Tokens: loader,
			Broker: KISMockBroker{Transport: transport},
		}
		receipt, err := service.Process(context.Background(), testCommand("gate-off"))
		if err != nil {
			t.Fatal(err)
		}
		if receipt.Disposition != executioncontracts.DispositionNotCreated || receipt.ErrorCode != ErrorPlaceDisabled {
			t.Fatalf("gate receipt = %+v", receipt)
		}
		if calls := transport.calls.Load(); calls != 0 {
			t.Fatalf("disabled gate reached transport %d times", calls)
		}
		if calls := loader.calls.Load(); calls != 0 {
			t.Fatalf("disabled gate loaded token %d times", calls)
		}
	})

	t.Run("caps reject before broker", func(t *testing.T) {
		store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
		broker := &fakeBroker{result: BrokerResult{Accepted: true, BrokerOrderID: "nope"}}
		command := testCommand("over-cap")
		command.Quantity = "101"
		receipt, err := newTestService(store, broker, true).Process(context.Background(), command)
		if err != nil {
			t.Fatal(err)
		}
		if receipt.Disposition != executioncontracts.DispositionNotCreated || receipt.ErrorCode != ErrorOrderLimitExceeded {
			t.Fatalf("cap receipt = %+v", receipt)
		}
		if calls := broker.sends.Load(); calls != 0 {
			t.Fatalf("cap reached broker %d times", calls)
		}
	})

	t.Run("notional cap rejects before broker", func(t *testing.T) {
		store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
		broker := &fakeBroker{result: BrokerResult{Accepted: true, BrokerOrderID: "nope"}}
		command := testCommand("over-notional-cap")
		command.Quantity = "15"
		receipt, err := newTestService(store, broker, true).Process(context.Background(), command)
		if err != nil {
			t.Fatal(err)
		}
		if receipt.Disposition != executioncontracts.DispositionNotCreated || receipt.ErrorCode != ErrorOrderLimitExceeded {
			t.Fatalf("notional cap receipt = %+v", receipt)
		}
		if calls := broker.sends.Load(); calls != 0 {
			t.Fatalf("notional cap reached broker %d times", calls)
		}
	})

	t.Run("tick mismatch rejects without repricing", func(t *testing.T) {
		store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
		broker := &fakeBroker{result: BrokerResult{Accepted: true, BrokerOrderID: "nope"}}
		command := testCommand("tick-mismatch")
		command.Price = "70001"
		receipt, err := newTestService(store, broker, true).Process(context.Background(), command)
		if err != nil {
			t.Fatal(err)
		}
		if receipt.Disposition != executioncontracts.DispositionNotCreated || receipt.ErrorCode != ErrorTickMismatch {
			t.Fatalf("tick receipt = %+v", receipt)
		}
		if calls := broker.sends.Load(); calls != 0 {
			t.Fatalf("tick mismatch reached broker %d times", calls)
		}
	})
}

type unexpectedStatus struct{ got int }

func (err *unexpectedStatus) Error() string { return "unexpected HTTP status" }

type invalidReceipt struct{}

func (*invalidReceipt) Error() string { return "invalid receipt" }
