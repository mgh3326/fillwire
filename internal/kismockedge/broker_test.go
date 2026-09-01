package kismockedge

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"testing"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	"github.com/mgh3326/broker-edge/internal/kismockread"
)

func TestBroker5xxAndTimeoutPersistUnknown(t *testing.T) {
	tests := []struct {
		name      string
		respond   func(*http.Request) (*http.Response, error)
		wantError string
	}{
		{
			name: "5xx",
			respond: func(*http.Request) (*http.Response, error) {
				return testHTTPResponse(http.StatusBadGateway, `{"rt_cd":"1"}`), nil
			},
			wantError: ErrorBroker5xx,
		},
		{
			name: "timeout",
			respond: func(*http.Request) (*http.Response, error) {
				return nil, timeoutError{}
			},
			wantError: ErrorBrokerTimeout,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
			transport := &countingTransport{respond: test.respond}
			service := newTestService(store, KISMockBroker{Transport: transport}, true)
			receipt, err := service.Process(context.Background(), testCommand("unknown-"+test.name))
			if err != nil {
				t.Fatal(err)
			}
			if receipt.Disposition != executioncontracts.DispositionUnknown || receipt.ErrorCode != test.wantError {
				t.Fatalf("receipt = %+v, want UNKNOWN/%s", receipt, test.wantError)
			}
			if calls := transport.calls.Load(); calls != 1 {
				t.Fatalf("transport calls = %d, want 1", calls)
			}
		})
	}
}

func TestBrokerUsesMockPinAndPreservesOriginalPriceAndQuantityStrings(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	transport := &countingTransport{respond: func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || request.URL.Scheme != "https" || request.URL.Host != kismockread.MockHost {
			t.Fatalf("unexpected request %s %s", request.Method, request.URL)
		}
		if request.URL.Path != mockOrderPath || request.Header.Get("tr_id") != "VTTC0012U" {
			t.Fatalf("unexpected mock order route: path=%s tr_id=%s", request.URL.Path, request.Header.Get("tr_id"))
		}
		var body map[string]string
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["ORD_QTY"] != "001" || body["ORD_UNPR"] != "070000" {
			t.Fatalf("edge changed command strings: %#v", body)
		}
		return testHTTPResponse(http.StatusOK, `{"rt_cd":"0","output":{"ODNO":"mock-42"}}`), nil
	}}
	service := newTestService(store, KISMockBroker{Transport: transport}, true)
	command := testCommand("original-strings")
	command.Quantity = "001"
	command.Price = "070000"
	receipt, err := service.Process(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Disposition != executioncontracts.DispositionAccepted || receipt.BrokerOrderID != "mock-42" {
		t.Fatalf("receipt = %+v", receipt)
	}
}

func TestPinnedBrokerRejectsNonVTSBeforeTransport(t *testing.T) {
	transport := &countingTransport{respond: func(*http.Request) (*http.Response, error) {
		return nil, errors.New("must not send")
	}}
	config := testBrokerConfig()
	config.BaseURL = "https://example.invalid"
	prepared, code := (KISMockBroker{Transport: transport}).Prepare(
		context.Background(), config, testCommand("not-vts"), "cached-token-for-test",
	)
	if prepared != nil || code == "" {
		t.Fatalf("prepare=%v code=%q", prepared, code)
	}
	if calls := transport.calls.Load(); calls != 0 {
		t.Fatalf("non-VTS config reached transport %d times", calls)
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }
