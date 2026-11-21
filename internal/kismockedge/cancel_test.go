package kismockedge

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
)

func TestAlpacaCancelDuplicate100PostsSendsDeleteExactlyOnce(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	var deletes atomic.Int64
	transport := &countingTransport{respond: func(request *http.Request) (*http.Response, error) {
		switch request.Method {
		case http.MethodPost:
			return testHTTPResponse(http.StatusOK, `{"id":"alpaca-cancel-1","status":"accepted"}`), nil
		case http.MethodDelete:
			deletes.Add(1)
			if request.URL.Path != "/v2/orders/alpaca-cancel-1" {
				t.Errorf("DELETE path = %q", request.URL.Path)
			}
			return testHTTPResponse(http.StatusNoContent, ""), nil
		default:
			t.Errorf("method = %s", request.Method)
			return testHTTPResponse(http.StatusInternalServerError, ""), nil
		}
	}}
	service := newTestServiceWithBrokers(store, map[string]Broker{executioncontracts.AccountScopeAlpacaPaperCrypto: testAlpacaPaperCryptoBroker(transport)}, true)
	command := testAlpacaCommand("cancel-duplicate-100")
	if _, err := service.Process(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewHandler(service))
	defer server.Close()

	start := make(chan struct{})
	failures := make(chan error, 100)
	var group sync.WaitGroup
	for range 100 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			response, err := http.Post(server.URL+"/v1/commands/"+command.CommandID+"/cancel", "application/json", nil)
			if err != nil {
				failures <- err
				return
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				failures <- &unexpectedStatus{got: response.StatusCode}
				return
			}
			var receipt CancelReceipt
			if err := json.NewDecoder(response.Body).Decode(&receipt); err != nil {
				failures <- err
				return
			}
			if !receipt.State.Valid() || receipt.CommandID != command.CommandID {
				failures <- &invalidCancelReceipt{}
			}
		}()
	}
	close(start)
	group.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if got := deletes.Load(); got != 1 {
		t.Fatalf("broker DELETE calls = %d, want 1", got)
	}
	receipt, err := service.Cancel(context.Background(), command.CommandID)
	if err != nil || receipt.State != CancelStateCancelled {
		t.Fatalf("final replay receipt=%+v err=%v", receipt, err)
	}
}

func TestCancelRejectsNonAcceptedBeforeBrokerSend(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	transport := &countingTransport{respond: func(*http.Request) (*http.Response, error) {
		t.Fatal("broker should not be called")
		return nil, io.ErrUnexpectedEOF
	}}
	service := newTestServiceWithBrokers(store, map[string]Broker{executioncontracts.AccountScopeAlpacaPaperCrypto: testAlpacaPaperCryptoBroker(transport)}, false)
	command := testAlpacaCommand("cancel-not-created")
	if receipt, err := service.Process(context.Background(), command); err != nil || receipt.Disposition != executioncontracts.DispositionNotCreated {
		t.Fatalf("place receipt=%+v err=%v", receipt, err)
	}
	receipt, err := service.Cancel(context.Background(), command.CommandID)
	if !errorsIsCancelNotEligible(err) || receipt.ErrorCode != ErrorCancelNotEligible {
		t.Fatalf("cancel receipt=%+v err=%v", receipt, err)
	}
	if got := transport.calls.Load(); got != 0 {
		t.Fatalf("broker calls = %d, want 0", got)
	}
}

func TestCancelReservedUnknownIsReplayedWithoutBrokerSend(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	transport := &countingTransport{respond: func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPost {
			return testHTTPResponse(http.StatusOK, `{"id":"alpaca-cancel-pending","status":"accepted"}`), nil
		}
		t.Fatal("cancel broker should not be called after durable pending marker")
		return nil, io.ErrUnexpectedEOF
	}}
	service := newTestServiceWithBrokers(store, map[string]Broker{executioncontracts.AccountScopeAlpacaPaperCrypto: testAlpacaPaperCryptoBroker(transport)}, true)
	command := testAlpacaCommand("cancel-pending")
	if _, err := service.Process(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	pending := service.cancelReceipt(command.CommandID, CancelStateUnknown, ErrorCancelSendPending)
	if _, reserved, err := store.ReserveCancel(context.Background(), pending); err != nil || !reserved {
		t.Fatalf("reserve=%t err=%v", reserved, err)
	}
	receipt, err := service.Cancel(context.Background(), command.CommandID)
	if err != nil || receipt.State != CancelStateUnknown || receipt.ErrorCode != ErrorCancelSendPending {
		t.Fatalf("cancel receipt=%+v err=%v", receipt, err)
	}
	if got := transport.calls.Load(); got != 1 {
		t.Fatalf("transport calls = %d, want placement only", got)
	}
}

func TestKISMockCancelMirrorsVTTCCancelTR(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	var cancelBody map[string]string
	transport := &countingTransport{respond: func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPost && request.URL.Path == mockOrderPath {
			return testHTTPResponse(http.StatusOK, `{"rt_cd":"0","output":{"ODNO":"kis-cancel-1","ORD_GNO_BRNO":"06010"}}`), nil
		}
		if request.Method != http.MethodPost || request.URL.Path != mockCancelPath {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("tr_id") != "VTTC0013U" {
			t.Errorf("tr_id = %q", request.Header.Get("tr_id"))
		}
		if err := json.NewDecoder(request.Body).Decode(&cancelBody); err != nil {
			t.Error(err)
		}
		return testHTTPResponse(http.StatusOK, `{"rt_cd":"0","output":{"ODNO":"kis-cancel-result"}}`), nil
	}}
	service := newTestService(store, testKISMockBroker(transport), true)
	command := testCommand("kis-cancel")
	if _, err := service.Process(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	receipt, err := service.Cancel(context.Background(), command.CommandID)
	if err != nil || receipt.State != CancelStateCancelled {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
	for key, want := range map[string]string{"ORGN_ODNO": "kis-cancel-1", "ORD_DVSN": "00", "RVSE_CNCL_DVSN_CD": "02", "ORD_QTY": "1", "ORD_UNPR": "70000", "QTY_ALL_ORD_YN": "N", "EXCG_ID_DVSN_CD": "KRX"} {
		if got := cancelBody[key]; got != want {
			t.Errorf("body[%q] = %q, want %q", key, got, want)
		}
	}
	if got := cancelBody["KRX_FWDG_ORD_ORGNO"]; got != "06010" {
		t.Errorf("KRX_FWDG_ORD_ORGNO = %q, want stored forwarding number", got)
	}
}

func TestAlpacaCancelNotFoundIsDistinct(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	transport := &countingTransport{respond: func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPost {
			return testHTTPResponse(http.StatusOK, `{"id":"missing-order","status":"accepted"}`), nil
		}
		return testHTTPResponse(http.StatusNotFound, ""), nil
	}}
	service := newTestServiceWithBrokers(store, map[string]Broker{executioncontracts.AccountScopeAlpacaPaperCrypto: testAlpacaPaperCryptoBroker(transport)}, true)
	command := testAlpacaCommand("cancel-not-found")
	if _, err := service.Process(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	receipt, err := service.Cancel(context.Background(), command.CommandID)
	if err != nil || receipt.State != CancelStateNotFound || receipt.ErrorCode != ErrorCancelNotFound {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
}

func TestCancelUnknownAfterSendBoundaryIsNeverResent(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	var deletes atomic.Int64
	transport := &countingTransport{respond: func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPost {
			return testHTTPResponse(http.StatusOK, `{"id":"ambiguous-cancel","status":"accepted"}`), nil
		}
		deletes.Add(1)
		return nil, io.ErrUnexpectedEOF
	}}
	service := newTestServiceWithBrokers(store, map[string]Broker{executioncontracts.AccountScopeAlpacaPaperCrypto: testAlpacaPaperCryptoBroker(transport)}, true)
	command := testAlpacaCommand("cancel-ambiguous")
	if _, err := service.Process(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	first, err := service.Cancel(context.Background(), command.CommandID)
	if err != nil || first.State != CancelStateUnknown || first.ErrorCode != ErrorBrokerUnknown {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := service.Cancel(context.Background(), command.CommandID)
	if err != nil || second != first {
		t.Fatalf("second=%+v first=%+v err=%v", second, first, err)
	}
	if got := deletes.Load(); got != 1 {
		t.Fatalf("broker DELETE calls = %d, want 1", got)
	}
}

type invalidCancelReceipt struct{}

func (*invalidCancelReceipt) Error() string { return "invalid cancel receipt" }

func errorsIsCancelNotEligible(err error) bool { return errors.Is(err, errCancelNotEligible) }
