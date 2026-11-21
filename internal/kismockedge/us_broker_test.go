package kismockedge

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
)

func TestKISMockUSPlaceAndCancelMirrorVTSReference(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	var placementBody, cancellationBody map[string]string
	transport := &countingTransport{respond: func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case mockUSOrderPath:
			if request.Method != http.MethodPost || request.Header.Get("tr_id") != "VTTT1002U" {
				t.Errorf("place request=%s %s tr_id=%q", request.Method, request.URL.Path, request.Header.Get("tr_id"))
			}
			if request.Header.Get("hashkey") != "" {
				t.Error("overseas reference request must not add hashkey")
			}
			if err := json.NewDecoder(request.Body).Decode(&placementBody); err != nil {
				t.Error(err)
			}
			return testHTTPResponse(http.StatusOK, `{"rt_cd":"0","output":{"ODNO":"us-order-1"}}`), nil
		case mockUSCancelPath:
			if request.Method != http.MethodPost || request.Header.Get("tr_id") != "VTTT1004U" {
				t.Errorf("cancel request=%s %s tr_id=%q", request.Method, request.URL.Path, request.Header.Get("tr_id"))
			}
			if request.Header.Get("hashkey") != "" {
				t.Error("overseas reference cancellation must not add hashkey")
			}
			if err := json.NewDecoder(request.Body).Decode(&cancellationBody); err != nil {
				t.Error(err)
			}
			return testHTTPResponse(http.StatusOK, `{"rt_cd":"0","output":{"ODNO":"us-cancel-1"}}`), nil
		default:
			t.Errorf("unexpected request path=%q", request.URL.Path)
			return testHTTPResponse(http.StatusInternalServerError, ""), nil
		}
	}}
	service := newTestServiceWithBrokers(store, map[string]Broker{
		executioncontracts.AccountScopeKISMockUS: testKISMockUSBroker(transport),
	}, true)
	command := testKISMockUSCommand("us-place-cancel")
	command.Price = "1.00"
	receipt, err := service.Process(context.Background(), command)
	if err != nil || receipt.Disposition != executioncontracts.DispositionAccepted || receipt.BrokerOrderID != "us-order-1" {
		t.Fatalf("place receipt=%+v err=%v", receipt, err)
	}
	wantPlacement := map[string]string{
		"CANO": "12345678", "ACNT_PRDT_CD": "01", "OVRS_EXCG_CD": "NASD", "PDNO": "AAPL",
		"ORD_QTY": "1", "OVRS_ORD_UNPR": "1.00", "CTAC_TLNO": "", "MGCO_APTM_ODNO": "",
		"SLL_TYPE": "", "ORD_SVR_DVSN_CD": "0", "ORD_DVSN": "00",
	}
	if !reflect.DeepEqual(placementBody, wantPlacement) {
		t.Fatalf("US placement body=%#v, want %#v", placementBody, wantPlacement)
	}
	cancelReceipt, err := service.Cancel(context.Background(), command.CommandID)
	if err != nil || cancelReceipt.State != CancelStateCancelled {
		t.Fatalf("cancel receipt=%+v err=%v", cancelReceipt, err)
	}
	wantCancellation := map[string]string{
		"CANO": "12345678", "ACNT_PRDT_CD": "01", "OVRS_EXCG_CD": "NASD", "PDNO": "AAPL",
		"ORGN_ODNO": "us-order-1", "RVSE_CNCL_DVSN_CD": "02", "ORD_QTY": "1",
		"OVRS_ORD_UNPR": "0", "MGCO_APTM_ODNO": "", "ORD_SVR_DVSN_CD": "0",
	}
	if !reflect.DeepEqual(cancellationBody, wantCancellation) {
		t.Fatalf("US cancellation body=%#v, want %#v", cancellationBody, wantCancellation)
	}
}

func TestKISMockUSSellUsesUSSpecificMockTRAndKISSymbol(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	transport := &countingTransport{respond: func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != mockUSOrderPath || request.Header.Get("tr_id") != "VTTT1001U" {
			t.Errorf("US sell route=%s tr_id=%q", request.URL.Path, request.Header.Get("tr_id"))
		}
		var body map[string]string
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["PDNO"] != "BRK/B" || body["SLL_TYPE"] != "00" {
			t.Errorf("US sell body=%#v", body)
		}
		return testHTTPResponse(http.StatusOK, `{"rt_cd":"0","output":{"ODNO":"us-sell-1"}}`), nil
	}}
	service := newTestServiceWithBrokers(store, map[string]Broker{
		executioncontracts.AccountScopeKISMockUS: testKISMockUSBroker(transport),
	}, true)
	command := testKISMockUSCommand("us-sell")
	command.Side = "sell"
	command.StockCode = "BRK.B"
	if receipt, err := service.Process(context.Background(), command); err != nil || receipt.Disposition != executioncontracts.DispositionAccepted {
		t.Fatalf("receipt=%+v err=%v", receipt, err)
	}
}
