package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/mgh3326/fillwire/internal/exec"
	"github.com/mgh3326/fillwire/internal/kisread"
)

type brokerTransport func(*http.Request) (*http.Response, error)

func (f brokerTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestExactMockBrokerReadbackCompleteness(t *testing.T) {
	for _, tc := range []struct {
		name, filled, cancel, reject string
		terminal, fail               bool
	}{
		{"working", "2", "N", "0", false, false}, {"filled", "10", "N", "0", true, false}, {"cancelled_partial", "2", "Y", "0", true, false}, {"expired_or_rejected", "0", "N", "10", true, false}, {"unknown_fill", "broken", "N", "0", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &kisread.Client{Mock: true, AppKey: "fixture", AppSecret: "fixture", Token: func() string { return "fixture" }, HTTP: &http.Client{Transport: brokerTransport(func(r *http.Request) (*http.Response, error) {
				q := r.URL.Query()
				if r.Method != "GET" || r.Header.Get("tr_id") != "VTTC0081R" || q.Get("ODNO") != "123" || q.Get("INQR_STRT_DT") != "20261102" || q.Get("PDNO") != "005930" {
					t.Error("incorrect exact readback query")
				}
				body := `{"rt_cd":"0","output1":[{"odno":"000123","ord_dt":"20261102","pdno":"005930","sll_buy_dvsn_cd":"02","ord_qty":"10","tot_ccld_qty":"` + tc.filled + `","cncl_yn":"` + tc.cancel + `","rjct_qty":"` + tc.reject + `"}]}`
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
			})}}
			view, err := (historyRead{Client: client, Account: "fixture", Product: "fixture", Symbol: "005930", Side: exec.Buy}).read(context.Background(), exec.BrokerOrder{BrokerOrderID: "123", TradingDay: "2026-11-02", Qty: 10})
			if (err != nil) != tc.fail || !tc.fail && view.Terminal != tc.terminal {
				t.Fatalf("view %+v error %v", view, err)
			}
		})
	}
}
