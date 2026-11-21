package kismockread

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func testPriceGetter(t *testing.T) *staticGetter {
	t.Helper()
	return &staticGetter{
		value:   cachedTokenPayload("cached-token-for-test", float64(testNow().Add(2*time.Hour).Unix())),
		present: true,
	}
}

func TestDomesticPriceBandRequestShape(t *testing.T) {
	transport := &recordingTransport{respond: func(request *http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"rt_cd":"0","msg_cd":"MCA00000","msg1":"ok","output":{"stck_prpr":"70500","stck_mxpr":"92300","stck_llam":"49700","stck_sdpr":"71000"}}`, nil), nil
	}}
	getter := testPriceGetter(t)
	executor := Executor{TokenGetter: getter, Transport: transport, Now: testNow}
	band, err := executor.DomesticPriceBand(context.Background(), testConfig(), "005930")
	if err != nil {
		t.Fatalf("DomesticPriceBand: %v", err)
	}
	if transport.calls != 1 {
		t.Fatalf("transport calls = %d, want exactly 1 GET", transport.calls)
	}
	request := transport.requests[0]
	if request.Method != http.MethodGet {
		t.Fatalf("method = %s, want GET (a mutation method is forbidden)", request.Method)
	}
	if request.URL.Scheme != "https" || request.URL.Host != MockHost || request.URL.Path != domesticPricePath {
		t.Fatalf("request URL = %s", request.URL)
	}
	if got := request.URL.Query().Get("FID_INPUT_ISCD"); got != "005930" {
		t.Fatalf("FID_INPUT_ISCD = %q", got)
	}
	if got := request.URL.Query().Get("FID_COND_MRKT_DIV_CODE"); got != "J" {
		t.Fatalf("FID_COND_MRKT_DIV_CODE = %q", got)
	}
	if got := request.Header.Get("tr_id"); got != domesticPriceTRID {
		t.Fatalf("tr_id = %q, want %q", got, domesticPriceTRID)
	}
	if request.Header.Get("authorization") != "Bearer cached-token-for-test" {
		t.Fatal("cached token was not attached")
	}
	if band.LastPrice != "70500" || band.UpperLimit != "92300" || band.LowerLimit != "49700" || band.BasePrice != "71000" {
		t.Fatalf("band = %#v", band)
	}
}

func TestDomesticPriceBandFailsClosed(t *testing.T) {
	tests := []struct {
		name      string
		stockCode string
		getter    *staticGetter
		status    int
		body      string
		want      ErrorCode
	}{
		{"bad stock", "00593X", testPriceGetter(t), http.StatusOK, ``, CodeInvalidInput},
		{"no token", "005930", &staticGetter{present: false}, http.StatusOK, ``, CodeTokenMissing},
		{"http 500", "005930", testPriceGetter(t), http.StatusInternalServerError, `{}`, CodeRequestFailed},
		{"broker rejected", "005930", testPriceGetter(t), http.StatusOK, `{"rt_cd":"1","msg_cd":"EGW00123"}`, CodeBrokerRejected},
		{"missing output", "005930", testPriceGetter(t), http.StatusOK, `{"rt_cd":"0"}`, CodeResponseInvalid},
		{"non-numeric field", "005930", testPriceGetter(t), http.StatusOK, `{"rt_cd":"0","output":{"stck_llam":"12a45"}}`, CodeResponseInvalid},
		{"non-string field", "005930", testPriceGetter(t), http.StatusOK, `{"rt_cd":"0","output":{"stck_llam":12345}}`, CodeResponseInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := &recordingTransport{respond: func(*http.Request) (*http.Response, error) {
				return jsonResponse(test.status, test.body, nil), nil
			}}
			executor := Executor{TokenGetter: test.getter, Transport: transport, Now: testNow}
			band, err := executor.DomesticPriceBand(context.Background(), testConfig(), test.stockCode)
			if err == nil || err.Code != test.want {
				t.Fatalf("err = %v, want %q", err, test.want)
			}
			if band != (DomesticPriceBand{}) {
				t.Fatalf("failed read must return an empty band: %#v", band)
			}
		})
	}
}

func TestDomesticPriceBandTransportFailureAndMissingFields(t *testing.T) {
	transport := &recordingTransport{respond: func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial failed")
	}}
	executor := Executor{TokenGetter: testPriceGetter(t), Transport: transport, Now: testNow}
	if _, err := executor.DomesticPriceBand(context.Background(), testConfig(), "005930"); err == nil || err.Code != CodeRequestFailed {
		t.Fatalf("err = %v, want %q", err, CodeRequestFailed)
	}

	partial := &recordingTransport{respond: func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"rt_cd":"0","output":{"stck_sdpr":"71000"}}`, nil), nil
	}}
	executor = Executor{TokenGetter: testPriceGetter(t), Transport: partial, Now: testNow}
	band, err := executor.DomesticPriceBand(context.Background(), testConfig(), "005930")
	if err != nil {
		t.Fatalf("DomesticPriceBand: %v", err)
	}
	if band.BasePrice != "71000" || band.LowerLimit != "" {
		t.Fatalf("band = %#v", band)
	}
}
