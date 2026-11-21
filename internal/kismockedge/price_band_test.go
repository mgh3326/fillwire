package kismockedge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	"github.com/mgh3326/broker-edge/internal/kismockread"
)

type fakeQuoter struct {
	band  kismockread.DomesticPriceBand
	code  string
	calls atomic.Int64
	stock string
}

func (quoter *fakeQuoter) PriceBand(_ context.Context, stockCode string) (kismockread.DomesticPriceBand, string) {
	quoter.calls.Add(1)
	quoter.stock = stockCode
	return quoter.band, quoter.code
}

func priceBandService(quoter Quoter) *Service {
	return &Service{
		Quoters: map[string]Quoter{executioncontracts.AccountScopeKISMock: quoter},
	}
}

func priceBandRequest(t *testing.T, handler http.Handler, target string) (int, map[string]json.RawMessage) {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, target, nil))
	var body map[string]json.RawMessage
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("response not JSON: %v body %q", err, recorder.Body.String())
	}
	return recorder.Code, body
}

func TestPriceBandHandlerValidatesQuery(t *testing.T) {
	quoter := &fakeQuoter{band: kismockread.DomesticPriceBand{LowerLimit: "49700", UpperLimit: "92300", BasePrice: "71000", LastPrice: "70500"}}
	handler := NewHandler(priceBandService(quoter))
	for _, target := range []string{
		"/v1/price-band",
		"/v1/price-band?scope=kis_mock",
		"/v1/price-band?scope=kis_mock&stock_code=005930&extra=1",
		"/v1/price-band?scope=kis_mock_us&stock_code=005930",
		"/v1/price-band?scope=kis_live&stock_code=005930",
		"/v1/price-band?scope=kis_mock&stock_code=5930",
		"/v1/price-band?scope=kis_mock&stock_code=ABCDEF",
		"/v1/price-band?stock_code=005930&scope=",
	} {
		status, body := priceBandRequest(t, handler, target)
		if status != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400", target, status)
		}
		var code string
		if err := json.Unmarshal(body["error_code"], &code); err != nil || code != ErrorInvalidCommand {
			t.Fatalf("%s: body = %v", target, body)
		}
	}
	if quoter.calls.Load() != 0 {
		t.Fatalf("invalid queries reached the quoter %d times", quoter.calls.Load())
	}
}

func TestPriceBandHandlerReturnsBand(t *testing.T) {
	quoter := &fakeQuoter{band: kismockread.DomesticPriceBand{LowerLimit: "49700", UpperLimit: "92300", BasePrice: "71000", LastPrice: "70500"}}
	handler := NewHandler(priceBandService(quoter))
	status, body := priceBandRequest(t, handler, "/v1/price-band?scope=kis_mock&stock_code=005930")
	if status != http.StatusOK {
		t.Fatalf("status = %d, body = %v", status, body)
	}
	var band executioncontracts.PriceBandV1
	raw, _ := json.Marshal(body)
	if err := json.Unmarshal(raw, &band); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if band.SchemaVersion != executioncontracts.PriceBandV1SchemaVersion || band.StockCode != "005930" ||
		band.LowerLimit != "49700" || band.UpperLimit != "92300" || band.BasePrice != "71000" || band.LastPrice != "70500" {
		t.Fatalf("band = %#v", band)
	}
	if quoter.calls.Load() != 1 || quoter.stock != "005930" {
		t.Fatalf("quoter calls = %d stock = %q", quoter.calls.Load(), quoter.stock)
	}
}

func TestPriceBandHandlerFailsClosed(t *testing.T) {
	for _, code := range []string{"token_missing", "token_expired", "request_failed", "broker_rejected", "response_invalid"} {
		quoter := &fakeQuoter{code: code}
		handler := NewHandler(priceBandService(quoter))
		status, body := priceBandRequest(t, handler, "/v1/price-band?scope=kis_mock&stock_code=005930")
		if status != http.StatusBadGateway {
			t.Fatalf("%s: status = %d, want 502", code, status)
		}
		var got string
		if err := json.Unmarshal(body["error_code"], &got); err != nil || got != code {
			t.Fatalf("error_code = %v, want %q", body, code)
		}
	}
	missing := NewHandler(priceBandService(nil))
	status, _ := priceBandRequest(t, missing, "/v1/price-band?scope=kis_mock&stock_code=005930")
	if status != http.StatusBadGateway {
		t.Fatalf("missing quoter: status = %d, want 502", status)
	}
}

// The real quoter must send exactly the pinned inquire-price request and
// surface the band; the token cache and KIS transport are faked.
func TestKISMockQuoterSendsPinnedInquiry(t *testing.T) {
	transport := &countingTransport{respond: func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.URL.Path != "/uapi/domestic-stock/v1/quotations/inquire-price" {
			t.Errorf("unexpected request %s %s", request.Method, request.URL)
		}
		if request.URL.Query().Get("FID_INPUT_ISCD") != "005930" || request.Header.Get("tr_id") != "FHKST01010100" {
			t.Errorf("request = %s tr_id=%s", request.URL, request.Header.Get("tr_id"))
		}
		return testHTTPResponse(http.StatusOK, `{"rt_cd":"0","output":{"stck_llam":"49700","stck_mxpr":"92300","stck_sdpr":"71000","stck_prpr":"70500"}}`), nil
	}}
	getter := &stubRedisGetter{value: `{"access_token":"cached-token-for-test","expires_at":99999999999}`}
	quoter := KISMockQuoter{
		Transport: transport,
		LoadConfig: func() (kismockread.Config, string) {
			return testBrokerConfig(), ""
		},
		NewGetter: func(kismockread.Config) (kismockread.RedisGetter, string) {
			return getter, ""
		},
	}
	band, code := quoter.PriceBand(context.Background(), "005930")
	if code != "" {
		t.Fatalf("code = %q", code)
	}
	if band.LowerLimit != "49700" || band.UpperLimit != "92300" || band.BasePrice != "71000" || band.LastPrice != "70500" {
		t.Fatalf("band = %#v", band)
	}
	if transport.calls.Load() != 1 {
		t.Fatalf("transport calls = %d", transport.calls.Load())
	}
}

func TestKISMockQuoterPropagatesFailures(t *testing.T) {
	quoter := KISMockQuoter{
		LoadConfig: func() (kismockread.Config, string) { return kismockread.Config{}, "configuration_missing" },
	}
	if _, code := quoter.PriceBand(context.Background(), "005930"); code != "configuration_missing" {
		t.Fatalf("config failure code = %q", code)
	}
	if _, code := (KISMockQuoter{}).PriceBand(context.Background(), "005930"); code != ErrorStorageFailure {
		t.Fatalf("nil loader code = %q", code)
	}
	quoter = KISMockQuoter{
		LoadConfig: func() (kismockread.Config, string) { return testBrokerConfig(), "" },
		NewGetter:  func(kismockread.Config) (kismockread.RedisGetter, string) { return nil, "token_cache_unavailable" },
	}
	if _, code := quoter.PriceBand(context.Background(), "005930"); code != "token_cache_unavailable" {
		t.Fatalf("getter failure code = %q", code)
	}
}

type stubRedisGetter struct {
	value string
}

func (getter *stubRedisGetter) Get(context.Context, string) (string, bool, error) {
	return getter.value, true, nil
}
