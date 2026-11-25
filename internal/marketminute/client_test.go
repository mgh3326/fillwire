package marketminute_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/marketminute"
)

func newClient(t *testing.T, handler http.HandlerFunc, token marketminute.TokenSource) (*marketminute.Client, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &marketminute.Client{
		Token:     token,
		AppKey:    "fixture-app-key",
		AppSecret: "fixture-app-secret",
		Host:      server.URL,
		HTTP:      server.Client(),
		Now:       func() time.Time { return time.Date(2026, 11, 24, 9, 5, 0, 0, time.UTC) },
	}, server
}

func fixedToken(value string) marketminute.TokenSource {
	return func(context.Context) (string, error) { return value, nil }
}

func TestDomesticFetchUsesThePinnedTransactionAndParams(t *testing.T) {
	var seen *http.Request
	client, _ := newClient(t, func(writer http.ResponseWriter, request *http.Request) {
		seen = request
		writer.Write([]byte(`{"rt_cd":"0","output2":[{"stck_bsop_date":"20261124","stck_cntg_hour":"090000","stck_oprc":"100","stck_hgpr":"101","stck_lwpr":"99","stck_prpr":"100","cntg_vol":"10","acml_tr_pbmn":"1000"}]}`))
	}, fixedToken("fixture-token"))
	result := client.Fetch(context.Background(), marketminute.Target{Market: "kr", Symbol: "5930", Venue: "NTX"})
	if result.Err != nil {
		t.Fatalf("Fetch: %v", result.Err)
	}
	if seen.URL.Path != marketminute.DomesticPath || seen.URL.Query().Get("FID_COND_MRKT_DIV_CODE") != "NX" ||
		seen.URL.Query().Get("FID_INPUT_ISCD") != "005930" || seen.Header.Get("tr_id") != marketminute.DomesticTR {
		t.Fatalf("request = %s tr=%s query=%s", seen.URL.Path, seen.Header.Get("tr_id"), seen.URL.RawQuery)
	}
	if seen.Header.Get("authorization") != "Bearer fixture-token" {
		t.Fatal("authorization header missing")
	}
	if len(result.Rows) != 1 || result.Rows[0].DateTime != "2026-11-24 09:00" || *result.Rows[0].Value != "1000" {
		t.Fatalf("rows = %+v", result.Rows)
	}
}

func TestOverseasFetchUsesThePinnedTransaction(t *testing.T) {
	var seen *http.Request
	client, _ := newClient(t, func(writer http.ResponseWriter, request *http.Request) {
		seen = request
		writer.Write([]byte(`{"rt_cd":"0","output2":[{"xymd":"20261215","xhms":"093000","open":"1.50","high":"2.00","low":"1.00","close":"1.75","volume":"30","value":"52.5"}]}`))
	}, fixedToken("fixture-token"))
	result := client.Fetch(context.Background(), marketminute.Target{Market: "us", Symbol: "BRK.B", Venue: "NYSE"})
	if result.Err != nil {
		t.Fatalf("Fetch: %v", result.Err)
	}
	if seen.URL.Path != marketminute.OverseasPath || seen.Header.Get("tr_id") != marketminute.OverseasTR ||
		seen.URL.Query().Get("EXCD") != "NYS" || seen.URL.Query().Get("SYMB") != "BRK/B" {
		t.Fatalf("request = %s tr=%s query=%s", seen.URL.Path, seen.Header.Get("tr_id"), seen.URL.RawQuery)
	}
	if len(result.Rows) != 1 || result.Rows[0].DateTime != "2026-12-15 09:30" {
		t.Fatalf("rows = %+v", result.Rows)
	}
}

func TestFetchMapsRateLimitsAndKeepsTheRetryAfter(t *testing.T) {
	client, _ := newClient(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Retry-After", "7")
		writer.WriteHeader(http.StatusTooManyRequests)
	}, fixedToken("fixture-token"))
	result := client.Fetch(context.Background(), marketminute.Target{Market: "kr", Symbol: "005930", Venue: "KRX"})
	if !result.RateLimited || !errors.Is(result.Err, marketminute.ErrRateLimited) || result.RetryAfter != 7*time.Second {
		t.Fatalf("result = %+v", result)
	}

	messageCode, _ := newClient(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.Write([]byte(`{"rt_cd":"1","msg_cd":"EGW00201"}`))
	}, fixedToken("fixture-token"))
	messageResult := messageCode.Fetch(context.Background(), marketminute.Target{Market: "kr", Symbol: "005930", Venue: "KRX"})
	if !messageResult.RateLimited || marketminute.ErrorClass(messageResult.Err) != "rate_limited" {
		t.Fatalf("result = %+v", messageResult)
	}
}

func TestFetchAsksForANewTokenOnceOnExpiry(t *testing.T) {
	var calls atomic.Int32
	tokens := atomic.Int32{}
	client, _ := newClient(t, func(writer http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			writer.Write([]byte(`{"rt_cd":"1","msg_cd":"EGW00123"}`))
			return
		}
		writer.Write([]byte(`{"rt_cd":"0","output2":[{"stck_bsop_date":"20261124","stck_cntg_hour":"090000","stck_oprc":"100","stck_hgpr":"101","stck_lwpr":"99","stck_prpr":"100","cntg_vol":"10","acml_tr_pbmn":"1000"}]}`))
	}, func(context.Context) (string, error) {
		value := tokens.Add(1)
		return "token-" + string(rune('0'+value)), nil
	})
	result := client.Fetch(context.Background(), marketminute.Target{Market: "kr", Symbol: "005930", Venue: "KRX"})
	if result.Err != nil || len(result.Rows) != 1 {
		t.Fatalf("result = %+v", result)
	}
	if calls.Load() != 2 || tokens.Load() != 2 {
		t.Fatalf("calls=%d token reads=%d, want 2 and 2", calls.Load(), tokens.Load())
	}
}

func TestFetchNeverLeaksCredentialsOrTheBody(t *testing.T) {
	client, _ := newClient(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte("fixture-app-secret fixture-token upstream detail"))
	}, fixedToken("fixture-token"))
	result := client.Fetch(context.Background(), marketminute.Target{Market: "kr", Symbol: "005930", Venue: "KRX"})
	if result.Err == nil {
		t.Fatal("5xx accepted")
	}
	for _, secret := range []string{"fixture-app-secret", "fixture-token", "upstream detail"} {
		if strings.Contains(result.Err.Error(), secret) {
			t.Fatalf("error leaks %q: %v", secret, result.Err)
		}
	}
}

func TestFetchRefusesAnUnknownVenueBeforeTheNetwork(t *testing.T) {
	requested := false
	client, _ := newClient(t, func(http.ResponseWriter, *http.Request) { requested = true }, fixedToken("fixture-token"))
	result := client.Fetch(context.Background(), marketminute.Target{Market: "kr", Symbol: "005930", Venue: "SEHK"})
	if result.Err == nil || requested {
		t.Fatal("an unknown venue was accepted")
	}
}
