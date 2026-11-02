package kisread

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

type fakeTransport func(*http.Request) (*http.Response, error)

func (f fakeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestReadAdapterRefusesOtherTransactionsBeforeTransport(t *testing.T) {
	calls := 0
	c := Client{Mock: true, AppKey: "fixture", AppSecret: "fixture", Token: func() string { return "fixture" }, HTTP: &http.Client{Transport: fakeTransport(func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("unexpected transport") })}}
	for _, path := range []string{"/trading/order-cash", "/oauth2/tokenP", "/uapi/domestic-stock/v1/trading/inquire-daily-ccld"} {
		if err := c.Read(context.Background(), path, "unexpected", nil, &struct{}{}); err == nil {
			t.Fatal("allowed other transaction")
		}
	}
	c.Mock = false
	if err := c.Read(context.Background(), "/uapi/domestic-stock/v1/trading/inquire-daily-ccld", "VTTC0081R", nil, &struct{}{}); err == nil {
		t.Fatal("allowed non-mock account history")
	}
	if calls != 0 {
		t.Fatal("refused request reached transport")
	}
}

func TestReadAdapterDoesNotExposeTransportCredentials(t *testing.T) {
	c := Client{Mock: true, AppKey: "fixture", AppSecret: "fixture-secret", Token: func() string { return "fixture-token" }, HTTP: &http.Client{Transport: fakeTransport(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("fixture-secret fixture-token upstream body")
	})}}
	err := c.Read(context.Background(), "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn", "FHKST01010200", nil, &struct{}{})
	if err == nil || strings.Contains(err.Error(), "fixture-secret") || strings.Contains(err.Error(), "fixture-token") {
		t.Fatalf("unsafe error %v", err)
	}
}

func TestMockReadPacingHonorsCancellation(t *testing.T) {
	c := Client{Mock: true, next: time.Now().Add(time.Minute)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.Read(ctx, "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn", "FHKST01010200", nil, &struct{}{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pacing ignored cancellation: %v", err)
	}
}
