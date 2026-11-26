package nhplugws

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestOAuthTokenSourceUsesSDKApprovalFormAndCaches(t *testing.T) {
	source, err := NewOAuthTokenSource("fixture-key", "fixture-secret")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 26, 0, 0, 0, 0, time.UTC)
	source.now = func() time.Time { return now }
	requestCount := 0
	source.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requestCount++
		if req.URL.String() != OAuthEndpoint || req.Method != http.MethodPost || req.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			t.Fatalf("OAuth request metadata = %s %s %s", req.Method, req.URL, req.Header.Get("Content-Type"))
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatal(err)
		}
		form := string(body)
		for _, field := range []string{"appkey=fixture-key", "appsecretkey=fixture-secret", "grant_type=client_credentials", "scope=oob"} {
			if !strings.Contains(form, field) {
				t.Fatalf("OAuth form %q omitted %q", form, field)
			}
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"access_token":"fixture-token","expires_in":"3600"}`)), Header: make(http.Header)}, nil
	})}
	token, err := source.Token(context.Background())
	if err != nil || token != "fixture-token" {
		t.Fatalf("first token = %q, %v", token, err)
	}
	token, err = source.Token(context.Background())
	if err != nil || token != "fixture-token" || requestCount != 1 {
		t.Fatalf("cached token = %q, requests=%d, error=%v", token, requestCount, err)
	}
	source.Invalidate("different-token")
	if _, err := source.Token(context.Background()); err != nil || requestCount != 1 {
		t.Fatalf("unmatched invalidation changed cache: requests=%d, error=%v", requestCount, err)
	}
	source.Invalidate("fixture-token")
	if _, err := source.Token(context.Background()); err != nil || requestCount != 2 {
		t.Fatalf("matched invalidation did not refresh: requests=%d, error=%v", requestCount, err)
	}
}

func TestOAuthEndpointIsPinnedToNHPLUGAuthHost(t *testing.T) {
	if OAuthEndpoint != "https://api.nhplug.com:8443/oauth2/token" {
		t.Fatalf("OAuth endpoint = %q", OAuthEndpoint)
	}
}

func TestOAuthUnauthorizedResponseIsPermanentCredentialError(t *testing.T) {
	source, err := NewOAuthTokenSource("fixture-key", "fixture-secret")
	if err != nil {
		t.Fatal(err)
	}
	source.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})}
	if _, err := source.Token(context.Background()); !errors.Is(err, ErrOAuthCredentialsRejected) {
		t.Fatalf("unauthorized token error = %v", err)
	}
}
