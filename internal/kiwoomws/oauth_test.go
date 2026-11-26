package kiwoomws

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestIssueMockTokenClassifiesPermanentAndTransientFailures(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		permanent bool
	}{
		{name: "unauthorized", status: http.StatusUnauthorized, body: `{}`, permanent: true},
		{name: "forbidden", status: http.StatusForbidden, body: `{}`, permanent: true},
		{name: "oauth return code", status: http.StatusOK, body: `{"return_code":1001}`, permanent: true},
		{name: "rate limited", status: http.StatusTooManyRequests, body: `{}`, permanent: false},
		{name: "server error", status: http.StatusServiceUnavailable, body: `{}`, permanent: false},
		{name: "invalid response", status: http.StatusOK, body: `<html>maintenance</html>`, permanent: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			_, err := issueMockToken(context.Background(), "app-key", "app-secret", server.URL, server.Client())
			if errors.Is(err, ErrPermanentAuth) != tc.permanent {
				t.Fatalf("permanent auth classification = %v, error = %v; want %v", errors.Is(err, ErrPermanentAuth), err, tc.permanent)
			}
		})
	}
}
