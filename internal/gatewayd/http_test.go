package gatewayd

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type staticEnsurer struct {
	state EnsureState
	err   error
}

func (ensurer staticEnsurer) Ensure(context.Context) (EnsureState, error) {
	return ensurer.state, ensurer.err
}

func TestHandlerHealthAndEnsureStateSchema(t *testing.T) {
	health := httptest.NewRecorder()
	handler := NewHandler(staticEnsurer{state: EnsureStateFresh}, nil)
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK || health.Body.String() != `{"status":"ok"}` {
		t.Fatalf("health response = %d %q", health.Code, health.Body.String())
	}
	for _, test := range []struct {
		state EnsureState
		body  string
	}{
		{state: EnsureStateFresh, body: `{"state":"fresh"}`},
		{state: EnsureStateIssued, body: `{"state":"issued"}`},
	} {
		ensure := httptest.NewRecorder()
		handler := NewHandler(staticEnsurer{state: test.state}, nil)
		handler.ServeHTTP(ensure, httptest.NewRequest(http.MethodPost, "/v1/tokens/kis-mock/ensure", nil))
		if ensure.Code != http.StatusOK || ensure.Body.String() != test.body {
			t.Fatalf("ensure response = %d %q", ensure.Code, ensure.Body.String())
		}
		if ensure.Header().Get("content-type") != "application/json" {
			t.Fatalf("content type = %q", ensure.Header().Get("content-type"))
		}
	}
}

func TestHandlerRedactsTokensFromResponseAndLogs(t *testing.T) {
	const redactionProbe = "opaque-redaction-probe-token"
	now := testGatewayNow()
	store := newFakeRedisStore(func() time.Time { return now })
	service := newTestEnsureService(t, store, &fakeOAuthIssuer{
		token: OAuthToken{AccessToken: redactionProbe, ExpiresIn: 3600},
	}, now)
	var logs bytes.Buffer
	handler := NewHandler(service, testLogger(&logs))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/tokens/kis-mock/ensure", nil))
	combined := response.Body.String() + "\n" + logs.String()
	if response.Code != http.StatusOK || response.Body.String() != `{"state":"issued"}` {
		t.Fatalf("issuance response = %d %q", response.Code, response.Body.String())
	}
	if strings.Contains(combined, redactionProbe) {
		t.Fatalf("token appeared in HTTP output or logs: %q", combined)
	}

	logs.Reset()
	handler = NewHandler(staticEnsurer{err: errors.New("upstream returned " + redactionProbe)}, testLogger(&logs))
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/tokens/kis-mock/ensure", nil))
	combined = response.Body.String() + "\n" + logs.String()
	if response.Code != http.StatusServiceUnavailable || response.Body.String() != `{"error":"ensure_failed"}` {
		t.Fatalf("failure response = %d %q", response.Code, response.Body.String())
	}
	if strings.Contains(combined, redactionProbe) {
		t.Fatalf("error token appeared in HTTP output or logs: %q", combined)
	}
}

type bufferLogger struct {
	buffer *bytes.Buffer
}

func testLogger(buffer *bytes.Buffer) *bufferLogger {
	return &bufferLogger{buffer: buffer}
}

func (logger *bufferLogger) Print(values ...any) {
	if logger == nil || logger.buffer == nil {
		return
	}
	for _, value := range values {
		_, _ = logger.buffer.WriteString(value.(string))
	}
	_ = logger.buffer.WriteByte('\n')
}
