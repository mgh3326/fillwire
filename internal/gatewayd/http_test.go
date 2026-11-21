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

func TestHandlerHealthAndEnsureStateSchemaForConfiguredProviders(t *testing.T) {
	health := httptest.NewRecorder()
	handler := NewHandler(ProviderEnsurers{
		ProviderKISMock: staticEnsurer{state: EnsureStateFresh},
		ProviderKISLive: staticEnsurer{state: EnsureStateIssued},
		ProviderToss:    staticEnsurer{state: EnsureStateFresh},
	}, nil)
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK || health.Body.String() != `{"status":"ok"}` {
		t.Fatalf("health response = %d %q", health.Code, health.Body.String())
	}
	for _, test := range []struct {
		provider TokenProvider
		state    EnsureState
		body     string
	}{
		{provider: ProviderKISMock, state: EnsureStateFresh, body: `{"state":"fresh"}`},
		{provider: ProviderKISLive, state: EnsureStateIssued, body: `{"state":"issued"}`},
		{provider: ProviderToss, state: EnsureStateFresh, body: `{"state":"fresh"}`},
	} {
		t.Run(string(test.provider), func(t *testing.T) {
			ensure := httptest.NewRecorder()
			handler.ServeHTTP(ensure, httptest.NewRequest(http.MethodPost, "/v1/tokens/"+string(test.provider)+"/ensure", nil))
			if ensure.Code != http.StatusOK || ensure.Body.String() != test.body {
				t.Fatalf("ensure response = %d %q", ensure.Code, ensure.Body.String())
			}
			if ensure.Header().Get("content-type") != "application/json" {
				t.Fatalf("content type = %q", ensure.Header().Get("content-type"))
			}
		})
	}
}

func TestHandlerRejectsUnknownAndUnconfiguredProviders(t *testing.T) {
	var logs bytes.Buffer
	handler := NewHandler(ProviderEnsurers{ProviderKISMock: staticEnsurer{state: EnsureStateFresh}}, testLogger(&logs))
	for _, path := range []string{
		"/v1/tokens/unknown/ensure",
		"/v1/tokens/kis-live/ensure",
		"/v1/tokens/toss/ensure",
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, nil))
		if response.Code != http.StatusNotFound || response.Body.String() != `{"error":"not_found"}` {
			t.Fatalf("%s response = %d %q", path, response.Code, response.Body.String())
		}
	}
	if strings.Contains(logs.String(), "unknown") || strings.Contains(logs.String(), "kis-live") || strings.Contains(logs.String(), "toss") {
		t.Fatalf("provider path leaked to logs: %q", logs.String())
	}
}

func TestHandlerRedactsTokensAndLiveCredentialLikeProbes(t *testing.T) {
	for _, test := range []struct {
		provider TokenProvider
		probe    string
	}{
		{provider: ProviderKISMock, probe: "opaque-mock-token-redaction-probe"},
		{provider: ProviderKISLive, probe: "live-kis-app-key-shape-redaction-probe-0123456789"},
		{provider: ProviderToss, probe: "live-toss-client-secret-shape-redaction-probe-0123456789"},
	} {
		t.Run(string(test.provider), func(t *testing.T) {
			now := testGatewayNow()
			store := newFakeRedisStore(func() time.Time { return now })
			service := newTestEnsureServiceForProvider(t, store, &fakeOAuthIssuer{
				token: OAuthToken{AccessToken: test.probe, ExpiresIn: 3600},
			}, now, test.provider)
			var logs bytes.Buffer
			handler := NewHandler(ProviderEnsurers{test.provider: service}, testLogger(&logs))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/tokens/"+string(test.provider)+"/ensure", nil))
			combined := response.Body.String() + "\n" + logs.String()
			if response.Code != http.StatusOK || response.Body.String() != `{"state":"issued"}` {
				t.Fatalf("issuance response = %d %q", response.Code, response.Body.String())
			}
			if strings.Contains(combined, test.probe) {
				t.Fatalf("credential-like value appeared in HTTP output or logs: %q", combined)
			}

			logs.Reset()
			handler = NewHandler(ProviderEnsurers{test.provider: staticEnsurer{err: errors.New("upstream returned " + test.probe)}}, testLogger(&logs))
			response = httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/tokens/"+string(test.provider)+"/ensure", nil))
			combined = response.Body.String() + "\n" + logs.String()
			if response.Code != http.StatusServiceUnavailable || response.Body.String() != `{"error":"ensure_failed"}` {
				t.Fatalf("failure response = %d %q", response.Code, response.Body.String())
			}
			if strings.Contains(combined, test.probe) {
				t.Fatalf("error value appeared in HTTP output or logs: %q", combined)
			}
		})
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
		text, ok := value.(string)
		if !ok {
			continue
		}
		_, _ = logger.buffer.WriteString(text)
	}
	_ = logger.buffer.WriteByte('\n')
}
