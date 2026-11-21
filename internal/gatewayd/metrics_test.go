package gatewayd

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsExposeEnsureOutcomes(t *testing.T) {
	handler := NewHandler(ProviderEnsurers{
		ProviderKISMock: staticEnsurer{state: EnsureStateFresh},
		ProviderToss:    staticEnsurer{state: EnsureStateIssued},
	}, nil)
	for _, path := range []string{
		"/v1/tokens/kis-mock/ensure",
		"/v1/tokens/toss/ensure",
		"/v1/tokens/kis-live/ensure", // configured failure is a bounded failed state.
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, nil))
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	text := response.Body.String()
	if response.Code != http.StatusOK || !strings.HasPrefix(response.Header().Get("content-type"), "text/plain") {
		t.Fatalf("metrics response = %d %q", response.Code, response.Header().Get("content-type"))
	}
	assertGatewayMetric(t, text, map[string]string{"provider": string(ProviderKISMock), "state": string(EnsureStateFresh)})
	assertGatewayMetric(t, text, map[string]string{"provider": string(ProviderToss), "state": string(EnsureStateIssued)})
	assertGatewayMetric(t, text, map[string]string{"provider": string(ProviderKISLive), "state": ensureMetricStateFailed})
	if !strings.Contains(text, "go_goroutines") || !strings.Contains(text, "process_cpu_seconds_total") {
		t.Fatal("gateway metrics endpoint did not expose Go and process collectors")
	}
}

func assertGatewayMetric(t *testing.T, text string, labels map[string]string) {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "gatewayd_ensure_results_total{") {
			continue
		}
		matches := true
		for key, value := range labels {
			if !strings.Contains(line, key+`="`+value+`"`) {
				matches = false
				break
			}
		}
		if matches {
			fields := strings.Fields(line)
			if len(fields) < 2 || fields[len(fields)-1] != "1" {
				t.Fatalf("ensure metric with labels %#v = %q, want 1", labels, line)
			}
			return
		}
	}
	t.Fatalf("ensure metric with labels %#v not found", labels)
}
