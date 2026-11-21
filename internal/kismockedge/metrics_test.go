package kismockedge

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
)

func TestMetricsExposeBoundedLabelsWithoutCommandPayload(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	transport := &countingTransport{respond: func(request *http.Request) (*http.Response, error) {
		switch request.Method {
		case http.MethodPost:
			return testHTTPResponse(http.StatusOK, `{"id":"metric-order","status":"accepted"}`), nil
		case http.MethodDelete:
			return testHTTPResponse(http.StatusNoContent, ""), nil
		default:
			t.Fatalf("unexpected broker metric request: %s %s", request.Method, request.URL)
			return nil, io.ErrUnexpectedEOF
		}
	}}
	service := newTestServiceWithBrokers(store, map[string]Broker{
		executioncontracts.AccountScopeAlpacaPaperCrypto: testAlpacaPaperCryptoBroker(transport),
	}, true)
	server := httptest.NewServer(NewHandler(service))
	defer server.Close()

	command := testAlpacaCommand("metric-command-id-must-not-be-a-label")
	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Post(server.URL+"/v1/commands", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		t.Fatalf("command status = %d", response.StatusCode)
	}
	response.Body.Close()
	response, err = http.Post(server.URL+"/v1/commands/"+command.CommandID+"/cancel", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		t.Fatalf("cancel status = %d", response.StatusCode)
	}
	response.Body.Close()

	response, err = http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	metrics, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(metrics)
	if response.StatusCode != http.StatusOK || !strings.HasPrefix(response.Header.Get("content-type"), "text/plain") {
		t.Fatalf("metrics response = %d %q", response.StatusCode, response.Header.Get("content-type"))
	}
	assertMetricSample(t, text, "broker_edge_commands_total", map[string]string{
		"scope":       executioncontracts.AccountScopeAlpacaPaperCrypto,
		"disposition": string(executioncontracts.DispositionAccepted),
	})
	assertMetricSample(t, text, "broker_edge_cancels_total", map[string]string{
		"scope": executioncontracts.AccountScopeAlpacaPaperCrypto,
		"state": string(CancelStateCancelled),
	})
	assertMetricSample(t, text, "broker_edge_broker_requests_total", map[string]string{
		"scope": executioncontracts.AccountScopeAlpacaPaperCrypto,
		"kind":  metricKindPlace,
	})
	assertMetricSample(t, text, "broker_edge_broker_requests_total", map[string]string{
		"scope": executioncontracts.AccountScopeAlpacaPaperCrypto,
		"kind":  metricKindCancel,
	})
	if !strings.Contains(text, "broker_edge_http_request_duration_seconds_bucket") ||
		!strings.Contains(text, "go_goroutines") || !strings.Contains(text, "process_cpu_seconds_total") {
		t.Fatal("metrics endpoint did not expose HTTP, Go, and process collectors")
	}

	// Mutant witness: adding command_id (or any order payload field) as a
	// CounterVec label must turn this test red. The full Prometheus text is
	// scanned instead of merely inspecting our source declarations.
	for _, forbidden := range []string{"command_id", "symbol", "price", "quantity"} {
		if strings.Contains(text, forbidden+`="`) {
			t.Fatalf("forbidden metric label %q present in collected text", forbidden)
		}
	}
	if strings.Contains(text, command.CommandID) || strings.Contains(text, command.StockCode) {
		t.Fatal("command identifier or symbol present in collected metrics")
	}
}

func assertMetricSample(t *testing.T, text, metric string, labels map[string]string) {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, metric+"{") {
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
				t.Fatalf("metric %s with labels %#v = %q, want 1", metric, labels, line)
			}
			return
		}
	}
	t.Fatalf("metric %s with labels %#v not found", metric, labels)
}
