package kismockedge

import (
	"bytes"
	"context"
	"encoding/json"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
)

func testLiveWitnessCommand(commandID string) executioncontracts.ExecutionCommandV1 {
	command := testCommand(commandID)
	command.AccountScope = executioncontracts.AccountScopeKISLive
	command.Price = "0" // Phase 1 records a valid zero price; it does not price or submit it.
	return command
}

func TestKISLiveWitnessIsIdempotentAndNeverUsesTransport(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	transport := panicTransport{t: t}
	service := NewEnvironmentService(store, func(name string) string {
		if name == "EDGE_KIS_LIVE_SHADOW_ENABLED" {
			return "true"
		}
		return ""
	}, transport)
	// A mock broker is also installed as a cross-mutant witness: a live branch
	// accidentally routed to mock submission would increment this counter.
	mockBroker := &fakeBroker{result: BrokerResult{Accepted: true, BrokerOrderID: "must-not-send"}}
	service.Brokers[executioncontracts.AccountScopeKISMock] = mockBroker
	server := httptest.NewServer(NewHandler(service))
	defer server.Close()

	command := testLiveWitnessCommand("live-no-egress")
	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	first := postJSON(t, server.URL+"/v1/commands", payload)
	defer first.Body.Close()
	if first.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(first.Body)
		t.Fatalf("first witness status = %d: %s", first.StatusCode, body)
	}
	var receipt WitnessReceipt
	if err := json.NewDecoder(first.Body).Decode(&receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.WitnessID != command.CommandID || receipt.CommandID != command.CommandID || receipt.Mode != shadowWitnessMode {
		t.Fatalf("witness receipt = %+v", receipt)
	}
	second := postJSON(t, server.URL+"/v1/commands", payload)
	defer second.Body.Close()
	var duplicate WitnessReceipt
	if err := json.NewDecoder(second.Body).Decode(&duplicate); err != nil {
		t.Fatal(err)
	}
	if second.StatusCode != http.StatusOK || duplicate != receipt {
		t.Fatalf("duplicate status/receipt = %d %+v, want 200 %+v", second.StatusCode, duplicate, receipt)
	}
	if mockBroker.sends.Load() != 0 {
		t.Fatalf("kis_live reached mock broker %d times", mockBroker.sends.Load())
	}
	var rows int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM kis_live_witnesses WHERE command_id = ?`, command.CommandID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("witness rows = %d, want 1", rows)
	}
}

func TestKISLiveWitnessEchoAndMissingEchoAudit(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	service := &Service{Store: store, KISLiveShadowEnabled: true, Now: func() time.Time {
		return time.Date(2026, 9, 2, 9, 0, 0, 0, time.UTC)
	}}
	server := httptest.NewServer(NewHandler(service))
	defer server.Close()
	first := testLiveWitnessCommand("live-missing-echo")
	second := testLiveWitnessCommand("live-with-echo")
	for _, command := range []executioncontracts.ExecutionCommandV1{first, second} {
		payload, _ := json.Marshal(command)
		response := postJSON(t, server.URL+"/v1/commands", payload)
		if response.StatusCode != http.StatusOK {
			response.Body.Close()
			t.Fatalf("witness status = %d", response.StatusCode)
		}
		response.Body.Close()
	}
	echo := []byte(`{"ODNO":"000123","rt_cd":"0","msg_cd":"M0000","msg1":"accepted","received_at":"2026-09-02T09:00:01Z"}`)
	response := postJSON(t, server.URL+"/v1/commands/"+second.CommandID+"/echo", echo)
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("echo status = %d: %s", response.StatusCode, body)
	}
	response.Body.Close()
	response = postJSON(t, server.URL+"/v1/commands/"+second.CommandID+"/echo", echo)
	if response.StatusCode != http.StatusConflict || !strings.Contains(readAndClose(t, response), "echo_already_recorded") {
		t.Fatalf("duplicate echo = %d", response.StatusCode)
	}

	response, err := http.Get(server.URL + "/v1/commands?scope=kis_live&missing_echo=true")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var audit struct {
		Witnesses []WitnessReceipt `json:"witnesses"`
	}
	if err := json.NewDecoder(response.Body).Decode(&audit); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || len(audit.Witnesses) != 1 || audit.Witnesses[0].CommandID != first.CommandID {
		t.Fatalf("missing echo audit = status %d %+v", response.StatusCode, audit)
	}
	metricsResponse, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	metrics := readAndClose(t, metricsResponse)
	if !strings.Contains(metrics, `edge_commands_total{outcome="recorded",phase="shadow",scope="kis_live"} 2`) ||
		!strings.Contains(metrics, `edge_witness_missing_echo{scope="kis_live"} 1`) {
		t.Fatalf("shadow metrics missing or wrong: %s", metrics)
	}
}

func TestKISLiveWitnessGateIsFailClosed(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	server := httptest.NewServer(NewHandler(&Service{Store: store}))
	defer server.Close()
	payload, _ := json.Marshal(testLiveWitnessCommand("live-gate-off"))
	response := postJSON(t, server.URL+"/v1/commands", payload)
	if response.StatusCode != http.StatusForbidden || !strings.Contains(readAndClose(t, response), ErrorScopeDisabled) {
		t.Fatalf("disabled live witness = %d", response.StatusCode)
	}
}

func TestKISLiveModeRejectsAnythingButShadow(t *testing.T) {
	_, err := ServerConfigFromEnv(func(name string) string {
		if name == "EDGE_KIS_LIVE_MODE" {
			return "send"
		}
		return ""
	})
	if err == nil {
		t.Fatal("non-shadow live mode was accepted")
	}
}

type panicTransport struct{ t *testing.T }

func (transport panicTransport) RoundTrip(*http.Request) (*http.Response, error) {
	transport.t.Fatal("kis_live witness attempted HTTP egress")
	return nil, nil
}

func postJSON(t *testing.T, url string, body []byte) *http.Response {
	t.Helper()
	response, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func readAndClose(t *testing.T, response *http.Response) string {
	t.Helper()
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestKISLiveWitnessStoreDoesNotRequireContextCancellation(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	service := &Service{Store: store, KISLiveShadowEnabled: true}
	if _, code, err := service.ProcessWitness(context.Background(), testLiveWitnessCommand("live-direct")); err != nil || code != "" {
		t.Fatalf("direct witness = code %q err %v", code, err)
	}
}

// This is intentionally structural as well as behavioral: merely wiring the
// declaration-only TR table to an HTTP builder is a Phase 2 design change and
// must fail review before a send can become reachable.
func TestKISLiveWitnessHasNoEgressDependencies(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate witness source")
	}
	directory := path.Dir(filename)
	file, err := parser.ParseFile(token.NewFileSet(), path.Join(directory, "witness.go"), nil, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, imported := range file.Imports {
		name, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			t.Fatal(err)
		}
		switch name {
		case "net/http", "net/url", "github.com/mgh3326/broker-edge/internal/kislive", "github.com/mgh3326/broker-edge/internal/kismockread":
			t.Fatalf("shadow witness imports egress dependency %q", name)
		}
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path.Join(directory, entry.Name()), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imported := range file.Imports {
			name, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if name == "github.com/mgh3326/broker-edge/internal/kislive" {
				t.Fatalf("Phase 1 edge imports live TR table from %s", entry.Name())
			}
		}
	}
}
