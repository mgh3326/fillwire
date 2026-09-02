// Package canary performs one deliberately bounded mock-order round trip.
package canary

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	_ "time/tzdata" // The production image is distroless and has no zoneinfo files.
)

const (
	ScopeKR        = executioncontracts.AccountScopeKISMock
	ScopeUS        = executioncontracts.AccountScopeKISMockUS
	ScopeNoSession = "no_session"

	OutcomeOK                 = "ok"
	OutcomePlaceNotAccepted   = "place_not_accepted"
	OutcomeCancelNotCancelled = "cancel_not_cancelled"
	OutcomeEdgeUnreachable    = "edge_unreachable"
	OutcomeNoSession          = "no_session"
	commandIDPrefix           = "broker-edge-canary:"
	defaultEdgeURL            = "http://127.0.0.1:8080"
	defaultTextfileDirectory  = "/var/lib/node_exporter/textfile"
	textfileName              = "broker_edge_canary.prom"
)

// Config is intentionally limited to mock command details and local output.
type Config struct {
	EdgeURL     string
	KRSymbol    string
	KRPrice     string
	TextfileDir string
}

// Result is the public, non-order-sensitive run record written to stdout.
type Result struct {
	Scope     string `json:"scope"`
	Outcome   string `json:"outcome"`
	Timestamp string `json:"timestamp"`
}

// Options makes the clock, HTTP client, and writers injectable for bounded tests.
type Options struct {
	Now    func() time.Time
	Client *http.Client
	Stdout io.Writer
	Stderr io.Writer
	Lookup func(string) string
}

type cancelReceipt struct {
	State string `json:"state"`
}

// ConfigFromEnv returns safe defaults; endpoint validation happens before a request.
func ConfigFromEnv(lookup func(string) string) Config {
	if lookup == nil {
		lookup = func(string) string { return "" }
	}
	config := Config{
		EdgeURL:     strings.TrimSpace(lookup("CANARY_EDGE_URL")),
		KRSymbol:    strings.TrimSpace(lookup("CANARY_KR_SYMBOL")),
		KRPrice:     strings.TrimSpace(lookup("CANARY_KR_PRICE")),
		TextfileDir: strings.TrimSpace(lookup("CANARY_TEXTFILE_DIR")),
	}
	if config.EdgeURL == "" {
		config.EdgeURL = defaultEdgeURL
	}
	if config.KRSymbol == "" {
		config.KRSymbol = "005930"
	}
	if config.KRPrice == "" {
		config.KRPrice = "1000"
	}
	if config.TextfileDir == "" {
		config.TextfileDir = defaultTextfileDirectory
	}
	return config
}

// SelectScope applies the two regular-session windows. Weekends are always skipped;
// exchange holidays remain an operator concern and are safely represented by an
// unaccepted placement rather than a second request.
func SelectScope(now time.Time) string {
	kr, err := time.LoadLocation("Asia/Seoul")
	if err == nil {
		local := now.In(kr)
		if weekday(local) && inWindow(local, 9*60+5, 15*60+15) {
			return ScopeKR
		}
	}
	et, err := time.LoadLocation("America/New_York")
	if err == nil {
		local := now.In(et)
		if weekday(local) && inWindow(local, 9*60+35, 15*60+55) {
			return ScopeUS
		}
	}
	return ScopeNoSession
}

func weekday(t time.Time) bool { return t.Weekday() != time.Saturday && t.Weekday() != time.Sunday }
func inWindow(t time.Time, start, end int) bool {
	minute := t.Hour()*60 + t.Minute()
	return minute >= start && minute <= end
}

// Run executes no more than one placement and one cancellation. It never retries.
func Run(ctx context.Context, options Options) int {
	now := options.Now
	if now == nil {
		now = time.Now
	}
	stdout := options.Stdout
	if stdout == nil {
		stdout = os.Stdout
	}
	stderr := options.Stderr
	if stderr == nil {
		stderr = os.Stderr
	}
	lookup := options.Lookup
	if lookup == nil {
		lookup = os.Getenv
	}
	config := ConfigFromEnv(lookup)
	result := execute(ctx, config, now, options.Client)
	_ = json.NewEncoder(stdout).Encode(result)
	if err := WriteTextfile(config.TextfileDir, result, now().UTC()); err != nil {
		_, _ = fmt.Fprintln(stderr, "edge-canary: textfile_write_failed")
		return 1
	}
	if result.Outcome == OutcomeOK || result.Outcome == OutcomeNoSession {
		return 0
	}
	return 1
}

func execute(ctx context.Context, config Config, now func() time.Time, client *http.Client) Result {
	runAt := now().UTC()
	scope := SelectScope(runAt)
	result := Result{Scope: scope, Timestamp: runAt.Format(time.RFC3339)}
	if scope == ScopeNoSession {
		result.Outcome = OutcomeNoSession
		return result
	}
	base, err := loopbackURL(config.EdgeURL)
	if err != nil {
		result.Outcome = OutcomeEdgeUnreachable
		return result
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	commandID := commandID(runAt)
	stock, price := config.KRSymbol, config.KRPrice
	if scope == ScopeUS {
		stock, price = "AAPL", "1"
	}
	command := executioncontracts.ExecutionCommandV1{
		SchemaVersion: executioncontracts.ExecutionCommandV1SchemaVersion, CommandID: commandID,
		AccountScope: scope, Side: "buy", StockCode: stock, Quantity: "1", Price: price,
		OrderType: "limit", IssuedAt: runAt.Format(time.RFC3339),
	}
	body, _ := json.Marshal(command)
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/commands", bytes.NewReader(body))
	request.Header.Set("content-type", "application/json")
	request.Header.Set("X-Correlation-ID", commandID)
	response, err := client.Do(request)
	if err != nil {
		result.Outcome = OutcomeEdgeUnreachable
		return result
	}
	var receipt executioncontracts.ExecutionReceiptV1
	decodeErr := json.NewDecoder(response.Body).Decode(&receipt)
	response.Body.Close()
	if decodeErr != nil || response.StatusCode < 200 || response.StatusCode >= 300 || receipt.Disposition != executioncontracts.DispositionAccepted {
		result.Outcome = OutcomePlaceNotAccepted
		return result
	}
	cancelRequest, _ := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/commands/"+url.PathEscape(commandID)+"/cancel", nil)
	cancelRequest.Header.Set("X-Correlation-ID", commandID)
	cancelResponse, err := client.Do(cancelRequest)
	if err != nil {
		result.Outcome = OutcomeEdgeUnreachable
		return result
	}
	var cancelled cancelReceipt
	decodeErr = json.NewDecoder(cancelResponse.Body).Decode(&cancelled)
	cancelResponse.Body.Close()
	if decodeErr != nil || cancelResponse.StatusCode < 200 || cancelResponse.StatusCode >= 300 || cancelled.State != "CANCELLED" {
		result.Outcome = OutcomeCancelNotCancelled
		return result
	}
	result.Outcome = OutcomeOK
	return result
}

func loopbackURL(value string) (string, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("invalid edge URL")
	}
	host := parsed.Hostname()
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return "", errors.New("edge must be loopback")
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func commandID(now time.Time) string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		return fmt.Sprintf("%s%s", commandIDPrefix, now.Format("20060102T150405.000000000Z"))
	}
	return fmt.Sprintf("%s%s-%s", commandIDPrefix, now.Format("20060102T150405.000000000Z"), hex.EncodeToString(bytes))
}

var successLine = regexp.MustCompile(`(?m)^broker_edge_canary_last_success_timestamp_seconds\{scope="(kis_mock|kis_mock_us)"\} ([0-9]+(?:\.[0-9]+)?)$`)

// WriteTextfile atomically replaces the canary snapshot. It preserves successful
// timestamps from prior scopes, which makes stale-success alerting meaningful.
func WriteTextfile(directory string, result Result, now time.Time) error {
	previous, _ := os.ReadFile(filepath.Join(directory, textfileName))
	successes := map[string]string{}
	for _, match := range successLine.FindAllStringSubmatch(string(previous), -1) {
		successes[match[1]] = match[2]
	}
	if result.Outcome == OutcomeOK {
		successes[result.Scope] = fmt.Sprintf("%d", now.Unix())
	}
	var text strings.Builder
	text.WriteString("# HELP broker_edge_canary_result Last canary result for the selected scope.\n# TYPE broker_edge_canary_result gauge\n")
	fmt.Fprintf(&text, "broker_edge_canary_result{scope=\"%s\",outcome=\"%s\"} 1\n", result.Scope, result.Outcome)
	text.WriteString("# HELP broker_edge_canary_last_run_timestamp_seconds Unix timestamp of the latest canary run.\n# TYPE broker_edge_canary_last_run_timestamp_seconds gauge\n")
	fmt.Fprintf(&text, "broker_edge_canary_last_run_timestamp_seconds %d\n", now.Unix())
	text.WriteString("# HELP broker_edge_canary_last_success_timestamp_seconds Unix timestamp of the latest successful canary per scope.\n# TYPE broker_edge_canary_last_success_timestamp_seconds gauge\n")
	for _, scope := range []string{ScopeKR, ScopeUS} {
		if timestamp := successes[scope]; timestamp != "" {
			fmt.Fprintf(&text, "broker_edge_canary_last_success_timestamp_seconds{scope=\"%s\"} %s\n", scope, timestamp)
		}
	}
	temporary, err := os.CreateTemp(directory, ".broker_edge_canary.prom-")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if _, err := io.WriteString(temporary, text.String()); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Chmod(0o644); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, filepath.Join(directory, textfileName))
}
