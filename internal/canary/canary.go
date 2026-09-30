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
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	"github.com/mgh3326/broker-edge/internal/kismockedge"
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
	OutcomePriceUnavailable   = "price_unavailable"
	OutcomeNoSession          = "no_session"
	commandIDPrefix           = "broker-edge-canary:"
	defaultEdgeURL            = "http://127.0.0.1:8080"
	defaultTextfileDirectory  = "/var/lib/node_exporter/textfile"
	textfileName              = "broker_edge_canary.prom"
)

// Config is intentionally limited to mock command details and local output.
// There is deliberately no price knob: the KR limit price is derived at run
// time from the edge's daily-band inquiry so it is always inside the band.
type Config struct {
	EdgeURL     string
	KRSymbol    string
	TextfileDir string
}

// Result is the public, non-order-sensitive run record written to stdout.
// ErrorCode and Rejection appear only when the edge produced a decodable
// rejection receipt; they never affect the success shape.
type Result struct {
	Scope     string `json:"scope"`
	Outcome   string `json:"outcome"`
	Timestamp string `json:"timestamp"`
	// ErrorCode is the edge's closed error vocabulary (e.g. broker_5xx,
	// tick_mismatch, token_expired) distinguishing which layer refused.
	ErrorCode string `json:"error_code,omitempty"`
	// Rejection carries the broker's own failure fields, already masked by the
	// edge so account numbers, tokens, and application keys cannot appear.
	Rejection *executioncontracts.BrokerRejectionV1 `json:"rejection,omitempty"`
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
	State     string                                `json:"state"`
	ErrorCode string                                `json:"error_code,omitempty"`
	Rejection *executioncontracts.BrokerRejectionV1 `json:"rejection,omitempty"`
}

// ConfigFromEnv returns safe defaults; endpoint validation happens before a request.
func ConfigFromEnv(lookup func(string) string) Config {
	if lookup == nil {
		lookup = func(string) string { return "" }
	}
	config := Config{
		EdgeURL:     strings.TrimSpace(lookup("CANARY_EDGE_URL")),
		KRSymbol:    strings.TrimSpace(lookup("CANARY_KR_SYMBOL")),
		TextfileDir: strings.TrimSpace(lookup("CANARY_TEXTFILE_DIR")),
	}
	if config.EdgeURL == "" {
		config.EdgeURL = defaultEdgeURL
	}
	if config.KRSymbol == "" {
		config.KRSymbol = "005930"
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
	stock := config.KRSymbol
	var price string
	if scope == ScopeUS {
		stock, price = "AAPL", "1"
	} else {
		derived, ok := inquireKRPrice(ctx, client, base, stock)
		if !ok {
			result.Outcome = OutcomePriceUnavailable
			return result
		}
		price = derived.String()
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
		if decodeErr == nil {
			result.ErrorCode = receipt.ErrorCode
			result.Rejection = receipt.Rejection
		}
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
		if decodeErr == nil {
			result.ErrorCode = cancelled.ErrorCode
			result.Rejection = cancelled.Rejection
		}
		return result
	}
	result.Outcome = OutcomeOK
	return result
}

// inquireKRPrice reads the symbol's daily band through the edge's read-only
// endpoint and derives the limit price inside it. Every failure fails closed:
// the caller places no order when this returns false.
func inquireKRPrice(ctx context.Context, client *http.Client, base, symbol string) (*big.Int, bool) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		base+"/v1/price-band?scope="+ScopeKR+"&stock_code="+url.QueryEscape(symbol), nil)
	if err != nil {
		return nil, false
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, false
	}
	var band executioncontracts.PriceBandV1
	decodeErr := json.NewDecoder(response.Body).Decode(&band)
	response.Body.Close()
	if decodeErr != nil || response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices ||
		band.SchemaVersion != executioncontracts.PriceBandV1SchemaVersion || band.StockCode != symbol {
		return nil, false
	}
	return deriveKRPrice(band)
}

// deriveKRPrice picks the order price inside the daily band. The exchange's
// own lower limit is used verbatim when the inquiry provides it; otherwise
// the band floor is recomputed as base x 0.70 rounded up to the KRX tick.
func deriveKRPrice(band executioncontracts.PriceBandV1) (*big.Int, bool) {
	if lower, ok := positiveInteger(band.LowerLimit); ok {
		return roundUpToTickKR(lower), true
	}
	if base, ok := positiveInteger(band.BasePrice); ok {
		return bandFloorKR(base), true
	}
	return nil, false
}

// roundUpToTickKR returns the smallest KRX tick multiple at or above price.
// A real lower limit is already tick-aligned, so this normally returns the
// limit unchanged; it exists so a malformed band can never produce a price
// the edge's own tick check would reject.
func roundUpToTickKR(price *big.Int) *big.Int {
	tick := kismockedge.GetTickSizeKR(price)
	quotient, remainder := new(big.Int).QuoRem(price, tick, new(big.Int))
	if remainder.Sign() != 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	return quotient.Mul(quotient, tick)
}

// bandFloorKR computes base x 0.70 rounded up to the KRX tick, entirely in
// integer arithmetic: scaled = base x 7 keeps tenths exact, and the smallest
// tick multiple at or above scaled/10 is the band floor.
func bandFloorKR(base *big.Int) *big.Int {
	scaled := new(big.Int).Mul(base, big.NewInt(7))
	ceiling := new(big.Int).Quo(scaled, big.NewInt(10))
	if new(big.Int).Rem(scaled, big.NewInt(10)).Sign() != 0 {
		ceiling.Add(ceiling, big.NewInt(1))
	}
	tick := kismockedge.GetTickSizeKR(ceiling)
	divisor := new(big.Int).Mul(tick, big.NewInt(10))
	multiples := new(big.Int).Quo(scaled, divisor)
	if new(big.Int).Rem(scaled, divisor).Sign() != 0 {
		multiples.Add(multiples, big.NewInt(1))
	}
	return multiples.Mul(multiples, tick)
}

func positiveInteger(value string) (*big.Int, bool) {
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return nil, false
		}
	}
	parsed, ok := new(big.Int).SetString(value, 10)
	return parsed, ok && parsed.Sign() > 0
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
