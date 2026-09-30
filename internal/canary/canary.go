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

	// Stage names recorded in every outcome line. A *_sent stage means the
	// request was dispatched without a usable answer; a *_acked stage means
	// the edge's response was decoded.
	StageBandGet     = "band_get"
	StagePlaceSent   = "place_sent"
	StagePlaceAcked  = "place_acked"
	StageCancelSent  = "cancel_sent"
	StageCancelAcked = "cancel_acked"

	// ErrorCanaryTimeout marks a stage whose own deadline elapsed before the
	// edge answered. It is deliberately distinct from the edge's closed
	// broker_timeout vocabulary: the edge may still have processed the send.
	ErrorCanaryTimeout = "canary_timeout"
	ErrorCheckFailed   = "check_failed"

	// Per-stage budgets (new values). Previously one shared http.Client{Timeout:
	// 15s} covered every call and there was no overall deadline, so a slow
	// band inquiry could consume the placement budget and a hung stage could
	// hold the run forever. Each edge-side broker call is internally capped
	// around 10s (kismockread.Config.Timeout default), so each stage timeout
	// leaves room for loopback and token overhead, and the overall deadline
	// covers the worst-case band+place+check+cancel sequence with margin.
	defaultBandTimeout    = 20 * time.Second
	defaultPlaceTimeout   = 25 * time.Second
	defaultCheckTimeout   = 25 * time.Second
	defaultCancelTimeout  = 25 * time.Second
	defaultOverallTimeout = 120 * time.Second
)

// Config is intentionally limited to mock command details and local output.
// There is deliberately no price knob: the KR limit price is derived at run
// time from the edge's daily-band inquiry so it is always inside the band.
type Config struct {
	EdgeURL     string
	KRSymbol    string
	TextfileDir string
}

// BandEvidence records the price-band fields the derived KR price was computed
// from. A price on the outcome line is never trusted alone; the band inputs
// that produced it travel with it.
type BandEvidence struct {
	StockCode  string `json:"stock_code"`
	LowerLimit string `json:"lower_limit,omitempty"`
	BasePrice  string `json:"base_price,omitempty"`
}

// OrderCheckEvidence records the bounded post-place-ambiguity check: one
// command-scoped resolve call answered through the edge's own evidence read.
// It carries dispositions and counts only — never order, account, or token
// values.
type OrderCheckEvidence struct {
	Disposition  string `json:"disposition,omitempty"`
	EvidenceRead string `json:"evidence_read,omitempty"`
	OrdersSeen   int    `json:"orders_seen"`
	Matched      int    `json:"matched"`
	// ErrorCode is the resolve endpoint's closed code when the check itself
	// could not answer (e.g. command_not_found, storage_failure, a read code).
	ErrorCode string `json:"error_code,omitempty"`
	// CancelState records the cleanup cancel's decoded state. It stays empty
	// unless the check proved this command's own order resting and the cancel
	// was actually dispatched.
	CancelState string `json:"cancel_state,omitempty"`
}

// Result is the public, non-order-sensitive run record written to stdout.
// ErrorCode and Rejection appear only when the edge produced a decodable
// rejection receipt; they never affect the success shape. Every outcome —
// success, rejection, timeout, or price_unavailable — records the derived
// price, the band fields used, the furthest stage reached, and per-stage
// durations.
type Result struct {
	Scope     string `json:"scope"`
	Outcome   string `json:"outcome"`
	Timestamp string `json:"timestamp"`
	// CommandID is this run's unique command identifier; it is also the only
	// key cleanup may ever cancel by.
	CommandID string `json:"command_id,omitempty"`
	// Price is the limit price sent (KR: derived inside the daily band;
	// US: the fixed canary price).
	Price string        `json:"price,omitempty"`
	Band  *BandEvidence `json:"band,omitempty"`
	// Stage is the furthest stage the run reached (band_get, place_sent,
	// place_acked, cancel_sent, cancel_acked).
	Stage string `json:"stage,omitempty"`
	// StageDurationsMS carries the wall duration of each stage that ran,
	// keyed by stage name (place covers send-to-ack; check covers the
	// post-ambiguity resolve call).
	StageDurationsMS map[string]int64 `json:"durations_ms,omitempty"`
	// ErrorCode is the edge's closed error vocabulary (e.g. broker_5xx,
	// tick_mismatch, token_expired) or the canary-local canary_timeout,
	// distinguishing which layer refused.
	ErrorCode string `json:"error_code,omitempty"`
	// Rejection carries the broker's own failure fields, already masked by the
	// edge so account numbers, tokens, and application keys cannot appear.
	Rejection *executioncontracts.BrokerRejectionV1 `json:"rejection,omitempty"`
	// OrderCheck is present whenever the run performed the post-ambiguity
	// evidence check on its own command.
	OrderCheck *OrderCheckEvidence `json:"order_check,omitempty"`
}

// Options makes the clock, HTTP client, writers, and deadlines injectable for
// bounded tests. A zero timeout uses the documented default for that stage.
type Options struct {
	Now    func() time.Time
	Client *http.Client
	Stdout io.Writer
	Stderr io.Writer
	Lookup func(string) string

	// OverallTimeout bounds the entire run including cleanup; the per-stage
	// timeouts bound each edge call independently.
	OverallTimeout time.Duration
	BandTimeout    time.Duration
	PlaceTimeout   time.Duration
	CheckTimeout   time.Duration
	CancelTimeout  time.Duration
}

type timeouts struct {
	overall, band, place, check, cancel time.Duration
}

func (options Options) timeouts() timeouts {
	t := timeouts{
		overall: defaultOverallTimeout,
		band:    defaultBandTimeout,
		place:   defaultPlaceTimeout,
		check:   defaultCheckTimeout,
		cancel:  defaultCancelTimeout,
	}
	if options.OverallTimeout > 0 {
		t.overall = options.OverallTimeout
	}
	if options.BandTimeout > 0 {
		t.band = options.BandTimeout
	}
	if options.PlaceTimeout > 0 {
		t.place = options.PlaceTimeout
	}
	if options.CheckTimeout > 0 {
		t.check = options.CheckTimeout
	}
	if options.CancelTimeout > 0 {
		t.cancel = options.CancelTimeout
	}
	return t
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
	result := execute(ctx, config, now, options.Client, options.timeouts())
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

func execute(ctx context.Context, config Config, now func() time.Time, client *http.Client, budgets timeouts) Result {
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
		// No client-level Timeout: every call carries its own stage deadline so
		// budgets compose instead of competing for one shared timeout.
		client = &http.Client{}
	}
	// The overall deadline bounds the whole run, including the post-ambiguity
	// check and cleanup cancel, so no stage sequence can hang the run.
	ctx, stop := context.WithTimeout(ctx, budgets.overall)
	defer stop()
	commandID := commandID(runAt)
	result.CommandID = commandID
	result.StageDurationsMS = map[string]int64{}

	// timed dispatches one request under its own stage deadline and records the
	// stage duration from a monotonic wall clock regardless of the outcome.
	timed := func(stage string, budget time.Duration, request *http.Request) (*http.Response, error) {
		stageCtx, cancelStage := context.WithTimeout(ctx, budget)
		defer cancelStage()
		started := time.Now()
		response, err := client.Do(request.WithContext(stageCtx))
		result.StageDurationsMS[stage] = time.Since(started).Milliseconds()
		return response, err
	}

	stock := config.KRSymbol
	var price string
	if scope == ScopeUS {
		stock, price = "AAPL", "1"
	} else {
		result.Stage = StageBandGet
		bandRequest, _ := http.NewRequestWithContext(ctx, http.MethodGet,
			base+"/v1/price-band?scope="+ScopeKR+"&stock_code="+url.QueryEscape(stock), nil)
		bandRequest.Header.Set("X-Correlation-ID", commandID)
		response, err := timed(StageBandGet, budgets.band, bandRequest)
		band, derived, ok := decodeBand(response, err, stock)
		if band != nil {
			result.Band = &BandEvidence{StockCode: band.StockCode, LowerLimit: band.LowerLimit, BasePrice: band.BasePrice}
		}
		if !ok {
			result.Outcome = OutcomePriceUnavailable
			if err != nil && isDeadline(err) {
				result.ErrorCode = ErrorCanaryTimeout
			}
			return result
		}
		price = derived.String()
	}
	result.Price = price

	command := executioncontracts.ExecutionCommandV1{
		SchemaVersion: executioncontracts.ExecutionCommandV1SchemaVersion, CommandID: commandID,
		AccountScope: scope, Side: "buy", StockCode: stock, Quantity: "1", Price: price,
		OrderType: "limit", IssuedAt: runAt.Format(time.RFC3339),
	}
	body, _ := json.Marshal(command)
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/commands", bytes.NewReader(body))
	request.Header.Set("content-type", "application/json")
	request.Header.Set("X-Correlation-ID", commandID)
	result.Stage = StagePlaceSent
	response, err := timed(StagePlaceSent, budgets.place, request)
	if err != nil {
		// The send boundary may have been crossed: the disposition is unknown
		// to us even though the wire gave no answer. Check before concluding.
		result.Outcome = OutcomeEdgeUnreachable
		if isDeadline(err) {
			result.Outcome = OutcomePlaceNotAccepted
			result.ErrorCode = ErrorCanaryTimeout
		}
		return result.checkOwnOrder(ctx, base, commandID, timed, budgets)
	}
	var receipt executioncontracts.ExecutionReceiptV1
	decodeErr := json.NewDecoder(response.Body).Decode(&receipt)
	response.Body.Close()
	if decodeErr == nil {
		result.Stage = StagePlaceAcked
	}
	if decodeErr != nil || response.StatusCode < 200 || response.StatusCode >= 300 || receipt.Disposition != executioncontracts.DispositionAccepted {
		result.Outcome = OutcomePlaceNotAccepted
		if decodeErr == nil {
			result.ErrorCode = receipt.ErrorCode
			result.Rejection = receipt.Rejection
		}
		if decodeErr == nil && receipt.Disposition == executioncontracts.DispositionNotCreated {
			// Conclusive: the edge never crossed the send boundary, so no own
			// order can exist and no check is needed.
			return result
		}
		return result.checkOwnOrder(ctx, base, commandID, timed, budgets)
	}
	result.cancel(ctx, base, commandID, timed, budgets)
	return result
}

// checkOwnOrder runs the bounded cleanup after an ambiguous place outcome: one
// command-scoped resolve call through the edge's own evidence machinery, then
// — only when that check proves this command's own order resting — the
// existing command-id cancel. It never selects an order by symbol or any
// other fact; the edge's resolve answers only this command's durable facts.
func (result *Result) checkOwnOrder(
	ctx context.Context,
	base, commandID string,
	timed func(string, time.Duration, *http.Request) (*http.Response, error),
	budgets timeouts,
) Result {
	checkRequest, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/v1/commands/"+url.PathEscape(commandID)+"/resolve", nil)
	checkRequest.Header.Set("X-Correlation-ID", commandID)
	response, err := timed("check", budgets.check, checkRequest)
	check := &OrderCheckEvidence{ErrorCode: ErrorCheckFailed}
	result.OrderCheck = check
	if err != nil {
		if isDeadline(err) {
			check.ErrorCode = ErrorCanaryTimeout
		}
		return *result
	}
	if response.StatusCode == http.StatusOK {
		var answered executioncontracts.CommandCheckV1
		if decodeErr := json.NewDecoder(response.Body).Decode(&answered); decodeErr == nil {
			check.Disposition = string(answered.Disposition)
			check.EvidenceRead = answered.EvidenceRead
			check.OrdersSeen = answered.OrdersSeen
			check.Matched = answered.Matched
			// A decoded 200 means the check answered; its error_code belongs to
			// the stored receipt, not to the check itself, so it stays empty.
			check.ErrorCode = ""
		}
	} else {
		var shadow struct {
			ErrorCode string `json:"error_code"`
		}
		if decodeErr := json.NewDecoder(response.Body).Decode(&shadow); decodeErr == nil && shadow.ErrorCode != "" {
			check.ErrorCode = shadow.ErrorCode
		}
	}
	response.Body.Close()
	if check.Disposition != string(executioncontracts.DispositionAccepted) {
		return *result
	}
	// The resolve proved this command's own order resting. Cancel is keyed by
	// the same command ID, so it can only ever touch that order. The place leg
	// still never acked inside its budget, so a successful cleanup keeps the
	// place failure as the outcome — the evidence fields carry the rest.
	priorOutcome, priorCode, priorRejection := result.Outcome, result.ErrorCode, result.Rejection
	cancelled := result.cancel(ctx, base, commandID, timed, budgets)
	if cancelled != nil {
		check.CancelState = cancelled.State
	}
	if result.Outcome == OutcomeOK {
		result.Outcome = priorOutcome
		result.ErrorCode, result.Rejection = priorCode, priorRejection
	}
	return *result
}

// cancel dispatches the command-id cancel for the normal round trip and the
// post-check cleanup alike, folding the outcome into the result and returning
// the decoded receipt so cleanup can record its state. A transport error
// keeps the prior error fields so a cleanup-cancel failure does not erase
// the place evidence.
func (result *Result) cancel(
	ctx context.Context,
	base, commandID string,
	timed func(string, time.Duration, *http.Request) (*http.Response, error),
	budgets timeouts,
) *cancelReceipt {
	cancelRequest, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/v1/commands/"+url.PathEscape(commandID)+"/cancel", nil)
	cancelRequest.Header.Set("X-Correlation-ID", commandID)
	result.Stage = StageCancelSent
	cancelResponse, err := timed(StageCancelSent, budgets.cancel, cancelRequest)
	if err != nil {
		if isDeadline(err) {
			result.Outcome = OutcomeCancelNotCancelled
			result.ErrorCode = ErrorCanaryTimeout
			result.Rejection = nil
		} else {
			result.Outcome = OutcomeEdgeUnreachable
		}
		return nil
	}
	var cancelled cancelReceipt
	decodeErr := json.NewDecoder(cancelResponse.Body).Decode(&cancelled)
	cancelResponse.Body.Close()
	if decodeErr == nil {
		result.Stage = StageCancelAcked
	}
	if decodeErr != nil || cancelResponse.StatusCode < 200 || cancelResponse.StatusCode >= 300 || cancelled.State != "CANCELLED" {
		result.Outcome = OutcomeCancelNotCancelled
		if decodeErr == nil {
			result.ErrorCode = cancelled.ErrorCode
			result.Rejection = cancelled.Rejection
		}
		return &cancelled
	}
	result.Outcome = OutcomeOK
	result.ErrorCode = ""
	result.Rejection = nil
	return &cancelled
}

// isDeadline reports whether the request failed because its own stage or the
// overall deadline elapsed — the ambiguous case — rather than because the
// edge could not be reached at all.
func isDeadline(err error) bool {
	return errors.Is(err, context.DeadlineExceeded)
}

// decodeBand validates the band inquiry answer and derives the limit price
// inside it. Every failure fails closed: the caller places no order when ok
// is false. The band is returned even when derivation fails so the outcome
// line can record exactly which inputs were unusable.
func decodeBand(response *http.Response, err error, symbol string) (*executioncontracts.PriceBandV1, *big.Int, bool) {
	if err != nil || response == nil {
		return nil, nil, false
	}
	var band executioncontracts.PriceBandV1
	decodeErr := json.NewDecoder(response.Body).Decode(&band)
	response.Body.Close()
	if decodeErr != nil || response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices ||
		band.SchemaVersion != executioncontracts.PriceBandV1SchemaVersion {
		return nil, nil, false
	}
	if band.StockCode != symbol {
		return &band, nil, false
	}
	derived, ok := deriveKRPrice(band)
	return &band, derived, ok
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
