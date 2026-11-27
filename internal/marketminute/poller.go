package marketminute

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mgh3326/fillwire/internal/ctxwait"
	"github.com/mgh3326/fillwire/internal/ratebudget"
)

// Target is one symbol x venue minute-chart read. The target list itself comes
// from auto_trader (build_targets); this poller never selects symbols.
type Target struct {
	Market string `json:"market"`
	Symbol string `json:"symbol"`
	Venue  string `json:"venue"`
}

func (t Target) validate() error {
	switch t.Market {
	case "kr":
		if t.Venue != "KRX" && t.Venue != "NTX" {
			return fmt.Errorf("%w: KR venue %q must be KRX or NTX", ErrConfig, t.Venue)
		}
	case "us":
		switch t.Venue {
		case "NASD", "NYSE", "AMEX", VenueDayNASD, VenueDayNYSE, VenueDayAMEX:
		default:
			return fmt.Errorf("%w: US venue %q is not NASD, NYSE or AMEX", ErrConfig, t.Venue)
		}
	default:
		return fmt.Errorf("%w: market %q must be kr or us", ErrConfig, t.Market)
	}
	if t.Symbol == "" {
		return fmt.Errorf("%w: empty symbol", ErrConfig)
	}
	return nil
}

// ErrConfig is wrapped by every poller configuration the constructor refuses.
var ErrConfig = errors.New("marketminute: invalid configuration")

// KIS minute-chart transactions. Both are GET quotations paths; the domestic
// TR and parameters match auto_trader's inquire_minute_chart, and the overseas
// TR and parameters match inquire_overseas_minute_chart.
const (
	DomesticTR   = "FHKST03010200"
	DomesticPath = "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
	OverseasTR   = "HHDFS76950200"
	OverseasPath = "/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice"
)

// Endpoint is the ratebudget endpoint name ("TR_ID|path") for a venue.
func Endpoint(venue string) (string, error) {
	switch venue {
	case "KRX", "NTX":
		return DomesticTR + "|" + DomesticPath, nil
	case "NASD", "NYSE", "AMEX":
		return OverseasTR + "|" + OverseasPath, nil
	case VenueDayNASD, VenueDayNYSE, VenueDayAMEX:
		return DayMarketEndpoint, nil
	default:
		return "", fmt.Errorf("%w: unknown venue %q", ErrConfig, venue)
	}
}

// FetchResult is one minute-chart read. Err is the transport or protocol
// failure; StatusCode, RetryAfter and RateLimited are what the limiter records.
type FetchResult struct {
	Rows        []FrameRow
	StatusCode  int
	RetryAfter  time.Duration
	RateLimited bool
	Err         error
}

// Reader performs exactly one minute-chart GET for a target.
type Reader interface {
	Fetch(context.Context, Target) FetchResult
}

// TargetError reports one target that produced no rows. Class is a closed
// reason word; it never carries an upstream message.
type TargetError struct {
	Market string `json:"market"`
	Symbol string `json:"symbol"`
	Venue  string `json:"venue"`
	Class  string `json:"class"`
}

// Report is the run summary the command prints.
type Report struct {
	Targets        int            `json:"targets"`
	Reads          int            `json:"reads"`
	Rows           int            `json:"rows"`
	SkippedForming int            `json:"skipped_forming"`
	SkippedInvalid int            `json:"skipped_invalid"`
	NullValue      int            `json:"null_value"`
	EmptyReads     int            `json:"empty_reads"`
	Duplicates     int            `json:"duplicates"`
	Refusals       map[string]int `json:"refusals,omitempty"`
	Errors         []TargetError  `json:"errors,omitempty"`
	// USDayMarket is absent unless the day-market source is on.
	USDayMarket *DayMarketReport `json:"us_day_market,omitempty"`
}

// DayMarketReport says why the day-market source did or did not read, so a run
// with no day rows is never silent. Report.Targets counts the listed targets
// only; the other Report counters include the day-market reads.
type DayMarketReport struct {
	State DayMarketState `json:"state"`
	// Targets is the number of day-market targets derived from the list.
	Targets int `json:"targets"`
	// Read is true when the targets were polled: the market is open, or a
	// window ended within the trail.
	Read bool `json:"read"`
	Rows int  `json:"rows"`
}

// Config wires one poll run.
type Config struct {
	Targets    []Target
	Reader     Reader
	Limiter    *ratebudget.Limiter
	Sink       Sink
	Calendar   *Calendar
	Provider   string
	AccountRef string
	Clock      func() time.Time
	// Workers is the concurrent request count. Zero takes 4.
	Workers int
	// Sleep is the cancellable wait used between ratebudget refusals. Zero
	// takes ctxwait.Sleep.
	Sleep func(context.Context, time.Duration) error
	// USDayMarket turns on the KIS US day-market source: every listed US target
	// is also read under its day-market code while the calendar's day-market
	// window allows it. Off by default.
	USDayMarket bool
	// DayMarketTrail is how long after a window ends the day targets are still
	// read. Zero takes DefaultDayMarketTrail.
	DayMarketTrail time.Duration
}

// Poller runs one concurrent shadow poll over a target list.
type Poller struct {
	config   Config
	endpoint map[string]string
	// written is the newest closed bar already written per target. A poll
	// re-reads the same trailing KIS page, so rows at or below the watermark
	// are duplicates and are not emitted again.
	written map[string]time.Time
	// dayTargets are derived from the listed US targets; empty when the
	// day-market source is off.
	dayTargets []Target
}

const defaultWorkers = 4

// NewPoller validates the configuration. A nil sink, reader, limiter or
// calendar is refused rather than silently dropping rows.
func NewPoller(config Config) (*Poller, error) {
	if config.Reader == nil {
		return nil, fmt.Errorf("%w: reader is required", ErrConfig)
	}
	if config.Limiter == nil {
		return nil, fmt.Errorf("%w: ratebudget limiter is required", ErrConfig)
	}
	if config.Sink == nil {
		return nil, ErrNoSink
	}
	if config.Calendar == nil {
		return nil, fmt.Errorf("%w: exchange calendar is required", ErrConfig)
	}
	if config.Provider == "" || config.AccountRef == "" {
		return nil, fmt.Errorf("%w: provider and account_ref are required", ErrConfig)
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.Sleep == nil {
		config.Sleep = ctxwait.Sleep
	}
	if config.Workers == 0 {
		config.Workers = defaultWorkers
	}
	if config.Workers < 0 {
		return nil, fmt.Errorf("%w: workers must not be negative", ErrConfig)
	}
	if config.USDayMarket && !config.Calendar.HasUSDayMarket() {
		return nil, fmt.Errorf("%w: the US day-market source needs us_day_market windows in the calendar", ErrConfig)
	}
	if config.DayMarketTrail == 0 {
		config.DayMarketTrail = DefaultDayMarketTrail
	}
	if config.DayMarketTrail < 0 {
		return nil, fmt.Errorf("%w: day-market trail must not be negative", ErrConfig)
	}
	var dayTargets []Target
	for _, target := range config.Targets {
		if err := target.validate(); err != nil {
			return nil, err
		}
		if isDayVenue(target.Venue) {
			return nil, fmt.Errorf("%w: day-market venue %q is derived by the US day-market source and must not be listed", ErrConfig, target.Venue)
		}
		if venue, ok := dayVenueOf[target.Venue]; ok && config.USDayMarket {
			dayTargets = append(dayTargets, Target{Market: target.Market, Symbol: target.Symbol, Venue: venue})
		}
	}
	endpoints := make(map[string]string, 6)
	for _, targets := range [][]Target{config.Targets, dayTargets} {
		for _, target := range targets {
			name, err := Endpoint(target.Venue)
			if err != nil {
				return nil, err
			}
			endpoints[target.Venue] = name
		}
	}
	return &Poller{config: config, endpoint: endpoints, written: make(map[string]time.Time), dayTargets: dayTargets}, nil
}

// Key is the ratebudget key for a target.
func (p *Poller) Key(target Target) ratebudget.Key {
	return ratebudget.Key{Provider: p.config.Provider, AccountRef: p.config.AccountRef, Endpoint: p.endpoint[target.Venue]}
}

type jobResult struct {
	bars           []Bar
	skippedForming int
	skippedInvalid int
	nullValue      int
	empty          bool
	refusals       []string
	targetError    *TargetError
}

// Run polls every target with a bounded worker pool. Bars are written in target
// order so the JSONL output is deterministic. The reader's ratebudget outcome
// is recorded for every request; a refusal is retried after its RetryAfter.
//
// With the day-market source on, its targets are polled after the listed ones
// are written, and only while the day market's own windows allow it. A day
// target refused by an open circuit is skipped instead of waited for.
func (p *Poller) Run(ctx context.Context) (Report, error) {
	report := Report{Targets: len(p.config.Targets), Refusals: map[string]int{}}
	if len(p.config.Targets) == 0 {
		return report, nil
	}
	if err := p.pass(ctx, p.config.Targets, &report); err != nil {
		return report, err
	}
	if !p.config.USDayMarket {
		return report, nil
	}
	at := p.config.Clock()
	day := &DayMarketReport{State: p.config.Calendar.USDayMarket(at), Targets: len(p.dayTargets)}
	report.USDayMarket = day
	if len(p.dayTargets) == 0 || !p.config.Calendar.inUSDayWindow(at, p.config.DayMarketTrail) {
		return report, nil
	}
	day.Read = true
	before := report.Rows
	err := p.pass(ctx, p.dayTargets, &report)
	day.Rows = report.Rows - before
	return report, err
}

// pass polls one target list and writes its closed bars.
func (p *Poller) pass(ctx context.Context, targets []Target, report *Report) error {
	results := make([]jobResult, len(targets))
	workers := min(p.config.Workers, len(targets))
	jobs := make(chan int)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := range jobs {
				results[index] = p.pollOne(ctx, targets[index])
			}
		}()
	}
	for index := range targets {
		select {
		case jobs <- index:
		case <-ctx.Done():
			close(jobs)
			wait.Wait()
			return ctx.Err()
		}
	}
	close(jobs)
	wait.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}

	for index, result := range results {
		report.Reads++
		report.SkippedForming += result.skippedForming
		report.SkippedInvalid += result.skippedInvalid
		report.NullValue += result.nullValue
		for _, code := range result.refusals {
			report.Refusals[code]++
		}
		if result.empty {
			report.EmptyReads++
		}
		if result.targetError != nil {
			report.Errors = append(report.Errors, *result.targetError)
			continue
		}
		target := targets[index]
		fresh := p.freshBars(target, result.bars)
		report.Duplicates += len(result.bars) - len(fresh)
		if err := p.write(ctx, fresh); err != nil {
			return err
		}
		report.Rows += len(fresh)
		if len(fresh) > 0 {
			p.written[targetKey(target)] = fresh[len(fresh)-1].TimeUTC
		}
	}
	return nil
}

// freshBars drops bars at or below the target's watermark. KIS returns the
// same trailing page every poll; without this the JSONL file would grow
// without bound and every repeated row would double count its minute.
func (p *Poller) freshBars(target Target, bars []Bar) []Bar {
	watermark, seen := p.written[targetKey(target)]
	if !seen {
		return bars
	}
	fresh := make([]Bar, 0, len(bars))
	for _, bar := range bars {
		if bar.TimeUTC.After(watermark) {
			fresh = append(fresh, bar)
		}
	}
	return fresh
}

func targetKey(target Target) string {
	return target.Market + "|" + target.Venue + "|" + target.Symbol
}

func (p *Poller) write(ctx context.Context, bars []Bar) error {
	for _, bar := range bars {
		if err := p.config.Sink.Write(ctx, bar); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}

func (p *Poller) pollOne(ctx context.Context, target Target) jobResult {
	result := jobResult{}
	key := p.Key(target)
	for {
		observedBefore := p.config.Clock()
		err := p.config.Limiter.Acquire(key)
		if err == nil {
			return p.fetch(ctx, target, key, observedBefore, result)
		}
		code := ratebudget.CodeOf(err)
		if code == "" {
			result.targetError = &TargetError{Market: target.Market, Symbol: target.Symbol, Venue: target.Venue, Class: ErrorClass(err)}
			return result
		}
		result.refusals = append(result.refusals, string(code))
		if code == ratebudget.CodeCircuitOpen && isDayVenue(target.Venue) {
			// Waiting would admit one probe per open window and hold the next
			// regular poll behind a failing day source.
			result.targetError = &TargetError{Market: target.Market, Symbol: target.Symbol, Venue: target.Venue, Class: classCircuitOpen}
			return result
		}
		wait := time.Second
		var refusal *ratebudget.Refusal
		if errors.As(err, &refusal) && refusal.RetryAfter > 0 {
			wait = max(refusal.RetryAfter, time.Millisecond)
		}
		if sleepErr := p.config.Sleep(ctx, wait); sleepErr != nil {
			result.targetError = &TargetError{Market: target.Market, Symbol: target.Symbol, Venue: target.Venue, Class: ErrorClass(sleepErr)}
			return result
		}
	}
}

// localFailure reports a failure of this process's own inputs (no token, a
// refused credential host), which must not change ratebudget's endpoint health.
func localFailure(err error) bool {
	return errors.Is(err, ErrTokenUnavailable) || errors.Is(err, errTokenExpired) || errors.Is(err, ErrCredentialHost)
}

func (p *Poller) fetch(ctx context.Context, target Target, key ratebudget.Key, observedBefore time.Time, result jobResult) jobResult {
	response := p.config.Reader.Fetch(ctx, target)
	if response.Err != nil && localFailure(response.Err) {
		// A missing token or a refused credential host is not an upstream
		// failure: recording it would open the endpoint circuit and blame KIS.
		result.targetError = &TargetError{Market: target.Market, Symbol: target.Symbol, Venue: target.Venue, Class: ErrorClass(response.Err)}
		return result
	}
	p.config.Limiter.Observe(key, ratebudget.Outcome{
		StatusCode:  response.StatusCode,
		Err:         response.Err,
		RetryAfter:  response.RetryAfter,
		RateLimited: response.RateLimited,
	})
	if response.Err != nil {
		result.targetError = &TargetError{Market: target.Market, Symbol: target.Symbol, Venue: target.Venue, Class: ErrorClass(response.Err)}
		return result
	}
	if len(response.Rows) == 0 {
		result.empty = true
		return result
	}
	frame := BarsFromFrame(response.Rows, target.Market, target.Symbol, target.Venue, observedBefore, p.config.Calendar)
	result.bars = frame.Bars
	result.skippedForming = frame.SkippedForming
	result.skippedInvalid = frame.SkippedInvalid
	result.nullValue = frame.NullValue
	return result
}
