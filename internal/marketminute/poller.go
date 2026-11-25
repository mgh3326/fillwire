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
		case "NASD", "NYSE", "AMEX":
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
	Refusals       map[string]int `json:"refusals,omitempty"`
	Errors         []TargetError  `json:"errors,omitempty"`
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
}

// Poller runs one concurrent shadow poll over a target list.
type Poller struct {
	config   Config
	endpoint map[string]string
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
	endpoints := make(map[string]string, 3)
	for _, target := range config.Targets {
		if err := target.validate(); err != nil {
			return nil, err
		}
		name, err := Endpoint(target.Venue)
		if err != nil {
			return nil, err
		}
		endpoints[target.Venue] = name
	}
	return &Poller{config: config, endpoint: endpoints}, nil
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
func (p *Poller) Run(ctx context.Context) (Report, error) {
	report := Report{Targets: len(p.config.Targets), Refusals: map[string]int{}}
	if len(p.config.Targets) == 0 {
		return report, nil
	}
	results := make([]jobResult, len(p.config.Targets))
	workers := min(p.config.Workers, len(p.config.Targets))
	jobs := make(chan int)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := range jobs {
				results[index] = p.pollOne(ctx, p.config.Targets[index])
			}
		}()
	}
	for index := range p.config.Targets {
		select {
		case jobs <- index:
		case <-ctx.Done():
			close(jobs)
			wait.Wait()
			return report, ctx.Err()
		}
	}
	close(jobs)
	wait.Wait()
	if err := ctx.Err(); err != nil {
		return report, err
	}

	for _, result := range results {
		report.Reads++
		report.Rows += len(result.bars)
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
		if err := p.write(ctx, result.bars); err != nil {
			return report, err
		}
	}
	return report, nil
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

func (p *Poller) fetch(ctx context.Context, target Target, key ratebudget.Key, observedBefore time.Time, result jobResult) jobResult {
	response := p.config.Reader.Fetch(ctx, target)
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
