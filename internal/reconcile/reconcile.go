// Package reconcile calls auto_trader's execution-ledger reconcile trigger
// after a gap in the KIS fills feed. It is off the fills hot path: a trigger
// runs on its own goroutine, never blocks its caller and never stops the
// process.
package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Reason is the only reason auto_trader accepts; any other value is a 422.
const Reason = "reconnect"

// maxResponseBytes bounds how much of a response body is read.
const maxResponseBytes = 64 << 10

// Response statuses in the auto_trader contract. Anything else is a contract
// error.
const (
	StatusExecuted = "executed"
	StatusDeduped  = "deduped"
	StatusFailed   = "failed"
)

// Request is the only body fillwire sends. Field order is part of the
// contract tests.
type Request struct {
	Market string `json:"market"`
	DryRun bool   `json:"dry_run"`
	Reason string `json:"reason"`
}

// Result is the parsed trigger response. Other response fields are ignored.
type Result struct {
	Status     string `json:"status"`
	Deduped    bool   `json:"deduped"`
	Backfilled int    `json:"backfilled"`
}

// HTTPConfig configures Client. The caller has already validated URL.
type HTTPConfig struct {
	URL         string
	Token       string
	TokenHeader string
	DryRun      bool
	Timeout     time.Duration
}

// Client posts one reconcile trigger per call.
type Client struct {
	cfg        HTTPConfig
	httpClient *http.Client
}

// NewClient builds a client that never follows redirects.
func NewClient(cfg HTTPConfig) (*Client, error) {
	if cfg.URL == "" || cfg.Token == "" || cfg.TokenHeader == "" {
		return nil, errors.New("reconcile: URL, token and token header are required")
	}
	if cfg.Timeout <= 0 {
		return nil, errors.New("reconcile: timeout must be positive")
	}
	return &Client{cfg: cfg, httpClient: &http.Client{
		Timeout: cfg.Timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}, nil
}

// FailureClass names why a trigger failed. It is a closed, credential-free
// set, so it is safe to log and count.
type FailureClass string

const (
	FailTimeout   FailureClass = "timeout"
	FailTransport FailureClass = "transport"
	FailHTTP      FailureClass = "http_status"
	FailRedirect  FailureClass = "redirect"
	FailContract  FailureClass = "contract"
	FailReported  FailureClass = "reported_failed"
)

// Error is a failed trigger. Its text carries no URL and no token.
type Error struct {
	Class  FailureClass
	Status int // HTTP status, when the class has one
	Detail string
}

func (e *Error) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("reconcile %s (HTTP %d)", e.Class, e.Status)
	}
	if e.Detail != "" {
		return fmt.Sprintf("reconcile %s: %s", e.Class, e.Detail)
	}
	return "reconcile " + string(e.Class)
}

// Trigger posts the request for market. A nil error means the response had a
// valid status of executed or deduped.
func (c *Client) Trigger(ctx context.Context, market string) (Result, error) {
	body, err := json.Marshal(Request{Market: market, DryRun: c.cfg.DryRun, Reason: Reason})
	if err != nil {
		return Result{}, &Error{Class: FailTransport, Detail: "encode request"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return Result{}, &Error{Class: FailTransport, Detail: "build request"}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(c.cfg.TokenHeader, c.cfg.Token)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		var timeout interface{ Timeout() bool }
		if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()) {
			return Result{}, &Error{Class: FailTimeout}
		}
		return Result{}, &Error{Class: FailTransport}
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return Result{}, &Error{Class: FailRedirect, Status: resp.StatusCode}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Result{}, &Error{Class: FailHTTP, Status: resp.StatusCode}
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		var timeout interface{ Timeout() bool }
		if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout()) {
			return Result{}, &Error{Class: FailTimeout}
		}
		return Result{}, &Error{Class: FailTransport, Detail: "read response"}
	}
	if len(raw) > maxResponseBytes {
		return Result{}, &Error{Class: FailContract, Detail: "response too large"}
	}
	var result Result
	if err := json.Unmarshal(raw, &result); err != nil {
		return Result{}, &Error{Class: FailContract, Detail: "response is not the expected JSON"}
	}
	switch result.Status {
	case StatusExecuted, StatusDeduped:
		return result, nil
	case StatusFailed:
		return result, &Error{Class: FailReported}
	default:
		return Result{}, &Error{Class: FailContract, Detail: "unknown response status"}
	}
}

// Caller is the part of Client the scheduler needs.
type Caller interface {
	Trigger(ctx context.Context, market string) (Result, error)
}

// Counters are monotonically increasing and safe to read at any time.
type Counters struct {
	Sent      atomic.Uint64
	Executed  atomic.Uint64
	Deduped   atomic.Uint64
	Failed    atomic.Uint64
	Coalesced atomic.Uint64
}

// SchedulerConfig configures a Scheduler.
type SchedulerConfig struct {
	Caller    Caller
	Markets   []string
	Interval  time.Duration // 0 = no periodic trigger
	RateLimit time.Duration // log window per failure class
	Logger    *slog.Logger
	Now       func() time.Time
}

type marketState struct {
	inFlight bool
	pending  bool
}

// Scheduler runs triggers off the caller's goroutine. At most one request per
// market is in flight; triggers that arrive meanwhile coalesce into a single
// follow-up, so a reconnect during a slow call is not lost.
type Scheduler struct {
	cfg      SchedulerConfig
	ctx      context.Context
	Counters Counters

	mu      sync.Mutex
	markets map[string]*marketState
	lastLog map[FailureClass]time.Time
	skipped map[FailureClass]uint64
	wg      sync.WaitGroup
}

// NewScheduler binds the scheduler to ctx: canceling it aborts in-flight
// requests and stops new ones.
func NewScheduler(ctx context.Context, cfg SchedulerConfig) *Scheduler {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	s := &Scheduler{cfg: cfg, ctx: ctx, markets: map[string]*marketState{}, lastLog: map[FailureClass]time.Time{}, skipped: map[FailureClass]uint64{}}
	for _, m := range cfg.Markets {
		s.markets[m] = &marketState{}
	}
	return s
}

// Fire requests one trigger for every market and returns immediately. It is
// safe to call from a reconnect callback.
func (s *Scheduler) Fire(source string) {
	if s == nil {
		return
	}
	for _, market := range s.cfg.Markets {
		s.fireMarket(market, source)
	}
}

func (s *Scheduler) fireMarket(market, source string) {
	if s.ctx.Err() != nil {
		return
	}
	s.mu.Lock()
	state := s.markets[market]
	if state.inFlight {
		state.pending = true
		s.mu.Unlock()
		s.Counters.Coalesced.Add(1)
		return
	}
	state.inFlight = true
	s.wg.Add(1)
	s.mu.Unlock()
	go s.drive(market, source, state)
}

func (s *Scheduler) drive(market, source string, state *marketState) {
	defer s.wg.Done()
	for {
		s.once(market, source)
		s.mu.Lock()
		if !state.pending || s.ctx.Err() != nil {
			state.inFlight, state.pending = false, false
			s.mu.Unlock()
			return
		}
		state.pending = false
		s.mu.Unlock()
	}
}

func (s *Scheduler) once(market, source string) {
	s.Counters.Sent.Add(1)
	result, err := s.cfg.Caller.Trigger(s.ctx, market)
	if err == nil {
		if result.Status == StatusDeduped {
			s.Counters.Deduped.Add(1)
		} else {
			s.Counters.Executed.Add(1)
		}
		s.cfg.Logger.Info("reconcile trigger done", "market", market, "source", source, "status", result.Status, "backfilled", result.Backfilled)
		return
	}
	if s.ctx.Err() != nil {
		return // shutting down; not a failure of the endpoint
	}
	s.Counters.Failed.Add(1)
	class := FailTransport
	var typed *Error
	if errors.As(err, &typed) {
		class = typed.Class
	}
	s.logFailure(class, market, source, err)
}

func (s *Scheduler) logFailure(class FailureClass, market, source string, err error) {
	now := s.cfg.Now()
	s.mu.Lock()
	last, seen := s.lastLog[class]
	if seen && now.Sub(last) < s.cfg.RateLimit {
		s.skipped[class]++
		s.mu.Unlock()
		return
	}
	s.lastLog[class] = now
	suppressed := s.skipped[class]
	s.skipped[class] = 0
	s.mu.Unlock()
	s.cfg.Logger.Error("reconcile trigger failed", "market", market, "source", source, "class", string(class), "reason", err.Error(), "suppressed_since_last_log", suppressed)
}

// Run fires on every interval tick until ctx ends. It does nothing when the
// interval is zero.
func (s *Scheduler) Run(ctx context.Context) {
	if s == nil || s.cfg.Interval <= 0 {
		return
	}
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Fire("interval")
		}
	}
}

// Wait blocks until every started request has returned. Cancel the scheduler's
// context first; each request is also bounded by the client timeout.
func (s *Scheduler) Wait() {
	if s != nil {
		s.wg.Wait()
	}
}
