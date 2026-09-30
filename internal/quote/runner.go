package quote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultStreamKey is the stream every tick is appended to.
const DefaultStreamKey = "quotes:toss"

// MaxSubscriptions is the Toss per-connection limit (codes summed over
// channels). Two channels per symbol keep MaxSymbols within it.
const MaxSubscriptions = 100

const (
	dialTimeout  = 30 * time.Second
	writeTimeout = 10 * time.Second
	xaddTimeout  = 2 * time.Second
	closeTimeout = 5 * time.Second
	// pingInterval is the AsyncAPI recommendation; the server closes a
	// connection that has sent nothing for 180s.
	pingInterval = 60 * time.Second
	// declareSpacing keeps declarations far below the 5 per second limit and
	// is the wait the AsyncAPI asks for after rate-limit-exceeded.
	declareSpacing = time.Second
	maxRedeclares  = 3
	// maxWaitSlice caps every sleep so a wall-clock jump (suspend, NTP step)
	// is noticed within a minute rather than after a whole overnight wait.
	maxWaitSlice = time.Minute
	// healthyRun is how long a window run must last before the retry delay
	// resets, so a socket that dies right after opening still backs off.
	healthyRun   = time.Minute
	defaultRetry = time.Second
	maxRetry     = 5 * time.Minute
	tokenPoll    = 15 * time.Second
)

var (
	errStreamStopped    = errors.New("quote: Toss websocket stream stopped")
	errNoSubscriptions  = errors.New("quote: Toss accepted no quote subscription")
	errServerShutdown   = errors.New("quote: Toss server-shutdown frame")
	errServerError      = errors.New("quote: Toss error frame")
	errTokenRejected    = errors.New("quote: cached Toss token was refused; waiting for a different one")
	errTooManyRedeclare = errors.New("quote: declaration rate limit persisted")
)

// Config wires one quote lane. Every dependency here is owned by the lane:
// callers must not pass the execution-fill pipeline's Redis client.
type Config struct {
	Symbols   []Symbol
	Token     TokenSource
	Dialer    Dialer
	Redis     redis.UniversalClient
	StreamKey string
	MaxLen    int64
	Buffer    int
	Clock     Clock
	Logger    *slog.Logger

	// RetryMin and RetryMax pace redials after a window run fails, doubling
	// with jitter (AsyncAPI: 1s, 2s, 4s, ...). Zero values take 1s and 5m.
	RetryMin, RetryMax time.Duration
	// PingInterval defaults to 60s.
	PingInterval time.Duration
}

// Counters are process-local observations of the quote lane.
type Counters struct {
	Frames            atomic.Uint64
	TicksWritten      atomic.Uint64
	DropMalformed     atomic.Uint64
	DropUnknownTopic  atomic.Uint64
	DropUnknownSymbol atomic.Uint64
	DropNoTimestamp   atomic.Uint64
	DropOutOfSession  atomic.Uint64
	DropBufferFull    atomic.Uint64
	UnknownFrames     atomic.Uint64
	XAddErrors        atomic.Uint64
	SubscribeRejected atomic.Uint64
	ErrorFrames       atomic.Uint64
	TokenUnavailable  atomic.Uint64
	TokenRejected     atomic.Uint64
	Declarations      atomic.Uint64
	Pings             atomic.Uint64
	Dials             atomic.Uint64
	WindowRuns        atomic.Uint64
}

func (c *Counters) drop(reason DropReason) {
	switch reason {
	case DropMalformed:
		c.DropMalformed.Add(1)
	case DropUnknownTopic:
		c.DropUnknownTopic.Add(1)
	case DropUnknownSym:
		c.DropUnknownSymbol.Add(1)
	case DropNoTimestamp:
		c.DropNoTimestamp.Add(1)
	case DropOutOfSession:
		c.DropOutOfSession.Add(1)
	}
}

func (c *Counters) logArgs() []any {
	return []any{
		"frames", c.Frames.Load(),
		"ticks_written", c.TicksWritten.Load(),
		"drop_malformed", c.DropMalformed.Load(),
		"drop_unknown_topic", c.DropUnknownTopic.Load(),
		"drop_unknown_symbol", c.DropUnknownSymbol.Load(),
		"drop_no_timestamp", c.DropNoTimestamp.Load(),
		"drop_out_of_session", c.DropOutOfSession.Load(),
		"drop_buffer_full", c.DropBufferFull.Load(),
		"unknown_frames", c.UnknownFrames.Load(),
		"xadd_errors", c.XAddErrors.Load(),
		"subscribe_rejected", c.SubscribeRejected.Load(),
		"error_frames", c.ErrorFrames.Load(),
		"token_unavailable", c.TokenUnavailable.Load(),
		"token_rejected", c.TokenRejected.Load(),
		"dials", c.Dials.Load(),
		"window_runs", c.WindowRuns.Load(),
	}
}

// Runner is the quote lane. Its Run never returns an error to the caller:
// nothing on this lane may stop the process or the execution-fill pipeline.
type Runner struct {
	cfg         Config
	schedule    Schedule
	decoder     *Decoder
	counters    *Counters
	ticks       chan Tick
	dialer      Dialer
	token       TokenSource
	declaration []byte

	// Lane-local state owned by the supervisor goroutine.
	rejectedToken string
	lastDeclare   time.Time
}

// NewRunner validates cfg.
func NewRunner(cfg Config) (*Runner, error) {
	if len(cfg.Symbols) == 0 || len(cfg.Symbols) > MaxSymbols {
		return nil, fmt.Errorf("quote: symbol count must be 1..%d", MaxSymbols)
	}
	seen := map[Symbol]bool{}
	for _, symbol := range cfg.Symbols {
		if !ValidSymbol(symbol) || seen[symbol] {
			return nil, errors.New("quote: invalid or duplicate symbol")
		}
		seen[symbol] = true
	}
	if cfg.Token == nil || cfg.Dialer == nil || cfg.Redis == nil {
		return nil, errors.New("quote: token source, dialer, and Redis client are required")
	}
	if !strings.HasPrefix(cfg.StreamKey, "quotes:") || cfg.MaxLen <= 0 || cfg.Buffer <= 0 {
		return nil, errors.New("quote: stream key (quotes:*), max length, and buffer are required")
	}
	if cfg.Clock == nil {
		cfg.Clock = systemClock{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.RetryMin <= 0 {
		cfg.RetryMin = defaultRetry
	}
	if cfg.RetryMax < cfg.RetryMin {
		cfg.RetryMax = max(maxRetry, cfg.RetryMin)
	}
	if cfg.PingInterval <= 0 {
		cfg.PingInterval = pingInterval
	}
	declaration, markets, err := buildDeclaration(cfg.Symbols)
	if err != nil {
		return nil, err
	}
	return &Runner{
		cfg:         cfg,
		schedule:    NewSchedule(markets),
		decoder:     NewDecoder(cfg.Symbols),
		counters:    &Counters{},
		ticks:       make(chan Tick, cfg.Buffer),
		dialer:      &singleConnection{dialer: safeDialer{dialer: cfg.Dialer, logger: cfg.Logger}},
		token:       safeToken{source: cfg.Token, logger: cfg.Logger},
		declaration: declaration,
	}, nil
}

// buildDeclaration returns the one full-replace subscription array: trade
// and orderbook for each configured market, plus a request id element.
func buildDeclaration(symbols []Symbol) ([]byte, []string, error) {
	codes := map[string][]string{}
	var markets []string
	for _, symbol := range symbols {
		if _, ok := codes[symbol.Market]; !ok {
			markets = append(markets, symbol.Market)
		}
		codes[symbol.Market] = append(codes[symbol.Market], symbol.Code)
	}
	declaration := []any{map[string]string{"id": "fillwire-quotes"}}
	total := 0
	for _, market := range markets {
		for _, channel := range []string{"trade", "orderbook"} {
			declaration = append(declaration, map[string]any{"type": channel + ":" + market, "codes": codes[market]})
			total += len(codes[market])
		}
	}
	if total > MaxSubscriptions {
		return nil, nil, fmt.Errorf("quote: %d subscriptions exceed the Toss limit of %d per connection", total, MaxSubscriptions)
	}
	raw, err := json.Marshal(declaration)
	return raw, markets, err
}

// Counters exposes the lane's observations.
func (r *Runner) Counters() *Counters { return r.counters }

// Run streams quotes during trading windows until ctx is canceled. A panic
// anywhere in the lane's own goroutines is contained and ends only the lane.
func (r *Runner) Run(ctx context.Context) {
	laneCtx, stopLane := context.WithCancel(ctx)
	defer stopLane()
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		r.guard("writer", stopLane, func() { r.writeLoop(laneCtx) })
	}()
	r.guard("supervisor", stopLane, func() { r.superviseLoop(laneCtx, stopLane) })
	stopLane()
	workers.Wait()
	r.cfg.Logger.Info("quote reader stopped", r.counters.logArgs()...)
}

func (r *Runner) guard(part string, stopLane context.CancelFunc, fn func()) {
	defer func() {
		if recovered := recover(); recovered != nil {
			r.cfg.Logger.Error("quote reader panic contained; quote lane stopped", "part", part)
			stopLane()
		}
	}()
	fn()
}

func (r *Runner) superviseLoop(ctx context.Context, stopLane context.CancelFunc) {
	delay := r.cfg.RetryMin
	for ctx.Err() == nil {
		now := r.cfg.Clock.Now()
		closes, open := r.schedule.At(now)
		if !open {
			next := r.schedule.NextOpen(now)
			r.cfg.Logger.Info("quote reader idle outside trading windows", "next_open", next.Format(time.RFC3339))
			r.waitUntil(ctx, next)
			continue
		}
		started := r.cfg.Clock.Now()
		r.counters.WindowRuns.Add(1)
		err := r.runWindow(ctx, stopLane, closes)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			delay = r.cfg.RetryMin
			continue
		}
		if r.cfg.Clock.Now().Sub(started) >= healthyRun {
			delay = r.cfg.RetryMin
		}
		wait := delay
		switch {
		case errors.Is(err, ErrForbidden):
			wait = r.cfg.RetryMax
		case errors.Is(err, ErrTokenUnavailable), errors.Is(err, errTokenRejected):
			// Re-reading the cache is one Redis GET; poll it steadily so a
			// token the owner publishes is picked up soon.
			wait = min(delay, tokenPoll)
		}
		wait += time.Duration(rand.Int64N(int64(wait)/5 + 1))
		r.cfg.Logger.Warn("quote reader window run failed; retrying", "reason", safeReason(err), "retry_in", wait.String())
		wake := r.cfg.Clock.Now().Add(wait)
		if wake.After(closes) {
			wake = closes
		}
		r.waitUntil(ctx, wake)
		delay = min(delay*2, r.cfg.RetryMax)
	}
}

// waitUntil sleeps until the lane clock reaches deadline or ctx ends.
func (r *Runner) waitUntil(ctx context.Context, deadline time.Time) {
	for ctx.Err() == nil {
		remaining := deadline.Sub(r.cfg.Clock.Now())
		if remaining <= 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-r.cfg.Clock.After(min(remaining, maxWaitSlice)):
		}
	}
}

// control carries ack and error frames from the reader to the connection loop.
type control struct {
	frame Frame
}

// runWindow owns one Toss websocket from open to window close. It returns
// nil when the window closes or ctx ends and an error when the socket failed.
func (r *Runner) runWindow(ctx context.Context, stopLane context.CancelFunc, closes time.Time) error {
	// windowCtx ends when the window closes, including a wall-clock jump out
	// of it, so a pending dial or write never outlives the window.
	windowCtx, closeWindow := context.WithCancel(ctx)
	defer closeWindow()
	go r.guard("window watcher", stopLane, func() {
		r.watchWindow(windowCtx, closes)
		closeWindow()
	})

	token, err := r.token.Token(windowCtx)
	if err != nil {
		if windowCtx.Err() != nil {
			return r.windowEnded(ctx)
		}
		r.counters.TokenUnavailable.Add(1)
		return err
	}
	if token == r.rejectedToken {
		// A non-owner never asks for a new token; it waits until the owner
		// publishes a different one.
		return errTokenRejected
	}

	r.counters.Dials.Add(1)
	dialCtx, cancelDial := context.WithTimeout(windowCtx, dialTimeout)
	transport, err := r.dialer.Dial(dialCtx, Endpoint, token)
	cancelDial()
	if err != nil {
		if windowCtx.Err() != nil {
			return r.windowEnded(ctx)
		}
		if errors.Is(err, ErrUnauthorized) {
			r.rejectedToken = token
			r.counters.TokenRejected.Add(1)
			return errTokenRejected
		}
		return err
	}
	// Break before make: this transport is closed before the supervisor can
	// dial another one.
	defer transport.Close()
	r.rejectedToken = ""

	controls := make(chan control, 8)
	readerDone := make(chan error, 1)
	go r.guard("reader", stopLane, func() {
		readerDone <- r.readLoop(windowCtx, transport, controls)
	})

	if err := r.declare(windowCtx, transport); err != nil {
		if windowCtx.Err() != nil {
			return r.windowEnded(ctx)
		}
		return err
	}
	redeclares := 0
	nextPing := r.cfg.Clock.Now().Add(r.cfg.PingInterval)
	for {
		select {
		case <-windowCtx.Done():
			return r.windowEnded(ctx)
		case err := <-readerDone:
			if windowCtx.Err() != nil {
				return r.windowEnded(ctx)
			}
			return fmt.Errorf("%w: %w", errStreamStopped, err)
		case message := <-controls:
			switch message.frame.Kind {
			case FrameSubscriptions:
				for _, rejected := range message.frame.Rejected {
					r.counters.SubscribeRejected.Add(1)
					r.cfg.Logger.Warn("quote subscription rejected", "target", rejected.Target, "code", rejected.Code)
				}
				if len(message.frame.Subscribed) == 0 {
					return errNoSubscriptions
				}
				r.cfg.Logger.Info("quote reader subscriptions active", "subscribed", len(message.frame.Subscribed), "rejected", len(message.frame.Rejected), "stream", r.cfg.StreamKey)
			case FrameError:
				r.counters.ErrorFrames.Add(1)
				code := message.frame.ErrorCode
				switch code {
				case "server-shutdown":
					return errServerShutdown
				case "rate-limit-exceeded":
					redeclares++
					if redeclares > maxRedeclares {
						return errTooManyRedeclare
					}
					if err := r.declare(windowCtx, transport); err != nil {
						if windowCtx.Err() != nil {
							return r.windowEnded(ctx)
						}
						return err
					}
				default:
					r.cfg.Logger.Error("quote reader error frame", "code", code)
					return fmt.Errorf("%w: %s", errServerError, code)
				}
			}
		case <-r.cfg.Clock.After(max(nextPing.Sub(r.cfg.Clock.Now()), 0)):
			if !r.cfg.Clock.Now().Before(nextPing) {
				if err := r.write(windowCtx, transport, []byte("PING")); err != nil {
					if windowCtx.Err() != nil {
						return r.windowEnded(ctx)
					}
					return err
				}
				r.counters.Pings.Add(1)
				nextPing = r.cfg.Clock.Now().Add(r.cfg.PingInterval)
			}
		}
	}
}

// declare sends the subscription array, spacing declarations at least a
// second apart across reconnects.
func (r *Runner) declare(ctx context.Context, transport Transport) error {
	if !r.lastDeclare.IsZero() {
		r.waitUntil(ctx, r.lastDeclare.Add(declareSpacing))
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	r.lastDeclare = r.cfg.Clock.Now()
	r.counters.Declarations.Add(1)
	return r.write(ctx, transport, r.declaration)
}

func (r *Runner) write(ctx context.Context, transport Transport, data []byte) error {
	writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return transport.Write(writeCtx, data)
}

// readLoop decodes frames until the transport fails. Ticks are offered to
// the writer without blocking: a slow Redis drops quotes (counted) instead of
// stalling the socket reader.
func (r *Runner) readLoop(ctx context.Context, transport Transport, controls chan<- control) error {
	for {
		raw, err := transport.Read(ctx)
		if err != nil {
			return err
		}
		r.counters.Frames.Add(1)
		frame := r.decoder.Decode(raw)
		switch frame.Kind {
		case FrameMessage:
			if frame.Drop != "" {
				r.counters.drop(frame.Drop)
				continue
			}
			select {
			case r.ticks <- frame.Tick:
			default:
				r.counters.DropBufferFull.Add(1)
			}
		case FrameSubscriptions, FrameError:
			select {
			case controls <- control{frame: frame}:
			case <-ctx.Done():
				return ctx.Err()
			}
		case FramePong:
		default:
			r.counters.UnknownFrames.Add(1)
		}
	}
}

// windowEnded reports a normal end of one window run.
func (r *Runner) windowEnded(ctx context.Context) error {
	if ctx.Err() == nil {
		r.cfg.Logger.Info("quote reader window closed", r.counters.logArgs()...)
	}
	return nil
}

// watchWindow returns once the lane clock is no longer inside the window
// that closes at closes, or when ctx ends. It rechecks at least once a
// minute, so a wall-clock jump in either direction is noticed.
func (r *Runner) watchWindow(ctx context.Context, closes time.Time) {
	for ctx.Err() == nil && r.windowOpen(closes) {
		select {
		case <-ctx.Done():
			return
		case <-r.cfg.Clock.After(min(closes.Sub(r.cfg.Clock.Now()), maxWaitSlice)):
		}
	}
}

// windowOpen reports whether the lane clock is still inside the window that
// closes at closes.
func (r *Runner) windowOpen(closes time.Time) bool {
	current, open := r.schedule.At(r.cfg.Clock.Now())
	return open && current.Equal(closes)
}

func (r *Runner) writeLoop(ctx context.Context) {
	var lastErrorLog time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-r.ticks:
			writeCtx, cancel := context.WithTimeout(ctx, xaddTimeout)
			err := r.cfg.Redis.XAdd(writeCtx, &redis.XAddArgs{
				Stream: r.cfg.StreamKey,
				MaxLen: r.cfg.MaxLen,
				Approx: true,
				Values: tick.values(),
			}).Err()
			cancel()
			if err != nil {
				r.counters.XAddErrors.Add(1)
				if now := time.Now(); now.Sub(lastErrorLog) >= time.Minute {
					lastErrorLog = now
					r.cfg.Logger.Warn("quote XADD failed; tick dropped", "xadd_errors", r.counters.XAddErrors.Load())
				}
				continue
			}
			r.counters.TicksWritten.Add(1)
		}
	}
}

// safeReason keeps URLs (which could carry Redis credentials) out of logs.
func safeReason(err error) string {
	text := err.Error()
	if strings.Contains(text, "://") {
		return "redacted transport failure"
	}
	return text
}

type systemClock struct{}

func (systemClock) Now() time.Time                         { return time.Now() }
func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
