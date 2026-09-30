package quote

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mgh3326/go-kis/kis"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

// DefaultStreamKey is the stream every tick is appended to.
const DefaultStreamKey = "quotes:kis"

const (
	dialTimeout      = 30 * time.Second
	subscribeTimeout = 10 * time.Second
	xaddTimeout      = 2 * time.Second
	// maxWaitSlice caps every sleep so a wall-clock jump (suspend, NTP step)
	// is noticed within a minute rather than after a whole overnight wait.
	maxWaitSlice = time.Minute
	// healthyRun is how long a window run must last before the retry delay
	// resets, so a socket that dies right after subscribing still backs off.
	healthyRun   = time.Minute
	defaultRetry = 5 * time.Second
	maxRetry     = 5 * time.Minute
)

var (
	errStreamStopped   = errors.New("quote: KIS quote stream stopped")
	errNoSubscriptions = errors.New("quote: KIS accepted no quote subscription")
)

// Config wires one quote lane. Every dependency here is owned by the lane:
// callers must not pass the execution-fill pipeline's approval provider,
// dialer, or Redis client.
type Config struct {
	Endpoint  string // ws.EndpointLive or ws.EndpointVTS
	Symbols   []string
	Approval  ws.ApprovalKeyProvider
	Dialer    ws.Dialer
	Redis     redis.UniversalClient
	StreamKey string
	MaxLen    int64
	Buffer    int
	Clock     kis.Clock
	Logger    *slog.Logger

	// RetryMin and RetryMax pace redials after a window run fails. Zero
	// values take 5s and 5m.
	RetryMin, RetryMax time.Duration
	// Backoff paces go-kis's own reconnects inside one window.
	Backoff ws.BackoffConfig
}

// Counters are process-local observations of the quote lane.
type Counters struct {
	Events            atomic.Uint64
	TicksWritten      atomic.Uint64
	DropMalformed     atomic.Uint64
	DropPartial       atomic.Uint64
	DropUnknownTR     atomic.Uint64
	DropUnknownSymbol atomic.Uint64
	DropOutOfSession  atomic.Uint64
	DropBufferFull    atomic.Uint64
	XAddErrors        atomic.Uint64
	SubscribeRejected atomic.Uint64
	Dials             atomic.Uint64
	WindowRuns        atomic.Uint64
}

func (c *Counters) drop(reason DropReason) {
	switch reason {
	case DropMalformed:
		c.DropMalformed.Add(1)
	case DropPartial:
		c.DropPartial.Add(1)
	case DropUnknownTR:
		c.DropUnknownTR.Add(1)
	case DropUnknownSym:
		c.DropUnknownSymbol.Add(1)
	case DropOutOfSession:
		c.DropOutOfSession.Add(1)
	}
}

func (c *Counters) logArgs() []any {
	return []any{
		"events", c.Events.Load(),
		"ticks_written", c.TicksWritten.Load(),
		"drop_malformed", c.DropMalformed.Load(),
		"drop_partial", c.DropPartial.Load(),
		"drop_unknown_tr", c.DropUnknownTR.Load(),
		"drop_unknown_symbol", c.DropUnknownSymbol.Load(),
		"drop_out_of_session", c.DropOutOfSession.Load(),
		"drop_buffer_full", c.DropBufferFull.Load(),
		"xadd_errors", c.XAddErrors.Load(),
		"subscribe_rejected", c.SubscribeRejected.Load(),
		"dials", c.Dials.Load(),
		"window_runs", c.WindowRuns.Load(),
	}
}

// Runner is the quote lane. Its Run never returns an error to the caller:
// nothing on this lane may stop the process or the execution-fill pipeline.
type Runner struct {
	cfg      Config
	decoder  *Decoder
	counters *Counters
	ticks    chan Tick
}

// NewRunner validates cfg.
func NewRunner(cfg Config) (*Runner, error) {
	if _, err := kis.ValidateWSURL(cfg.Endpoint); err != nil {
		return nil, errors.New("quote: endpoint is not an allowlisted KIS websocket")
	}
	if len(cfg.Symbols) == 0 || len(cfg.Symbols) > MaxSymbols {
		return nil, fmt.Errorf("quote: symbol count must be 1..%d", MaxSymbols)
	}
	for _, symbol := range cfg.Symbols {
		if !ValidSymbol(symbol) {
			return nil, errors.New("quote: invalid symbol")
		}
	}
	if cfg.Approval == nil || cfg.Dialer == nil || cfg.Redis == nil {
		return nil, errors.New("quote: approval provider, dialer, and Redis client are required")
	}
	if strings.TrimSpace(cfg.StreamKey) == "" || cfg.MaxLen <= 0 || cfg.Buffer <= 0 {
		return nil, errors.New("quote: stream key, max length, and buffer are required")
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
	return &Runner{
		cfg:      cfg,
		decoder:  NewDecoder(cfg.Symbols),
		counters: &Counters{},
		ticks:    make(chan Tick, cfg.Buffer),
	}, nil
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
		session, closes, open := SessionAt(now)
		if !open {
			next := NextOpen(now)
			r.cfg.Logger.Info("quote reader idle outside trading windows", "next_open", next.Format(time.RFC3339))
			r.waitUntil(ctx, next)
			continue
		}
		started := r.cfg.Clock.Now()
		r.counters.WindowRuns.Add(1)
		err := r.runWindow(ctx, stopLane, session, closes)
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
		r.cfg.Logger.Warn("quote reader window run failed; retrying", "reason", safeReason(err), "retry_in", delay.String())
		wake := r.cfg.Clock.Now().Add(delay)
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

// runWindow owns one quote socket from open to window close. It returns nil
// when the window closes or ctx ends and an error when the socket failed.
func (r *Runner) runWindow(ctx context.Context, stopLane context.CancelFunc, session string, closes time.Time) error {
	// windowCtx ends when the window closes, including a wall-clock jump out
	// of it, so a pending dial or subscribe never outlives the window.
	windowCtx, closeWindow := context.WithCancel(ctx)
	defer closeWindow()
	go r.guard("window watcher", stopLane, func() {
		r.watchWindow(windowCtx, closes)
		closeWindow()
	})

	r.counters.Dials.Add(1)
	dialCtx, cancelDial := context.WithTimeout(windowCtx, dialTimeout)
	conn, err := ws.Dial(dialCtx, ws.Config{
		Endpoint:    r.cfg.Endpoint,
		Approval:    safeApproval{provider: r.cfg.Approval, logger: r.cfg.Logger},
		Dialer:      safeDialer{dialer: r.cfg.Dialer, logger: r.cfg.Logger},
		EventBuffer: r.cfg.Buffer,
		Backoff:     r.cfg.Backoff,
		Clock:       r.cfg.Clock,
		OnReconnect: func(info ws.ReconnectInfo) {
			if info.Stopped {
				r.cfg.Logger.Warn("quote websocket reconnect loop stopped", "attempt", info.Attempt)
				return
			}
			r.cfg.Logger.Info("quote websocket reconnected", "attempt", info.Attempt, "subscriptions", info.Subscriptions)
		},
	})
	cancelDial()
	if err != nil {
		if windowCtx.Err() != nil {
			return nil
		}
		return err
	}
	// Close sends an unsubscribe for every subscription before dropping the
	// socket, and go-kis bounds those writes.
	defer conn.Close()

	// Drain before subscribing: ACKs and data share one socket reader, so an
	// undrained Events channel would stall the ACKs of later subscriptions.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		r.guard("decoder", stopLane, func() { r.drain(conn.Events()) })
	}()

	accepted := 0
	for _, symbol := range r.cfg.Symbols {
		for _, tr := range []string{ws.TRQuotePrice, ws.TRQuoteBook} {
			if windowCtx.Err() != nil || !r.windowOpen(closes) {
				return r.windowEnded(ctx, session)
			}
			subscribeCtx, cancel := context.WithTimeout(windowCtx, subscribeTimeout)
			err := conn.Subscribe(subscribeCtx, tr, symbol)
			cancel()
			var rejected *ws.SubscribeError
			switch {
			case err == nil:
				accepted++
			case errors.As(err, &rejected):
				// One refused registration leaves the socket usable.
				r.counters.SubscribeRejected.Add(1)
				r.cfg.Logger.Warn("quote subscription rejected", "tr", tr, "symbol", symbol, "msg_cd", rejected.MsgCD)
			default:
				if windowCtx.Err() != nil {
					return r.windowEnded(ctx, session)
				}
				return err
			}
		}
	}
	if accepted == 0 {
		return errNoSubscriptions
	}
	r.cfg.Logger.Info("quote reader subscriptions active", "session", session, "accepted", accepted, "requested", 2*len(r.cfg.Symbols), "stream", r.cfg.StreamKey)

	select {
	case <-windowCtx.Done():
		return r.windowEnded(ctx, session)
	case <-drained:
		if windowCtx.Err() != nil {
			return r.windowEnded(ctx, session)
		}
		return errStreamStopped
	}
}

// windowEnded reports a normal end of one window run.
func (r *Runner) windowEnded(ctx context.Context, session string) error {
	if ctx.Err() == nil {
		r.cfg.Logger.Info("quote reader window closed", append([]any{"session", session}, r.counters.logArgs()...)...)
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
	_, current, open := SessionAt(r.cfg.Clock.Now())
	return open && current.Equal(closes)
}

// drain decodes events until the connection closes Events. Ticks are offered
// to the writer without blocking: a slow Redis drops quotes (counted) instead
// of stalling the quote socket.
func (r *Runner) drain(events <-chan ws.Event) {
	for event := range events {
		r.counters.Events.Add(1)
		ticks, drops := r.decoder.Decode(event)
		for _, reason := range drops {
			r.counters.drop(reason)
		}
		for _, tick := range ticks {
			select {
			case r.ticks <- tick:
			default:
				r.counters.DropBufferFull.Add(1)
			}
		}
	}
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
