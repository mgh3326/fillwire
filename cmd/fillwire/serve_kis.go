package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/fillwire/internal/alert"
	"github.com/mgh3326/fillwire/internal/kisfutures"
	"github.com/mgh3326/fillwire/internal/reader"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/pelletier/go-toml/v2"
	"github.com/redis/go-redis/v9"
)

// kisHub publishes each generation of the single KIS socket. Module attempts
// own their registrations and stop using the socket when that generation ends.
type kisHub struct {
	mu      sync.Mutex
	changed chan struct{}
	current *kisSession
}

type kisSession struct {
	reader.Session
	ctx    context.Context
	cancel context.CancelFunc

	mu               sync.RWMutex
	fills            *kisFillSubscription
	quotes           func(ws.Event)
	futures          func(ws.Event)
	futuresReconnect func()
}

type kisFillSubscription struct {
	mu          sync.Mutex
	ctx         context.Context
	out         chan<- ws.Event
	reconnected func()
}

func newKISHub() *kisHub { return &kisHub{changed: make(chan struct{})} }

func (h *kisHub) publish(session *kisSession) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.current = session
	close(h.changed)
	h.changed = make(chan struct{})
}

func (h *kisHub) wait(ctx context.Context) (*kisSession, error) {
	for ctx.Err() == nil {
		h.mu.Lock()
		session, changed := h.current, h.changed
		h.mu.Unlock()
		if session != nil && session.ctx.Err() == nil {
			return session, nil
		}
		select {
		case <-ctx.Done():
		case <-changed:
		}
	}
	return nil, ctx.Err()
}

func (s *kisSession) onFill(ctx context.Context, event ws.Event) {
	s.mu.RLock()
	fills := s.fills
	s.mu.RUnlock()
	if fills == nil {
		return
	}
	fills.mu.Lock()
	defer fills.mu.Unlock()
	if fills.ctx.Err() != nil {
		return
	}
	// Retain fill backpressure while attached. Detaching waits for a
	// potentially blocked send before the pipeline closes its channel.
	select {
	case fills.out <- event:
	case <-fills.ctx.Done():
	case <-ctx.Done():
	}
}

func (s *kisSession) onQuote(event ws.Event) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.quotes != nil {
		s.quotes(event)
	}
}

func (s *kisSession) onFutures(event ws.Event) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.futures != nil {
		s.futures(event)
	}
}

func (s *kisSession) onReconnect() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.fills != nil && s.fills.reconnected != nil {
		s.fills.reconnected()
	}
	if s.futuresReconnect != nil {
		s.futuresReconnect()
	}
}

func (h *kisHub) run(ctx context.Context, cfg runtimeConfig, logger *slog.Logger, ready func()) error {
	options, err := redis.ParseURL(cfg.Redis.URL)
	if err != nil {
		return errors.New("config: invalid Redis URL")
	}
	client := redis.NewClient(options)
	defer client.Close()
	approvalSource, approval, err := newKISApproval(cfg, logger, runDependencies{}, client)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	session := &kisSession{ctx: ctx, cancel: cancel}
	defer h.publish(nil)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runAttempt(func() error {
			return reader.New(reader.Config{
				SharedSession: func(conn reader.Session) {
					session.Session = conn
					h.publish(session)
					ready()
				},
				OnFill: session.onFill, OnQuote: session.onQuote, OnFutures: session.onFutures,
				OnReconnected: session.onReconnect, Approval: approvalSource,
				Dialer: ws.NewDialer(), Endpoint: cfg.KIS.Endpoint, EventBuffer: cfg.KIS.EventBuffer, Logger: logger,
			}).Run(ctx, nil)
		})
	}()
	if cfg.KIS.ApprovalMode != "" {
		return <-done
	}
	refreshDone := make(chan error, 1)
	go func() { refreshDone <- runAttempt(func() error { return approval.RefreshLoop(ctx) }) }()
	select {
	case err := <-done:
		cancel()
		<-refreshDone
		return err
	case err := <-refreshDone:
		cancel()
		<-done
		return err
	}
}

func parseKISTransport(contents []byte) (runtimeConfig, error) {
	var input struct {
		KIS   kisSettings   `toml:"kis"`
		Redis redisSettings `toml:"redis"`
	}
	if err := toml.Unmarshal(contents, &input); err != nil {
		return runtimeConfig{}, errors.New("config: invalid KIS settings")
	}
	cfg := runtimeConfig{fileConfig: fileConfig{KIS: input.KIS, Redis: input.Redis}}
	if err := validateGatewaydTableKeys(contents); err != nil {
		return cfg, err
	}
	if cfg.KIS.Gatewayd.TokenHeader == "" {
		cfg.KIS.Gatewayd.TokenHeader = "Authorization"
	}
	if err := cfg.validateKISConnection(); err != nil {
		return cfg, err
	}
	if cfg.KIS.Broker != "kis" || cfg.KIS.EventBuffer <= 0 {
		return cfg, errors.New("config: KIS broker and positive event_buffer required")
	}
	if err := validateRedisURL(cfg.Redis.URL); err != nil {
		return cfg, err
	}
	if cfg.KIS.RefreshMargin == "" {
		cfg.KIS.RefreshMargin = "1h"
	}
	margin, err := time.ParseDuration(cfg.KIS.RefreshMargin)
	if err != nil || margin <= 0 || margin > reader.MaxRefreshMargin {
		return cfg, errors.New("config: invalid KIS approval_refresh_margin")
	}
	cfg.refreshMargin = margin
	if cfg.KIS.ApprovalMode == "" {
		if err := cfg.loadKISCredentials(); err != nil {
			return cfg, err
		}
	}
	if cfg.usesGatewayd() && strings.TrimSpace(cfg.KIS.Gatewayd.TokenEnv) != "" {
		cfg.gatewaydToken = os.Getenv(cfg.KIS.Gatewayd.TokenEnv)
		if cfg.gatewaydToken == "" {
			return cfg, fmt.Errorf("config: required environment variable %s is unset", cfg.KIS.Gatewayd.TokenEnv)
		}
	}
	return cfg, nil
}

func (h *kisHub) fills(cfg runtimeConfig, ready func()) fillSource {
	return func(ctx context.Context, out chan<- ws.Event, subscribed, reconnected func()) error {
		session, err := h.wait(ctx)
		if err != nil {
			return err
		}
		tr := ws.TRExecutionLive
		if cfg.KIS.Endpoint == "mock" {
			tr = ws.TRExecutionVTS
		}
		// The pipeline owns out and closes it after this function returns.
		// A separate attachment context lets us unblock any in-flight send.
		streamCtx, cancel := context.WithCancel(ctx)
		sub := &kisFillSubscription{ctx: streamCtx, out: out, reconnected: reconnected}
		session.mu.Lock()
		session.fills = sub
		session.mu.Unlock()
		defer func() {
			cancel()
			session.mu.Lock()
			session.fills = nil
			session.mu.Unlock()
			sub.mu.Lock()
			sub.mu.Unlock()
		}()
		if err := session.add(ctx, tr, cfg.KIS.HTSID); err != nil {
			return err
		}
		defer func() {
			cancel()
			session.remove(tr, cfg.KIS.HTSID)
		}()
		if subscribed != nil {
			subscribed()
		}
		ready()
		return session.wait(ctx)
	}
}

func (s *kisSession) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return reader.ErrEventsClosed
	}
}

func (s *kisSession) add(ctx context.Context, tr, key string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := s.Subscribe(ctx, tr, key)
	var rejected *ws.SubscribeError
	if err != nil && !errors.As(err, &rejected) {
		// The client may remember a failed or unacknowledged subscription.
		s.remove(tr, key)
	}
	return err
}

func (s *kisSession) remove(tr, key string) {
	if s.ctx.Err() == nil {
		ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
		defer cancel()
		if s.Unsubscribe(ctx, tr, key) != nil && s.ctx.Err() == nil {
			// The next fill attempt must not silently reuse a stale registration.
			s.cancel()
		}
	}
}

func runKISFills(ctx context.Context, contents []byte, h *kisHub, logger *slog.Logger, ready func()) error {
	cfg, err := parseConfig(contents, false)
	if err != nil {
		return err
	}
	alerter := alert.FromEnv(logger, cfg.alertRateLimit)
	return runWithDependencies(ctx, cfg, logger, runDependencies{alerter: alerter, sharedFills: h.fills(cfg, ready)})
}

func runKISQuotes(ctx context.Context, contents []byte, h *kisHub, logger *slog.Logger, reserveFutures bool, ready func()) error {
	var input struct {
		KIS   kisSettings   `toml:"kis"`
		Redis redisSettings `toml:"redis"`
		Books bookSettings  `toml:"books"`
	}
	if err := toml.Unmarshal(contents, &input); err != nil {
		return errors.New("config: invalid KIS quote settings")
	}
	cfg := runtimeConfig{fileConfig: fileConfig{KIS: input.KIS, Redis: input.Redis, Books: input.Books}, kisQuotes: resolveKisQuoteSettings(contents)}
	if cfg.kisQuotes.err != nil {
		return cfg.kisQuotes.err
	}
	if err := cfg.loadKISCredentials(); err != nil {
		return err
	}
	var err error
	if cfg.recording, err = resolveRecordingSettings(contents); err != nil {
		return err
	}
	if reserveFutures && cfg.kisQuotes.enabled {
		cfg.kisQuotes.limit -= kisfutures.Registrations
	}
	session, err := h.wait(ctx)
	if err != nil {
		return err
	}
	bookEvent, stopBooks, err := startBooks(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer stopBooks()
	var symbols []string
	if cfg.Books.Enabled {
		symbols = []string{cfg.Books.Symbol}
	}
	lane, stopQuotes := startKisQuotes(ctx, cfg, logger, symbols, nil)
	defer stopQuotes()
	if cfg.kisQuotes.enabled && lane == nil {
		return errors.New("KIS 승격 호가 모듈을 초기화하지 못했습니다")
	}
	session.mu.Lock()
	session.quotes = func(event ws.Event) {
		if bookEvent != nil && event.TR == ws.TRQuoteBook {
			bookEvent(event)
		}
		if lane != nil {
			lane.Handle(event)
		}
	}
	session.mu.Unlock()
	defer func() {
		session.mu.Lock()
		session.quotes = nil
		session.mu.Unlock()
	}()
	if cfg.Books.Enabled {
		if err := session.add(ctx, ws.TRQuoteBook, cfg.Books.Symbol); err != nil {
			logger.Warn("KIS book subscription unavailable; mock REST fallback remains active")
		} else {
			defer session.remove(ws.TRQuoteBook, cfg.Books.Symbol)
		}
	}
	if lane != nil {
		lane.Attach(ctx, session)
	}
	ready()
	return session.wait(ctx)
}

func runKISFutures(ctx context.Context, contents []byte, h *kisHub, logger *slog.Logger, ready func()) error {
	var input struct {
		KIS   kisSettings   `toml:"kis"`
		Redis redisSettings `toml:"redis"`
	}
	if err := toml.Unmarshal(contents, &input); err != nil {
		return errors.New("config: invalid KIS futures settings")
	}
	settings, err := resolveKisFuturesSettings(contents)
	if err != nil {
		return err
	}
	if settings.err != nil {
		return settings.err
	}
	cfg := runtimeConfig{fileConfig: fileConfig{KIS: input.KIS, Redis: input.Redis}, kisFutures: settings}
	session, err := h.wait(ctx)
	if err != nil {
		return err
	}
	lane, _, stop := startKisFutures(cfg, logger, runDependencies{})
	defer stop()
	if lane == nil {
		return errors.New("KIS 선물 모듈을 초기화하지 못했습니다")
	}
	session.mu.Lock()
	session.futures, session.futuresReconnect = lane.Handle, lane.Reconnected
	session.mu.Unlock()
	defer func() {
		session.mu.Lock()
		session.futures, session.futuresReconnect = nil, nil
		session.mu.Unlock()
	}()
	lane.Attach(ctx, session)
	ready()
	return session.wait(ctx)
}
