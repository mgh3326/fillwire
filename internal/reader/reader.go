// Package reader drains KIS websocket events into a bounded pipeline channel.
package reader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/mgh3326/go-kis/kis"
	"github.com/mgh3326/go-kis/kis/ws"
)

// ExitCodeSessionOccupied is reserved for KIS OPSP8996. A supervisor can use
// it to distinguish the session-holder case from other process failures.
const ExitCodeSessionOccupied = 42

// ErrEventsClosed reports that KIS stopped the event stream without a caller
// initiated shutdown. Treating this as success would leave the process alive
// after the feed has died.
var ErrEventsClosed = errors.New("reader: KIS event stream closed unexpectedly")

// Config selects a matched KIS endpoint/TR pair and a websocket transport.
type Config struct {
	Endpoint    string // live or mock
	HTSID       string
	Approval    ws.ApprovalKeyProvider
	Dialer      ws.Dialer
	EventBuffer int
	Clock       kis.Clock
	Logger      *slog.Logger

	// dial is an internal test seam. Production always uses ws.Dial, while the
	// public Dialer/Transport interfaces remain the integration seam for KIS.
	dial dialFunc
}

// Reader owns one KIS execution subscription.
type Reader struct{ cfg Config }

type session interface {
	Events() <-chan ws.Event
	Subscribe(context.Context, string, string) error
	Close() error
}

type dialFunc func(context.Context, ws.Config) (session, error)

type reconnectState struct {
	mu   sync.Mutex
	info ws.ReconnectInfo
	seen bool
}

func (s *reconnectState) record(info ws.ReconnectInfo) bool {
	s.mu.Lock()
	// resubscribe runs beside the connection reader upstream. If it wakes after
	// a terminal reconnect verdict, it can report its stale successful outcome
	// after the stop notification. Keep the terminal outcome authoritative: the
	// Events channel is closing and no future reconnect can occur.
	if s.seen && s.info.Stopped && !info.Stopped {
		s.mu.Unlock()
		return false
	}
	s.info = info
	s.seen = true
	s.mu.Unlock()
	return true
}

func (s *reconnectState) stoppedError() error {
	s.mu.Lock()
	info, seen := s.info, s.seen
	s.mu.Unlock()
	if seen && info.Err != nil {
		return fmt.Errorf("%w: %w", ErrEventsClosed, info.Err)
	}
	return ErrEventsClosed
}

// New creates a reader. Endpoint validation happens before dialing.
func New(cfg Config) *Reader { return &Reader{cfg: cfg} }

func (r *Reader) dial(ctx context.Context, cfg ws.Config) (session, error) {
	if r.cfg.dial != nil {
		return r.cfg.dial(ctx, cfg)
	}
	return ws.Dial(ctx, cfg)
}

type observedDialer struct {
	dialer ws.Dialer
	logger *slog.Logger

	mu     sync.Mutex
	called bool
}

func (d *observedDialer) Dial(ctx context.Context, endpoint string) (ws.Transport, error) {
	d.mu.Lock()
	reconnect := d.called
	d.called = true
	d.mu.Unlock()
	if reconnect && d.logger != nil {
		d.logger.Info("KIS websocket reconnect attempted")
	}
	return d.dialer.Dial(ctx, endpoint)
}

// Run dials once, subscribes once, and drains Events until cancellation. The
// out send is intentionally blocking: a full bounded channel applies
// backpressure instead of silently dropping a fill.
func (r *Reader) Run(ctx context.Context, out chan<- ws.Event) error {
	endpoint, tr, err := endpointAndTR(r.cfg.Endpoint)
	if err != nil {
		return err
	}
	if strings.TrimSpace(r.cfg.HTSID) == "" {
		return errors.New("reader: HTS ID is required")
	}
	if out == nil {
		return errors.New("reader: output channel is required")
	}

	var reconnect reconnectState
	dialer := r.cfg.Dialer
	if dialer != nil {
		dialer = &observedDialer{dialer: dialer, logger: r.cfg.Logger}
	}
	conn, err := r.dial(ctx, ws.Config{
		Endpoint:    endpoint,
		Approval:    r.cfg.Approval,
		Dialer:      dialer,
		EventBuffer: r.cfg.EventBuffer,
		Clock:       r.cfg.Clock,
		// The KIS callback runs on its internal reader goroutine. Recording the
		// latest value under a mutex is deliberately short and non-blocking.
		OnReconnect: func(info ws.ReconnectInfo) {
			if reconnect.record(info) && !info.Stopped && info.Err == nil && r.cfg.Logger != nil {
				r.cfg.Logger.Info("KIS websocket reconnect succeeded")
			}
		},
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.Subscribe(ctx, tr, r.cfg.HTSID); err != nil {
		return err
	}
	if r.cfg.Logger != nil {
		r.cfg.Logger.Info("KIS websocket initial subscription active")
	}
	for {
		// Prefer the caller's shutdown over a simultaneously closed Events
		// channel. This is the only expected close initiated by this process.
		if ctx.Err() != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case event, open := <-conn.Events():
			if !open {
				if ctx.Err() != nil {
					return nil
				}
				return reconnect.stoppedError()
			}
			select {
			case out <- event:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func endpointAndTR(endpoint string) (string, string, error) {
	switch endpoint {
	case "live":
		return ws.EndpointLive, ws.TRExecutionLive, nil
	case "mock":
		return ws.EndpointVTS, ws.TRExecutionVTS, nil
	default:
		return "", "", fmt.Errorf("reader: endpoint must be live or mock")
	}
}

// ProcessExitCode centralizes the PR1 session-occupied policy so a later
// release can extend it without scattering OPSP8996 checks through main.
func ProcessExitCode(err error) int {
	if errors.Is(err, ws.ErrSessionOccupied) {
		return ExitCodeSessionOccupied
	}
	return 1
}
