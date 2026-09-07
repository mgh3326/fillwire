// Package reader drains KIS websocket events into a bounded pipeline channel.
package reader

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/mgh3326/go-kis/kis"
	"github.com/mgh3326/go-kis/kis/ws"
)

// ExitCodeSessionOccupied is reserved for KIS OPSP8996. A supervisor can use
// it to distinguish the session-holder case from other process failures.
const ExitCodeSessionOccupied = 42

// Config selects a matched KIS endpoint/TR pair and a websocket transport.
type Config struct {
	Endpoint    string // live or mock
	HTSID       string
	Approval    ws.ApprovalKeyProvider
	Dialer      ws.Dialer
	EventBuffer int
	Clock       kis.Clock
}

// Reader owns one KIS execution subscription.
type Reader struct{ cfg Config }

// New creates a reader. Endpoint validation happens before dialing.
func New(cfg Config) *Reader { return &Reader{cfg: cfg} }

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

	conn, err := ws.Dial(ctx, ws.Config{
		Endpoint:    endpoint,
		Approval:    r.cfg.Approval,
		Dialer:      r.cfg.Dialer,
		EventBuffer: r.cfg.EventBuffer,
		Clock:       r.cfg.Clock,
		// OnReconnect is intentionally not wired in PR1.
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := conn.Subscribe(ctx, tr, r.cfg.HTSID); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, open := <-conn.Events():
			if !open {
				return nil
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
