package upbit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	// The public market websocket recorder, reused as-is: it owns the
	// subscription, the ping, and the reconnect with backoff.
	upstream "github.com/mgh3326/fillwire/internal/upbit"
)

// Source event kinds.
const (
	EventBook        = "book"
	EventGap         = "gap"
	EventDecodeError = "decode_error"
)

// SourceEvent is one thing the live source observed. A book is a parsed
// touch; a gap is a disconnect the recorder saw, carrying its reason; a
// decode error is a frame this model refused.
type SourceEvent struct {
	Kind   string
	At     time.Time
	Book   Book
	Reason string
}

// SourceConfig configures the live book source. Root is the recording root
// internal/upbit writes its evidence under and locks; it is required.
type SourceConfig struct {
	Market       string
	Root         string
	Dialer       upstream.Dialer
	Clock        upstream.Clock
	RetryMin     time.Duration
	RetryMax     time.Duration
	PingInterval time.Duration
	PongTimeout  time.Duration
	Logger       *slog.Logger
}

// Source is the live Upbit book: internal/upbit's reconnect loop with each
// orderbook frame decoded into a Book. Emit runs on the source goroutine and
// must return quickly.
type Source struct {
	emit   func(SourceEvent)
	reader *upstream.Reader
}

// NewSource wires the source. It fails on a market this model does not
// accept, a missing recording root, or a missing emit.
func NewSource(cfg SourceConfig, emit func(SourceEvent)) (*Source, error) {
	switch {
	case !ValidMarket(cfg.Market):
		return nil, fmt.Errorf("upbit: market %q", cfg.Market)
	case cfg.Root == "":
		return nil, errors.New("upbit: recording root is required")
	case emit == nil:
		return nil, errors.New("upbit: emit is required")
	}
	s := &Source{emit: emit}
	reader, err := upstream.New(upstream.Config{
		Markets:      []string{cfg.Market},
		Root:         cfg.Root,
		Dialer:       cfg.Dialer,
		Clock:        cfg.Clock,
		RetryMin:     cfg.RetryMin,
		RetryMax:     cfg.RetryMax,
		PingInterval: cfg.PingInterval,
		PongTimeout:  cfg.PongTimeout,
		Logger:       cfg.Logger,
		OnRecord:     s.onRecord,
	})
	if err != nil {
		return nil, err
	}
	s.reader = reader
	return s, nil
}

// Run listens until ctx ends, reconnecting as internal/upbit does.
func (s *Source) Run(ctx context.Context) error { return s.reader.Run(ctx) }

func (s *Source) onRecord(rec upstream.Record) {
	at := rec.ReceivedAt
	switch rec.Kind {
	case "orderbook":
		b, err := ParseBook(rec.Raw, at)
		if err != nil {
			s.emit(SourceEvent{Kind: EventDecodeError, At: at, Reason: err.Error()})
			return
		}
		s.emit(SourceEvent{Kind: EventBook, At: at, Book: b})
	case "gap":
		s.emit(SourceEvent{Kind: EventGap, At: at, Reason: rec.Reason})
	}
}
