package kismockws

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mgh3326/fillwire/internal/reader"
	"github.com/mgh3326/fillwire/internal/recording"
	"github.com/mgh3326/go-kis/kis/ws"
)

const (
	Name                  = "kis-mock-ws"
	RegistrationLimit     = 41 // existing fillwire conservative cap; exact KIS limit remains unverified
	SubscriptionPace      = 500 * time.Millisecond
	quoteRecordingName    = "quotes"
	overseasRecordingName = "overseas-fills"
	futuresRecordingName  = "futures-fills"
	readerOutputBuffer    = 32
	malformedRecording    = "malformed-notices"
	noticeAckTimeout      = 5 * time.Second
	subscribeRetryMin     = 500 * time.Millisecond
	subscribeRetryMax     = 5 * time.Second
)

// Config describes one record-only VTS session. Endpoint must be "mock";
// there is deliberately no live endpoint fallback.
type Config struct {
	Endpoint           string
	HTSID              string
	Symbols            []string
	RecordingDirectory string
	Approval           ws.ApprovalKeyProvider
	FuturesNoticeTR    string
	Dialer             ws.Dialer
	Logger             *slog.Logger
	SubscribePace      time.Duration
}

// Health is a snapshot of this module's local stream state.
type Health struct {
	Connected       bool      `json:"connected"`
	LastReceivedAt  time.Time `json:"last_received_at,omitempty"`
	MalformedFrames uint64    `json:"malformed_frames"`
	Reconnects      uint64    `json:"reconnects"`
	LastError       string    `json:"last_error,omitempty"`
}

// Module records KIS mock quote and notice frames without ingesting them.
type Module struct {
	cfg          Config
	appendRecord func(stream string, at time.Time, value any) error

	connected atomic.Bool
	malformed atomic.Uint64
	reconnect atomic.Uint64
	mu        sync.RWMutex
	lastAt    time.Time
	lastErr   string
	writeErr  error
}

func New(cfg Config) (*Module, error) {
	if cfg.Endpoint != "mock" {
		return nil, errors.New("kismockws: only the mock endpoint is supported")
	}
	if cfg.HTSID == "" || cfg.RecordingDirectory == "" || cfg.Approval == nil {
		return nil, errors.New("kismockws: HTS ID, recording directory and approval provider are required")
	}
	if len(cfg.Symbols) == 0 {
		return nil, errors.New("kismockws: at least one domestic symbol is required")
	}
	if cfg.FuturesNoticeTR == "" {
		cfg.FuturesNoticeTR = TRFuturesNotice
	}
	if cfg.FuturesNoticeTR != TRFuturesNotice && cfg.FuturesNoticeTR != TRFuturesNoticeLegacy {
		return nil, errors.New("kismockws: unsupported futures notice TR")
	}
	seen := make(map[string]bool, len(cfg.Symbols))
	for _, symbol := range cfg.Symbols {
		if !validSymbol(symbol) || seen[symbol] {
			return nil, errors.New("kismockws: invalid or duplicate domestic symbol")
		}
		seen[symbol] = true
	}
	// The parent reader registers the domestic mock execution notice. Add two
	// quotes per symbol plus the overseas and futures notices. Keep the sum
	// within fillwire's conservative 41-registration cap.
	if 1+2*len(cfg.Symbols)+2 > RegistrationLimit {
		return nil, errors.New("kismockws: subscriptions exceed per-session registration limit")
	}
	if cfg.SubscribePace <= 0 {
		cfg.SubscribePace = SubscriptionPace
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Module{cfg: cfg}, nil
}

func validSymbol(value string) bool {
	if len(value) != 6 {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// Name identifies this module to a future serve registry.
func (m *Module) Name() string { return Name }

// Health reports connection and decode status without exposing credentials or
// raw frames.
func (m *Module) Health() Health {
	m.mu.RLock()
	lastAt, lastErr := m.lastAt, m.lastErr
	m.mu.RUnlock()
	return Health{Connected: m.connected.Load(), LastReceivedAt: lastAt,
		MalformedFrames: m.malformed.Load(), Reconnects: m.reconnect.Load(), LastError: lastErr}
}

// Run owns one VTS reader and the append-only recording sink until ctx ends.
func (m *Module) Run(ctx context.Context) (runErr error) {
	if m.cfg.Endpoint != "mock" {
		return errors.New("kismockws: refusing non-mock endpoint")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	sink, err := recording.NewSink(m.cfg.RecordingDirectory, Name)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, sink.Close()) }()
	quotes := func(event ws.Event) {
		m.recordQuote(sink, event, cancel)
	}
	attachErr := make(chan error, 1)
	attachStarted := atomic.Bool{}
	attachDone := make(chan struct{})
	attach := func(sessionCtx context.Context, session reader.Session) {
		registrations := make([][2]string, 0, 2*len(m.cfg.Symbols)+2)
		for _, symbol := range m.cfg.Symbols {
			registrations = append(registrations, [2]string{"H0STCNT0", symbol}, [2]string{"H0STASP0", symbol})
		}
		registrations = append(registrations, [2]string{TROverseasNotice, m.cfg.HTSID}, [2]string{m.cfg.FuturesNoticeTR, m.cfg.HTSID})
		for i, registration := range registrations {
			if i > 0 {
				timer := time.NewTimer(m.cfg.SubscribePace)
				select {
				case <-sessionCtx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			err, canceled := subscribeRegistration(sessionCtx, session, registration[0], registration[1])
			if canceled {
				return
			}
			if err != nil {
				m.setError("subscription failed")
				m.cfg.Logger.Error("KIS mock websocket subscription failed", "tr", registration[0])
				attachErr <- fmt.Errorf("kismockws: subscription failed for %s: %w", registration[0], err)
				cancel()
				return
			}
		}
	}
	output := make(chan ws.Event, readerOutputBuffer)
	events := (<-chan ws.Event)(output)
	readerDone := make(chan error, 1)
	go func() {
		defer close(output)
		readerDone <- reader.New(reader.Config{
			Endpoint:    m.cfg.Endpoint,
			HTSID:       m.cfg.HTSID,
			Approval:    m.cfg.Approval,
			Dialer:      m.cfg.Dialer,
			EventBuffer: readerOutputBuffer,
			Logger:      m.cfg.Logger,
			OnSubscribed: func() {
				if runCtx.Err() == nil {
					m.connected.Store(true)
					m.clearError()
				}
			},
			OnReconnected: func() {
				if runCtx.Err() == nil {
					m.connected.Store(true)
					m.reconnect.Add(1)
					m.clearError()
				}
			},
			OnQuote: quotes,
			OnSession: func(sessionCtx context.Context, session reader.Session) {
				if !attachStarted.CompareAndSwap(false, true) {
					return
				}
				defer close(attachDone)
				attach(sessionCtx, session)
			},
		}).Run(runCtx, output)
	}()
	processEvent := func(event ws.Event) {
		if event.TR == TROverseasNotice || event.TR == TRFuturesNotice || event.TR == TRFuturesNoticeLegacy {
			m.recordNotice(sink, event, cancel)
		}
	}
	drain := func() {
		for event := range output {
			processEvent(event)
		}
	}
	readerEnded := false
	var readerErr error
	finish := func() error {
		cancel()
		if !readerEnded {
			readerErr = <-readerDone
			readerEnded = true
		}
		if attachStarted.Load() {
			<-attachDone
		}
		drain()
		m.connected.Store(false)
		var subscriptionErr error
		select {
		case subscriptionErr = <-attachErr:
		default:
		}
		result := m.recordingError(errors.Join(readerErr, subscriptionErr))
		if result != nil && !errors.Is(result, context.Canceled) {
			m.setError("KIS websocket stopped")
		}
		return result
	}
	for {
		select {
		case <-ctx.Done():
			return finish()
		case err := <-readerDone:
			readerEnded = true
			readerErr = err
			return finish()
		case event, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			processEvent(event)
		}
	}
}

func subscribeWithRetry(ctx context.Context, session reader.Session, tr, key string) error {
	delay := subscribeRetryMin
	for {
		requestCtx, cancel := context.WithTimeout(ctx, noticeAckTimeout)
		err := session.Subscribe(requestCtx, tr, key)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil || errors.Is(err, ws.ErrSubscribeFailed) {
			return err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if delay < subscribeRetryMax/2 {
			delay *= 2
		} else {
			delay = subscribeRetryMax
		}
	}
}

func subscribeRegistration(ctx context.Context, session reader.Session, tr, key string) (error, bool) {
	err := subscribeWithRetry(ctx, session, tr, key)
	if err != nil && ctx.Err() != nil {
		return nil, true
	}
	return err, false
}

func record(sink *recording.Sink, stream string, at time.Time, value any) error {
	return sink.Append(Name, stream, at, value)
}

func (m *Module) recordQuote(sink *recording.Sink, event ws.Event, cancel context.CancelFunc) {
	value, err := DecodeQuote(event)
	if err != nil {
		m.malformed.Add(1)
		return
	}
	at := event.ReceivedAt
	if at.IsZero() {
		at = time.Now().UTC()
	}
	if m.append(sink, quoteRecordingName, at, value, cancel) {
		m.received(at)
	}
}

func (m *Module) recordNotice(sink *recording.Sink, event ws.Event, cancel context.CancelFunc) {
	notices, err := DecodeNotices(event)
	if err != nil {
		m.malformed.Add(1)
		at := event.ReceivedAt
		if at.IsZero() {
			at = time.Now().UTC()
		}
		bad := struct {
			Kind       string    `json:"kind"`
			TR         string    `json:"tr"`
			ReceivedAt time.Time `json:"received_at"`
			Reason     string    `json:"reason"`
			Fields     []string  `json:"fields"`
			Raw        []byte    `json:"raw,omitempty"`
		}{"malformed_notice", event.TR, at.UTC(), "invalid notice layout", event.Fields, event.Raw}
		if m.append(sink, malformedRecording, at, bad, cancel) {
			m.received(at)
		}
		if m.cfg.Logger != nil {
			m.cfg.Logger.Warn("KIS mock websocket malformed notice recorded", "tr", event.TR)
		}
		return
	}
	for _, notice := range notices {
		stream := overseasRecordingName
		if event.TR == TRFuturesNotice || event.TR == TRFuturesNoticeLegacy {
			stream = futuresRecordingName
		}
		if m.append(sink, stream, notice.ReceivedAt, notice, cancel) {
			m.received(notice.ReceivedAt)
		}
	}
}

func (m *Module) append(sink *recording.Sink, name string, at time.Time, value any, cancel context.CancelFunc) bool {
	m.mu.RLock()
	failed := m.writeErr != nil
	m.mu.RUnlock()
	if failed {
		return false
	}
	appendRecord := m.appendRecord
	if appendRecord == nil {
		appendRecord = func(stream string, at time.Time, value any) error {
			return record(sink, stream, at, value)
		}
	}
	if err := appendRecord(name, at, value); err != nil {
		m.setWriteError(err)
		m.setError("recording append failed")
		if m.cfg.Logger != nil {
			m.cfg.Logger.Error("KIS mock websocket recording failed")
		}
		cancel()
		return false
	}
	return true
}

func (m *Module) received(at time.Time) {
	m.mu.Lock()
	m.lastAt = at
	m.mu.Unlock()
}

func (m *Module) setError(message string) {
	m.mu.Lock()
	m.lastErr = message
	m.mu.Unlock()
}

func (m *Module) clearError() {
	m.mu.Lock()
	m.lastErr = ""
	m.mu.Unlock()
}

func (m *Module) setWriteError(err error) {
	m.mu.Lock()
	if m.writeErr == nil {
		m.writeErr = err
	}
	m.mu.Unlock()
}

func (m *Module) recordingError(other error) error {
	m.mu.RLock()
	writeErr := m.writeErr
	m.mu.RUnlock()
	return errors.Join(other, writeErr)
}
