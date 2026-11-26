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
)

// Config describes one record-only VTS session. Endpoint must be "mock";
// there is deliberately no live endpoint fallback.
type Config struct {
	Endpoint           string
	HTSID              string
	Symbols            []string
	RecordingDirectory string
	Approval           ws.ApprovalKeyProvider
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
	cfg Config

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
	seen := make(map[string]bool, len(cfg.Symbols))
	for _, symbol := range cfg.Symbols {
		if !validSymbol(symbol) || seen[symbol] {
			return nil, errors.New("kismockws: invalid or duplicate domestic symbol")
		}
		seen[symbol] = true
	}
	// The parent reader registers the domestic mock execution notice. Add two
	// quotes per symbol plus the overseas and futures notices. Leave the sum
	// within the 41-registration ceiling shown by the KIS websocket examples.
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
	sink, err := recording.NewSink(m.cfg.RecordingDirectory, Name)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, sink.Close()) }()
	quotes := func(event ws.Event) {
		m.recordQuote(sink, event)
	}
	var attachErrMu sync.Mutex
	var attachErr error
	setAttachError := func(err error) {
		attachErrMu.Lock()
		attachErr = err
		attachErrMu.Unlock()
	}
	getAttachError := func() error {
		attachErrMu.Lock()
		defer attachErrMu.Unlock()
		return attachErr
	}
	attach := func(sessionCtx context.Context, session reader.Session) {
		registrations := make([][2]string, 0, 2*len(m.cfg.Symbols)+2)
		for _, symbol := range m.cfg.Symbols {
			registrations = append(registrations, [2]string{"H0STCNT0", symbol}, [2]string{"H0STASP0", symbol})
		}
		registrations = append(registrations, [2]string{TROverseasNotice, m.cfg.HTSID}, [2]string{TRFuturesNotice, m.cfg.HTSID})
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
			if err := session.Subscribe(sessionCtx, registration[0], registration[1]); err != nil {
				setAttachError(fmt.Errorf("kismockws: subscription failed for %s", registration[0]))
				m.setError("subscription failed")
				return
			}
		}
	}
	output := make(chan ws.Event, readerOutputBuffer)
	readerDone := make(chan error, 1)
	go func() {
		defer close(output)
		readerDone <- reader.New(reader.Config{
			Endpoint:      m.cfg.Endpoint,
			HTSID:         m.cfg.HTSID,
			Approval:      m.cfg.Approval,
			Dialer:        m.cfg.Dialer,
			EventBuffer:   readerOutputBuffer,
			Logger:        m.cfg.Logger,
			OnSubscribed:  func() { m.connected.Store(true); m.clearError() },
			OnReconnected: func() { m.connected.Store(true); m.reconnect.Add(1); m.clearError() },
			OnQuote:       quotes,
			OnSession:     attach,
		}).Run(ctx, output)
	}()
	processEvent := func(event ws.Event) {
		if event.TR == TROverseasNotice || event.TR == TRFuturesNotice {
			m.recordNotice(sink, event)
		}
	}
	drain := func() {
		for event := range output {
			processEvent(event)
		}
	}
	for {
		select {
		case <-ctx.Done():
			m.connected.Store(false)
			readerErr := <-readerDone
			drain()
			err := m.recordingError(errors.Join(readerErr, getAttachError()))
			if err != nil && !errors.Is(err, context.Canceled) {
				m.setError("KIS websocket stopped")
			}
			return err
		case err := <-readerDone:
			m.connected.Store(false)
			drain()
			err = m.recordingError(errors.Join(err, getAttachError()))
			if err != nil && !errors.Is(err, context.Canceled) {
				m.setError("KIS websocket stopped")
			}
			return err
		case event, ok := <-output:
			if !ok {
				output = nil
				continue
			}
			processEvent(event)
		}
	}
}

func record(sink *recording.Sink, stream string, at time.Time, value any) error {
	return sink.Append(Name, stream, at, value)
}

func (m *Module) recordQuote(sink *recording.Sink, event ws.Event) {
	value, err := DecodeQuote(event)
	if err != nil {
		m.malformed.Add(1)
		return
	}
	at := event.ReceivedAt
	if at.IsZero() {
		at = time.Now().UTC()
	}
	m.append(sink, quoteRecordingName, at, value)
	m.received(at)
}

func (m *Module) recordNotice(sink *recording.Sink, event ws.Event) {
	notice, err := DecodeNotice(event)
	if err != nil {
		m.malformed.Add(1)
		return
	}
	stream := overseasRecordingName
	if event.TR == TRFuturesNotice {
		stream = futuresRecordingName
	}
	m.append(sink, stream, notice.ReceivedAt, notice)
	m.received(notice.ReceivedAt)
}

func (m *Module) append(sink *recording.Sink, name string, at time.Time, value any) {
	if err := record(sink, name, at, value); err != nil {
		m.setWriteError(err)
		m.setError("recording append failed")
	}
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
