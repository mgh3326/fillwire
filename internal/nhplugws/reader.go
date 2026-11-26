// Package nhplugws reads NHPLUG mock WebSocket frames into the local recorder.
package nhplugws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

const (
	Name             = "nhplug_ws"
	MockEndpoint     = "wss://moapi.nhplug.com:17070/websocket"
	maxRegistrations = 30
	maxSendInterval  = 100 * time.Millisecond
	readLimit        = 1 << 20
)

var ErrRegistrationLimit = errors.New("NHPLUG WS registrations exceed the per-session limit")

// Socket is the WebSocket boundary used by Reader and its tests.
type Socket interface {
	Read(context.Context) (websocket.MessageType, []byte, error)
	Write(context.Context, websocket.MessageType, []byte) error
	Close(websocket.StatusCode, string) error
}

type Dialer func(context.Context, string) (Socket, error)
type TokenSource func(context.Context) (string, error)
type Clock func() time.Time

type Config struct {
	Endpoint string
	UserID   string
	Quotes   []Registration
	Token    TokenSource
	Append   func(Record) error
	Dial     Dialer
	Now      Clock
	Logger   *slog.Logger
	RetryMin time.Duration
	RetryMax time.Duration
}

type Status struct {
	Connected       bool   `json:"connected"`
	MalformedFrames uint64 `json:"malformed_frames"`
	LastError       string `json:"last_error,omitempty"`
}

type Reader struct {
	cfg       Config
	connected atomic.Bool
	malformed atomic.Uint64
	lastError atomic.Value
}

var _ interface {
	Name() string
	Run(context.Context) error
	Health() bool
} = (*Reader)(nil)

func ValidateEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "wss" || u.Host != "moapi.nhplug.com:17070" || u.User != nil || u.Path != "/websocket" || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("NHPLUG WS accepts only the mock WebSocket endpoint on port 17070")
	}
	return nil
}

func BuildRegistrations(userID string, quotes []Registration) ([]Registration, error) {
	if userID == "" {
		return nil, errors.New("NHPLUG WS user_id is required")
	}
	registrations := make([]Registration, 0, 4+len(quotes))
	for _, channel := range []string{"d0", "d1", "d2", "d3"} {
		registrations = append(registrations, Registration{TRCD: channel, TRKey: userID})
	}
	if len(registrations)+len(quotes) > maxRegistrations {
		return nil, ErrRegistrationLimit
	}
	for _, registration := range quotes {
		if registration.TRCD == "" || registration.TRKey == "" {
			return nil, errors.New("NHPLUG WS quote registration requires tr_cd and tr_key")
		}
		switch registration.TRCD {
		case "oc", "ob", "oa", "nc", "nb", "na", "mc", "mb", "ma", "mg", "mn", "RC", "RH":
		default:
			return nil, fmt.Errorf("NHPLUG WS unsupported quote channel %q", registration.TRCD)
		}
	}
	registrations = append(registrations, quotes...)
	return registrations, nil
}

func New(cfg Config) (*Reader, error) {
	if cfg.Endpoint == "" {
		cfg.Endpoint = MockEndpoint
	}
	if err := ValidateEndpoint(cfg.Endpoint); err != nil {
		return nil, err
	}
	registrations, err := BuildRegistrations(cfg.UserID, cfg.Quotes)
	if err != nil {
		return nil, err
	}
	if cfg.Token == nil || cfg.Append == nil {
		return nil, errors.New("NHPLUG WS requires token source and recorder")
	}
	if cfg.Dial == nil {
		cfg.Dial = dial
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.RetryMin <= 0 {
		cfg.RetryMin = time.Second
	}
	if cfg.RetryMax < cfg.RetryMin {
		cfg.RetryMax = 30 * time.Second
	}
	cfg.Quotes = registrations[4:]
	return &Reader{cfg: cfg}, nil
}

func dial(ctx context.Context, endpoint string) (Socket, error) {
	// Validate immediately before the only network dial as a defense in depth.
	if err := ValidateEndpoint(endpoint); err != nil {
		return nil, err
	}
	conn, _, err := websocket.Dial(ctx, endpoint, nil)
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(readLimit)
	return conn, nil
}

func (r *Reader) Name() string { return Name }

func (r *Reader) Health() bool { return r.connected.Load() }

func (r *Reader) Status() Status {
	h := Status{Connected: r.connected.Load(), MalformedFrames: r.malformed.Load()}
	if value := r.lastError.Load(); value != nil {
		h.LastError, _ = value.(string)
	}
	return h
}

func (r *Reader) Run(ctx context.Context) error {
	delay := r.cfg.RetryMin
	for ctx.Err() == nil {
		err := r.session(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			r.lastError.Store(err.Error())
			r.cfg.Logger.Warn("NHPLUG WS 재연결 대기", "reason", err, "malformed_frames", r.malformed.Load())
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, r.cfg.RetryMax)
	}
	return ctx.Err()
}

func (r *Reader) session(ctx context.Context) error {
	if err := ValidateEndpoint(r.cfg.Endpoint); err != nil {
		return err
	}
	token, err := r.cfg.Token(ctx)
	if err != nil {
		return err
	}
	if token == "" {
		return errors.New("NHPLUG WS token source returned an empty token")
	}
	conn, err := r.cfg.Dial(ctx, r.cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("NHPLUG WS mock dial failed: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "Bye")
	r.connected.Store(true)
	defer r.connected.Store(false)

	registrations, err := BuildRegistrations(r.cfg.UserID, r.cfg.Quotes)
	if err != nil {
		return err
	}
	lastSent := time.Time{}
	for _, registration := range registrations {
		if !lastSent.IsZero() {
			wait := max(time.Until(lastSent.Add(maxSendInterval)), 0)
			if err := waitContext(ctx, wait); err != nil {
				return err
			}
		}
		payload, _ := json.Marshal(map[string]any{
			"header": map[string]string{"token": token, "tr_type": "1"},
			"body":   registration,
		})
		if err := conn.Write(ctx, websocket.MessageText, payload); err != nil {
			return fmt.Errorf("NHPLUG WS registration write failed: %w", err)
		}
		lastSent = time.Now()
	}
	for {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("NHPLUG WS read failed: %w", err)
		}
		record, ok := parseFrame(raw)
		if !ok {
			r.malformed.Add(1)
			continue
		}
		record.ReceivedAt = r.cfg.Now().UTC()
		if err := r.cfg.Append(record); err != nil {
			return fmt.Errorf("NHPLUG WS record append failed: %w", err)
		}
	}
}

func waitContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
