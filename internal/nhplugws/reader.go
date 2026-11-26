// Package nhplugws reads NHPLUG mock WebSocket frames into the local recorder.
package nhplugws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

const (
	Name                 = "nhplug_ws"
	MockEndpoint         = "wss://moapi.nhplug.com:17070/websocket"
	maxRegistrations     = 30
	maxSendInterval      = 100 * time.Millisecond
	defaultDialTimeout   = 10 * time.Second
	defaultAckTimeout    = 10 * time.Second
	defaultPingInterval  = 30 * time.Second
	defaultPingTimeout   = 10 * time.Second
	healthySessionWindow = 30 * time.Second
	readLimit            = 1 << 20
)

var (
	ErrRegistrationLimit    = errors.New("NHPLUG WS registrations exceed the per-session limit")
	ErrRegistrationRejected = errors.New("NHPLUG WS registration was rejected")
	ErrAckTimeout           = errors.New("NHPLUG WS registration acknowledgements timed out")
	ErrAppend               = errors.New("NHPLUG WS recorder append failed")
)

// Socket is the WebSocket boundary used by Reader and its tests.
type Socket interface {
	Read(context.Context) (websocket.MessageType, []byte, error)
	Write(context.Context, websocket.MessageType, []byte) error
	Ping(context.Context) error
	Close(websocket.StatusCode, string) error
}

type Dialer func(context.Context, string) (Socket, error)
type TokenSource func(context.Context) (string, error)
type Clock func() time.Time

type Config struct {
	Endpoint        string
	UserID          string
	Quotes          []Registration
	Token           TokenSource
	Append          func(Record) error
	InvalidateToken func(string)
	Dial            Dialer
	Now             Clock
	Logger          *slog.Logger
	RetryMin        time.Duration
	RetryMax        time.Duration
	DialTimeout     time.Duration
	AckTimeout      time.Duration
	PingInterval    time.Duration
	PingTimeout     time.Duration
}

type Status struct {
	Connected       bool   `json:"connected"`
	MalformedFrames uint64 `json:"malformed_frames"`
	LastError       string `json:"last_error,omitempty"`
}

type Reader struct {
	cfg       Config
	running   atomic.Bool
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
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = defaultDialTimeout
	}
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = defaultAckTimeout
	}
	if cfg.PingInterval <= 0 {
		cfg.PingInterval = defaultPingInterval
	}
	if cfg.PingTimeout <= 0 {
		cfg.PingTimeout = defaultPingTimeout
	}
	cfg.Quotes = registrations[4:]
	return &Reader{cfg: cfg}, nil
}

func dial(ctx context.Context, endpoint string) (Socket, error) {
	// Validate immediately before the only network dial as a defense in depth.
	if err := ValidateEndpoint(endpoint); err != nil {
		return nil, err
	}
	conn, _, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: noRedirectHTTPClient()})
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(readLimit)
	return conn, nil
}

func noRedirectHTTPClient() *http.Client {
	return &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
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
	if !r.running.CompareAndSwap(false, true) {
		return errors.New("NHPLUG WS reader is already running")
	}
	defer r.running.Store(false)
	delay := r.cfg.RetryMin
	for ctx.Err() == nil {
		connectedFor, err := r.session(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, ErrAppend) || errors.Is(err, ErrOAuthCredentialsRejected) {
			r.lastError.Store(err.Error())
			return err
		}
		if err != nil {
			r.lastError.Store(err.Error())
			r.cfg.Logger.Warn("NHPLUG WS 재연결 대기", "reason", err, "malformed_frames", r.malformed.Load())
		}
		delay = retryDelay(delay, connectedFor, r.cfg.RetryMin, r.cfg.RetryMax)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

type readResult struct {
	record Record
	err    error
}

func (r *Reader) session(ctx context.Context) (time.Duration, error) {
	if err := ValidateEndpoint(r.cfg.Endpoint); err != nil {
		return 0, err
	}
	token, err := r.cfg.Token(ctx)
	if err != nil {
		return 0, err
	}
	if token == "" {
		return 0, errors.New("NHPLUG WS token source returned an empty token")
	}
	dialCtx, cancelDial := context.WithTimeout(ctx, r.cfg.DialTimeout)
	conn, err := r.cfg.Dial(dialCtx, r.cfg.Endpoint)
	cancelDial()
	if err != nil {
		return 0, fmt.Errorf("NHPLUG WS mock dial failed: %w", err)
	}
	r.lastError.Store("")
	connectedAt := time.Now()
	sessionCtx, cancelSession := context.WithCancel(ctx)
	readEvents := make(chan readResult, 1)
	readDone := make(chan struct{})
	var writeMu sync.Mutex
	go func() {
		defer close(readDone)
		r.readLoop(sessionCtx, conn, readEvents)
	}()
	pingDone := make(chan struct{})
	go func() {
		defer close(pingDone)
		r.pingLoop(sessionCtx, conn, &writeMu, readEvents)
	}()
	defer func() {
		cancelSession()
		_ = conn.Close(websocket.StatusNormalClosure, "Bye")
		<-readDone
		<-pingDone
	}()
	r.connected.Store(true)
	defer func() { r.connected.Store(false) }()

	registrations, err := BuildRegistrations(r.cfg.UserID, r.cfg.Quotes)
	if err != nil {
		return time.Since(connectedAt), err
	}
	pendingNotices := map[string]bool{}
	for _, registration := range registrations[:4] {
		pendingNotices[registration.TRCD+"\x00"+registration.TRKey] = true
	}
	process := func(in readResult) error {
		if in.err != nil {
			return in.err
		}
		if err := r.cfg.Append(in.record); err != nil {
			return errors.Join(ErrAppend, err)
		}
		if !in.record.isAck {
			return nil
		}
		if in.record.ackType != "1" {
			return nil
		}
		if in.record.ackCode != "00000" {
			message := strings.ToLower(in.record.ackMessage)
			if r.cfg.InvalidateToken != nil && (strings.Contains(message, "token") || strings.Contains(message, "auth")) {
				r.cfg.InvalidateToken(token)
			}
			return fmt.Errorf("%w: %s", ErrRegistrationRejected, in.record.ackCode)
		}
		for _, key := range in.record.ackKeys {
			delete(pendingNotices, in.record.TRCD+"\x00"+key)
		}
		return nil
	}
	lastSent := time.Time{}
	for _, registration := range registrations {
		if !lastSent.IsZero() {
			if err := waitAndProcess(sessionCtx, time.Until(lastSent.Add(maxSendInterval)), readEvents, process); err != nil {
				return time.Since(connectedAt), err
			}
		}
		payload, _ := json.Marshal(map[string]any{
			"header": map[string]string{"token": token, "tr_type": "1"},
			"body":   registration,
		})
		writeCtx, cancelWrite := context.WithTimeout(sessionCtx, r.cfg.DialTimeout)
		writeMu.Lock()
		writeErr := conn.Write(writeCtx, websocket.MessageText, payload)
		writeMu.Unlock()
		cancelWrite()
		if writeErr != nil {
			return time.Since(connectedAt), fmt.Errorf("NHPLUG WS registration write failed: %w", writeErr)
		}
		lastSent = time.Now()
	}
	if len(pendingNotices) > 0 {
		ackTimer := time.NewTimer(r.cfg.AckTimeout)
		defer ackTimer.Stop()
		for len(pendingNotices) > 0 {
			select {
			case <-sessionCtx.Done():
				return time.Since(connectedAt), sessionCtx.Err()
			case in := <-readEvents:
				if err := process(in); err != nil {
					return time.Since(connectedAt), err
				}
			case <-ackTimer.C:
				return time.Since(connectedAt), ErrAckTimeout
			}
		}
	}
	for {
		select {
		case <-sessionCtx.Done():
			return time.Since(connectedAt), sessionCtx.Err()
		case in := <-readEvents:
			if err := process(in); err != nil {
				return time.Since(connectedAt), err
			}
		}
	}
}

func (r *Reader) readLoop(ctx context.Context, conn Socket, out chan<- readResult) {
	for {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			select {
			case out <- readResult{err: fmt.Errorf("NHPLUG WS read failed: %w", err)}:
			case <-ctx.Done():
			}
			return
		}
		record, ok := parseFrame(raw)
		if !ok {
			r.malformed.Add(1)
			continue
		}
		record.ReceivedAt = r.cfg.Now().UTC()
		select {
		case out <- readResult{record: record}:
		case <-ctx.Done():
			return
		}
	}
}

func (r *Reader) pingLoop(ctx context.Context, conn Socket, writeMu *sync.Mutex, out chan<- readResult) {
	ticker := time.NewTicker(r.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, r.cfg.PingTimeout)
			writeMu.Lock()
			err := conn.Ping(pingCtx)
			writeMu.Unlock()
			cancel()
			if err != nil {
				select {
				case out <- readResult{err: fmt.Errorf("NHPLUG WS ping failed: %w", err)}:
				case <-ctx.Done():
				}
				return
			}
		}
	}
}

func waitAndProcess(ctx context.Context, duration time.Duration, events <-chan readResult, process func(readResult) error) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case in := <-events:
			if err := process(in); err != nil {
				return err
			}
		case <-timer.C:
			return nil
		}
	}
}

func retryDelay(current, connectedFor, minDelay, maxDelay time.Duration) time.Duration {
	if connectedFor >= healthySessionWindow {
		return minDelay
	}
	return min(current*2, maxDelay)
}
