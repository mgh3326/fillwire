package binancedemo

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/mgh3326/fillwire/internal/recording"
)

const (
	APIKeyEnv       = "BINANCE_SPOT_DEMO_API_KEY"
	APISecretEnv    = "BINANCE_SPOT_DEMO_API_SECRET"
	FallbackKeyEnv  = "BINANCE_DEMO_API_KEY"
	FallbackSecret  = "BINANCE_DEMO_API_SECRET"
	SubscribeMethod = "userDataStream.subscribe.signature"
	RecvWindowMS    = 5000
	maxSymbols      = 20
	maxFrameBytes   = 1 << 20
	dialTimeout     = 10 * time.Second
	pingInterval    = 20 * time.Second
	pingTimeout     = 10 * time.Second
)

var errRecording = errors.New("binance demo: recording append failed")

type Socket interface {
	Read(context.Context) (websocket.MessageType, []byte, error)
	Write(context.Context, websocket.MessageType, []byte) error
	Ping(context.Context) error
	CloseNow() error
}

type Dialer func(context.Context, string, string) (Socket, error)

type Config struct {
	Symbols          []string
	RecordingRoot    string
	EnableUserStream bool
	APIKey           string
	APISecret        string
	MarketURL        string
	UserURL          string
	Logger           *slog.Logger
	Dial             Dialer
	Append           func(Record) error
	Now              func() time.Time
	Sleep            func(context.Context, time.Duration) error
}

type Record struct {
	Kind       string          `json:"kind"`
	ReceivedAt time.Time       `json:"received_at"`
	Stream     string          `json:"stream,omitempty"`
	Symbol     string          `json:"symbol,omitempty"`
	EventType  string          `json:"event_type,omitempty"`
	Status     int             `json:"status,omitempty"`
	Code       int             `json:"code,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

type Health struct {
	Running          bool   `json:"running"`
	MarketConnected  bool   `json:"market_connected"`
	UserConnected    bool   `json:"user_connected"`
	MarketFrames     uint64 `json:"market_frames"`
	UserEvents       uint64 `json:"user_events"`
	MalformedFrames  uint64 `json:"malformed_frames"`
	SubscribeRejects uint64 `json:"subscribe_rejections"`
	Reconnects       uint64 `json:"reconnects"`
	RecordErrors     uint64 `json:"record_errors"`
	LastError        string `json:"last_error,omitempty"`
}

type counters struct {
	running, marketConnected, userConnected    atomic.Bool
	marketFrames, userEvents, malformedFrames  atomic.Uint64
	subscribeRejects, reconnects, recordErrors atomic.Uint64
	lastError                                  atomic.Value // string; stores only an error type, never an error message
}

type Reader struct {
	cfg          Config
	append       func(Record) error
	sink         *recording.Sink
	counters     counters
	running      atomic.Bool
	started      atomic.Bool
	pingInterval time.Duration
	pingTimeout  time.Duration
	dialTimeout  time.Duration
}

func Name() string { return "binance_demo" }

func (*Reader) Name() string { return Name() }

// Credentials reads values only from the documented environment names. The
// primary name wins when present; fallback names match the Python collector.
func Credentials(environ map[string]string) (string, string, bool) {
	key, secret := strings.TrimSpace(environ[APIKeyEnv]), strings.TrimSpace(environ[APISecretEnv])
	if key != "" || secret != "" {
		return key, secret, key != "" && secret != ""
	}
	key, secret = strings.TrimSpace(environ[FallbackKeyEnv]), strings.TrimSpace(environ[FallbackSecret])
	return key, secret, key != "" && secret != ""
}

func FromEnvironment(cfg Config) (Config, error) {
	if !cfg.EnableUserStream {
		return cfg, nil
	}
	env := map[string]string{}
	for _, name := range []string{APIKeyEnv, APISecretEnv, FallbackKeyEnv, FallbackSecret} {
		if value, ok := os.LookupEnv(name); ok {
			env[name] = value
		}
	}
	key, secret, ok := Credentials(env)
	if !ok {
		return Config{}, errors.New("binance demo: user stream credentials are not set")
	}
	cfg.APIKey, cfg.APISecret = key, secret
	return cfg, nil
}

func New(cfg Config) (*Reader, error) {
	if len(cfg.Symbols) == 0 || len(cfg.Symbols) > maxSymbols {
		return nil, errors.New("binance demo: configure between 1 and 20 symbols")
	}
	seen := map[string]bool{}
	for i, symbol := range cfg.Symbols {
		symbol = strings.ToUpper(strings.TrimSpace(symbol))
		if !validSymbol(symbol) || seen[symbol] {
			return nil, errors.New("binance demo: invalid or duplicate symbol")
		}
		seen[symbol] = true
		cfg.Symbols[i] = symbol
	}
	if cfg.MarketURL == "" {
		cfg.MarketURL = marketURL(cfg.Symbols)
	}
	if cfg.UserURL == "" {
		cfg.UserURL = UserURL
	}
	if err := ValidateURL(cfg.MarketURL, string(marketStream)); err != nil {
		return nil, err
	}
	if cfg.EnableUserStream {
		if cfg.APIKey == "" || cfg.APISecret == "" {
			return nil, errors.New("binance demo: user stream requires API key and secret")
		}
		if err := ValidateURL(cfg.UserURL, string(userStream)); err != nil {
			return nil, err
		}
	}
	if cfg.Append == nil && strings.TrimSpace(cfg.RecordingRoot) == "" {
		return nil, errors.New("binance demo: recording directory is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepContext
	}
	if cfg.Dial == nil {
		cfg.Dial = dial
	}
	r := &Reader{cfg: cfg, pingInterval: pingInterval, pingTimeout: pingTimeout, dialTimeout: dialTimeout}
	r.counters.lastError.Store("")
	if cfg.Append != nil {
		r.append = cfg.Append
	} else {
		sink, err := recording.NewSink(cfg.RecordingRoot, "binance-demo")
		if err != nil {
			return nil, err
		}
		r.sink = sink
		r.append = func(record Record) error {
			name := "user"
			if record.Symbol != "" {
				name = record.Symbol
			}
			return sink.Append("binance-demo", name, record.ReceivedAt, record)
		}
	}
	return r, nil
}

func (r *Reader) Health() Health {
	last, _ := r.counters.lastError.Load().(string)
	return Health{
		Running: r.counters.running.Load(), MarketConnected: r.counters.marketConnected.Load(),
		UserConnected: r.counters.userConnected.Load(), MarketFrames: r.counters.marketFrames.Load(),
		UserEvents: r.counters.userEvents.Load(), MalformedFrames: r.counters.malformedFrames.Load(),
		SubscribeRejects: r.counters.subscribeRejects.Load(), Reconnects: r.counters.reconnects.Load(),
		RecordErrors: r.counters.recordErrors.Load(), LastError: last,
	}
}

func (r *Reader) Run(ctx context.Context) (runErr error) {
	if !r.started.CompareAndSwap(false, true) {
		return errors.New("binance demo: reader is already running")
	}
	r.counters.running.Store(true)
	defer func() {
		r.counters.running.Store(false)
		if r.sink != nil {
			if err := r.sink.Close(); err != nil {
				r.cfg.Logger.Warn("Binance demo recorder close failed")
				if errors.Is(runErr, context.Canceled) {
					runErr = nil
				}
				runErr = errors.Join(runErr, fmt.Errorf("binance demo: recording close failed: %w", err))
			}
		}
	}()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	finished := make(chan error, 2)
	go func() { finished <- r.supervise(ctx, marketStream) }()
	count := 1
	if r.cfg.EnableUserStream {
		count++
		go func() { finished <- r.supervise(ctx, userStream) }()
	}
	select {
	case <-ctx.Done():
		cancel()
		for i := 0; i < count; i++ {
			if err := <-finished; errors.Is(err, errRecording) {
				runErr = err
			}
		}
		if runErr == nil {
			runErr = ctx.Err()
		}
		return runErr
	case err := <-finished:
		cancel()
		runErr = err
		for i := 1; i < count; i++ {
			if other := <-finished; runErr == nil && other != nil {
				runErr = other
			}
		}
		if runErr == nil {
			runErr = errors.New("binance demo: stream supervisor stopped")
		}
		return runErr
	}
}

func (r *Reader) supervise(ctx context.Context, kind streamKind) error {
	delay := time.Second
	for ctx.Err() == nil {
		started := r.cfg.Now()
		err := r.session(ctx, kind)
		r.setConnected(kind, false)
		if errors.Is(err, errRecording) {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
		if r.cfg.Now().Sub(started) >= time.Minute {
			delay = time.Second
		}
		if err != nil {
			r.counters.lastError.Store(fmt.Sprintf("%T", err))
			r.cfg.Logger.Warn("Binance demo stream reconnecting", "stream", kind, "reason", fmt.Sprintf("%T", err))
		}
		r.counters.reconnects.Add(1)
		wait := delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
		if err := r.cfg.Sleep(ctx, wait); err != nil {
			return nil
		}
		delay = min(delay*2, 30*time.Second)
	}
	return nil
}

func (r *Reader) session(ctx context.Context, kind streamKind) error {
	endpoint := r.cfg.MarketURL
	if kind == userStream {
		endpoint = r.cfg.UserURL
	}
	if err := ValidateURL(endpoint, string(kind)); err != nil {
		return err
	}
	dialCtx, cancelDial := context.WithTimeout(ctx, r.dialTimeout)
	conn, err := r.cfg.Dial(dialCtx, endpoint, string(kind))
	cancelDial()
	if err != nil {
		return err
	}
	r.setConnected(kind, true)
	defer func() {
		r.setConnected(kind, false)
		_ = conn.CloseNow()
	}()
	if kind == userStream {
		if err := r.subscribe(ctx, conn); err != nil {
			return err
		}
	}
	sessionCtx, stop := context.WithCancel(ctx)
	pingFailure := make(chan struct{}, 1)
	pingDone := make(chan struct{})
	go func() {
		defer close(pingDone)
		r.pingLoop(sessionCtx, stop, conn, pingFailure)
	}()
	defer func() {
		stop()
		<-pingDone
	}()
	for sessionCtx.Err() == nil {
		msgType, raw, err := conn.Read(sessionCtx)
		if err != nil {
			select {
			case <-pingFailure:
				return errors.New("binance demo: stream ping failed")
			default:
			}
			return err
		}
		if msgType != websocket.MessageText {
			r.malformed()
			continue
		}
		if kind == marketStream {
			if err := r.handleMarket(raw); err != nil {
				return err
			}
		} else {
			continueSession, err := r.handleUser(raw)
			if err != nil {
				return err
			}
			if !continueSession {
				return errors.New("binance demo: server requested user stream shutdown")
			}
		}
	}
	return sessionCtx.Err()
}

func (r *Reader) pingLoop(ctx context.Context, stop context.CancelFunc, conn Socket, failed chan<- struct{}) {
	for ctx.Err() == nil {
		if err := r.cfg.Sleep(ctx, r.pingInterval); err != nil {
			return
		}
		pingCtx, cancel := context.WithTimeout(ctx, r.pingTimeout)
		err := conn.Ping(pingCtx)
		cancel()
		if err != nil {
			r.counters.lastError.Store("ping")
			select {
			case failed <- struct{}{}:
			default:
			}
			stop()
			return
		}
	}
}

func (r *Reader) subscribe(ctx context.Context, conn Socket) error {
	id := fmt.Sprintf("fillwire-%d", r.cfg.Now().UnixNano())
	timestamp := r.cfg.Now().UnixMilli()
	params := map[string]any{"apiKey": r.cfg.APIKey, "recvWindow": RecvWindowMS, "timestamp": timestamp}
	params["signature"] = Sign(params, r.cfg.APISecret)
	request, err := json.Marshal(map[string]any{"id": id, "method": SubscribeMethod, "params": params})
	if err != nil {
		return errors.New("binance demo: could not encode subscription")
	}
	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = conn.Write(writeCtx, websocket.MessageText, request)
	cancel()
	if err != nil {
		return errors.New("binance demo: subscription write failed")
	}
	ackCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	msgType, raw, err := conn.Read(ackCtx)
	if err != nil || msgType != websocket.MessageText {
		r.malformed()
		return errors.New("binance demo: invalid subscription acknowledgement")
	}
	var ack map[string]json.RawMessage
	if err := json.Unmarshal(raw, &ack); err != nil {
		r.malformed()
		return errors.New("binance demo: invalid subscription acknowledgement")
	}
	var responseID string
	var status int
	if json.Unmarshal(ack["id"], &responseID) != nil || responseID != id || json.Unmarshal(ack["status"], &status) != nil {
		r.malformed()
		return errors.New("binance demo: invalid subscription acknowledgement")
	}
	if status == http.StatusOK {
		return nil
	}
	code := 0
	var responseError map[string]json.RawMessage
	if json.Unmarshal(ack["error"], &responseError) == nil {
		_ = json.Unmarshal(responseError["code"], &code)
	}
	if err := r.record(Record{Kind: "subscribe_rejected", ReceivedAt: r.cfg.Now().UTC(), Status: status, Code: code}); err != nil {
		return err
	}
	r.counters.subscribeRejects.Add(1)
	return errors.New("binance demo: user stream subscription rejected")
}

func (r *Reader) handleMarket(raw []byte) error {
	streamName, symbol, err := validateMarketFrame(raw, r.cfg.Symbols)
	if err != nil {
		r.malformed()
		return nil
	}
	if err := r.record(Record{Kind: "market_frame", ReceivedAt: r.cfg.Now().UTC(), Stream: streamName, Symbol: symbol, Payload: append(json.RawMessage(nil), raw...)}); err != nil {
		return err
	}
	r.counters.marketFrames.Add(1)
	return nil
}

func (r *Reader) handleUser(raw []byte) (bool, error) {
	eventType, err := validateUserFrame(raw)
	if err != nil {
		r.malformed()
		return true, nil
	}
	if err := r.record(Record{Kind: "user_event", ReceivedAt: r.cfg.Now().UTC(), EventType: eventType, Payload: append(json.RawMessage(nil), raw...)}); err != nil {
		return true, err
	}
	r.counters.userEvents.Add(1)
	return eventType != "serverShutdown", nil
}

func (r *Reader) record(record Record) error {
	if err := r.append(record); err != nil {
		count := r.counters.recordErrors.Add(1)
		r.counters.lastError.Store("recording")
		r.cfg.Logger.Error("Binance demo recording append failed; stopping stream readers", "kind", record.Kind, "record_errors", count)
		return errRecording
	}
	return nil
}

func (r *Reader) malformed() {
	count := r.counters.malformedFrames.Add(1)
	if count&(count-1) == 0 {
		r.cfg.Logger.Warn("Binance demo malformed frame dropped", "malformed_frames", count)
	}
}

func (r *Reader) setConnected(kind streamKind, connected bool) {
	if kind == marketStream {
		r.counters.marketConnected.Store(connected)
	} else {
		r.counters.userConnected.Store(connected)
	}
}

func dial(ctx context.Context, endpoint, kind string) (Socket, error) {
	if err := ValidateURL(endpoint, kind); err != nil {
		return nil, err
	}
	client := &http.Client{
		Transport:     &http.Transport{Proxy: nil, DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	conn, _, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: client})
	if err != nil {
		return nil, errors.New("binance demo: websocket dial failed")
	}
	conn.SetReadLimit(maxFrameBytes)
	return conn, nil
}

func marketURL(symbols []string) string {
	streams := make([]string, 0, len(symbols)*3)
	for _, symbol := range symbols {
		lower := strings.ToLower(symbol)
		streams = append(streams, lower+"@depth20", lower+"@bookTicker", lower+"@aggTrade")
	}
	return MarketURL + "?streams=" + strings.Join(streams, "/")
}

func Sign(params map[string]any, secret string) string {
	keys := make([]string, 0, len(params))
	for key := range params {
		if key != "signature" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	var payload strings.Builder
	for i, key := range keys {
		if i > 0 {
			payload.WriteByte('&')
		}
		fmt.Fprintf(&payload, "%s=%v", key, params[key])
	}
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write([]byte(payload.String()))
	return hex.EncodeToString(h.Sum(nil))
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
