// Package alpaca records Alpaca paper trade updates and IEX market data.
// It has no order, ledger, or broker client dependencies.
package alpaca

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/mgh3326/fillwire/internal/recording"
)

const (
	PaperTradingEndpoint = "wss://paper-api.alpaca.markets/stream"
	IEXEndpoint          = "wss://stream.data.alpaca.markets/v2/iex"
	TradeStream          = "alpaca-trade-updates"
	MarketStream         = "alpaca-iex"
	readLimit            = 1 << 20
	defaultPingInterval  = 30 * time.Second
	defaultPingTimeout   = 10 * time.Second
	defaultStableSession = time.Minute
)

var (
	ErrLiveTradingEndpoint = errors.New("alpaca: refusing non-paper trading stream endpoint")
	ErrRecordingAppend     = errors.New("alpaca: recording append failed")
	errPing                = errors.New("alpaca: websocket ping failed")
)

type Socket interface {
	Read(context.Context) (websocket.MessageType, []byte, error)
	Write(context.Context, websocket.MessageType, []byte) error
	Ping(context.Context) error
	CloseNow() error
}

type Dialer func(context.Context, string, http.Header) (Socket, error)

type Record struct {
	Kind       string          `json:"kind"`
	ReceivedAt time.Time       `json:"received_at"`
	Stream     string          `json:"stream"`
	Data       json.RawMessage `json:"data,omitempty"`
	Raw        json.RawMessage `json:"raw,omitempty"`
}

type Config struct {
	PaperKeyEnv    string
	PaperSecretEnv string
	DataKeyEnv     string
	DataSecretEnv  string
	Symbols        []string
	Root           string
	TradeEndpoint  string
	MarketEndpoint string
	Dial           Dialer
	Now            func() time.Time
	RetryMin       time.Duration
	RetryMax       time.Duration
	PingInterval   time.Duration
	PingTimeout    time.Duration
	StableSession  time.Duration
	Logger         *slog.Logger
}

type Module struct {
	cfg         Config
	tradeKey    string
	tradeSec    string
	dataKey     string
	dataSec     string
	recorder    *recording.Sink
	tradeReady  atomic.Bool
	marketReady atomic.Bool
	running     atomic.Bool
	malformed   atomic.Uint64
	closeOnce   sync.Once
}

func New(cfg Config) (*Module, error) {
	if strings.TrimSpace(cfg.Root) == "" || len(cfg.Symbols) == 0 {
		return nil, errors.New("alpaca: recording root and at least one symbol are required")
	}
	seen := make(map[string]bool, len(cfg.Symbols))
	for _, symbol := range cfg.Symbols {
		if !validSymbol(symbol) || seen[symbol] {
			return nil, errors.New("alpaca: invalid or duplicate symbol")
		}
		seen[symbol] = true
	}
	if cfg.PaperKeyEnv == "" || cfg.PaperSecretEnv == "" || cfg.DataKeyEnv == "" || cfg.DataSecretEnv == "" {
		return nil, errors.New("alpaca: credential environment variable names are required")
	}
	tradeKey, ok1 := os.LookupEnv(cfg.PaperKeyEnv)
	tradeSec, ok2 := os.LookupEnv(cfg.PaperSecretEnv)
	dataKey, ok3 := os.LookupEnv(cfg.DataKeyEnv)
	dataSec, ok4 := os.LookupEnv(cfg.DataSecretEnv)
	if !ok1 || !ok2 || !ok3 || !ok4 || tradeKey == "" || tradeSec == "" || dataKey == "" || dataSec == "" {
		return nil, errors.New("alpaca: required credential environment variable is unset")
	}
	if cfg.TradeEndpoint == "" {
		cfg.TradeEndpoint = PaperTradingEndpoint
	}
	if cfg.MarketEndpoint == "" {
		cfg.MarketEndpoint = IEXEndpoint
	}
	if cfg.Dial == nil {
		cfg.Dial = dial
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.RetryMin <= 0 {
		cfg.RetryMin = time.Second
	}
	if cfg.RetryMax < cfg.RetryMin {
		cfg.RetryMax = 30 * time.Second
	}
	if cfg.PingInterval <= 0 {
		cfg.PingInterval = defaultPingInterval
	}
	if cfg.PingTimeout <= 0 {
		cfg.PingTimeout = defaultPingTimeout
	}
	if cfg.StableSession <= 0 {
		cfg.StableSession = defaultStableSession
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	recorder, err := recording.NewSink(cfg.Root, TradeStream, MarketStream)
	if err != nil {
		return nil, err
	}
	return &Module{cfg: cfg, tradeKey: tradeKey, tradeSec: tradeSec, dataKey: dataKey, dataSec: dataSec, recorder: recorder}, nil
}

func validSymbol(symbol string) bool {
	if symbol == "" || len(symbol) > 16 {
		return false
	}
	for _, r := range symbol {
		if !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') && r != '.' && r != '-' {
			return false
		}
	}
	return true
}

func (m *Module) Name() string { return "alpaca" }
func (m *Module) Health() bool { return m.tradeReady.Load() && m.marketReady.Load() }

func (m *Module) Run(ctx context.Context) error {
	if !m.running.CompareAndSwap(false, true) {
		return errors.New("alpaca: module is already running")
	}
	defer m.running.Store(false)
	defer m.tradeReady.Store(false)
	defer m.marketReady.Store(false)
	defer m.closeWriters()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, 2)
	go func() { errs <- m.reconnect(runCtx, "trade_updates", m.cfg.TradeEndpoint, m.tradeKey, m.tradeSec) }()
	go func() { errs <- m.reconnect(runCtx, "market_data", m.cfg.MarketEndpoint, m.dataKey, m.dataSec) }()
	var fatal error
	for range 2 {
		if err := <-errs; err != nil && ctx.Err() == nil {
			if fatal == nil {
				fatal = err
				cancel()
			}
		}
	}
	if fatal != nil {
		return fatal
	}
	return ctx.Err()
}

func (m *Module) reconnect(ctx context.Context, stream, endpoint, key, secret string) error {
	delay := m.cfg.RetryMin
	for ctx.Err() == nil {
		healthyFor, err := m.sessionHealth(ctx, stream, endpoint, key, secret)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		delay = nextReconnectDelay(delay, healthyFor, m.cfg.StableSession, m.cfg.RetryMin, m.cfg.RetryMax)
		if err != nil {
			if errors.Is(err, ErrRecordingAppend) || errors.Is(err, ErrLiveTradingEndpoint) {
				return err
			}
			m.cfg.Logger.Warn("alpaca stream reconnecting", "stream", stream, "reason", err.Error(), "malformed_frames", m.malformed.Load())
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

func nextReconnectDelay(delay, healthyFor, stableFor, retryMin, retryMax time.Duration) time.Duration {
	if healthyFor >= stableFor {
		return retryMin
	}
	return min(2*delay, retryMax)
}

func (m *Module) session(ctx context.Context, stream, endpoint, key, secret string) error {
	_, err := m.sessionHealth(ctx, stream, endpoint, key, secret)
	return err
}

func (m *Module) sessionHealth(ctx context.Context, stream, endpoint, key, secret string) (healthyFor time.Duration, resultErr error) {
	if stream == "trade_updates" && !paperEndpoint(endpoint) {
		return 0, ErrLiveTradingEndpoint
	}
	headers := make(http.Header)
	if stream == "trade_updates" {
		headers.Set("Content-Type", "application/json")
	}
	conn, err := m.cfg.Dial(ctx, endpoint, headers)
	if err != nil {
		return 0, errors.New("alpaca: websocket dial failed")
	}
	defer conn.CloseNow()
	if err := writeJSON(ctx, conn, map[string]string{"action": "auth", "key": key, "secret": secret}); err != nil {
		return 0, errors.New("alpaca: auth write failed")
	}
	if err := m.readControl(ctx, conn, "authorization"); err != nil {
		return 0, err
	}
	var listen any
	if stream == "trade_updates" {
		listen = map[string]any{"action": "listen", "data": map[string]any{"streams": []string{"trade_updates"}}}
	} else {
		listen = map[string]any{"action": "subscribe", "trades": m.cfg.Symbols, "quotes": m.cfg.Symbols, "bars": m.cfg.Symbols}
	}
	if err := writeJSON(ctx, conn, listen); err != nil {
		return 0, errors.New("alpaca: subscription write failed")
	}
	if err := m.readControl(ctx, conn, "listening"); err != nil {
		return 0, err
	}
	ready := &m.marketReady
	if stream == "trade_updates" {
		ready = &m.tradeReady
	}
	ready.Store(true)
	readyAt := time.Now()
	defer func() {
		healthyFor = time.Since(readyAt)
		ready.Store(false)
	}()
	sessionCtx, cancel := context.WithCancel(ctx)
	pingErrors := make(chan error, 1)
	pingDone := make(chan struct{})
	go func() {
		defer close(pingDone)
		m.keepalive(sessionCtx, cancel, conn, pingErrors)
	}()
	defer func() {
		cancel()
		<-pingDone
	}()
	for {
		kind, raw, err := conn.Read(sessionCtx)
		if err != nil {
			select {
			case pingErr := <-pingErrors:
				return 0, pingErr
			default:
				return 0, errors.New("alpaca: websocket disconnected")
			}
		}
		at := m.cfg.Now().UTC()
		frames, ok := decodeFrames(raw)
		if (kind != websocket.MessageText && kind != websocket.MessageBinary) || !ok || len(frames) == 0 {
			m.malformed.Add(1)
			continue
		}
		for _, frame := range frames {
			if !frame.valid || !validEventFrame(stream, frame.fields, m.cfg.Symbols) {
				m.malformed.Add(1)
				continue
			}
			name := "market_data"
			if stream == "trade_updates" {
				name = "trade_updates"
			}
			frameRaw := append(json.RawMessage(nil), frame.raw...)
			if err := m.recorder.Append(streamName(stream), name, at, Record{Kind: eventKind(stream, frame.fields), ReceivedAt: at, Stream: stream, Data: frameRaw, Raw: append(json.RawMessage(nil), frameRaw...)}); err != nil {
				return 0, fmt.Errorf("%w: %w", ErrRecordingAppend, err)
			}
		}
	}
}

func (m *Module) keepalive(ctx context.Context, cancel context.CancelFunc, conn Socket, failures chan<- error) {
	ticker := time.NewTicker(m.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, stop := context.WithTimeout(ctx, m.cfg.PingTimeout)
			err := conn.Ping(pingCtx)
			stop()
			if err != nil {
				select {
				case failures <- errors.Join(errPing, err):
				default:
				}
				cancel()
				return
			}
		}
	}
}

func streamName(stream string) string {
	if stream == "trade_updates" {
		return TradeStream
	}
	return MarketStream
}

func paperEndpoint(raw string) bool { return raw == PaperTradingEndpoint }

func writeJSON(ctx context.Context, conn Socket, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, payload)
}

func (m *Module) readControl(ctx context.Context, conn Socket, expected string) error {
	controlCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		kind, raw, err := conn.Read(controlCtx)
		if err != nil {
			return errors.New("alpaca: control response unavailable")
		}
		frames, ok := decodeFrames(raw)
		if (kind != websocket.MessageText && kind != websocket.MessageBinary) || !ok || len(frames) == 0 {
			m.malformed.Add(1)
			continue
		}
		for _, part := range frames {
			if !part.valid {
				m.malformed.Add(1)
				continue
			}
			frame := part.fields
			var typ string
			_ = json.Unmarshal(frame["stream"], &typ)
			var marketType, message string
			_ = json.Unmarshal(frame["T"], &marketType)
			_ = json.Unmarshal(frame["msg"], &message)
			if typ == "error" || marketType == "error" {
				return errors.New("alpaca: server rejected stream setup")
			}
			if expected == "authorization" && typ == expected {
				var data struct {
					Status string `json:"status"`
				}
				if json.Unmarshal(frame["data"], &data) != nil || data.Status != "authorized" {
					return errors.New("alpaca: authorization rejected")
				}
				return nil
			}
			if expected == "listening" && typ == expected {
				return nil
			}
			if expected == "authorization" && marketType == "success" && message == "authenticated" {
				return nil
			}
			if expected == "listening" && marketType == "subscription" {
				return nil
			}
		}
	}
}

func validEventFrame(stream string, frame map[string]json.RawMessage, symbols []string) bool {
	var typ, symbol string
	_ = json.Unmarshal(frame["stream"], &typ)
	if stream == "trade_updates" {
		if typ != "trade_updates" {
			return false
		}
		var data struct {
			Event string          `json:"event"`
			Order json.RawMessage `json:"order"`
		}
		return json.Unmarshal(frame["data"], &data) == nil && data.Event != "" && len(data.Order) > 0
	}
	if json.Unmarshal(frame["T"], &typ) != nil || !contains([]string{"t", "q", "b", "c", "x"}, typ) {
		return false
	}
	return json.Unmarshal(frame["S"], &symbol) == nil && contains(symbols, symbol)
}

func eventKind(stream string, frame map[string]json.RawMessage) string {
	if stream == "trade_updates" {
		var update struct {
			Data struct {
				Event string `json:"event"`
			} `json:"data"`
		}
		_ = json.Unmarshal(mustJSON(frame), &update)
		return update.Data.Event
	}
	var typ string
	_ = json.Unmarshal(frame["T"], &typ)
	switch typ {
	case "t":
		return "trade"
	case "q":
		return "quote"
	case "b":
		return "bar"
	case "c":
		return "correction"
	case "x":
		return "cancel_error"
	default:
		return ""
	}
}

type decodedFrame struct {
	raw    json.RawMessage
	fields map[string]json.RawMessage
	valid  bool
}

func decodeFrames(raw []byte) ([]decodedFrame, bool) {
	var batch []json.RawMessage
	if err := json.Unmarshal(raw, &batch); err == nil && batch != nil {
		frames := make([]decodedFrame, 0, len(batch))
		for _, item := range batch {
			frames = append(frames, decodeFrame(item))
		}
		return frames, true
	}
	if !json.Valid(raw) {
		return nil, false
	}
	return []decodedFrame{decodeFrame(raw)}, true
}

func decodeFrame(raw json.RawMessage) decodedFrame {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) == 0 {
		return decodedFrame{raw: append(json.RawMessage(nil), raw...)}
	}
	return decodedFrame{raw: append(json.RawMessage(nil), raw...), fields: fields, valid: true}
}

func mustJSON(value any) []byte { b, _ := json.Marshal(value); return b }

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

func dial(ctx context.Context, endpoint string, headers http.Header) (Socket, error) {
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	conn, _, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: client, HTTPHeader: headers})
	if err != nil {
		return nil, errors.New("alpaca: websocket dial failed")
	}
	conn.SetReadLimit(readLimit)
	return conn, nil
}

func (m *Module) closeWriters() {
	m.closeOnce.Do(func() {
		if err := m.recorder.Close(); err != nil {
			m.cfg.Logger.Error("alpaca recorder close failed", "reason", err.Error())
		}
	})
}
