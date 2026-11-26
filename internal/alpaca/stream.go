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
)

var ErrLiveTradingEndpoint = errors.New("alpaca: refusing non-paper trading stream endpoint")

type Socket interface {
	Read(context.Context) (websocket.MessageType, []byte, error)
	Write(context.Context, websocket.MessageType, []byte) error
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
	var workers atomic.Int32
	workers.Store(2)
	errs := make(chan error, 2)
	go func() { errs <- m.reconnect(ctx, "trade_updates", m.cfg.TradeEndpoint, m.tradeKey, m.tradeSec) }()
	go func() { errs <- m.reconnect(ctx, "market_data", m.cfg.MarketEndpoint, m.dataKey, m.dataSec) }()
	for i := int32(0); i < workers.Load(); i++ {
		if err := <-errs; ctx.Err() == nil && err != nil {
			m.cfg.Logger.Error("alpaca stream stopped", "reason", err)
		}
	}
	return ctx.Err()
}

func (m *Module) reconnect(ctx context.Context, stream, endpoint, key, secret string) error {
	delay := m.cfg.RetryMin
	for ctx.Err() == nil {
		if err := m.session(ctx, stream, endpoint, key, secret); ctx.Err() != nil {
			return ctx.Err()
		} else if err != nil {
			m.cfg.Logger.Warn("alpaca stream reconnecting", "stream", stream, "reason", err.Error(), "malformed_frames", m.malformed.Load())
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay = min(2*delay, m.cfg.RetryMax)
	}
	return ctx.Err()
}

func (m *Module) session(ctx context.Context, stream, endpoint, key, secret string) error {
	if stream == "trade_updates" && !paperEndpoint(endpoint) {
		return ErrLiveTradingEndpoint
	}
	headers := make(http.Header)
	if stream == "trade_updates" {
		headers.Set("Content-Type", "application/json")
	}
	conn, err := m.cfg.Dial(ctx, endpoint, headers)
	if err != nil {
		return errors.New("alpaca: websocket dial failed")
	}
	defer conn.CloseNow()
	if err := writeJSON(ctx, conn, map[string]string{"action": "auth", "key": key, "secret": secret}); err != nil {
		return errors.New("alpaca: auth write failed")
	}
	if err := m.readControl(ctx, conn, "authorization"); err != nil {
		return err
	}
	var listen any
	if stream == "trade_updates" {
		listen = map[string]any{"action": "listen", "data": map[string]any{"streams": []string{"trade_updates"}}}
	} else {
		listen = map[string]any{"action": "subscribe", "trades": m.cfg.Symbols, "quotes": m.cfg.Symbols, "bars": m.cfg.Symbols}
	}
	if err := writeJSON(ctx, conn, listen); err != nil {
		return errors.New("alpaca: subscription write failed")
	}
	if err := m.readControl(ctx, conn, "listening"); err != nil {
		return err
	}
	ready := &m.marketReady
	if stream == "trade_updates" {
		ready = &m.tradeReady
	}
	ready.Store(true)
	defer ready.Store(false)
	for {
		kind, raw, err := conn.Read(ctx)
		if err != nil {
			return errors.New("alpaca: websocket disconnected")
		}
		at := m.cfg.Now().UTC()
		if (kind != websocket.MessageText && kind != websocket.MessageBinary) || !validEventFrame(stream, raw, m.cfg.Symbols) {
			m.malformed.Add(1)
			continue
		}
		name := "market_data"
		if stream == "trade_updates" {
			name = "trade_updates"
		}
		if err := m.recorder.Append(streamName(stream), name, at, Record{Kind: eventKind(stream, raw), ReceivedAt: at, Stream: stream, Data: append(json.RawMessage(nil), raw...), Raw: append(json.RawMessage(nil), raw...)}); err != nil {
			return errors.New("alpaca: recording append failed")
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
		for _, frame := range frames {
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

func validEventFrame(stream string, raw []byte, symbols []string) bool {
	frames, ok := decodeFrames(raw)
	if !ok || len(frames) == 0 {
		return false
	}
	for _, frame := range frames {
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
			if json.Unmarshal(frame["data"], &data) != nil || data.Event == "" || len(data.Order) == 0 {
				return false
			}
			continue
		}
		if json.Unmarshal(frame["T"], &typ) != nil || (typ != "t" && typ != "q" && typ != "b") {
			return false
		}
		if json.Unmarshal(frame["S"], &symbol) != nil || !contains(symbols, symbol) {
			return false
		}
	}
	return true
}

func eventKind(stream string, raw []byte) string {
	frames, _ := decodeFrames(raw)
	if len(frames) == 0 {
		return ""
	}
	if stream == "trade_updates" {
		var frame struct {
			Data struct {
				Event string `json:"event"`
			} `json:"data"`
		}
		_ = json.Unmarshal(mustJSON(frames[0]), &frame)
		return frame.Data.Event
	}
	var typ string
	_ = json.Unmarshal(frames[0]["T"], &typ)
	switch typ {
	case "t":
		return "trade"
	case "q":
		return "quote"
	case "b":
		return "bar"
	default:
		return ""
	}
}

func decodeFrames(raw []byte) ([]map[string]json.RawMessage, bool) {
	var frames []map[string]json.RawMessage
	if json.Unmarshal(raw, &frames) == nil && len(frames) > 0 {
		return frames, true
	}
	var frame map[string]json.RawMessage
	if json.Unmarshal(raw, &frame) == nil && len(frame) > 0 {
		return []map[string]json.RawMessage{frame}, true
	}
	return nil, false
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
	conn, _, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		return nil, fmt.Errorf("alpaca: websocket dial failed")
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
