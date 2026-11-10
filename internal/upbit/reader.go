// Package upbit reads Upbit's public market websocket without authentication.
package upbit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/mgh3326/fillwire/internal/recording"
)

const Endpoint = "wss://api.upbit.com/websocket/v1"

var ErrDisconnected = errors.New("upbit: 웹소켓 연결이 끊어졌습니다")

type Dialer interface {
	Dial(context.Context, string) (*websocket.Conn, error)
}
type dialer struct{}

func (dialer) Dial(ctx context.Context, endpoint string) (*websocket.Conn, error) {
	c, _, err := websocket.Dial(ctx, endpoint, nil)
	return c, err
}

type Clock interface {
	Now() time.Time
	Sleep(context.Context, time.Duration) error
}
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type Config struct {
	Markets            []string
	Root, Endpoint     string
	Dialer             Dialer
	Clock              Clock
	RetryMin, RetryMax time.Duration
	PingInterval       time.Duration
	PongTimeout        time.Duration
	Logger             *slog.Logger
	OnRecord           func(Record)
}
type wireFrame struct {
	Type         string `json:"type"`
	Code         string `json:"code"`
	Timestamp    int64  `json:"timestamp"`
	SequentialID int64  `json:"sequential_id,omitempty"`
}
type Record struct {
	Kind              string          `json:"kind"`
	Market            string          `json:"market"`
	ExchangeTimestamp int64           `json:"exchange_timestamp,omitempty"`
	ReceivedAt        time.Time       `json:"received_at"`
	Sequence          *int64          `json:"sequential_id,omitempty"`
	Raw               json.RawMessage `json:"raw,omitempty"`
	Reason            string          `json:"reason,omitempty"`
}
type Reader struct {
	cfg     Config
	writers map[string]*recording.DailyWriter
	lock    *recording.StreamLock
}

func ValidateMarkets(markets []string) error {
	if len(markets) < 1 || len(markets) > 20 {
		return errors.New("upbit: 마켓은 1~20개여야 합니다")
	}
	seen := map[string]bool{}
	for _, m := range markets {
		if !strings.HasPrefix(m, "KRW-") || len(m) < 6 || len(m) > 24 || seen[m] {
			return fmt.Errorf("upbit: 잘못되었거나 중복된 KRW 마켓입니다: %q", m)
		}
		for _, c := range m[4:] {
			if !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') {
				return fmt.Errorf("upbit: 잘못되었거나 중복된 KRW 마켓입니다: %q", m)
			}
		}
		seen[m] = true
	}
	return nil
}

func New(cfg Config) (*Reader, error) {
	if err := ValidateMarkets(cfg.Markets); err != nil {
		return nil, err
	}
	if cfg.Root == "" {
		return nil, errors.New("upbit: 출력 디렉터리가 필요합니다")
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = Endpoint
	}
	if cfg.Dialer == nil {
		cfg.Dialer = dialer{}
	}
	if cfg.Clock == nil {
		cfg.Clock = realClock{}
	}
	if cfg.RetryMin <= 0 {
		cfg.RetryMin = time.Second
	}
	if cfg.RetryMax < cfg.RetryMin {
		cfg.RetryMax = 30 * time.Second
	}
	if cfg.PingInterval <= 0 {
		cfg.PingInterval = 30 * time.Second
	}
	if cfg.PongTimeout <= 0 {
		cfg.PongTimeout = 10 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	lock, err := recording.AcquireStreamLock(cfg.Root, "upbit")
	if err != nil {
		return nil, err
	}
	r := &Reader{cfg: cfg, writers: map[string]*recording.DailyWriter{}, lock: lock}
	for _, m := range cfg.Markets {
		w, err := recording.NewDailyWriter(cfg.Root, "upbit", m)
		if err != nil {
			_ = r.closeWriters()
			return nil, err
		}
		r.writers[m] = w
	}
	return r, nil
}

func (r *Reader) Run(ctx context.Context) error {
	defer func() {
		now := r.cfg.Clock.Now().UTC()
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.writeGapWithRetry(closeCtx, now, "reader stopped"); err != nil {
			r.cfg.Logger.Error("Upbit 중지 gap 기록에 실패했습니다", "reason", err.Error())
		}
		if err := r.closeWriters(); err != nil {
			r.cfg.Logger.Error("Upbit 기록 파일 닫기에 실패했습니다", "reason", err.Error())
		}
	}()
	delay := r.cfg.RetryMin
	for ctx.Err() == nil {
		started := r.cfg.Clock.Now()
		gotFrame, err := r.session(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		now := r.cfg.Clock.Now().UTC()
		if gapErr := r.writeGapWithRetry(ctx, now, err.Error()); gapErr != nil {
			return gapErr
		}
		if err == nil {
			err = ErrDisconnected
		}
		if gotFrame || now.Sub(started) >= time.Minute {
			delay = r.cfg.RetryMin
		}
		r.cfg.Logger.Warn("upbit stream disconnected; reconnecting", "reason", err.Error(), "retry_in", delay.String())
		if e := r.cfg.Clock.Sleep(ctx, delay); e != nil {
			return e
		}
		delay = min(delay*2, r.cfg.RetryMax)
	}
	return ctx.Err()
}
func (r *Reader) session(ctx context.Context) (bool, error) {
	c, err := r.cfg.Dialer.Dial(ctx, r.cfg.Endpoint)
	if err != nil {
		return false, err
	}
	defer c.Close(websocket.StatusNormalClosure, "")
	req := make([]any, 0, 4)
	req = append(req, map[string]string{"ticket": "fillwire-upbit"})
	orderbookCodes := make([]string, len(r.cfg.Markets))
	copy(orderbookCodes, r.cfg.Markets)
	for i := range orderbookCodes {
		orderbookCodes[i] += ".15"
	}
	req = append(req, map[string]any{"type": "orderbook", "codes": orderbookCodes})
	req = append(req, map[string]any{"type": "trade", "codes": r.cfg.Markets})
	req = append(req, map[string]string{"format": "DEFAULT"})
	b, _ := json.Marshal(req)
	if err := c.Write(ctx, websocket.MessageText, b); err != nil {
		return false, err
	}
	for _, m := range r.cfg.Markets {
		now := r.cfg.Clock.Now().UTC()
		if err := r.append(m, Record{Kind: "connected", Market: m, ReceivedAt: now}); err != nil {
			return false, err
		}
	}
	sessionCtx, cancelSession := context.WithCancel(ctx)
	defer cancelSession()
	pingFailure := make(chan error, 1)
	go r.pingLoop(sessionCtx, cancelSession, c, pingFailure)
	gotFrame := false
	for {
		_, data, err := c.Read(sessionCtx)
		if err != nil {
			select {
			case pingErr := <-pingFailure:
				return gotFrame, pingErr
			default:
				return gotFrame, err
			}
		}
		now := r.cfg.Clock.Now().UTC()
		var f wireFrame
		if json.Unmarshal(data, &f) != nil {
			return gotFrame, errors.New("upbit: 웹소켓 프레임 형식이 잘못되었습니다")
		}
		m := strings.SplitN(f.Code, ".", 2)[0]
		if _, ok := r.writers[m]; !ok {
			continue
		}
		var seq *int64
		if f.SequentialID != 0 {
			v := f.SequentialID
			seq = &v
		}
		kind := f.Type
		if kind != "orderbook" && kind != "trade" {
			kind = "unknown"
		}
		if err := r.append(m, Record{Kind: kind, Market: m, ExchangeTimestamp: f.Timestamp, ReceivedAt: now, Sequence: seq, Raw: append(json.RawMessage(nil), data...)}); err != nil {
			return gotFrame, err
		}
		gotFrame = true
	}
}

func (r *Reader) pingLoop(ctx context.Context, cancel context.CancelFunc, c *websocket.Conn, failures chan<- error) {
	ticker := time.NewTicker(r.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, stop := context.WithTimeout(ctx, r.cfg.PongTimeout)
			err := c.Ping(pingCtx)
			stop()
			if err == nil {
				err = r.recordAll(Record{Kind: "heartbeat", ReceivedAt: r.cfg.Clock.Now().UTC()})
			}
			if err != nil {
				select {
				case failures <- err:
				default:
				}
				cancel()
				return
			}
		}
	}
}

func (r *Reader) writeGapWithRetry(ctx context.Context, at time.Time, reason string) error {
	delay := r.cfg.RetryMin
	pending := append([]string(nil), r.cfg.Markets...)
	for ctx.Err() == nil {
		remaining := pending[:0]
		var errs []error
		for _, market := range pending {
			if err := r.append(market, Record{Kind: "gap", Market: market, ReceivedAt: at.UTC(), Reason: reason}); err != nil {
				remaining = append(remaining, market)
				errs = append(errs, err)
			}
		}
		pending = remaining
		if len(pending) == 0 {
			return nil
		} else {
			r.cfg.Logger.Error("Upbit gap 기록 실패, 다시 시도합니다", "markets", pending, "reason", errors.Join(errs...).Error(), "retry_in", delay.String())
		}
		if err := r.cfg.Clock.Sleep(ctx, delay); err != nil {
			return err
		}
		delay = min(delay*2, r.cfg.RetryMax)
	}
	return ctx.Err()
}
func (r *Reader) append(m string, v Record) error {
	w := r.writers[m]
	if err := w.Append(v.ReceivedAt, v); err != nil {
		return err
	}
	if r.cfg.OnRecord != nil {
		r.cfg.OnRecord(v)
	}
	return nil
}
func (r *Reader) recordAll(v Record) error {
	var errs []error
	for _, m := range r.cfg.Markets {
		v.Market = m
		if err := r.append(m, v); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
func (r *Reader) closeWriters() error {
	var errs []error
	for _, w := range r.writers {
		if err := w.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := r.lock.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
