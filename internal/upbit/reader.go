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
	Logger             *slog.Logger
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
	cfg       Config
	writers   map[string]*recording.DailyWriter
	active    map[string]time.Time
	intervals map[string][]recording.Interval
}

func New(cfg Config) (*Reader, error) {
	if len(cfg.Markets) < 1 || len(cfg.Markets) > 20 {
		return nil, errors.New("upbit: 마켓은 1~20개여야 합니다")
	}
	seen := map[string]bool{}
	for _, m := range cfg.Markets {
		if !strings.HasPrefix(m, "KRW-") || len(m) < 5 || seen[m] {
			return nil, fmt.Errorf("upbit: 잘못되었거나 중복된 KRW 마켓입니다: %q", m)
		}
		seen[m] = true
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
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	r := &Reader{cfg: cfg, writers: map[string]*recording.DailyWriter{}, active: map[string]time.Time{}, intervals: map[string][]recording.Interval{}}
	for _, m := range cfg.Markets {
		w, err := recording.NewDailyWriter(cfg.Root, "upbit", m)
		if err != nil {
			return nil, err
		}
		r.writers[m] = w
	}
	return r, nil
}

func (r *Reader) Run(ctx context.Context) error {
	defer func() {
		now := r.cfg.Clock.Now().UTC()
		_ = r.recordAll(Record{Kind: "gap", ReceivedAt: now, Reason: "reader stopped"})
		r.endAll(now)
		r.closeWriters()
	}()
	delay := r.cfg.RetryMin
	for ctx.Err() == nil {
		err := r.session(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		now := r.cfg.Clock.Now().UTC()
		if gapErr := r.recordAll(Record{Kind: "gap", ReceivedAt: now, Reason: err.Error()}); gapErr != nil {
			return gapErr
		}
		r.endAll(now)
		if err == nil {
			err = ErrDisconnected
		}
		r.cfg.Logger.Warn("upbit stream disconnected; reconnecting", "reason", err.Error(), "retry_in", delay.String())
		if e := r.cfg.Clock.Sleep(ctx, delay); e != nil {
			return e
		}
		delay = min(delay*2, r.cfg.RetryMax)
	}
	return ctx.Err()
}
func (r *Reader) session(ctx context.Context) error {
	c, err := r.cfg.Dialer.Dial(ctx, r.cfg.Endpoint)
	if err != nil {
		return err
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
		return err
	}
	for _, m := range r.cfg.Markets {
		r.active[m] = r.cfg.Clock.Now().UTC()
		_ = r.append(m, Record{Kind: "connected", Market: m, ReceivedAt: r.cfg.Clock.Now().UTC()})
	}
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			return err
		}
		now := r.cfg.Clock.Now().UTC()
		var f wireFrame
		if json.Unmarshal(data, &f) != nil {
			if gapErr := r.recordAll(Record{Kind: "gap", ReceivedAt: now, Reason: "malformed Upbit frame"}); gapErr != nil {
				return gapErr
			}
			return errors.New("upbit: 웹소켓 프레임 형식이 잘못되었습니다")
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
			return err
		}
	}
}
func (r *Reader) append(m string, v Record) error {
	w := r.writers[m]
	if err := w.Append(v.ReceivedAt, v); err != nil {
		return err
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
func (r *Reader) endAll(at time.Time) {
	for m, start := range r.active {
		r.intervals[m] = append(r.intervals[m], recording.Interval{Start: start, End: at})
		delete(r.active, m)
	}
}
func (r *Reader) closeWriters() {
	for _, w := range r.writers {
		_ = w.Close()
	}
}

// Coverage returns the collected intervals plus a currently active interval clipped at now.
func (r *Reader) Coverage(market, day string, now time.Time) (recording.Coverage, error) {
	intervals := append([]recording.Interval(nil), r.intervals[market]...)
	if s, ok := r.active[market]; ok {
		intervals = append(intervals, recording.Interval{Start: s, End: now})
	}
	return recording.SummarizeCoverage(market, day, intervals)
}
