package quote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// laneClock runs at real speed from a settable instant and wakes every
// sleeper within a few milliseconds, so window logic runs on test time
// without long real waits.
type laneClock struct {
	mu      sync.Mutex
	at      time.Time
	setReal time.Time
}

func newLaneClock(at time.Time) *laneClock {
	clock := &laneClock{}
	clock.set(at)
	return clock
}
func (c *laneClock) set(at time.Time) {
	c.mu.Lock()
	c.at, c.setReal = at, time.Now()
	c.mu.Unlock()
}
func (c *laneClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at.Add(time.Since(c.setReal))
}
func (c *laneClock) After(d time.Duration) <-chan time.Time {
	return time.After(min(d, 2*time.Millisecond))
}

// fakeConn is one fake Toss websocket.
type fakeConn struct {
	token   string
	in      chan []byte
	closed  chan struct{}
	once    sync.Once
	onClose func()
	mu      sync.Mutex
	writes  [][]byte
	onWrite func(*fakeConn, []byte)
	panicOn string
}

func (c *fakeConn) Read(ctx context.Context) ([]byte, error) {
	if c.panicOn == "read" {
		panic("fixture read panic")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, io.EOF
	case frame := <-c.in:
		return frame, nil
	}
}

func (c *fakeConn) Write(_ context.Context, data []byte) error {
	select {
	case <-c.closed:
		return io.ErrClosedPipe
	default:
	}
	c.mu.Lock()
	c.writes = append(c.writes, append([]byte(nil), data...))
	c.mu.Unlock()
	if c.onWrite != nil {
		c.onWrite(c, data)
	}
	return nil
}

func (c *fakeConn) Close() error {
	c.once.Do(func() {
		close(c.closed)
		if c.onClose != nil {
			c.onClose()
		}
	})
	return nil
}

func (c *fakeConn) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

func (c *fakeConn) push(frame []byte) {
	select {
	case c.in <- frame:
	case <-c.closed:
	}
}

func (c *fakeConn) sent() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.writes...)
}

func (c *fakeConn) count(prefix string) int {
	n := 0
	for _, write := range c.sent() {
		if bytes.HasPrefix(write, []byte(prefix)) {
			n++
		}
	}
	return n
}

// ackAll answers a declaration by subscribing every requested key.
func ackAll(c *fakeConn, data []byte) {
	if !bytes.HasPrefix(data, []byte("[")) {
		return
	}
	var declared []struct {
		Type  string   `json:"type"`
		Codes []string `json:"codes"`
	}
	if json.Unmarshal(data, &declared) != nil {
		return
	}
	var subscribed []string
	for _, entry := range declared {
		for _, code := range entry.Codes {
			subscribed = append(subscribed, entry.Type+":"+code)
		}
	}
	raw, _ := json.Marshal(map[string]any{"type": "subscriptions", "id": "fillwire-quotes", "subscribed": subscribed, "rejected": []any{}})
	go c.push(raw)
}

type fakeDialer struct {
	mu        sync.Mutex
	conns     []*fakeConn
	tokens    []string
	endpoints []string
	open      atomic.Int32
	maxOpen   atomic.Int32
	dialed    chan *fakeConn
	onWrite   func(*fakeConn, []byte)
	refuse    func(token string) error
	panics    bool
	panicOn   string
}

func newFakeDialer(onWrite func(*fakeConn, []byte)) *fakeDialer {
	return &fakeDialer{onWrite: onWrite, dialed: make(chan *fakeConn, 1024)}
}

func (d *fakeDialer) Dial(_ context.Context, endpoint, token string) (Transport, error) {
	if d.panics {
		panic("fixture dialer panic")
	}
	d.mu.Lock()
	d.tokens = append(d.tokens, token)
	d.endpoints = append(d.endpoints, endpoint)
	refuse := d.refuse
	d.mu.Unlock()
	if refuse != nil {
		if err := refuse(token); err != nil {
			return nil, err
		}
	}
	conn := &fakeConn{token: token, in: make(chan []byte, 1024), closed: make(chan struct{}), onWrite: d.onWrite, panicOn: d.panicOn}
	if now := d.open.Add(1); now > d.maxOpen.Load() {
		d.maxOpen.Store(now)
	}
	conn.onClose = func() { d.open.Add(-1) }
	d.mu.Lock()
	d.conns = append(d.conns, conn)
	d.mu.Unlock()
	d.dialed <- conn
	return conn, nil
}

func (d *fakeDialer) dials() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.tokens)
}

func (d *fakeDialer) usedTokens() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.tokens...)
}

// switchToken is a TokenSource whose answer tests can change.
type switchToken struct {
	mu    sync.Mutex
	token string
	err   error
	calls atomic.Int32
	panic bool
}

func (s *switchToken) Token(context.Context) (string, error) {
	s.calls.Add(1)
	if s.panic {
		panic("fixture token panic")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token, s.err
}

func (s *switchToken) set(token string, err error) {
	s.mu.Lock()
	s.token, s.err = token, err
	s.mu.Unlock()
}

type argsHook struct {
	mu   sync.Mutex
	args [][]interface{}
}

func (h *argsHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *argsHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.mu.Lock()
		h.args = append(h.args, cmd.Args())
		h.mu.Unlock()
		return next(ctx, cmd)
	}
}
func (h *argsHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

type lane struct {
	runner *Runner
	dialer *fakeDialer
	token  *switchToken
	mini   *miniredis.Miniredis
	client *redis.Client // the lane's own client, audited by hook
	reader *redis.Client // a separate client for test assertions
	hook   *argsHook
	clock  *laneClock
	cancel context.CancelFunc
	done   chan struct{}
}

func startLane(t *testing.T, at time.Time, onWrite func(*fakeConn, []byte), mutate func(*Config, *lane)) *lane {
	t.Helper()
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	hook := &argsHook{}
	client.AddHook(hook)
	reader := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = reader.Close() })
	l := &lane{dialer: newFakeDialer(onWrite), token: &switchToken{token: "cached-token-1"}, mini: mini, client: client, reader: reader, hook: hook, clock: newLaneClock(at), done: make(chan struct{})}
	cfg := Config{
		Symbols:      testSymbols,
		Token:        l.token,
		Dialer:       l.dialer,
		Redis:        client,
		StreamKey:    DefaultStreamKey,
		MaxLen:       500,
		Buffer:       64,
		Clock:        l.clock,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		RetryMin:     time.Millisecond,
		RetryMax:     5 * time.Millisecond,
		PingInterval: 10 * time.Millisecond,
	}
	if mutate != nil {
		mutate(&cfg, l)
	}
	runner, err := NewRunner(cfg)
	if err != nil {
		t.Fatal(err)
	}
	l.runner = runner
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	go func() {
		defer close(l.done)
		runner.Run(ctx)
	}()
	t.Cleanup(l.stop)
	return l
}

func (l *lane) stop() {
	l.cancel()
	select {
	case <-l.done:
	case <-time.After(10 * time.Second):
		panic("quote runner did not stop")
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// Wednesday 30 September 2026, 13:15 KST: KRX regular session.
var wednesdayMidday = time.Date(2026, time.September, 30, 13, 15, 0, 0, KST)

func krOnly(cfg *Config, _ *lane) { cfg.Symbols = []Symbol{{MarketKR, "005930"}, {MarketKR, "000660"}} }

func TestRunnerStreamsFramesWithExactFields(t *testing.T) {
	l := startLane(t, wednesdayMidday, ackAll, nil)
	conn := <-l.dialer.dialed
	waitFor(t, "declaration", func() bool { return conn.count("[") == 1 })
	wantDeclaration := `[{"id":"fillwire-quotes"},{"codes":["005930","000660"],"type":"trade:kr"},{"codes":["005930","000660"],"type":"orderbook:kr"},{"codes":["AAPL"],"type":"trade:us"},{"codes":["AAPL"],"type":"orderbook:us"}]`
	if got := string(conn.sent()[0]); got != wantDeclaration {
		t.Fatalf("declaration = %s\nwant %s", got, wantDeclaration)
	}
	if conn.token != "cached-token-1" || l.dialer.endpoints[0] != Endpoint {
		t.Fatalf("dial token=%q endpoint=%q", conn.token, l.dialer.endpoints[0])
	}
	conn.push(fixture(t, "trade_kr.json"))
	conn.push(fixture(t, "orderbook_kr.json"))
	conn.push([]byte(`{"type":"message","topic":"trade:kr:035420","data":{"price":"1","timestamp":"2026-09-30T13:15:02.000+09:00"}}`))
	conn.push([]byte(`{"type":"message","topic":"trade:kr:005930","data":{"price":"x"}}`))
	conn.push([]byte(`{"type":"pong"}`))
	waitFor(t, "two stream entries", func() bool { return l.reader.XLen(context.Background(), "quotes:toss").Val() == 2 })
	waitFor(t, "drop counters", func() bool {
		c := l.runner.Counters()
		return c.DropUnknownSymbol.Load() == 1 && c.DropMalformed.Load() == 1
	})
	entries, err := l.reader.XRange(context.Background(), "quotes:toss", "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	want := []map[string]string{
		{"symbol": "005930", "ts": "2026-09-30T13:15:02.123+09:00", "price": "72000", "bid1": "", "ask1": "", "bid_qty": "", "ask_qty": "", "session": "krx_regular"},
		{"symbol": "005930", "ts": "2026-09-30T13:15:02.456+09:00", "price": "", "bid1": "72000", "ask1": "72100", "bid_qty": "12000", "ask_qty": "8500", "session": "krx_regular"},
	}
	for index, entry := range entries {
		if len(entry.Values) != len(StreamFields) {
			t.Fatalf("entry %d has %d fields: %v", index, len(entry.Values), entry.Values)
		}
		for name, value := range want[index] {
			if entry.Values[name] != value {
				t.Fatalf("entry %d %s = %v, want %q", index, name, entry.Values[name], value)
			}
		}
	}
	l.hook.mu.Lock()
	defer l.hook.mu.Unlock()
	for _, args := range l.hook.args {
		name := strings.ToLower(fmt.Sprint(args[0]))
		if name == "hello" || name == "client" {
			continue // connection handshake
		}
		if name != "xadd" {
			t.Fatalf("lane Redis client issued %s; only XADD expected", name)
		}
		if got := strings.ToLower(fmt.Sprint(args[:6])); got != "[xadd quotes:toss maxlen ~ 500 *]" {
			t.Fatalf("XADD args = %s", got)
		}
		var names []interface{}
		for index := 6; index < len(args); index += 2 {
			names = append(names, args[index])
		}
		if fmt.Sprint(names) != fmt.Sprint(StreamFields) {
			t.Fatalf("XADD field names = %v", names)
		}
	}
}

func TestRunnerSendsTextPing(t *testing.T) {
	l := startLane(t, wednesdayMidday, ackAll, nil)
	conn := <-l.dialer.dialed
	waitFor(t, "two pings", func() bool { return conn.count("PING") >= 2 })
	for _, write := range conn.sent() {
		if s := string(write); s != "PING" && !strings.HasPrefix(s, "[") {
			t.Fatalf("unexpected client frame %q", s)
		}
	}
}

func TestRunnerStaysOffOutsideTradingWindows(t *testing.T) {
	for name, at := range map[string]time.Time{
		"before KR open": time.Date(2026, time.September, 30, 7, 30, 0, 0, KST),
		"KR night":       time.Date(2026, time.September, 30, 21, 0, 0, 0, KST),
		"saturday":       time.Date(2026, time.October, 3, 12, 0, 0, 0, KST),
	} {
		t.Run(name, func(t *testing.T) {
			l := startLane(t, at, ackAll, krOnly)
			time.Sleep(40 * time.Millisecond)
			if l.dialer.dials() != 0 || l.token.calls.Load() != 0 {
				t.Fatalf("outside window: dials=%d token reads=%d, want 0", l.dialer.dials(), l.token.calls.Load())
			}
		})
	}
}

func TestRunnerClosesSocketWhenWindowCloses(t *testing.T) {
	l := startLane(t, time.Date(2026, time.September, 30, 20, 0, 50, 0, KST), ackAll, krOnly)
	conn := <-l.dialer.dialed
	waitFor(t, "declaration", func() bool { return conn.count("[") == 1 })
	l.clock.set(time.Date(2026, time.September, 30, 20, 1, 0, 0, KST))
	waitFor(t, "socket close at window end", conn.isClosed)
	time.Sleep(30 * time.Millisecond)
	if l.dialer.dials() != 1 {
		t.Fatalf("dials after window close = %d, want 1", l.dialer.dials())
	}
}

func TestRunnerClosesSocketOnBackwardClockJump(t *testing.T) {
	l := startLane(t, wednesdayMidday, ackAll, krOnly)
	conn := <-l.dialer.dialed
	l.clock.set(time.Date(2026, time.September, 30, 6, 0, 0, 0, KST))
	waitFor(t, "socket close after the clock left the window", conn.isClosed)
	time.Sleep(20 * time.Millisecond)
	if l.dialer.dials() != 1 {
		t.Fatalf("dials outside the window = %d, want 1", l.dialer.dials())
	}
}

func TestRunnerWaitsForCachedTokenWithoutDialing(t *testing.T) {
	l := startLane(t, wednesdayMidday, ackAll, func(_ *Config, l *lane) { l.token.set("", ErrTokenUnavailable) })
	waitFor(t, "repeated token reads", func() bool { return l.token.calls.Load() >= 3 })
	if l.dialer.dials() != 0 {
		t.Fatalf("dialled %d times without a token", l.dialer.dials())
	}
	l.token.set("cached-token-2", nil)
	conn := <-l.dialer.dialed
	if conn.token != "cached-token-2" {
		t.Fatalf("dial token = %q", conn.token)
	}
}

func TestRunnerNeverRedialsWithARefusedToken(t *testing.T) {
	l := startLane(t, wednesdayMidday, ackAll, func(_ *Config, l *lane) {
		l.dialer.refuse = func(token string) error {
			if token == "cached-token-1" {
				return ErrUnauthorized
			}
			return nil
		}
	})
	waitFor(t, "token re-reads after the refusal", func() bool { return l.token.calls.Load() >= 4 })
	if got := l.dialer.dials(); got != 1 {
		t.Fatalf("dials with the refused token = %d, want exactly 1", got)
	}
	if l.runner.Counters().TokenRejected.Load() != 1 {
		t.Fatalf("token_rejected = %d", l.runner.Counters().TokenRejected.Load())
	}
	// The owner publishes a new token: the lane uses it.
	l.token.set("cached-token-2", nil)
	conn := <-l.dialer.dialed
	if conn.token != "cached-token-2" {
		t.Fatalf("dial token = %q", conn.token)
	}
	if tokens := l.dialer.usedTokens(); len(tokens) != 2 {
		t.Fatalf("dial tokens = %v", tokens)
	}
}

func TestRunnerReconnectsAfterServerShutdownWithOneConnection(t *testing.T) {
	var shutdowns atomic.Int32
	onWrite := func(c *fakeConn, data []byte) {
		ackAll(c, data)
		if bytes.HasPrefix(data, []byte("[")) && shutdowns.Add(1) <= 5 {
			go func() {
				c.push(fixture(t, "error_server_shutdown.json"))
			}()
		}
	}
	l := startLane(t, wednesdayMidday, onWrite, nil)
	waitFor(t, "six dials", func() bool { return l.dialer.dials() >= 6 })
	if got := l.dialer.maxOpen.Load(); got != 1 {
		t.Fatalf("maximum simultaneously open Toss connections = %d, want 1", got)
	}
	if l.runner.Counters().ErrorFrames.Load() < 5 {
		t.Fatalf("error frames = %d", l.runner.Counters().ErrorFrames.Load())
	}
}

func TestRunnerRedeclaresAfterRateLimitNoSoonerThanOneSecond(t *testing.T) {
	var first atomic.Bool
	var times sync.Map
	var declarations atomic.Int32
	l := startLane(t, wednesdayMidday, func(c *fakeConn, data []byte) {
		if !bytes.HasPrefix(data, []byte("[")) {
			return
		}
		times.Store(declarations.Add(1), time.Now())
		if first.CompareAndSwap(false, true) {
			go c.push(fixture(t, "error_rate_limit.json"))
			return
		}
		ackAll(c, data)
	}, nil)
	conn := <-l.dialer.dialed
	waitFor(t, "redeclaration", func() bool { return conn.count("[") == 2 })
	t1, _ := times.Load(int32(1))
	t2, _ := times.Load(int32(2))
	if gap := t2.(time.Time).Sub(t1.(time.Time)); gap < 900*time.Millisecond {
		t.Fatalf("redeclared after %s, want about 1s", gap)
	}
	if l.dialer.dials() != 1 {
		t.Fatalf("rate limit caused a redial: %d", l.dialer.dials())
	}
}

func TestRunnerKeepsSocketOnPartialRejection(t *testing.T) {
	l := startLane(t, wednesdayMidday, func(c *fakeConn, data []byte) {
		if bytes.HasPrefix(data, []byte("[")) {
			go c.push([]byte(`{"type":"subscriptions","subscribed":["trade:kr:005930"],"rejected":[{"target":"trade:us:AAPL","code":"stock-not-found","message":"x"}]}`))
		}
	}, nil)
	conn := <-l.dialer.dialed
	waitFor(t, "rejection counted", func() bool { return l.runner.Counters().SubscribeRejected.Load() == 1 })
	conn.push(fixture(t, "trade_kr.json"))
	waitFor(t, "one entry", func() bool { return l.reader.XLen(context.Background(), "quotes:toss").Val() == 1 })
	if conn.isClosed() || l.dialer.dials() != 1 {
		t.Fatalf("partial rejection closed=%t dials=%d", conn.isClosed(), l.dialer.dials())
	}
}

func TestRunnerRedisFailureNeverStallsSocket(t *testing.T) {
	l := startLane(t, wednesdayMidday, ackAll, func(cfg *Config, _ *lane) { cfg.Buffer = 4 })
	conn := <-l.dialer.dialed
	l.mini.SetError("ERR synthetic redis failure")
	const frames = 300
	go func() {
		for index := 0; index < frames; index++ {
			conn.push(fixture(t, "trade_kr.json"))
		}
	}()
	waitFor(t, "all frames read", func() bool { return l.runner.Counters().Frames.Load() >= frames })
	c := l.runner.Counters()
	if c.XAddErrors.Load() == 0 {
		t.Fatal("no XADD error observed")
	}
	if c.XAddErrors.Load()+c.DropBufferFull.Load()+c.TicksWritten.Load() > frames {
		t.Fatalf("tick accounting exceeds frames: %v", c.logArgs())
	}
}

func TestRunnerContainsPanics(t *testing.T) {
	for name, mutate := range map[string]func(*Config, *lane){
		"dialer": func(_ *Config, l *lane) { l.dialer.panics = true },
		"read":   func(_ *Config, l *lane) { l.dialer.panicOn = "read" },
		"token":  func(_ *Config, l *lane) { l.token.panic = true },
	} {
		t.Run(name, func(t *testing.T) {
			l := startLane(t, wednesdayMidday, ackAll, mutate)
			time.Sleep(40 * time.Millisecond)
			if name == "read" {
				// A read panic is contained as a failed read; the lane redials.
				waitFor(t, "redials after contained read panics", func() bool { return l.dialer.dials() >= 2 })
			}
			select {
			case <-l.done:
				if name != "read" {
					return // a supervisor-side panic stops only the lane
				}
				t.Fatal("lane stopped on a contained read panic")
			default:
			}
		})
	}
}

func TestSingleConnectionRefusesASecondDial(t *testing.T) {
	inner := newFakeDialer(nil)
	guard := &singleConnection{dialer: inner}
	first, err := guard.Dial(context.Background(), Endpoint, "t")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := guard.Dial(context.Background(), Endpoint, "t"); !errors.Is(err, errSecondConnection) {
		t.Fatalf("second dial = %v, want errSecondConnection", err)
	}
	_ = first.Close()
	second, err := guard.Dial(context.Background(), Endpoint, "t")
	if err != nil {
		t.Fatalf("dial after close = %v", err)
	}
	_ = second.Close()
	if inner.dials() != 2 {
		t.Fatalf("inner dials = %d, want 2", inner.dials())
	}
}

func TestNewRunnerRejectsUnsafeConfig(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	defer client.Close()
	valid := Config{Symbols: testSymbols, Token: &switchToken{}, Dialer: newFakeDialer(nil), Redis: client, StreamKey: "quotes:toss", MaxLen: 1, Buffer: 1}
	if _, err := NewRunner(valid); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	many := make([]Symbol, MaxSymbols+1)
	for index := range many {
		many[index] = Symbol{MarketKR, fmt.Sprintf("%06d", index)}
	}
	for name, mutate := range map[string]func(*Config){
		"41 symbols":        func(c *Config) { c.Symbols = many },
		"no symbols":        func(c *Config) { c.Symbols = nil },
		"invalid symbol":    func(c *Config) { c.Symbols = []Symbol{{MarketKR, "5930"}} },
		"duplicate symbol":  func(c *Config) { c.Symbols = []Symbol{{MarketKR, "005930"}, {MarketKR, "005930"}} },
		"no token source":   func(c *Config) { c.Token = nil },
		"no dialer":         func(c *Config) { c.Dialer = nil },
		"no redis":          func(c *Config) { c.Redis = nil },
		"fills stream key":  func(c *Config) { c.StreamKey = "fills:kis" },
		"approval key area": func(c *Config) { c.StreamKey = "kis:websocket:approval_key" },
		"zero max len":      func(c *Config) { c.MaxLen = 0 },
		"zero buffer":       func(c *Config) { c.Buffer = 0 },
	} {
		cfg := valid
		mutate(&cfg)
		if _, err := NewRunner(cfg); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	over := make([]Symbol, 51)
	for index := range over {
		over[index] = Symbol{MarketKR, fmt.Sprintf("%06d", index)}
	}
	if _, _, err := buildDeclaration(over); err == nil {
		t.Fatal("102 subscriptions accepted by buildDeclaration")
	}
}
