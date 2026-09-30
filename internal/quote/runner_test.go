package quote

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/go-kis/kis/ws"
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

type request struct{ trType, tr, key string }

type fakeTransport struct {
	in     chan []byte
	closed chan struct{}
	once   sync.Once
	mu     sync.Mutex
	writes []request
	ack    func(request) string
}

func (t *fakeTransport) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, io.EOF
	case frame := <-t.in:
		return frame, nil
	}
}

func (t *fakeTransport) Write(_ context.Context, raw []byte) error {
	var frame struct {
		Header struct {
			TRType string `json:"tr_type"`
		} `json:"header"`
		Body struct {
			Input struct {
				TRID  string `json:"tr_id"`
				TRKey string `json:"tr_key"`
			} `json:"input"`
		} `json:"body"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil {
		return err
	}
	req := request{trType: frame.Header.TRType, tr: frame.Body.Input.TRID, key: frame.Body.Input.TRKey}
	t.mu.Lock()
	t.writes = append(t.writes, req)
	t.mu.Unlock()
	if req.trType == "1" && t.ack != nil {
		if reply := t.ack(req); reply != "" {
			t.push(reply)
		}
	}
	return nil
}

func (t *fakeTransport) WriteControl(context.Context, int, []byte) error { return nil }
func (t *fakeTransport) Close() error {
	t.once.Do(func() { close(t.closed) })
	return nil
}
func (t *fakeTransport) isClosed() bool {
	select {
	case <-t.closed:
		return true
	default:
		return false
	}
}
func (t *fakeTransport) push(frame string) {
	select {
	case t.in <- []byte(frame):
	case <-t.closed:
	}
}
func (t *fakeTransport) requests() []request {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]request(nil), t.writes...)
}

func ackOK(req request) string {
	return `{"header":{"tr_id":"` + req.tr + `"},"body":{"rt_cd":"0","msg_cd":"OPSP0000","msg1":"SUBSCRIBE SUCCESS","output":{}}}`
}

func ackCode(req request, rtCD, msgCD string) string {
	return `{"header":{"tr_id":"` + req.tr + `"},"body":{"rt_cd":"` + rtCD + `","msg_cd":"` + msgCD + `","msg1":"fixture","output":{}}}`
}

type fakeDialer struct {
	mu         sync.Mutex
	transports []*fakeTransport
	ack        func(request) string
	panics     bool
	dialed     chan *fakeTransport
}

func newFakeDialer(ack func(request) string) *fakeDialer {
	return &fakeDialer{ack: ack, dialed: make(chan *fakeTransport, 1024)}
}

func (d *fakeDialer) Dial(context.Context, string) (ws.Transport, error) {
	if d.panics {
		panic("fixture dialer panic")
	}
	transport := &fakeTransport{in: make(chan []byte, 1024), closed: make(chan struct{}), ack: d.ack}
	d.mu.Lock()
	d.transports = append(d.transports, transport)
	d.mu.Unlock()
	d.dialed <- transport
	return transport, nil
}

func (d *fakeDialer) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.transports)
}

type fakeApproval struct{ calls atomic.Int32 }

func (a *fakeApproval) ApprovalKey(context.Context) (string, error) {
	a.calls.Add(1)
	return "quote-fixture-key", nil
}
func (a *fakeApproval) Reissue(context.Context) (string, error) {
	a.calls.Add(1)
	return "quote-fixture-key-2", nil
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
	mini   *miniredis.Miniredis
	client *redis.Client
	hook   *argsHook
	clock  *laneClock
	cancel context.CancelFunc
	done   chan struct{}
}

func startLane(t *testing.T, at time.Time, ack func(request) string, mutate func(*Config)) *lane {
	t.Helper()
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	hook := &argsHook{}
	client.AddHook(hook)
	l := &lane{dialer: newFakeDialer(ack), mini: mini, client: client, hook: hook, clock: newLaneClock(at), done: make(chan struct{})}
	cfg := Config{
		Endpoint:  ws.EndpointLive,
		Symbols:   []string{"005930", "000660"},
		Approval:  &fakeApproval{},
		Dialer:    l.dialer,
		Redis:     client,
		StreamKey: DefaultStreamKey,
		MaxLen:    500,
		Buffer:    64,
		Clock:     l.clock,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		RetryMin:  time.Millisecond,
		RetryMax:  5 * time.Millisecond,
		Backoff:   ws.BackoffConfig{Min: time.Millisecond, Max: 2 * time.Millisecond, Jitter: -1},
	}
	if mutate != nil {
		mutate(&cfg)
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

func subscribed(transport *fakeTransport, n int) func() bool {
	return func() bool {
		count := 0
		for _, req := range transport.requests() {
			if req.trType == "1" {
				count++
			}
		}
		return count >= n
	}
}

var wednesdayMidday = time.Date(2026, time.September, 30, 13, 15, 0, 0, KST)

func TestRunnerStreamsFixtureFramesWithExactFields(t *testing.T) {
	l := startLane(t, wednesdayMidday, ackOK, nil)
	transport := <-l.dialer.dialed
	waitFor(t, "four subscriptions", subscribed(transport, 4))
	transport.push(readFrame(t, "h0stcnt0.frame"))
	transport.push(readFrame(t, "h0stasp0.frame"))
	transport.push(strings.Replace(readFrame(t, "h0stcnt0.frame"), "005930", "035420", 1)) // unconfigured
	transport.push("0|H0STCNT0|001|005930^1315")                                           // partial
	waitFor(t, "two stream entries", func() bool { return l.client.XLen(context.Background(), "quotes:kis").Val() == 2 })
	waitFor(t, "drop counters", func() bool {
		return l.runner.Counters().DropUnknownSymbol.Load() == 1 && l.runner.Counters().DropPartial.Load() == 1
	})

	got := transport.requests()
	sort.Slice(got, func(i, j int) bool { return got[i].tr+got[i].key < got[j].tr+got[j].key })
	want := []request{{"1", "H0STASP0", "000660"}, {"1", "H0STASP0", "005930"}, {"1", "H0STCNT0", "000660"}, {"1", "H0STCNT0", "005930"}}
	if len(got) != len(want) {
		t.Fatalf("socket requests = %+v, want %+v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("socket requests = %+v, want %+v", got, want)
		}
	}

	entries, err := l.client.XRange(context.Background(), "quotes:kis", "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	wantValues := []map[string]string{
		{"symbol": "005930", "ts": "2026-09-30T13:15:02+09:00", "price": "70500", "bid1": "", "ask1": "", "bid_qty": "", "ask_qty": "", "session": "regular"},
		{"symbol": "005930", "ts": "2026-09-30T13:15:02+09:00", "price": "", "bid1": "70500", "ask1": "70600", "bid_qty": "2300", "ask_qty": "1200", "session": "regular"},
	}
	for index, entry := range entries {
		if len(entry.Values) != len(StreamFields) {
			t.Fatalf("entry %d has %d fields, want exactly %v: %v", index, len(entry.Values), StreamFields, entry.Values)
		}
		for name, value := range wantValues[index] {
			if entry.Values[name] != value {
				t.Fatalf("entry %d %s = %v, want %q", index, name, entry.Values[name], value)
			}
		}
	}

	l.hook.mu.Lock()
	defer l.hook.mu.Unlock()
	xadds := 0
	for _, args := range l.hook.args {
		if strings.EqualFold(args[0].(string), "xadd") {
			xadds++
			if got := strings.ToLower(fmt.Sprint(args[:6])); got != "[xadd quotes:kis maxlen ~ 500 *]" {
				t.Fatalf("XADD args = %s, want MAXLEN ~ 500 on quotes:kis", got)
			}
			var names []interface{}
			for index := 6; index < len(args); index += 2 {
				names = append(names, args[index])
			}
			if got := fmt.Sprint(names); got != fmt.Sprint(StreamFields) {
				t.Fatalf("XADD field names = %s, want %v", got, StreamFields)
			}
		}
	}
	if xadds != 2 {
		t.Fatalf("XADD count = %d, want 2", xadds)
	}
}

func TestRunnerStaysOffOutsideTradingWindows(t *testing.T) {
	for name, at := range map[string]time.Time{
		"before open":      time.Date(2026, time.September, 30, 8, 30, 0, 0, KST),
		"between sessions": time.Date(2026, time.September, 30, 15, 45, 0, 0, KST),
		"night":            time.Date(2026, time.September, 30, 20, 30, 0, 0, KST),
		"saturday":         time.Date(2026, time.October, 3, 10, 0, 0, 0, KST),
	} {
		t.Run(name, func(t *testing.T) {
			approval := &fakeApproval{}
			l := startLane(t, at, ackOK, func(cfg *Config) { cfg.Approval = approval })
			time.Sleep(50 * time.Millisecond)
			if l.dialer.count() != 0 || approval.calls.Load() != 0 {
				t.Fatalf("outside window: dials=%d approvals=%d, want 0", l.dialer.count(), approval.calls.Load())
			}
		})
	}
}

func TestRunnerClosesSocketWhenWindowCloses(t *testing.T) {
	l := startLane(t, time.Date(2026, time.September, 30, 15, 30, 50, 0, KST), ackOK, nil)
	transport := <-l.dialer.dialed
	waitFor(t, "four subscriptions", subscribed(transport, 4))
	l.clock.set(time.Date(2026, time.September, 30, 15, 31, 0, 0, KST))
	waitFor(t, "socket close", transport.isClosed)
	unsubscribes := 0
	for _, req := range transport.requests() {
		if req.trType == "2" {
			unsubscribes++
		}
	}
	if unsubscribes != 4 {
		t.Fatalf("unsubscribe frames = %d, want 4", unsubscribes)
	}
	time.Sleep(30 * time.Millisecond)
	if l.dialer.count() != 1 {
		t.Fatalf("dials after window close = %d, want 1", l.dialer.count())
	}
	// Reopen at after-hours: one new socket.
	l.clock.set(time.Date(2026, time.September, 30, 16, 0, 0, 0, KST))
	next := <-l.dialer.dialed
	waitFor(t, "after-hours subscriptions", subscribed(next, 4))
}

func TestRunnerKeepsStreamingWhenOneSubscriptionIsRejected(t *testing.T) {
	ack := func(req request) string {
		if req.tr == ws.TRQuoteBook && req.key == "000660" {
			return ackCode(req, "1", "OPSP0008")
		}
		return ackOK(req)
	}
	l := startLane(t, wednesdayMidday, ack, nil)
	transport := <-l.dialer.dialed
	waitFor(t, "four subscribe attempts", subscribed(transport, 4))
	transport.push(readFrame(t, "h0stcnt0.frame"))
	waitFor(t, "one entry", func() bool { return l.client.XLen(context.Background(), "quotes:kis").Val() == 1 })
	if got := l.runner.Counters().SubscribeRejected.Load(); got != 1 {
		t.Fatalf("rejected subscriptions = %d, want 1", got)
	}
	if transport.isClosed() || l.dialer.count() != 1 {
		t.Fatalf("rejection closed=%t dials=%d, want the socket kept", transport.isClosed(), l.dialer.count())
	}
}

func TestRunnerRetriesSessionOccupiedWithoutReturning(t *testing.T) {
	l := startLane(t, wednesdayMidday, func(req request) string { return ackCode(req, "1", "OPSP8996") }, nil)
	waitFor(t, "repeated dials", func() bool { return l.dialer.count() >= 3 })
	select {
	case <-l.done:
		t.Fatal("quote runner returned on OPSP8996; it must keep retrying on its own lane")
	default:
	}
}

func TestRunnerRedisFailureNeverStallsSocket(t *testing.T) {
	l := startLane(t, wednesdayMidday, ackOK, func(cfg *Config) { cfg.Buffer = 4 })
	transport := <-l.dialer.dialed
	waitFor(t, "four subscriptions", subscribed(transport, 4))
	l.mini.SetError("ERR synthetic redis failure")
	const frames = 300
	go func() {
		for index := 0; index < frames; index++ {
			transport.push(readFrame(t, "h0stcnt0.frame"))
		}
	}()
	waitFor(t, "all frames drained", func() bool { return l.runner.Counters().Events.Load() == frames })
	counters := l.runner.Counters()
	if counters.XAddErrors.Load() == 0 {
		t.Fatal("no XADD error observed")
	}
	if counters.XAddErrors.Load()+counters.DropBufferFull.Load()+counters.TicksWritten.Load() > frames {
		t.Fatalf("tick accounting exceeds frames: %+v", counters.logArgs())
	}
}

func TestRunnerContainsPanic(t *testing.T) {
	l := startLane(t, wednesdayMidday, ackOK, func(cfg *Config) {
		cfg.Dialer.(*fakeDialer).panics = true
	})
	select {
	case <-l.done:
	case <-time.After(5 * time.Second):
		t.Fatal("panicking quote lane did not stop")
	}
}

func TestNewRunnerRejectsUnsafeConfig(t *testing.T) {
	valid := Config{Endpoint: ws.EndpointLive, Symbols: []string{"005930"}, Approval: &fakeApproval{}, Dialer: newFakeDialer(ackOK), Redis: redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"}), StreamKey: "quotes:kis", MaxLen: 1, Buffer: 1}
	defer valid.Redis.Close()
	if _, err := NewRunner(valid); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	many := make([]string, MaxSymbols+1)
	for index := range many {
		many[index] = "005930"
	}
	for name, mutate := range map[string]func(*Config){
		"41 symbols":       func(c *Config) { c.Symbols = many },
		"no symbols":       func(c *Config) { c.Symbols = nil },
		"invalid symbol":   func(c *Config) { c.Symbols = []string{"5930"} },
		"foreign endpoint": func(c *Config) { c.Endpoint = "ws://example.invalid:21000" },
		"no approval":      func(c *Config) { c.Approval = nil },
		"no dialer":        func(c *Config) { c.Dialer = nil },
		"no redis":         func(c *Config) { c.Redis = nil },
		"no stream key":    func(c *Config) { c.StreamKey = " " },
		"zero max len":     func(c *Config) { c.MaxLen = 0 },
		"zero buffer":      func(c *Config) { c.Buffer = 0 },
	} {
		cfg := valid
		mutate(&cfg)
		if _, err := NewRunner(cfg); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}
