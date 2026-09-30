package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/fillwire/internal/quote"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

// --- configuration -------------------------------------------------------

const testTokenKey = "toss:oauth:0123456789abcdef:access_token"

func shippedConfig(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("../../fillwire.toml")
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func writeTemp(t *testing.T, name, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func setFillsEnv(t *testing.T) {
	t.Setenv("KIS_APP_KEY", "fixture-fills-app")
	t.Setenv("KIS_APP_SECRET", "fixture-fills-secret")
	t.Setenv("EXECUTION_LEDGER_INGEST_TOKEN", "fixture-ingest")
}

// withQuotes replaces the shipped [quotes] table with body.
func withQuotes(t *testing.T, shipped, body string) string {
	t.Helper()
	index := strings.Index(shipped, "\n[quotes]\n")
	if index < 0 {
		t.Fatal("shipped fillwire.toml has no [quotes] table")
	}
	rest := shipped[index+1:]
	end := strings.Index(rest[len("[quotes]\n"):], "\n[")
	tail := ""
	if end >= 0 {
		tail = rest[len("[quotes]\n")+end:]
	}
	return shipped[:index+1] + body + tail
}

func symbolFile(t *testing.T, n int) string {
	t.Helper()
	var builder strings.Builder
	for index := 0; index < n; index++ {
		fmt.Fprintf(&builder, "kr %06d\n", index+1)
	}
	return writeTemp(t, "symbols.txt", builder.String())
}

func enabledQuotes(symbolsPath string) string {
	return fmt.Sprintf(`[quotes]
enabled = true
provider = "toss"
symbols_file = %q
token_key = "toss:oauth:0123456789abcdef:access_token"
stream_key = "quotes:toss"
max_len = 50000
buffer = 512
`, symbolsPath)
}

func fillsOnly(cfg runtimeConfig) runtimeConfig {
	cfg.quotes = quoteSettings{}
	return cfg
}

type panicDialer struct{}

func (panicDialer) Dial(context.Context, string, string) (quote.Transport, error) {
	panic("a disabled quote lane must never dial")
}

type panicToken struct{}

func (panicToken) Token(context.Context) (string, error) {
	panic("a disabled quote lane must never read a token")
}

func TestQuoteReaderDefaultOff(t *testing.T) {
	setFillsEnv(t)
	shipped := shippedConfig(t)
	if !strings.Contains(shipped, "[quotes]\nenabled = false\n") {
		t.Fatal("shipped fillwire.toml must show the quote reader disabled")
	}
	for name, text := range map[string]string{
		"shipped":            shipped,
		"no [quotes] table":  withQuotes(t, shipped, ""),
		"enabled omitted":    withQuotes(t, shipped, "[quotes]\nprovider = \"toss\"\n"),
		"disabled with junk": withQuotes(t, shipped, "[quotes]\nenabled = false\nprovider = 7\nclient_secret = \"x\"\n"),
	} {
		t.Run(name, func(t *testing.T) {
			if name == "no [quotes] table" && strings.Contains(text, "[quotes]") {
				t.Fatal("fixture mutation failed")
			}
			cfg, err := loadConfig(writeTemp(t, "fillwire.toml", text))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.quotes.enabled || cfg.quotes.err != nil {
				t.Fatalf("quotes = %+v, want off", cfg.quotes)
			}
			lane := startQuoteLane(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), runDependencies{quoteDialer: panicDialer{}, quoteToken: panicToken{}})
			if lane.done != nil || lane.stop != nil {
				t.Fatal("disabled quote reader started a lane")
			}
		})
	}
}

func TestQuoteSectionNeverChangesFillsConfig(t *testing.T) {
	setFillsEnv(t)
	shipped := shippedConfig(t)
	baseline, err := loadConfig(writeTemp(t, "fillwire.toml", withQuotes(t, shipped, "")))
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"shipped disabled":     "[quotes]\nenabled = false\n",
		"valid enabled":        enabledQuotes(symbolFile(t, 40)),
		"too many symbols":     enabledQuotes(symbolFile(t, 41)),
		"type error in quotes": "[quotes]\nenabled = \"yes\"\nmax_len = \"many\"\n",
		"unknown quote keys":   "[quotes]\nenabled = true\nsurprise = 1\n",
		"quotes not a table":   "quotes = 3\n",
	} {
		t.Run(name, func(t *testing.T) {
			text := withQuotes(t, shipped, body)
			if name == "quotes not a table" {
				// A top-level key must precede the first table header.
				text = body + withQuotes(t, shipped, "")
			}
			cfg, err := loadConfig(writeTemp(t, "fillwire.toml", text))
			if err != nil {
				t.Fatalf("[quotes] content failed the fills config: %v", err)
			}
			if !reflect.DeepEqual(fillsOnly(cfg), fillsOnly(baseline)) {
				t.Fatal("[quotes] content changed the fills configuration")
			}
		})
	}
}

func TestQuoteSettingsResolution(t *testing.T) {
	setFillsEnv(t)
	shipped := shippedConfig(t)
	load := func(t *testing.T, body string) quoteSettings {
		t.Helper()
		cfg, err := loadConfig(writeTemp(t, "fillwire.toml", withQuotes(t, shipped, body)))
		if err != nil {
			t.Fatalf("fills config failed: %v", err)
		}
		return cfg.quotes
	}

	valid := load(t, enabledQuotes(symbolFile(t, 40)))
	if !valid.enabled || valid.err != nil || len(valid.symbols) != 40 || valid.streamKey != "quotes:toss" || valid.maxLen != 50000 || valid.buffer != 512 || valid.tokenKey != testTokenKey {
		t.Fatalf("valid quote settings = %+v", valid)
	}
	defaults := load(t, strings.NewReplacer("stream_key = \"quotes:toss\"\n", "", "max_len = 50000\n", "", "buffer = 512\n", "").Replace(enabledQuotes(symbolFile(t, 1))))
	if defaults.err != nil || defaults.streamKey != quote.DefaultStreamKey || defaults.maxLen != defaultQuoteMaxLen || defaults.buffer != defaultQuoteBuffer {
		t.Fatalf("defaulted quote settings = %+v", defaults)
	}
	if capped := load(t, strings.Replace(enabledQuotes(symbolFile(t, 1)), "buffer = 512", "buffer = 65536", 1)); capped.err != nil || capped.buffer != 65536 {
		t.Fatalf("buffer at the cap = %+v, want accepted", capped)
	}

	malformed := writeTemp(t, "bad.txt", "kr 005930\nkr 5930\n")
	one := enabledQuotes(symbolFile(t, 1))
	replace := func(old, new string) string { return strings.Replace(one, old, new, 1) }
	for name, body := range map[string]string{
		"41 symbols":             enabledQuotes(symbolFile(t, 41)),
		"malformed list":         enabledQuotes(malformed),
		"missing list file":      enabledQuotes(filepath.Join(t.TempDir(), "absent.txt")),
		"no symbols_file":        replace("symbols_file = ", "# symbols_file = "),
		"provider kis":           replace(`provider = "toss"`, `provider = "kis"`),
		"no provider":            replace(`provider = "toss"`, ""),
		"no token_key":           replace("token_key = ", "# token_key = "),
		"token lock key":         replace(testTokenKey, "toss:oauth:0123456789abcdef:lock"),
		"kis approval key":       replace(testTokenKey, "kis:websocket:approval_key"),
		"upper-case fingerprint": replace(testTokenKey, "toss:oauth:0123456789ABCDEF:access_token"),
		"client_secret key":      one + "client_secret = \"x\"\n",
		"client_id key":          one + "client_id = \"x\"\n",
		"unknown key":            one + "endpoint = \"wss://example.invalid\"\n",
		"fills stream key":       replace(`stream_key = "quotes:toss"`, `stream_key = "fills:kis"`),
		"approval stream key":    replace(`stream_key = "quotes:toss"`, `stream_key = "kis:websocket:approval_key"`),
		"token stream key":       replace(`stream_key = "quotes:toss"`, `stream_key = "toss:oauth:0123456789abcdef:access_token"`),
		"bare prefix":            replace(`stream_key = "quotes:toss"`, `stream_key = "quotes:"`),
		"padded stream key":      replace(`stream_key = "quotes:toss"`, `stream_key = " quotes:toss"`),
		"negative max_len":       replace("max_len = 50000", "max_len = -1"),
		"negative buffer":        replace("buffer = 512", "buffer = -1"),
		"oversized buffer":       replace("buffer = 512", "buffer = 65537"),
		"wrong type max_len":     replace("max_len = 50000", `max_len = "many"`),
		"type error in enabled":  "[quotes]\nenabled = \"yes\"\n",
	} {
		t.Run(name, func(t *testing.T) {
			settings := load(t, body)
			if !settings.enabled || settings.err == nil {
				t.Fatalf("%s: settings = %+v, want enabled with a rejection", name, settings)
			}
			for _, secret := range []string{"fixture-fills-app", "fixture-fills-secret", "fixture-ingest"} {
				if strings.Contains(settings.err.Error(), secret) {
					t.Fatalf("rejection leaked a credential: %v", settings.err)
				}
			}
		})
	}
}

func TestRejectedQuoteSettingsNeverStartALane(t *testing.T) {
	var logs strings.Builder
	var mu sync.Mutex
	logger := slog.New(slog.NewTextHandler(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return logs.Write(p)
	}), nil))
	cfg := runtimeConfig{quotes: quoteSettings{enabled: true, err: errors.New("quotes: symbol list rejected")}}
	lane := startQuoteLane(cfg, logger, runDependencies{quoteDialer: panicDialer{}, quoteToken: panicToken{}})
	if lane.done != nil {
		t.Fatal("rejected quote settings started a lane")
	}
	if !strings.Contains(logs.String(), "quote reader disabled: configuration rejected") {
		t.Fatalf("logs = %s", logs.String())
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// --- AC3 isolation: fills path with the quote lane on and off -------------

type isoRequest struct{ trType, tr, key string }

type isoTransport struct {
	in      chan []byte
	closed  chan struct{}
	once    sync.Once
	mu      sync.Mutex
	reqs    []isoRequest
	onWrite func(*isoTransport, isoRequest)
	closedN atomic.Int32
}

func newIsoTransport(onWrite func(*isoTransport, isoRequest)) *isoTransport {
	return &isoTransport{in: make(chan []byte, 1024), closed: make(chan struct{}), onWrite: onWrite}
}

func (t *isoTransport) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, io.EOF
	case frame := <-t.in:
		return frame, nil
	}
}

func (t *isoTransport) Write(_ context.Context, raw []byte) error {
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
	req := isoRequest{frame.Header.TRType, frame.Body.Input.TRID, frame.Body.Input.TRKey}
	t.mu.Lock()
	t.reqs = append(t.reqs, req)
	t.mu.Unlock()
	if t.onWrite != nil {
		t.onWrite(t, req)
	}
	return nil
}

func (t *isoTransport) WriteControl(context.Context, int, []byte) error { return nil }
func (t *isoTransport) Close() error {
	t.closedN.Add(1)
	t.once.Do(func() { close(t.closed) })
	return nil
}
func (t *isoTransport) push(frame string) {
	select {
	case t.in <- []byte(frame):
	case <-t.closed:
	}
}
func (t *isoTransport) requests() []isoRequest {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]isoRequest(nil), t.reqs...)
}

func isoAck(tr, rtCD, msgCD string) string {
	return `{"header":{"tr_id":"` + tr + `"},"body":{"rt_cd":"` + rtCD + `","msg_cd":"` + msgCD + `","msg1":"fixture","output":{}}}`
}

type isoDialer struct {
	mu         sync.Mutex
	transports []*isoTransport
	dial       func(*isoDialer) (ws.Transport, error)
	endpoints  []string
}

func (d *isoDialer) Dial(_ context.Context, endpoint string) (ws.Transport, error) {
	d.mu.Lock()
	d.endpoints = append(d.endpoints, endpoint)
	d.mu.Unlock()
	return d.dial(d)
}

func (d *isoDialer) add(transport *isoTransport) *isoTransport {
	d.mu.Lock()
	d.transports = append(d.transports, transport)
	d.mu.Unlock()
	return transport
}

func (d *isoDialer) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.endpoints)
}

const isoFills = 5

func fillsFrame(index int) string {
	return fmt.Sprintf("0|H0STCNI9|1|HTS_EXAMPLE^00000000^A12345678%d^0000000000^02^00^00^00^005930^%d^71200^09301%d^0^2", index, index+1, index)
}

// fillsDialer answers the fills subscription and then delivers isoFills
// execution frames, once.
func fillsDialer() *isoDialer {
	return &isoDialer{dial: func(d *isoDialer) (ws.Transport, error) {
		return d.add(newIsoTransport(func(t *isoTransport, req isoRequest) {
			if req.trType != "1" {
				return
			}
			t.push(isoAck(req.tr, "0", "OPSP0000"))
			go func() {
				for index := 0; index < isoFills; index++ {
					t.push(fillsFrame(index))
				}
			}()
		})), nil
	}}
}

// --- Toss fakes for the quote lane --------------------------------------------

type tossConn struct {
	token   string
	in      chan []byte
	closed  chan struct{}
	once    sync.Once
	onWrite func(*tossConn, []byte)
	release func()
}

func (c *tossConn) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, io.EOF
	case frame := <-c.in:
		return frame, nil
	}
}

func (c *tossConn) Write(_ context.Context, data []byte) error {
	select {
	case <-c.closed:
		return io.ErrClosedPipe
	default:
	}
	if c.onWrite != nil {
		c.onWrite(c, data)
	}
	return nil
}

func (c *tossConn) Close() error {
	c.once.Do(func() {
		close(c.closed)
		if c.release != nil {
			c.release()
		}
	})
	return nil
}

func (c *tossConn) push(frame string) {
	select {
	case c.in <- []byte(frame):
	case <-c.closed:
	}
}

func tossAck(c *tossConn, data []byte) {
	if bytes.HasPrefix(data, []byte("[")) {
		go c.push(`{"type":"subscriptions","subscribed":["trade:kr:005930","orderbook:kr:005930"],"rejected":[]}`)
	}
}

type tossDialer struct {
	mu        sync.Mutex
	endpoints []string
	tokens    []string
	open      atomic.Int32
	maxOpen   atomic.Int32
	dial      func(*tossDialer, string) (quote.Transport, error)
}

func (d *tossDialer) Dial(_ context.Context, endpoint, token string) (quote.Transport, error) {
	d.mu.Lock()
	d.endpoints = append(d.endpoints, endpoint)
	d.tokens = append(d.tokens, token)
	d.mu.Unlock()
	return d.dial(d, token)
}

func (d *tossDialer) conn(onWrite func(*tossConn, []byte)) *tossConn {
	if now := d.open.Add(1); now > d.maxOpen.Load() {
		d.maxOpen.Store(now)
	}
	return &tossConn{in: make(chan []byte, 1024), closed: make(chan struct{}), onWrite: onWrite, release: func() { d.open.Add(-1) }}
}

func (d *tossDialer) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.endpoints)
}

type quoteScenario struct {
	name    string
	dialer  func() *tossDialer
	token   func(*miniredis.Miniredis)
	settled func(*tossDialer, *miniredis.Miniredis) bool
}

const tossTrade = `{"type":"message","topic":"trade:kr:005930","data":{"price":"72000","volume":"1","timestamp":"2026-09-30T13:15:02.000+09:00","currency":"KRW"}}`

func quoteScenarios() []quoteScenario {
	return []quoteScenario{
		{
			name: "healthy quote traffic",
			dialer: func() *tossDialer {
				return &tossDialer{dial: func(d *tossDialer, _ string) (quote.Transport, error) {
					return d.conn(func(c *tossConn, data []byte) {
						tossAck(c, data)
						if bytes.HasPrefix(data, []byte("[")) {
							go func() {
								for index := 0; index < 200; index++ {
									c.push(tossTrade)
								}
							}()
						}
					}), nil
				}}
			},
			settled: func(_ *tossDialer, mini *miniredis.Miniredis) bool {
				entries, _ := mini.Stream("quotes:toss")
				return len(entries) >= 50
			},
		},
		{
			name: "quote dial failure storm",
			dialer: func() *tossDialer {
				return &tossDialer{dial: func(*tossDialer, string) (quote.Transport, error) {
					return nil, errors.New("synthetic Toss dial failure")
				}}
			},
			settled: func(d *tossDialer, _ *miniredis.Miniredis) bool { return d.count() >= 8 },
		},
		{
			name: "quote socket reconnect storm",
			dialer: func() *tossDialer {
				return &tossDialer{dial: func(d *tossDialer, _ string) (quote.Transport, error) {
					return d.conn(func(c *tossConn, data []byte) {
						tossAck(c, data)
						go func() { _ = c.Close() }()
					}), nil
				}}
			},
			// Declarations are spaced a second apart, even across reconnects.
			settled: func(d *tossDialer, _ *miniredis.Miniredis) bool { return d.count() >= 3 },
		},
		{
			name: "server-shutdown frames",
			dialer: func() *tossDialer {
				return &tossDialer{dial: func(d *tossDialer, _ string) (quote.Transport, error) {
					return d.conn(func(c *tossConn, data []byte) {
						tossAck(c, data)
						go c.push(`{"type":"error","error":{"code":"server-shutdown","message":"x"}}`)
					}), nil
				}}
			},
			settled: func(d *tossDialer, _ *miniredis.Miniredis) bool { return d.count() >= 3 },
		},
		{
			name: "cached token refused (401)",
			dialer: func() *tossDialer {
				return &tossDialer{dial: func(*tossDialer, string) (quote.Transport, error) {
					return nil, quote.ErrUnauthorized
				}}
			},
			settled: func(d *tossDialer, _ *miniredis.Miniredis) bool { return d.count() >= 1 },
		},
		{
			name: "no cached token",
			dialer: func() *tossDialer {
				return &tossDialer{dial: func(*tossDialer, string) (quote.Transport, error) { panic("dial without a token") }}
			},
			token: func(mini *miniredis.Miniredis) { mini.Del(testTokenKey) },
			settled: func(*tossDialer, *miniredis.Miniredis) bool {
				return true
			},
		},
		{
			name: "quote dialer panic",
			dialer: func() *tossDialer {
				return &tossDialer{dial: func(*tossDialer, string) (quote.Transport, error) { panic("synthetic quote panic") }}
			},
			settled: func(d *tossDialer, _ *miniredis.Miniredis) bool { return d.count() >= 3 },
		},
	}
}

// quoteClockAt runs at real speed from a fixed Wednesday 13:15 KST, inside
// the KRX regular session, and caps sleeps so retries run quickly.
type quoteClockAt struct{ base, real time.Time }

func (c quoteClockAt) Now() time.Time { return c.base.Add(time.Since(c.real)) }
func (quoteClockAt) After(d time.Duration) <-chan time.Time {
	return time.After(min(d, 2*time.Millisecond))
}

type fillsRun struct {
	entries      []map[string]string
	fillsDials   int
	fillsReqs    []isoRequest
	closedEarly  bool
	approvalKeys map[string]string
	writes       int32
	mainKeys     []string
	tokenValue   string
	quoteDials   int
	maxOpen      int32
	err          error
}

// runFills runs the real runWithDependencies until isoFills fills are in
// the fills stream and the quote scenario (if any) has settled. The quote
// lane reads its token through the real cached-token path from the same
// Redis as the fills stream.
func runFills(t *testing.T, scenario *quoteScenario) fillsRun {
	t.Helper()
	mainRedis := miniredis.RunT(t)
	approvalRedis := miniredis.RunT(t)
	approvalRedis.Set("kis_mock:websocket:approval_key", "fills-cached-key")
	approvalRedis.SetTTL("kis_mock:websocket:approval_key", 20*time.Hour)
	approvalClient := redis.NewClient(&redis.Options{Addr: approvalRedis.Addr()})
	t.Cleanup(func() { _ = approvalClient.Close() })
	writes := &redisWriteHook{}
	approvalClient.AddHook(writes)
	ingest := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(ingest.Close)

	clock := quoteClockAt{base: time.Date(2026, time.September, 30, 13, 15, 0, 0, quote.KST), real: time.Now()}
	tokenValue := fmt.Sprintf(`{"access_token": "toss-live-token", "expires_at": %d}`, clock.base.Add(time.Hour).Unix())
	mainRedis.Set(testTokenKey, tokenValue)

	cfg := cacheOnlyRuntimeConfig(mainRedis.Addr(), ingest.URL)
	cfg.Stream.MaxLen = 1000
	cfg.Channel.Buffer = 16
	cfg.KIS.EventBuffer = 16
	cfg.KIS.DupTrackMax = 100
	fills := fillsDialer()
	deps := runDependencies{approvalRedis: approvalClient, approvalFallback: &runApprovalFallback{}, dialer: fills}
	var quotes *tossDialer
	if scenario != nil {
		quotes = scenario.dialer()
		if scenario.token != nil {
			scenario.token(mainRedis)
		}
		cfg.quotes = quoteSettings{enabled: true, symbols: []quote.Symbol{{Market: quote.MarketKR, Code: "005930"}}, tokenKey: testTokenKey, streamKey: "quotes:toss", maxLen: 1000, buffer: 64}
		deps.quoteDialer = quotes
		deps.quoteClock = clock
		deps.quoteRetryMin = time.Millisecond
		deps.quoteRetryMax = 5 * time.Millisecond
		deps.quotePing = 5 * time.Millisecond
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runWithDependencies(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), deps)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		entries, _ := mainRedis.Stream("fills:test")
		ready := len(entries) == isoFills
		if scenario != nil {
			ready = ready && scenario.settled(quotes, mainRedis)
		}
		if ready {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("fillwire stopped before cancellation: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out: fills=%d", len(entries))
		}
		time.Sleep(5 * time.Millisecond)
	}
	if scenario != nil {
		time.Sleep(30 * time.Millisecond) // let the quote scenario keep churning
	}
	// The fills socket must still be open while the quote lane churned.
	var result fillsRun
	fills.mu.Lock()
	for _, transport := range fills.transports {
		if transport.closedN.Load() != 0 {
			result.closedEarly = true
		}
	}
	fills.mu.Unlock()

	cancel()
	select {
	case result.err = <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("fillwire did not stop")
	}
	entries, _ := mainRedis.Stream("fills:test")
	for _, entry := range entries {
		result.entries = append(result.entries, normalizeFillEntry(t, entry.Values))
	}
	result.fillsDials = fills.count()
	fills.mu.Lock()
	for _, transport := range fills.transports {
		result.fillsReqs = append(result.fillsReqs, transport.requests()...)
	}
	fills.mu.Unlock()
	result.approvalKeys = map[string]string{}
	for _, key := range approvalRedis.Keys() {
		value, _ := approvalRedis.Get(key)
		result.approvalKeys[key] = value
	}
	result.writes = writes.writes.Load()
	result.mainKeys = mainRedis.Keys()
	sort.Strings(result.mainKeys)
	result.tokenValue, _ = mainRedis.Get(testTokenKey)
	if scenario != nil && scenario.token == nil && result.tokenValue != tokenValue {
		t.Fatalf("the cached Toss token changed: %q", result.tokenValue)
	}
	if quotes != nil {
		result.quoteDials = quotes.count()
		result.maxOpen = quotes.maxOpen.Load()
		quotes.mu.Lock()
		for index, endpoint := range quotes.endpoints {
			if endpoint != quote.Endpoint || quotes.tokens[index] != "toss-live-token" {
				t.Fatalf("quote lane dialled %q with an unexpected token", endpoint)
			}
		}
		quotes.mu.Unlock()
	}
	return result
}

// normalizeFillEntry drops only the socket receipt time, which is the wall
// clock at which each run happened to read the frame.
func normalizeFillEntry(t *testing.T, values []string) map[string]string {
	t.Helper()
	fields := map[string]string{}
	for index := 0; index+1 < len(values); index += 2 {
		fields[values[index]] = values[index+1]
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(fields["payload"]), &payload); err != nil {
		t.Fatalf("fills payload: %v", err)
	}
	raw, _ := payload["raw_payload_json"].(map[string]any)
	delete(raw, "received_at")
	normalized, _ := json.Marshal(payload)
	fields["payload"] = string(normalized)
	return fields
}

func TestFillsStreamIdenticalWithQuoteReaderOnAndOff(t *testing.T) {
	off := runFills(t, nil)
	if off.err != nil || len(off.entries) != isoFills {
		t.Fatalf("baseline run err=%v entries=%d", off.err, len(off.entries))
	}
	for _, scenario := range quoteScenarios() {
		t.Run(scenario.name, func(t *testing.T) {
			on := runFills(t, &scenario)
			if on.err != nil {
				t.Fatalf("fillwire returned %v with the quote lane on, want a clean stop", on.err)
			}
			if !reflect.DeepEqual(on.entries, off.entries) {
				t.Fatalf("fills stream differs with quote lane on:\n on=%v\noff=%v", on.entries, off.entries)
			}
			if on.fillsDials != 1 || off.fillsDials != 1 {
				t.Fatalf("fills dials on=%d off=%d, want exactly one each", on.fillsDials, off.fillsDials)
			}
			if on.closedEarly {
				t.Fatal("the fills socket was closed while the quote lane was running")
			}
			if !reflect.DeepEqual(on.fillsReqs, off.fillsReqs) {
				t.Fatalf("fills socket requests on=%v off=%v", on.fillsReqs, off.fillsReqs)
			}
			if !reflect.DeepEqual(on.approvalKeys, off.approvalKeys) || on.writes != 0 || off.writes != 0 {
				t.Fatalf("fills approval cache on=%v (writes %d) off=%v (writes %d)", on.approvalKeys, on.writes, off.approvalKeys, off.writes)
			}
			for _, key := range on.mainKeys {
				if key != "fills:test" && key != "quotes:toss" && key != testTokenKey {
					t.Fatalf("unexpected Redis key %q with the quote lane on", key)
				}
			}
			if on.maxOpen > 1 {
				t.Fatalf("the quote lane held %d Toss connections at once", on.maxOpen)
			}
			if scenario.name == "no cached token" && on.quoteDials != 0 {
				t.Fatalf("dialled %d times without a cached token", on.quoteDials)
			}
			if scenario.name == "cached token refused (401)" && on.quoteDials != 1 {
				t.Fatalf("redialled with a refused token: %d dials", on.quoteDials)
			}
			if scenario.name != "no cached token" && on.quoteDials == 0 {
				t.Fatal("quote lane never dialled; the scenario did not exercise it")
			}
		})
	}
}
