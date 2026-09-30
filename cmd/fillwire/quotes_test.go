package main

import (
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
		fmt.Fprintf(&builder, "%06d\n", index+1)
	}
	return writeTemp(t, "symbols.txt", builder.String())
}

func enabledQuotes(symbolsPath string) string {
	return fmt.Sprintf(`[quotes]
enabled = true
endpoint = "live"
app_key_env = "KIS_QUOTE_APP_KEY"
app_secret_env = "KIS_QUOTE_APP_SECRET"
symbols_file = %q
stream_key = "quotes:kis"
max_len = 50000
buffer = 512
`, symbolsPath)
}

func fillsOnly(cfg runtimeConfig) runtimeConfig {
	cfg.quotes = quoteSettings{}
	return cfg
}

func TestQuoteReaderDefaultOff(t *testing.T) {
	setFillsEnv(t)
	shipped := shippedConfig(t)
	if !strings.Contains(shipped, "[quotes]\nenabled = false\n") {
		t.Fatal("shipped fillwire.toml must show the quote reader disabled")
	}
	cfg, err := loadConfig(writeTemp(t, "fillwire.toml", shipped))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.quotes.enabled || cfg.quotes.err != nil {
		t.Fatalf("shipped quotes = %+v, want disabled", cfg.quotes)
	}
	// A pre-upgrade file without the table is also off.
	index := strings.Index(shipped, "\n[quotes]\n")
	legacy := withQuotes(t, shipped, "")
	if index < 0 || strings.Contains(legacy, "[quotes]") {
		t.Fatal("fixture mutation failed")
	}
	legacyCfg, err := loadConfig(writeTemp(t, "fillwire.toml", legacy))
	if err != nil {
		t.Fatal(err)
	}
	if legacyCfg.quotes.enabled {
		t.Fatal("config without [quotes] enabled the quote reader")
	}
	// enabled omitted inside the table is off too.
	omitted, err := loadConfig(writeTemp(t, "fillwire.toml", withQuotes(t, shipped, "[quotes]\nendpoint = \"live\"\n")))
	if err != nil || omitted.quotes.enabled {
		t.Fatalf("omitted enabled = %+v, %v; want off", omitted.quotes, err)
	}
	// Off means no quote startup at all.
	lane := startQuoteLane(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), runDependencies{quoteDialer: &panicDialer{}})
	if lane.done != nil || lane.stop != nil {
		t.Fatal("disabled quote reader started a lane")
	}
}

type panicDialer struct{}

func (panicDialer) Dial(context.Context, string) (ws.Transport, error) {
	panic("a disabled quote lane must never dial")
}

func TestQuoteSectionNeverChangesFillsConfig(t *testing.T) {
	setFillsEnv(t)
	t.Setenv("KIS_QUOTE_APP_KEY", "fixture-quote-app")
	t.Setenv("KIS_QUOTE_APP_SECRET", "fixture-quote-secret")
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
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := loadConfig(writeTemp(t, "fillwire.toml", withQuotes(t, shipped, body)))
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
	t.Setenv("KIS_QUOTE_APP_KEY", "fixture-quote-app")
	t.Setenv("KIS_QUOTE_APP_SECRET", "fixture-quote-secret")
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
	if !valid.enabled || valid.err != nil || len(valid.symbols) != 40 || valid.streamKey != "quotes:kis" || valid.maxLen != 50000 || valid.buffer != 512 || valid.endpoint != "live" {
		t.Fatalf("valid quote settings = %+v", valid)
	}
	defaults := load(t, strings.NewReplacer("stream_key = \"quotes:kis\"\n", "", "max_len = 50000\n", "", "buffer = 512\n", "").Replace(enabledQuotes(symbolFile(t, 1))))
	if defaults.err != nil || defaults.streamKey != quote.DefaultStreamKey || defaults.maxLen != defaultQuoteMaxLen || defaults.buffer != defaultQuoteBuffer {
		t.Fatalf("defaulted quote settings = %+v", defaults)
	}

	malformed := writeTemp(t, "bad.txt", "005930\n5930\n")
	for name, body := range map[string]string{
		"41 symbols":            enabledQuotes(symbolFile(t, 41)),
		"malformed list":        enabledQuotes(malformed),
		"missing list file":     enabledQuotes(filepath.Join(t.TempDir(), "absent.txt")),
		"no symbols_file":       strings.Replace(enabledQuotes("x"), "symbols_file = \"x\"\n", "", 1),
		"bad endpoint":          strings.Replace(enabledQuotes(symbolFile(t, 1)), "endpoint = \"live\"", "endpoint = \"prod\"", 1),
		"fills app key env":     strings.Replace(enabledQuotes(symbolFile(t, 1)), "KIS_QUOTE_APP_KEY", "KIS_APP_KEY", 1),
		"fills app secret env":  strings.Replace(enabledQuotes(symbolFile(t, 1)), "KIS_QUOTE_APP_SECRET", "KIS_APP_SECRET", 1),
		"unset key env":         strings.Replace(enabledQuotes(symbolFile(t, 1)), "KIS_QUOTE_APP_KEY", "FILLWIRE_TEST_UNSET_QUOTE_KEY", 1),
		"fills stream key":      strings.Replace(enabledQuotes(symbolFile(t, 1)), "stream_key = \"quotes:kis\"", "stream_key = \"fills:kis\"", 1),
		"padded stream key":     strings.Replace(enabledQuotes(symbolFile(t, 1)), "stream_key = \"quotes:kis\"", "stream_key = \" quotes:kis\"", 1),
		"negative max_len":      strings.Replace(enabledQuotes(symbolFile(t, 1)), "max_len = 50000", "max_len = -1", 1),
		"negative buffer":       strings.Replace(enabledQuotes(symbolFile(t, 1)), "buffer = 512", "buffer = -1", 1),
		"type error in enabled": "[quotes]\nenabled = \"yes\"\n",
	} {
		t.Run(name, func(t *testing.T) {
			settings := load(t, body)
			if !settings.enabled || settings.err == nil {
				t.Fatalf("%s: settings = %+v, want enabled with a rejection", name, settings)
			}
			for _, secret := range []string{"fixture-quote-app", "fixture-quote-secret", "fixture-fills-app"} {
				if strings.Contains(settings.err.Error(), secret) {
					t.Fatalf("rejection leaked a credential: %v", settings.err)
				}
			}
		})
	}

	t.Run("same app key value as fills", func(t *testing.T) {
		t.Setenv("KIS_QUOTE_APP_KEY", "fixture-fills-app")
		settings := load(t, enabledQuotes(symbolFile(t, 1)))
		if settings.err == nil || !strings.Contains(settings.err.Error(), "one KIS app key admits one websocket session") {
			t.Fatalf("shared app key settings = %+v, want rejection", settings)
		}
		if strings.Contains(settings.err.Error(), "fixture-fills-app") {
			t.Fatal("rejection leaked the app key")
		}
	})
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
	lane := startQuoteLane(cfg, logger, runDependencies{quoteDialer: &panicDialer{}})
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

type quoteScenario struct {
	name    string
	dialer  func() *isoDialer
	settled func(*isoDialer, *miniredis.Miniredis) bool
}

func quoteScenarios() []quoteScenario {
	quoteTrade := "0|H0STCNT0|001|005930^131502^70500"
	return []quoteScenario{
		{
			name: "healthy quote traffic",
			dialer: func() *isoDialer {
				return &isoDialer{dial: func(d *isoDialer) (ws.Transport, error) {
					return d.add(newIsoTransport(func(t *isoTransport, req isoRequest) {
						if req.trType != "1" {
							return
						}
						t.push(isoAck(req.tr, "0", "OPSP0000"))
						if req.tr == ws.TRQuoteBook {
							go func() {
								for index := 0; index < 200; index++ {
									t.push(quoteTrade)
								}
							}()
						}
					})), nil
				}}
			},
			settled: func(_ *isoDialer, mini *miniredis.Miniredis) bool {
				entries, _ := mini.Stream("quotes:kis")
				return len(entries) >= 50
			},
		},
		{
			name: "quote dial failure storm",
			dialer: func() *isoDialer {
				return &isoDialer{dial: func(*isoDialer) (ws.Transport, error) {
					return nil, errors.New("synthetic quote dial failure")
				}}
			},
			// The lane's own retry delay doubles from 1ms, so dials slow down.
			settled: func(d *isoDialer, _ *miniredis.Miniredis) bool { return d.count() >= 8 },
		},
		{
			name: "quote socket reconnect storm",
			dialer: func() *isoDialer {
				return &isoDialer{dial: func(d *isoDialer) (ws.Transport, error) {
					var acked atomic.Int32
					return d.add(newIsoTransport(func(t *isoTransport, req isoRequest) {
						if req.trType != "1" {
							return
						}
						t.push(isoAck(req.tr, "0", "OPSP0000"))
						// Drop every socket once all four subscriptions are
						// restored, driving go-kis's own reconnect loop.
						if acked.Add(1) == 4 {
							go func() { _ = t.Close() }()
						}
					})), nil
				}}
			},
			settled: func(d *isoDialer, _ *miniredis.Miniredis) bool { return d.count() >= 30 },
		},
		{
			name: "quote app key occupied",
			dialer: func() *isoDialer {
				return &isoDialer{dial: func(d *isoDialer) (ws.Transport, error) {
					return d.add(newIsoTransport(func(t *isoTransport, req isoRequest) {
						if req.trType == "1" {
							t.push(isoAck(req.tr, "1", "OPSP8996"))
						}
					})), nil
				}}
			},
			settled: func(d *isoDialer, _ *miniredis.Miniredis) bool { return d.count() >= 5 },
		},
		{
			name: "quote lane panic",
			dialer: func() *isoDialer {
				return &isoDialer{dial: func(*isoDialer) (ws.Transport, error) { panic("synthetic quote panic") }}
			},
			settled: func(*isoDialer, *miniredis.Miniredis) bool { return true },
		},
	}
}

type quoteApprovalCounter struct{ calls atomic.Int32 }

func (a *quoteApprovalCounter) ApprovalKey(context.Context) (string, error) {
	a.calls.Add(1)
	return "quote-lane-key", nil
}
func (a *quoteApprovalCounter) Reissue(context.Context) (string, error) {
	a.calls.Add(1)
	return "quote-lane-key", nil
}

// quoteClockAt runs at real speed from a fixed Wednesday 13:15 KST, inside
// the regular window, and caps sleeps so retries run quickly.
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
	quoteDials   int
	quoteKeys    int32
	err          error
}

// runFills runs the real runWithDependencies until isoFills fills are in
// the fills stream and the quote scenario (if any) has settled.
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

	cfg := cacheOnlyRuntimeConfig(mainRedis.Addr(), ingest.URL)
	cfg.Stream.MaxLen = 1000
	cfg.Channel.Buffer = 16
	cfg.KIS.EventBuffer = 16
	cfg.KIS.DupTrackMax = 100
	fills := fillsDialer()
	deps := runDependencies{approvalRedis: approvalClient, approvalFallback: &runApprovalFallback{}, dialer: fills}
	var quotes *isoDialer
	quoteApproval := &quoteApprovalCounter{}
	if scenario != nil {
		quotes = scenario.dialer()
		cfg.quotes = quoteSettings{enabled: true, endpoint: "mock", appKey: "quote-app", appSecret: "quote-secret", symbols: []string{"005930", "000660"}, streamKey: "quotes:kis", maxLen: 1000, buffer: 64}
		deps.quoteDialer = quotes
		deps.quoteApproval = quoteApproval
		deps.quoteClock = quoteClockAt{base: time.Date(2026, time.September, 30, 13, 15, 0, 0, quote.KST), real: time.Now()}
		deps.quoteRetryMin = time.Millisecond
		deps.quoteBackoff = ws.BackoffConfig{Min: time.Millisecond, Max: 2 * time.Millisecond, Jitter: -1}
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
			t.Fatalf("timed out: fills=%d quote dials=%v", len(entries), quotes != nil && quotes.count() > 0)
		}
		time.Sleep(5 * time.Millisecond)
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
	if quotes != nil {
		result.quoteDials = quotes.count()
		result.quoteKeys = quoteApproval.calls.Load()
		for _, endpoint := range quotes.endpoints {
			if endpoint != ws.EndpointVTS {
				t.Fatalf("quote lane dialled %q", endpoint)
			}
		}
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
			for _, req := range on.fillsReqs {
				if req.tr == ws.TRQuotePrice || req.tr == ws.TRQuoteBook {
					t.Fatalf("a quote subscription reached the fills socket: %+v", req)
				}
			}
			if !reflect.DeepEqual(on.approvalKeys, off.approvalKeys) || on.writes != 0 || off.writes != 0 {
				t.Fatalf("fills approval cache on=%v (writes %d) off=%v (writes %d)", on.approvalKeys, on.writes, off.approvalKeys, off.writes)
			}
			for _, key := range on.mainKeys {
				if key != "fills:test" && key != "quotes:kis" {
					t.Fatalf("unexpected Redis key %q with the quote lane on", key)
				}
			}
			if scenario.name != "quote lane panic" && (on.quoteDials == 0 || on.quoteKeys == 0) {
				t.Fatalf("quote lane dials=%d own approval calls=%d; the scenario did not exercise its own socket and key", on.quoteDials, on.quoteKeys)
			}
		})
	}
}
