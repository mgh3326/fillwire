package main

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/fillwire/internal/quote"
	"github.com/mgh3326/fillwire/internal/tokenfetch"
	"github.com/mgh3326/fillwire/internal/tokenfetch/fetchtest"
	"github.com/redis/go-redis/v9"
)

const fetchTestCredential = "synthetic-fetch-credential"

func TestGatewaydFetchApprovalModeValidation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		url         string
		tokenEnv    string
		tokenHeader string
		wantErr     bool
	}{
		{"https remote", "https://gatewayd.example.invalid:8791", "GATEWAYD_TEST_TOKEN", "Authorization", false},
		{"loopback http", "http://127.0.0.1:8791", "GATEWAYD_TEST_TOKEN", "Authorization", false},
		{"credential http remote", "http://gatewayd.example.invalid:8791", "GATEWAYD_TEST_TOKEN", "Authorization", true},
		{"no credential", "https://gatewayd.example.invalid:8791", "", "Authorization", true},
		{"custom header", "https://gatewayd.example.invalid:8791", "GATEWAYD_TEST_TOKEN", "X-Gatewayd-Token", true},
		{"missing url", "", "GATEWAYD_TEST_TOKEN", "Authorization", true},
		{"url with query", "https://gatewayd.example.invalid:8791?x=1", "GATEWAYD_TEST_TOKEN", "Authorization", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validStaticConfig()
			cfg.KIS.ApprovalMode = "gatewayd-fetch"
			cfg.KIS.Gatewayd.URL = tc.url
			cfg.KIS.Gatewayd.TokenEnv = tc.tokenEnv
			cfg.KIS.Gatewayd.TokenHeader = tc.tokenHeader
			cfg.Redis.URL = "redis://127.0.0.1:6379/0"
			cfg.Ingest.URL = "http://127.0.0.1:8080/ingest"
			if err := cfg.validateStatic(); (err != nil) != tc.wantErr {
				t.Fatalf("validateStatic() error = %v, wantErr %t", err, tc.wantErr)
			}
		})
	}
	cfg := validStaticConfig()
	cfg.KIS.ApprovalMode = "gatewayd-fetched"
	cfg.Redis.URL = "redis://127.0.0.1:6379/0"
	cfg.Ingest.URL = "http://127.0.0.1:8080/ingest"
	if err := cfg.validateStatic(); err == nil {
		t.Fatal("unknown approval mode accepted")
	}
}

func TestGatewaydFetchConfigLoadResolvesCredentialAndNeedsNoAppCredentials(t *testing.T) {
	t.Setenv("EXECUTION_LEDGER_INGEST_TOKEN", "fixture-ingest")
	t.Setenv("GATEWAYD_TOKEN", "synthetic-gatewayd-token")
	t.Setenv("KIS_APP_KEY", "")
	t.Setenv("KIS_APP_SECRET", "")
	os.Unsetenv("KIS_APP_KEY")
	os.Unsetenv("KIS_APP_SECRET")
	mutated := strings.Replace(shippedConfig(t), `approval_mode = ""`, `approval_mode = "gatewayd-fetch"`, 1)
	path := writeTemp(t, "fillwire.toml", mutated)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig gatewayd-fetch = %v", err)
	}
	if cfg.KIS.ApprovalMode != "gatewayd-fetch" || cfg.gatewaydToken != "synthetic-gatewayd-token" || cfg.appKey != "" {
		t.Fatalf("config = mode %q token-set %t app key %q", cfg.KIS.ApprovalMode, cfg.gatewaydToken != "", cfg.appKey)
	}
	os.Unsetenv("GATEWAYD_TOKEN")
	if _, err := loadConfig(path); err == nil || !strings.Contains(err.Error(), "GATEWAYD_TOKEN") {
		t.Fatalf("missing credential env error = %v", err)
	}
}

// TestGatewaydFetchRunUsesGatewaydOnly runs the real wiring against a fake
// /fetch server: the approval key comes from it, and neither the approval
// Redis nor a local issuer is touched.
func TestGatewaydFetchRunUsesGatewaydOnly(t *testing.T) {
	mainRedis := miniredis.RunT(t)
	approvalRedis := miniredis.RunT(t)
	approvalClient := redis.NewClient(&redis.Options{Addr: approvalRedis.Addr()})
	t.Cleanup(func() { _ = approvalClient.Close() })
	server := fetchtest.New(&fetchtest.Clock{}, fetchTestCredential)
	t.Cleanup(server.Close)
	ingest := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(ingest.Close)

	cfg := cacheOnlyRuntimeConfig(mainRedis.Addr(), ingest.URL)
	cfg.KIS.ApprovalMode = "gatewayd-fetch"
	cfg.KIS.Gatewayd.URL = server.URL
	cfg.KIS.Gatewayd.TokenHeader = "Authorization"
	cfg.gatewaydToken = fetchTestCredential
	cfg.appKey, cfg.appSecret = "", ""

	fallback := &runApprovalFallback{}
	var logs lockedLogs
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runWithDependencies(ctx, cfg, slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})), runDependencies{
			approvalRedis:    approvalClient,
			approvalFallback: fallback,
			dialer:           blockingDialer{},
		})
	}()
	deadline := time.Now().Add(3 * time.Second)
	for len(server.Calls()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("run did not stop after cancel")
	}
	calls := server.Calls()
	if len(calls) != 1 || calls[0].Provider != "kis-mock-approval" || calls[0].Auth != "Bearer "+fetchTestCredential || calls[0].Body != "{}" {
		t.Fatalf("fetch calls = %+v", calls)
	}
	if fallback.approvalCalls.Load() != 0 || fallback.reissueCalls.Load() != 0 {
		t.Fatal("local issuer was used")
	}
	if got := approvalRedis.CommandCount(); got != 0 {
		t.Fatalf("approval Redis saw %d commands, want 0", got)
	}
	if strings.Contains(logs.String(), "fake-kis-mock-approval-token") || strings.Contains(logs.String(), fetchTestCredential) {
		t.Fatalf("logs leak a secret:\n%s", logs.String())
	}
}

type lockedLogs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *lockedLogs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}
func (l *lockedLogs) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.buf.String() }

func TestQuoteFetchSettingsResolution(t *testing.T) {
	setFillsEnv(t)
	t.Setenv("GATEWAYD_TOKEN", fetchTestCredential)
	shipped := shippedConfig(t)
	symbols := symbolFile(t, 1)
	fetchBody := func(extra string) string {
		return strings.Replace(enabledQuotes(symbols), `token_key = "toss:oauth:0123456789abcdef:access_token"`, `token_source = "gatewayd-fetch"`+extra, 1)
	}
	gatewaydTable := func(url, header string) string {
		return strings.Replace(strings.Replace(shipped, `url = "https://gatewayd.example.invalid:8791"`, `url = "`+url+`"`, 1), `token_header = "Authorization"`, `token_header = "`+header+`"`, 1)
	}
	load := func(t *testing.T, text string) runtimeConfig {
		t.Helper()
		cfg, err := loadConfig(writeTemp(t, "fillwire.toml", text))
		if err != nil {
			t.Fatalf("fills config must not fail on the quote lane: %v", err)
		}
		return cfg
	}
	ok := load(t, withQuotes(t, shipped, fetchBody("")))
	if q := ok.quotes; !q.enabled || q.err != nil || !q.fetch || q.tokenKey != "" || q.fetchURL != "https://gatewayd.example.invalid:8791" || q.fetchCredential != fetchTestCredential {
		t.Fatalf("fetch quote settings = %+v", q)
	}
	if redis := load(t, withQuotes(t, shipped, enabledQuotes(symbols))); redis.quotes.fetch || redis.quotes.tokenKey != testTokenKey {
		t.Fatalf("default token source changed: %+v", redis.quotes)
	}
	explicit := strings.Replace(enabledQuotes(symbols), "stream_key", `token_source = "redis"`+"\nstream_key", 1)
	if q := load(t, withQuotes(t, shipped, explicit)).quotes; q.err != nil || q.fetch || q.tokenKey != testTokenKey {
		t.Fatalf("explicit redis source = %+v", q)
	}

	for name, text := range map[string]string{
		"token_key alongside":    withQuotes(t, shipped, strings.Replace(enabledQuotes(symbols), "stream_key", `token_source = "gatewayd-fetch"`+"\nstream_key", 1)),
		"unknown source":         withQuotes(t, shipped, strings.Replace(fetchBody(""), "gatewayd-fetch", "memory", 1)),
		"http remote credential": withQuotes(t, gatewaydTable("http://gatewayd.example.invalid:8791", "Authorization"), fetchBody("")),
		"bad url":                withQuotes(t, gatewaydTable("ftp://gatewayd.example.invalid", "Authorization"), fetchBody("")),
		"custom header":          withQuotes(t, gatewaydTable("https://gatewayd.example.invalid:8791", "X-Token"), fetchBody("")),
	} {
		t.Run(name, func(t *testing.T) {
			q := load(t, text).quotes
			if !q.enabled || q.err == nil {
				t.Fatalf("settings = %+v, want enabled with a rejection", q)
			}
			if strings.Contains(q.err.Error(), fetchTestCredential) {
				t.Fatalf("rejection leaked the credential: %v", q.err)
			}
		})
	}
	// An unset credential variable disables only the lane.
	os.Unsetenv("GATEWAYD_TOKEN")
	if q := load(t, withQuotes(t, shipped, fetchBody(""))).quotes; q.err == nil {
		t.Fatalf("unset credential env accepted: %+v", q)
	}
}

// TestQuoteLaneFetchesTokenAndReissuesAfter401 starts the real lane with the
// fetch source: no Redis token key exists, the first token is refused, and the
// lane dials again with the token gatewayd reissues for the refused hash.
func TestQuoteLaneFetchesTokenAndReissuesAfter401(t *testing.T) {
	mini := miniredis.RunT(t)
	server := fetchtest.New(&fetchtest.Clock{}, fetchTestCredential)
	t.Cleanup(server.Close)
	client, err := tokenfetch.NewClient(server.URL, fetchTestCredential)
	if err != nil {
		t.Fatal(err)
	}
	var cfg runtimeConfig
	cfg.Redis.URL = "redis://" + mini.Addr() + "/0"
	cfg.quotes = quoteSettings{enabled: true, symbols: []quote.Symbol{{Market: quote.MarketKR, Code: "005930"}}, fetch: true, streamKey: "quotes:toss", maxLen: 100, buffer: 16}
	first := "fake-toss-token-1"
	dialer := &tossDialer{}
	dialer.dial = func(d *tossDialer, token string) (quote.Transport, error) {
		if token == first {
			return nil, quote.ErrUnauthorized
		}
		return d.conn(tossAck), nil
	}
	clock := quoteClockAt{base: time.Date(2026, time.September, 30, 13, 15, 0, 0, quote.KST), real: time.Now()}
	var logs lockedLogs
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	lane := startQuoteLane(cfg, logger, runDependencies{
		quoteFetcher: client, quoteDialer: dialer, quoteClock: clock,
		quoteRetryMin: time.Millisecond, quoteRetryMax: 5 * time.Millisecond, quotePing: 5 * time.Millisecond,
	})
	if lane.done == nil {
		t.Fatalf("lane did not start: %s", logs.String())
	}
	deadline := time.Now().Add(5 * time.Second)
	for dialer.count() < 2 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	lane.halt()
	lane.wait(time.Now().Add(5*time.Second), logger)
	dialer.mu.Lock()
	tokens := append([]string(nil), dialer.tokens...)
	dialer.mu.Unlock()
	if len(tokens) < 2 || tokens[0] != first || tokens[1] == first {
		t.Fatalf("dial tokens = %v", tokens)
	}
	calls := server.Calls()
	if len(calls) < 2 || calls[0].Provider != "toss" || calls[0].Failed != "" || calls[1].Failed != tokenfetch.Hash(first) {
		t.Fatalf("fetch calls = %+v", calls)
	}
	if mini.Exists(testTokenKey) {
		t.Fatal("token key appeared in Redis")
	}
	if strings.Contains(logs.String(), "fake-toss-token") || strings.Contains(logs.String(), fetchTestCredential) {
		t.Fatalf("logs leak a secret:\n%s", logs.String())
	}
}
