package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	fillreader "github.com/mgh3326/fillwire/internal/reader"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

func TestGatewaydApprovalModeValidation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		mode        string
		url         string
		tokenEnv    string
		tokenHeader string
		wantErr     bool
	}{
		{"gatewayd with url", "gatewayd", "http://127.0.0.1:8791", "", "", false},
		{"gatewayd with auth", "gatewayd", "http://127.0.0.1:8791", "GATEWAYD_TEST_TOKEN", "Authorization", false},
		{"gatewayd missing url", "gatewayd", "", "", "", true},
		{"gatewayd bad scheme", "gatewayd", "ftp://127.0.0.1:8791", "", "", true},
		{"gatewayd url with query", "gatewayd", "http://127.0.0.1:8791?x=1", "", "", true},
		{"gatewayd bad token header", "gatewayd", "http://127.0.0.1:8791", "GATEWAYD_TEST_TOKEN", "Bad Header", true},
		{"cache-only ignores gatewayd table", "cache-only", "", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validStaticConfig()
			cfg.KIS.ApprovalMode = tc.mode
			cfg.KIS.Gatewayd.URL = tc.url
			cfg.KIS.Gatewayd.TokenEnv = tc.tokenEnv
			cfg.KIS.Gatewayd.TokenHeader = tc.tokenHeader
			cfg.Redis.URL = "redis://127.0.0.1:6379/0"
			cfg.Ingest.URL = "http://127.0.0.1:8080/ingest"
			err := cfg.validateStatic()
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateStatic() error = %v, wantErr %t", err, tc.wantErr)
			}
		})
	}
}

func TestGatewaydConfigLoadResolvesTokenEnv(t *testing.T) {
	t.Setenv("KIS_APP_KEY", "fixture-app")
	t.Setenv("KIS_APP_SECRET", "fixture-secret")
	t.Setenv("EXECUTION_LEDGER_INGEST_TOKEN", "fixture-ingest")
	t.Setenv("GATEWAYD_TOKEN", "synthetic-gatewayd-token")
	raw, err := os.ReadFile("../../fillwire.toml")
	if err != nil {
		t.Fatal(err)
	}
	mutated := strings.Replace(string(raw), `approval_mode = ""`, `approval_mode = "gatewayd"`, 1)
	if mutated == string(raw) {
		t.Fatal("fixture replacement failed")
	}
	path := t.TempDir() + "/fillwire.toml"
	if err := os.WriteFile(path, []byte(mutated), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig gatewayd = %v", err)
	}
	if cfg.KIS.ApprovalMode != "gatewayd" || cfg.gatewaydToken != "synthetic-gatewayd-token" || cfg.KIS.Gatewayd.TokenHeader != "Authorization" {
		t.Fatalf("gatewayd config = mode %q token-set %t header %q", cfg.KIS.ApprovalMode, cfg.gatewaydToken != "", cfg.KIS.Gatewayd.TokenHeader)
	}
	// A named token_env that is unset fails config load without echoing values.
	os.Unsetenv("GATEWAYD_TOKEN")
	if _, err := loadConfig(path); err == nil || !strings.Contains(err.Error(), "GATEWAYD_TOKEN") {
		t.Fatalf("missing gatewayd token env error = %v", err)
	}
}

func TestGatewaydConfigRejectsUnknownTableKey(t *testing.T) {
	t.Setenv("KIS_APP_KEY", "fixture-app")
	t.Setenv("KIS_APP_SECRET", "fixture-secret")
	t.Setenv("EXECUTION_LEDGER_INGEST_TOKEN", "fixture-ingest")
	t.Setenv("GATEWAYD_TOKEN", "synthetic-gatewayd-token")
	raw, err := os.ReadFile("../../fillwire.toml")
	if err != nil {
		t.Fatal(err)
	}
	mutated := strings.Replace(string(raw), `approval_mode = ""`, `approval_mode = "gatewayd"`, 1)
	mutated = strings.Replace(mutated, `token_header = "Authorization"`, "token_header = \"Authorization\"\ntokne_env = \"TYPO\"", 1)
	if !strings.Contains(mutated, "tokne_env") {
		t.Fatal("fixture replacement failed")
	}
	path := t.TempDir() + "/fillwire.toml"
	if err := os.WriteFile(path, []byte(mutated), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil || !strings.Contains(err.Error(), "gatewayd") {
		t.Fatalf("unknown gatewayd key error = %v", err)
	}
}

// blockingDialer parks inside the websocket dial until shutdown so a run test
// can observe the approval path without a real KIS connection.
type blockingDialer struct{}

func (blockingDialer) Dial(ctx context.Context, _ string) (ws.Transport, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestGatewaydRunNeverCallsLocalIssuer(t *testing.T) {
	mainRedis := miniredis.RunT(t)
	approvalRedis := miniredis.RunT(t)
	approvalClient := redis.NewClient(&redis.Options{Addr: approvalRedis.Addr()})
	t.Cleanup(func() { _ = approvalClient.Close() })

	var ensureCalls atomic.Int32
	var sawPath, sawAuth atomic.Value
	gatewayd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ensureCalls.Add(1)
		sawPath.Store(r.URL.Path)
		sawAuth.Store(r.Header.Get("Authorization"))
		// gatewayd publishes the shared approval key, then acknowledges.
		approvalRedis.Set("kis_mock:websocket:approval_key", "gatewayd-key")
		approvalRedis.SetTTL("kis_mock:websocket:approval_key", fillreader.ApprovalCacheTTL)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(gatewayd.Close)

	ingest := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(ingest.Close)
	cfg := cacheOnlyRuntimeConfig(mainRedis.Addr(), ingest.URL)
	cfg.KIS.ApprovalMode = "gatewayd"
	cfg.KIS.Gatewayd.URL = gatewayd.URL
	cfg.KIS.Gatewayd.TokenHeader = "Authorization"
	cfg.gatewaydToken = "synthetic-gatewayd-token"

	fallback := &runApprovalFallback{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runWithDependencies(ctx, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), runDependencies{
			approvalRedis:    approvalClient,
			approvalFallback: fallback,
			dialer:           blockingDialer{},
		})
	}()
	deadline := time.Now().Add(3 * time.Second)
	for ensureCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("run did not stop after cancel")
	}
	if ensureCalls.Load() == 0 {
		t.Fatal("gatewayd ensure was never called")
	}
	if got := sawPath.Load(); got != "/v1/tokens/kis-mock-approval/ensure" {
		t.Fatalf("ensure path = %v, want /v1/tokens/kis-mock-approval/ensure", got)
	}
	if got := sawAuth.Load(); got != "synthetic-gatewayd-token" {
		t.Fatal("gatewayd auth header not sent")
	}
	if got := fallback.approvalCalls.Load(); got != 0 {
		t.Fatalf("local issuer approval calls = %d, want 0", got)
	}
	if got := fallback.reissueCalls.Load(); got != 0 {
		t.Fatalf("local issuer reissue calls = %d, want 0", got)
	}
	if approvalRedis.Exists("kis_mock:websocket:approval_key:lock") {
		t.Fatal("gatewayd mode created the issuance lock")
	}
	if value, _ := approvalRedis.Get("kis_mock:websocket:approval_key"); value != "gatewayd-key" {
		t.Fatalf("approval cache = %q, want gatewayd-key", value)
	}
}

func TestBooksTokenEnvSource(t *testing.T) {
	t.Setenv("BOOKS_TEST_TOKEN_ENV", "synthetic-env-token")
	var cfg runtimeConfig
	cfg.Books.AccessTokenEnv = "BOOKS_TEST_TOKEN_ENV"
	token, err := booksToken(context.Background(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := token(); got != "synthetic-env-token" {
		t.Fatalf("env token = %q", got)
	}
}

func TestBooksTokenRedisSource(t *testing.T) {
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	// Live mode reads the fixed token_keys.py key.
	var liveCfg runtimeConfig
	liveCfg.KIS.Endpoint = "live"
	liveCfg.Books.AccessTokenSource = "redis"
	expiresAt := float64(time.Now().Add(time.Hour).Unix())
	payload, _ := json.Marshal(map[string]any{"access_token": "synthetic-live-token", "expires_at": expiresAt, "created_at": expiresAt - 100})
	mini.Set("kis:access_token", string(payload))
	liveToken, err := booksToken(context.Background(), liveCfg, client)
	if err != nil {
		t.Fatal(err)
	}
	if got := liveToken(); got != "synthetic-live-token" {
		t.Fatalf("live redis token = %q", got)
	}

	// Mock mode derives the golden token_keys.py key from the VTS netloc and
	// the app-key fingerprint (same result as broker-edge kismockread).
	var mockCfg runtimeConfig
	mockCfg.KIS.Endpoint = "mock"
	mockCfg.appKey = "synthetic-kis-app-key-golden-87"
	mockCfg.Books.AccessTokenSource = "redis"
	mockKey := "kis_mock:openapivts.koreainvestment.com:29443:599f2c2499498229:access_token"
	payload, _ = json.Marshal(map[string]any{"access_token": "synthetic-mock-token", "expires_at": expiresAt})
	mini.Set(mockKey, string(payload))
	mockToken, err := booksToken(context.Background(), mockCfg, client)
	if err != nil {
		t.Fatal(err)
	}
	if got := mockToken(); got != "synthetic-mock-token" {
		t.Fatalf("mock redis token = %q", got)
	}

	// Missing, expired, and near-expiry entries all read as absent.
	mini.Del(mockKey)
	if got := mockToken(); got != "" {
		t.Fatalf("missing mock token = %q, want empty", got)
	}
	nearExpiry, _ := json.Marshal(map[string]any{"access_token": "synthetic", "expires_at": float64(time.Now().Add(30 * time.Second).Unix())})
	mini.Set(mockKey, string(nearExpiry))
	if got := mockToken(); got != "" {
		t.Fatalf("near-expiry mock token = %q, want empty", got)
	}
	// The reader is read-only: it never creates the key it reads.
	if !mini.Exists("kis:access_token") {
		t.Fatal("live token key vanished")
	}
}

func TestBooksTokenRedisSourceDoesNotMutate(t *testing.T) {
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	var cfg runtimeConfig
	cfg.KIS.Endpoint = "live"
	cfg.Books.AccessTokenSource = "redis"
	expiresAt := float64(time.Now().Add(time.Hour).Unix())
	payload, _ := json.Marshal(map[string]any{"access_token": "synthetic", "expires_at": expiresAt})
	mini.Set("kis:access_token", string(payload))
	mini.SetTTL("kis:access_token", time.Hour)
	token, err := booksToken(context.Background(), cfg, client)
	if err != nil {
		t.Fatal(err)
	}
	_ = token()
	if ttl := mini.TTL("kis:access_token"); ttl <= 0 || ttl > time.Hour {
		t.Fatalf("token TTL mutated: %s", ttl)
	}
	if value, _ := mini.Get("kis:access_token"); value != string(payload) {
		t.Fatal("token payload mutated")
	}
}

func TestBooksStartRejectsBadSourceCombination(t *testing.T) {
	for _, tc := range []struct {
		name    string
		books   bookSettings
		wantErr bool
	}{
		{"neither source", bookSettings{Enabled: true, Symbol: "005930"}, true},
		{"both sources", bookSettings{Enabled: true, Symbol: "005930", AccessTokenEnv: "X", AccessTokenSource: "redis"}, true},
		{"missing symbol", bookSettings{Enabled: true, AccessTokenSource: "redis"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := cacheOnlyRuntimeConfig("127.0.0.1:1", "http://127.0.0.1:1")
			cfg.Books = tc.books
			// The source check runs before any Redis client or producer starts.
			_, _, err := startBooks(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if (err != nil) != tc.wantErr {
				t.Fatalf("startBooks() error = %v, wantErr %t", err, tc.wantErr)
			}
		})
	}
}
