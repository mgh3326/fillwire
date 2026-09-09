package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/fillwire/internal/decode"
	fillreader "github.com/mgh3326/fillwire/internal/reader"
	"github.com/mgh3326/fillwire/internal/stream"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

func TestT20ShutdownDrainsReceivedEventToStream(t *testing.T) {
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	counters := decode.NewCounters()
	queue, err := stream.New(client, stream.Config{
		Key: "fills:kis", MaxLen: 100, Group: "fillwire-ingest", Consumer: "consumer",
		BatchSize: 1, Block: time.Millisecond, ClaimMinIdle: 0, Counters: counters,
	})
	if err != nil {
		t.Fatal(err)
	}
	decoder := decode.New(decode.Config{AccountMode: "live", Venue: "krx", DupTrackMax: 10, Counters: counters})
	events := make(chan ws.Event, 1)
	events <- shutdownFixtureEvent()
	close(events)
	records := make(chan decode.Record, 1)

	shutdownCtx, stopShutdown := context.WithCancel(context.Background())
	stopShutdown() // Model SIGTERM after the socket has already received a fill.
	pipeline := newIngressPipeline(shutdownCtx, events, records, decoder, queue)
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), time.Second)
	defer cancelDrain()
	pipeline.beginDrain(drainCtx)
	done := pipeline.start()
	if err := <-done; err != nil {
		t.Fatalf("shutdown drain = %v, want nil", err)
	}
	length, err := client.XLen(context.Background(), "fills:kis").Result()
	if err != nil {
		t.Fatal(err)
	}
	if length != 1 {
		t.Fatalf("stream length after shutdown drain = %d, want 1", length)
	}
	if got := counters.Snapshot().XAdded; got != 1 {
		t.Fatalf("XADD counter after shutdown drain = %d, want 1", got)
	}
}

func TestT22TransportURLValidation(t *testing.T) {
	tests := []struct {
		name      string
		ingestURL string
		redisURL  string
		wantErr   bool
	}{
		{
			name:      "remote HTTP ingest is rejected",
			ingestURL: "http://auto-trader.example.invalid/trading/api/execution-ledger/fills/ingest",
			redisURL:  "rediss://redis.example.invalid:6380/0",
			wantErr:   true,
		},
		{
			name:      "loopback HTTP ingest is allowed",
			ingestURL: "http://127.0.0.1:8080/trading/api/execution-ledger/fills/ingest",
			redisURL:  "rediss://redis.example.invalid:6380/0",
			wantErr:   false,
		},
		{
			name:      "remote plaintext Redis is rejected",
			ingestURL: "https://auto-trader.example.invalid/trading/api/execution-ledger/fills/ingest",
			redisURL:  "redis://redis.example.invalid:6379/0",
			wantErr:   true,
		},
		{
			name:      "TLS Redis is allowed",
			ingestURL: "https://auto-trader.example.invalid/trading/api/execution-ledger/fills/ingest",
			redisURL:  "rediss://redis.example.invalid:6380/0",
			wantErr:   false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validStaticConfig()
			cfg.Ingest.URL = test.ingestURL
			cfg.Redis.URL = test.redisURL
			err := cfg.validateStatic()
			if (err != nil) != test.wantErr {
				t.Fatalf("validateStatic() error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}

func TestApprovalModeValidation(t *testing.T) {
	tests := []struct {
		name string
		mode string
		want bool
	}{
		{name: "omitted preserves default", mode: "", want: true},
		{name: "cache only", mode: "cache-only", want: true},
		{name: "typo", mode: "cache_only", want: false},
		{name: "unsupported", mode: "rest-only", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validStaticConfig()
			cfg.KIS.ApprovalMode = test.mode
			cfg.Redis.URL = "redis://127.0.0.1:6379/0"
			cfg.Ingest.URL = "http://127.0.0.1:8080/ingest"
			err := cfg.validateStatic()
			if (err == nil) != test.want {
				t.Fatalf("validateStatic() error = %v, want valid=%t", err, test.want)
			}
			if test.want && test.mode == "" && cfg.KIS.ApprovalMode != "" {
				t.Fatalf("omitted approval mode = %q, want empty default", cfg.KIS.ApprovalMode)
			}
		})
	}
}

type runApprovalFallback struct {
	approvalCalls atomic.Int32
	reissueCalls  atomic.Int32
}

func (p *runApprovalFallback) ApprovalKey(context.Context) (string, error) {
	p.approvalCalls.Add(1)
	return "synthetic-rest-key", nil
}

func (p *runApprovalFallback) Reissue(context.Context) (string, error) {
	p.reissueCalls.Add(1)
	return "synthetic-rest-reissue", nil
}

type countingDialer struct{ calls atomic.Int32 }

func (d *countingDialer) Dial(context.Context, string) (ws.Transport, error) {
	d.calls.Add(1)
	return nil, errors.New("unexpected KIS dial in cache-only test")
}

type redisWriteHook struct{ writes atomic.Int32 }

func (h *redisWriteHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *redisWriteHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if isRedisWriteCommand(cmd.Name()) {
			h.writes.Add(1)
		}
		return next(ctx, cmd)
	}
}

func (h *redisWriteHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if isRedisWriteCommand(cmd.Name()) {
				h.writes.Add(1)
			}
		}
		return next(ctx, cmds)
	}
}

func isRedisWriteCommand(name string) bool {
	switch strings.ToUpper(name) {
	case "APPEND", "DEL", "DECR", "DECRBY", "EXPIRE", "EXPIREAT", "HDEL", "HINCRBY", "HINCRBYFLOAT", "HMSET", "HSET", "INCR", "INCRBY", "INCRBYFLOAT", "MSET", "MSETNX", "PERSIST", "PEXPIRE", "PEXPIREAT", "PSETEX", "RENAME", "RENAMENX", "SET", "SETEX", "SETNX", "SADD", "SREM", "XADD":
		return true
	default:
		return false
	}
}

func TestCacheOnlyRunFailsClosedForCacheFailures(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*miniredis.Miniredis)
	}{
		{name: "miss", setup: func(*miniredis.Miniredis) {}},
		{name: "empty", setup: func(mini *miniredis.Miniredis) { mini.Set("kis:websocket:approval_key", "") }},
		{name: "redis error", setup: func(mini *miniredis.Miniredis) { mini.SetError("ERR synthetic redis failure") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mainRedis := miniredis.RunT(t)
			approvalRedis := miniredis.RunT(t)
			test.setup(approvalRedis)
			approvalClient := redis.NewClient(&redis.Options{Addr: approvalRedis.Addr()})
			t.Cleanup(func() { _ = approvalClient.Close() })
			writeHook := &redisWriteHook{}
			approvalClient.AddHook(writeHook)
			ingest := httptest.NewServer(http.NotFoundHandler())
			t.Cleanup(ingest.Close)

			cfg := cacheOnlyRuntimeConfig(mainRedis.Addr(), ingest.URL)
			fallback := &runApprovalFallback{}
			dialer := &countingDialer{}
			started := time.Now()
			err := runWithDependencies(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), runDependencies{
				approvalRedis:    approvalClient,
				approvalFallback: fallback,
				dialer:           dialer,
			})
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("cache-only %s completed in %s, want under 1s", test.name, elapsed)
			}
			if !errors.Is(err, fillreader.ErrCacheOnlyApprovalUnavailable) {
				t.Fatalf("run error = %v, want named cache-only failure", err)
			}
			if got := fillreader.ProcessExitCode(err); got != fillreader.ExitCodeCacheOnlyApprovalUnavailable {
				t.Fatalf("process exit code = %d, want %d", got, fillreader.ExitCodeCacheOnlyApprovalUnavailable)
			}
			if got := fallback.approvalCalls.Load(); got != 0 {
				t.Fatalf("REST approval calls = %d, want 0", got)
			}
			if got := fallback.reissueCalls.Load(); got != 0 {
				t.Fatalf("REST reissue calls = %d, want 0", got)
			}
			if got := dialer.calls.Load(); got != 0 {
				t.Fatalf("KIS dial calls = %d, want 0", got)
			}
			if got := writeHook.writes.Load(); got != 0 {
				t.Fatalf("approval Redis writes = %d, want 0", got)
			}
			if test.name != "redis error" && approvalRedis.Exists("kis:websocket:approval_key") && test.name == "miss" {
				t.Fatal("cache miss created an approval key")
			}
		})
	}
}

func TestCacheOnlyMissMainProcessExit78(t *testing.T) {
	if os.Getenv("FILLWIRE_CACHE_ONLY_EXIT_CHILD") == "1" {
		flag.CommandLine = flag.NewFlagSet("fillwire", flag.ExitOnError)
		os.Args = []string{"fillwire", "-config", os.Getenv("FILLWIRE_CACHE_ONLY_CONFIG")}
		main()
		return
	}

	mini := miniredis.RunT(t)
	ingest := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(ingest.Close)
	config := fmt.Sprintf(`[kis]
broker = "kis"
endpoint = "mock"
account_mode = "mock"
approval_mode = "cache-only"
venue = "krx"
hts_id = "EXAMPLE_HTS_ID"
app_key_env = "FILLWIRE_TEST_APP_KEY"
app_secret_env = "FILLWIRE_TEST_APP_SECRET"
event_buffer = 1
dup_track_max = 1

[redis]
url = "redis://%s/0"

[stream]
key = "fills:test"
max_len = 10
consumer_group = "fillwire-test"
consumer_name = "consumer"
claim_min_idle = "0s"
read_block = "1ms"

[ingest]
url = "%s"
token_env = "FILLWIRE_TEST_INGEST_TOKEN"
batch_size = 1
timeout = "100ms"

[channel]
buffer = 1
drain_timeout = "100ms"

[retry]
min = "1ms"
max = "5ms"
factor = 2
`, mini.Addr(), ingest.URL)
	configPath := t.TempDir() + "/fillwire.toml"
	if err := os.WriteFile(configPath, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestCacheOnlyMissMainProcessExit78$", "-test.v")
	command.Env = append(os.Environ(),
		"FILLWIRE_CACHE_ONLY_EXIT_CHILD=1",
		"FILLWIRE_CACHE_ONLY_CONFIG="+configPath,
		"FILLWIRE_TEST_APP_KEY=synthetic-app-key",
		"FILLWIRE_TEST_APP_SECRET=synthetic-app-secret",
		"FILLWIRE_TEST_INGEST_TOKEN=synthetic-ingest-token",
	)
	output, err := command.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("child error = %v, output = %s", err, output)
	}
	if got := exitErr.ExitCode(); got != fillreader.ExitCodeCacheOnlyApprovalUnavailable {
		t.Fatalf("child exit code = %d, want %d; output = %s", got, fillreader.ExitCodeCacheOnlyApprovalUnavailable, output)
	}
	if mini.Exists("kis:websocket:approval_key") {
		t.Fatal("child cache-only miss created an approval key")
	}
	if strings.Contains(string(output), "synthetic-app-secret") || strings.Contains(string(output), "synthetic-ingest-token") {
		t.Fatalf("child output leaked a credential value: %s", output)
	}
}

func cacheOnlyRuntimeConfig(redisAddr, ingestURL string) runtimeConfig {
	var cfg runtimeConfig
	cfg.KIS.Broker = "kis"
	cfg.KIS.Endpoint = "mock"
	cfg.KIS.AccountMode = "mock"
	cfg.KIS.ApprovalMode = "cache-only"
	cfg.KIS.Venue = "krx"
	cfg.KIS.HTSID = "EXAMPLE_HTS_ID"
	cfg.KIS.EventBuffer = 1
	cfg.KIS.DupTrackMax = 1
	cfg.Redis.URL = "redis://" + redisAddr + "/0"
	cfg.Stream.Key = "fills:test"
	cfg.Stream.MaxLen = 10
	cfg.Stream.Group = "fillwire-test"
	cfg.Stream.Consumer = "consumer"
	cfg.Ingest.URL = ingestURL
	cfg.Ingest.Batch = 1
	cfg.Channel.Buffer = 1
	cfg.Retry.Factor = 2
	cfg.claimMinIdle = 0
	cfg.readBlock = time.Millisecond
	cfg.timeout = 100 * time.Millisecond
	cfg.retryMin = time.Millisecond
	cfg.retryMax = 5 * time.Millisecond
	cfg.drainTimeout = 100 * time.Millisecond
	cfg.appKey = "synthetic-app-key"
	cfg.appSecret = "synthetic-app-secret"
	cfg.ingestToken = "synthetic-ingest-token"
	return cfg
}

func validStaticConfig() runtimeConfig {
	var cfg runtimeConfig
	cfg.KIS.Broker = "kis"
	cfg.KIS.Endpoint = "live"
	cfg.KIS.AccountMode = "live"
	cfg.KIS.Venue = "krx"
	cfg.KIS.HTSID = "EXAMPLE_HTS_ID"
	cfg.KIS.AppKeyEnv = "KIS_APP_KEY"
	cfg.KIS.AppSecretEnv = "KIS_APP_SECRET"
	cfg.KIS.EventBuffer = 1
	cfg.KIS.DupTrackMax = 1
	cfg.Stream.Key = "fills:kis"
	cfg.Stream.MaxLen = 1
	cfg.Stream.Group = "fillwire-ingest"
	cfg.Stream.Consumer = "consumer"
	cfg.Ingest.TokenEnv = "EXECUTION_LEDGER_INGEST_TOKEN"
	cfg.Ingest.Batch = 1
	cfg.Channel.Buffer = 1
	return cfg
}

func shutdownFixtureEvent() ws.Event {
	fields := []string{"HTS_EXAMPLE", "00000000", "A123456789", "0000000000", "02", "00", "00", "00", "005930", "3", "71200", "093015", "0", "2"}
	return ws.Event{
		TR:         ws.TRExecutionLive,
		Fields:     fields,
		ReceivedAt: time.Date(2026, time.September, 7, 9, 30, 20, 0, time.FixedZone("KST", 9*60*60)),
		Execution: &ws.Execution{
			OrderNo:  "A123456789",
			Symbol:   "005930",
			Side:     ws.SideBuy,
			Qty:      "3",
			Price:    "71200",
			FilledAt: "093015",
			Filled:   "2",
		},
	}
}
