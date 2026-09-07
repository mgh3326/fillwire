package main

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/fillwire/internal/decode"
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
