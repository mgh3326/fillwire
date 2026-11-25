package main

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

// withKisQuotes appends body to the shipped configuration, replacing any
// existing [kis_quotes] table.
func withKisQuotes(t *testing.T, shipped, body string) string {
	t.Helper()
	if index := strings.Index(shipped, "\n[kis_quotes]\n"); index >= 0 {
		shipped = shipped[:index+1]
	}
	if body == "" {
		return shipped
	}
	return shipped + "\n" + body + "\n"
}

func TestKisQuoteLaneDefaultOff(t *testing.T) {
	setFillsEnv(t)
	shipped := shippedConfig(t)
	for name, body := range map[string]string{
		"no table":          "",
		"enabled false":     "[kis_quotes]\nenabled = false\n",
		"enabled with junk": "[kis_quotes]\nenabled = false\nsymbols = 7\nsurprise = 1\n",
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := loadConfig(writeTemp(t, "fillwire.toml", withKisQuotes(t, shipped, body)))
			if err != nil {
				t.Fatalf("[kis_quotes] content failed the fills config: %v", err)
			}
			if cfg.kisQuotes.enabled || cfg.kisQuotes.err != nil {
				t.Fatalf("kisQuotes = %+v, want off", cfg.kisQuotes)
			}
			lane, stop := startKisQuotes(t.Context(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
			if lane != nil {
				t.Fatal("disabled lane started")
			}
			stop()
		})
	}
}

func TestKisQuoteLaneRequiresNamedCredentials(t *testing.T) {
	// cache-only approval, books off: the only reason the app key is needed is
	// the enabled [kis_quotes] lane, and the static check must name the env
	// variable instead of failing on an unnamed empty one.
	t.Setenv("EXECUTION_LEDGER_INGEST_TOKEN", "fixture-ingest")
	shipped := shippedConfig(t)
	text := strings.Replace(shipped, `approval_mode = ""`, `approval_mode = "cache-only"`, 1)
	text = strings.Replace(text, `app_key_env = "KIS_APP_KEY"`, `app_key_env = ""`, 1)
	text = withKisQuotes(t, text, "[kis_quotes]\nenabled = true\nsymbols = [\"005930\"]\naccess_token_env = \"KIS_QUOTE_TOKEN\"\n")
	_, err := loadConfig(writeTemp(t, "fillwire.toml", text))
	if err == nil || !strings.Contains(err.Error(), "KIS app_key_env is required") {
		t.Fatalf("loadConfig error = %v, want the named KIS app_key_env requirement", err)
	}
}

func TestKisQuoteSettingsResolution(t *testing.T) {
	setFillsEnv(t)
	shipped := shippedConfig(t)
	load := func(t *testing.T, body string) kisQuoteSettings {
		t.Helper()
		cfg, err := loadConfig(writeTemp(t, "fillwire.toml", withKisQuotes(t, shipped, body)))
		if err != nil {
			t.Fatalf("fills config failed: %v", err)
		}
		return cfg.kisQuotes
	}

	defaults := load(t, "[kis_quotes]\nenabled = true\nsymbols = [\"005930\", \"000660\"]\naccess_token_env = \"KIS_MOCK_ACCESS_TOKEN\"\n")
	if !defaults.enabled || defaults.err != nil {
		t.Fatalf("defaults = %+v", defaults)
	}
	if defaults.limit != kisQuoteDefaultLimit || defaults.minHold != 15*time.Minute || defaults.feedGap != 30*time.Second || defaults.maxLen != 100000 || defaults.buffer != 1024 {
		t.Fatalf("default values = %+v", defaults)
	}
	if len(defaults.symbols) != 2 || defaults.symbols[0] != "005930" || defaults.tokenEnv != "KIS_MOCK_ACCESS_TOKEN" {
		t.Fatalf("symbols/token = %+v", defaults)
	}

	explicit := load(t, `[kis_quotes]
enabled = true
symbols = ["005930"]
registration_limit = 20
min_hold = "5m"
feed_gap = "10s"
max_len = 5000
buffer = 64
access_token_source = "redis"
`)
	if explicit.err != nil || explicit.limit != 20 || explicit.minHold != 5*time.Minute || explicit.feedGap != 10*time.Second || explicit.maxLen != 5000 || explicit.buffer != 64 || explicit.tokenSource != "redis" {
		t.Fatalf("explicit settings = %+v", explicit)
	}
}

func TestKisQuoteSettingsAreRejectedNotFatal(t *testing.T) {
	setFillsEnv(t)
	shipped := shippedConfig(t)
	for name, body := range map[string]string{
		"no symbols":       "[kis_quotes]\nenabled = true\naccess_token_env = \"KIS_MOCK_ACCESS_TOKEN\"\n",
		"bad symbol":       "[kis_quotes]\nenabled = true\nsymbols = [\"abc\"]\naccess_token_env = \"KIS_MOCK_ACCESS_TOKEN\"\n",
		"duplicate symbol": "[kis_quotes]\nenabled = true\nsymbols = [\"005930\", \"005930\"]\naccess_token_env = \"KIS_MOCK_ACCESS_TOKEN\"\n",
		"bad min_hold":     "[kis_quotes]\nenabled = true\nsymbols = [\"005930\"]\nmin_hold = \"soon\"\naccess_token_env = \"KIS_MOCK_ACCESS_TOKEN\"\n",
		"bad feed_gap":     "[kis_quotes]\nenabled = true\nsymbols = [\"005930\"]\nfeed_gap = \"-1s\"\naccess_token_env = \"KIS_MOCK_ACCESS_TOKEN\"\n",
		"bad limit":        "[kis_quotes]\nenabled = true\nsymbols = [\"005930\"]\nregistration_limit = 100000\ntoken = \"secret\"\n",
		"no token source":  "[kis_quotes]\nenabled = true\nsymbols = [\"005930\"]\n",
		"two token sources": `[kis_quotes]
enabled = true
symbols = ["005930"]
access_token_env = "KIS_MOCK_ACCESS_TOKEN"
access_token_source = "redis"
`,
		"unknown key": "[kis_quotes]\nenabled = true\nsymbols = [\"005930\"]\naccess_token_env = \"KIS_MOCK_ACCESS_TOKEN\"\ncredential = \"x\"\n",
		"wrong type":  "[kis_quotes]\nenabled = true\nsymbols = \"005930\"\naccess_token_env = \"KIS_MOCK_ACCESS_TOKEN\"\n",
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := loadConfig(writeTemp(t, "fillwire.toml", withKisQuotes(t, shipped, body)))
			if err != nil {
				t.Fatalf("[kis_quotes] content stopped the fills config: %v", err)
			}
			if !cfg.kisQuotes.enabled || cfg.kisQuotes.err == nil {
				t.Fatalf("kisQuotes = %+v, want enabled with a rejection", cfg.kisQuotes)
			}
			lane, stop := startKisQuotes(t.Context(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
			if lane != nil {
				t.Fatal("rejected lane started")
			}
			stop()
		})
	}
}

type quoteSession struct {
	mu   sync.Mutex
	subs map[string]bool
}

func (s *quoteSession) Subscribe(_ context.Context, tr, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs[tr+"|"+key] = true
	return nil
}

func (s *quoteSession) Unsubscribe(_ context.Context, tr, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subs, tr+"|"+key)
	return nil
}

// A configured lane, attached to one session, registers both promoted streams
// on that session and writes the tick to quotes:kis:<symbol>.
func TestKisQuoteLaneWiringPublishes(t *testing.T) {
	setFillsEnv(t)
	t.Setenv("KIS_QUOTE_TOKEN", "fixture-token")
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	var cfg runtimeConfig
	cfg.Redis.URL = "redis://" + mr.Addr()
	cfg.KIS.Endpoint = "live"
	cfg.kisQuotes = kisQuoteSettings{
		enabled: true, symbols: []string{"005930"}, limit: 41,
		minHold: 15 * time.Minute, feedGap: time.Hour, maxLen: 100, buffer: 16,
		tokenEnv: "KIS_QUOTE_TOKEN",
	}
	lane, stop := startKisQuotes(t.Context(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	if lane == nil {
		t.Fatal("enabled lane did not start")
	}
	defer stop()
	session := &quoteSession{subs: map[string]bool{}}
	lane.Attach(context.Background(), session)
	session.mu.Lock()
	subs := len(session.subs)
	session.mu.Unlock()
	if subs != 2 {
		t.Fatalf("session subscriptions = %d, want trade+book on one session", subs)
	}
	lane.Handle(ws.Event{
		TR:         "H0STCNT0",
		ReceivedAt: time.Date(2026, 11, 2, 9, 0, 5, 0, time.FixedZone("KST", 9*60*60)),
		Raw:        []byte(promotedTradeFrame("005930", "090005", 71000, 10, 1000)),
	})
	deadline := time.Now().Add(2 * time.Second)
	for {
		rows, err := rdb.XRange(context.Background(), "quotes:kis:005930", "-", "+").Result()
		if err != nil {
			t.Fatalf("XRange: %v", err)
		}
		if len(rows) == 1 {
			if rows[0].Values["kind"] != "trade" || rows[0].Values["price"] != "71000" {
				t.Fatalf("published tick = %v", rows[0].Values)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("tick not published")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// promotedTradeFrame builds one full-width H0STCNT0 frame.
func promotedTradeFrame(symbol, hour string, price, qty, acml int64) string {
	fields := make([]string, 46)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0], fields[1] = symbol, hour
	fields[2] = strconv.FormatInt(price, 10)
	fields[12] = strconv.FormatInt(qty, 10)
	fields[13] = strconv.FormatInt(acml, 10)
	fields[21] = "1"
	return "0|H0STCNT0|001|" + strings.Join(fields, "^")
}
