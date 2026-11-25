package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/fillwire/internal/kisfutures"
	"github.com/mgh3326/fillwire/internal/kst"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

// withKisFutures replaces the shipped [kis_futures] table with body.
func withKisFutures(t *testing.T, shipped, body string) string {
	t.Helper()
	index := strings.Index(shipped, "\n[kis_futures]\n")
	if index < 0 {
		t.Fatal("shipped fillwire.toml has no [kis_futures] table")
	}
	rest := shipped[index+1:]
	tail := ""
	if end := strings.Index(rest[len("[kis_futures]\n"):], "\n["); end >= 0 {
		tail = rest[len("[kis_futures]\n")+end:]
	}
	return shipped[:index+1] + body + tail
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestKisFuturesDefaultOff(t *testing.T) {
	setFillsEnv(t)
	shipped := shippedConfig(t)
	if !strings.Contains(shipped, "[kis_futures]\nenabled = false\ningest = false") {
		t.Fatal("shipped fillwire.toml must show the futures lane and its ingest switch off")
	}
	for name, text := range map[string]string{
		"shipped":           shipped,
		"no table":          withKisFutures(t, shipped, ""),
		"enabled omitted":   withKisFutures(t, shipped, "[kis_futures]\njsonl_path = \"x\"\n"),
		"disabled, junk":    withKisFutures(t, shipped, "[kis_futures]\nenabled = false\nmax_len = \"x\"\ntoken = \"y\"\n"),
		"ingest false only": withKisFutures(t, shipped, "[kis_futures]\ningest = false\n"),
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := loadConfig(writeTemp(t, "fillwire.toml", text))
			if err != nil {
				t.Fatalf("[kis_futures] content failed the fills config: %v", err)
			}
			if cfg.kisFutures.enabled || cfg.kisFutures.err != nil {
				t.Fatalf("kisFutures = %+v, want off", cfg.kisFutures)
			}
			lane, slots, stop := startKisFutures(cfg, quietLogger(), runDependencies{futuresMaster: panicMaster{}})
			if lane != nil || slots != nil {
				t.Fatal("disabled lane started")
			}
			stop()
		})
	}
}

type panicMaster struct{}

func (panicMaster) Master(context.Context) ([]kisfutures.MasterRow, error) {
	panic("a disabled futures lane must never read the master")
}

// The ingest switch is refused at config load, with the lane on or off.
func TestKisFuturesIngestRefusedAtConfigLoad(t *testing.T) {
	setFillsEnv(t)
	shipped := shippedConfig(t)
	for name, body := range map[string]string{
		"lane off":      "[kis_futures]\nenabled = false\ningest = true\n",
		"lane on":       "[kis_futures]\nenabled = true\ningest = true\ncalendar_file = \"c.json\"\n",
		"lane omitted":  "[kis_futures]\ningest = true\n",
		"not a boolean": "[kis_futures]\ningest = \"false\"\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := loadConfig(writeTemp(t, "fillwire.toml", withKisFutures(t, shipped, body)))
			if err == nil || !strings.Contains(err.Error(), "ingest must stay false") || !strings.Contains(err.Error(), "hk 6430") {
				t.Fatalf("loadConfig error = %v, want the ingest refusal", err)
			}
		})
	}
}

func calendarPath(t *testing.T, now time.Time) string {
	t.Helper()
	day := now.In(kst.Zone)
	return writeTemp(t, "calendar.json", `{"from":"`+day.AddDate(0, 0, -10).Format(time.DateOnly)+
		`","to":"`+day.AddDate(0, 0, 40).Format(time.DateOnly)+`","kr_closed":[]}`)
}

func TestKisFuturesRejectedLaneIsNotFatal(t *testing.T) {
	setFillsEnv(t)
	shipped := shippedConfig(t)
	calendar := calendarPath(t, time.Now())
	good := "[kis_futures]\nenabled = true\ncalendar_file = " + strconv.Quote(calendar) + "\n"
	for name, body := range map[string]string{
		"no calendar":   "[kis_futures]\nenabled = true\n",
		"unknown key":   good + "endpoint = \"x\"\n",
		"wrong type":    good + "max_len = \"many\"\n",
		"negative":      good + "max_len = -1\n",
		"enabled text":  "[kis_futures]\nenabled = \"yes\"\n",
		"calendar gone": "[kis_futures]\nenabled = true\ncalendar_file = \"/nonexistent/calendar.json\"\n",
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := loadConfig(writeTemp(t, "fillwire.toml", withKisFutures(t, shipped, body)))
			if err != nil {
				t.Fatalf("[kis_futures] content stopped the fills config: %v", err)
			}
			if !cfg.kisFutures.enabled {
				t.Fatalf("kisFutures = %+v", cfg.kisFutures)
			}
			lane, slots, stop := startKisFutures(cfg, quietLogger(), runDependencies{futuresMaster: panicMaster{}})
			defer stop()
			if lane != nil || slots != nil {
				t.Fatal("rejected lane started")
			}
		})
	}

	cfg, err := loadConfig(writeTemp(t, "fillwire.toml", withKisFutures(t, shipped, good)))
	if err != nil || cfg.kisFutures.err != nil {
		t.Fatalf("good settings: %v, %v", err, cfg.kisFutures.err)
	}
	if cfg.kisFutures.maxLen != kisfutures.DefaultMaxLen {
		t.Fatalf("max_len = %d", cfg.kisFutures.maxLen)
	}
	for name, adjust := range map[string]func(*runtimeConfig){
		"mock endpoint": func(c *runtimeConfig) { c.KIS.Endpoint = "mock" },
		"no free registrations": func(c *runtimeConfig) {
			c.kisQuotes = kisQuoteSettings{enabled: true, limit: 10}
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := cfg
			adjust(&changed)
			lane, slots, stop := startKisFutures(changed, quietLogger(), runDependencies{futuresMaster: panicMaster{}})
			defer stop()
			if lane != nil || slots != nil {
				t.Fatal("lane started")
			}
		})
	}
}

type fixedMaster []kisfutures.MasterRow

func (m fixedMaster) Master(context.Context) ([]kisfutures.MasterRow, error) { return m, nil }

// upcomingMaster lists the four months after now for both products, so every
// contract is live whenever the test runs.
func upcomingMaster(now time.Time) fixedMaster {
	first := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	var rows fixedMaster
	for ahead := 1; ahead <= 4; ahead++ {
		month := first.AddDate(0, ahead, 0)
		code := month.Format("2006")[3:] + month.Format("01")
		rows = append(rows,
			kisfutures.MasterRow{ProductType: "1", ShortCode: "A01" + code, Name: "F " + month.Format("200601"), UnderlyingName: "KOSPI200"},
			// "미니F " in cp949.
			kisfutures.MasterRow{ProductType: "B", ShortCode: "A05" + code, Name: "\xb9\xcc\xb4\xcfF " + month.Format("200601"), UnderlyingName: "KOSPI200"})
	}
	return rows
}

// The whole runtime with the futures and the promoted quote lanes on: one
// websocket dial, every registration on it, the two lanes drawing on one
// budget, and a futures trade becoming a bar without touching the fills path.
func TestRuntimeSharesOneSessionAndBudgetWithTheFuturesLane(t *testing.T) {
	t.Setenv("KIS_QUOTE_TOKEN", "fixture-token")
	mainRedis := miniredis.RunT(t)
	approvalRedis := miniredis.RunT(t)
	approvalRedis.Set("kis:websocket:approval_key", "fills-cached-key")
	approvalRedis.SetTTL("kis:websocket:approval_key", 20*time.Hour)
	approval := redis.NewClient(&redis.Options{Addr: approvalRedis.Addr()})
	t.Cleanup(func() { _ = approval.Close() })
	ingest := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(ingest.Close)

	cfg := cacheOnlyRuntimeConfig(mainRedis.Addr(), ingest.URL)
	cfg.KIS.Endpoint, cfg.KIS.AccountMode = "live", "live"
	cfg.Stream.MaxLen = 1000
	cfg.Channel.Buffer, cfg.KIS.EventBuffer, cfg.KIS.DupTrackMax = 16, 16, 100
	now := time.Now()
	cfg.kisFutures = kisFuturesSettings{enabled: true, calendarFile: calendarPath(t, now), maxLen: 100}
	// 16 registrations - fills 1 - safety 2 - futures 8 = 5: two promoted
	// symbols of the five configured.
	cfg.kisQuotes = kisQuoteSettings{
		enabled: true, symbols: []string{"000001", "000002", "000003", "000004", "000005"}, limit: 16,
		minHold: 15 * time.Minute, feedGap: time.Hour, maxLen: 100, buffer: 16, tokenEnv: "KIS_QUOTE_TOKEN",
	}

	dialer := &isoDialer{dial: func(d *isoDialer) (ws.Transport, error) {
		return d.add(newIsoTransport(func(transport *isoTransport, req isoRequest) {
			transport.push(isoAck(req.tr, "0", "OPSP0000"))
			if req.tr == "H0STCNI0" && req.trType == "1" {
				go func() {
					for index := range isoFills {
						transport.push(strings.Replace(fillsFrame(index), "H0STCNI9", "H0STCNI0", 1))
					}
				}()
			}
		})), nil
	}}
	var ahead atomic.Int64
	deps := runDependencies{
		approvalRedis: approval, approvalFallback: &runApprovalFallback{}, dialer: dialer,
		futuresMaster:   upcomingMaster(now),
		futuresNow:      func() time.Time { return time.Now().Add(time.Duration(ahead.Load())) },
		futuresInterval: 2 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runWithDependencies(ctx, cfg, quietLogger(), deps) }()
	wait := func(what string, ready func() bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for !ready() {
			select {
			case err := <-done:
				t.Fatalf("fillwire stopped while waiting for %s: %v", what, err)
			default:
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
	requests := func(trType string) map[string][]string {
		dialer.mu.Lock()
		defer dialer.mu.Unlock()
		byTR := map[string][]string{}
		for _, transport := range dialer.transports {
			for _, req := range transport.requests() {
				if req.trType == trType {
					byTR[req.tr] = append(byTR[req.tr], req.key)
				}
			}
		}
		return byTR
	}
	wait("registrations", func() bool {
		subs := requests("1")
		return len(subs["H0IFCNT0"]) == 4 && len(subs["H0MFCNT0"]) == 4 && len(subs["H0STASP0"]) == 2
	})
	wait("fills", func() bool { entries, _ := mainRedis.Stream("fills:test"); return len(entries) == isoFills })
	// Give the quote lane's pass over its remaining symbols time to finish.
	time.Sleep(50 * time.Millisecond)
	subs := requests("1")
	total := 0
	for _, keys := range subs {
		total += len(keys)
	}
	if total != 13 || len(subs["H0STCNI0"]) != 1 || !slices.Equal(subs["H0STCNT0"], []string{"000001", "000002"}) || !slices.Equal(subs["H0STASP0"], []string{"000001", "000002"}) {
		t.Fatalf("subscriptions = %v; want fills 1, futures 8 and two promoted symbols", subs)
	}

	// The nearest F200 month of the fixture is its front contract.
	front := upcomingMaster(now)[0].ShortCode
	if !slices.Contains(subs["H0IFCNT0"], front) {
		t.Fatalf("front %q is not subscribed: %v", front, subs["H0IFCNT0"])
	}
	fields := make([]string, 50)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0], fields[1], fields[5], fields[9], fields[10] = front, time.Now().In(kst.Zone).Format("150405"), "401.25", "7", "7"
	dialer.mu.Lock()
	transport := dialer.transports[0]
	dialer.mu.Unlock()
	transport.push("0|H0IFCNT0|001|" + strings.Join(fields, "^"))
	// Frames are handled in order, so after this fill the trade was handled.
	transport.push(strings.Replace(fillsFrame(isoFills), "H0STCNI9", "H0STCNI0", 1))
	wait("the following fill", func() bool { entries, _ := mainRedis.Stream("fills:test"); return len(entries) == isoFills+1 })
	ahead.Store(int64(3 * time.Minute))
	var bars []miniredis.StreamEntry
	wait("the bar", func() bool {
		for _, key := range mainRedis.Keys() {
			if strings.HasPrefix(key, "minute:kis:futures:F200.") {
				bars, _ = mainRedis.Stream(key)
			}
		}
		return len(bars) == 1
	})
	if joined := strings.Join(bars[0].Values, " "); !strings.Contains(joined, "close 401.25") || !strings.Contains(joined, "volume 7") {
		t.Fatalf("bar = %v", bars[0].Values)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("fillwire returned %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("fillwire did not stop")
	}
	if dialer.count() != 1 {
		t.Fatalf("websocket dials = %d, want exactly one", dialer.count())
	}
	removed := requests("2")
	if len(removed["H0IFCNT0"]) < 4 || len(removed["H0MFCNT0"]) < 4 {
		t.Fatalf("futures registrations removed at shutdown = %v", removed)
	}
}
