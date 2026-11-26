package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/mgh3326/fillwire/internal/alert"
	"github.com/mgh3326/fillwire/internal/gatewayd"
	"github.com/mgh3326/fillwire/internal/reader"
	"github.com/mgh3326/go-kis/kis/ws"

	"github.com/pelletier/go-toml/v2"
)

func runServeCLI(args []string) int {
	flags := flag.NewFlagSet("fillwire serve", flag.ContinueOnError)
	path := flags.String("config", "fillwire.toml", "fillwire TOML 설정 파일")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		flags.Usage()
		return 2
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	contents, err := os.ReadFile(*path)
	if err != nil {
		logger.Error("serve 설정 파일을 읽지 못했습니다")
		return 1
	}
	slots, healthAddr, err := serveModules(contents, logger)
	if err != nil {
		logger.Error("serve 설정 오류", "reason", err)
		return 1
	}
	listener, err := net.Listen("tcp", healthAddr)
	if err != nil {
		logger.Error("serve 상태 포트를 열지 못했습니다", "reason", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger.Info("serve 상태 엔드포인트 시작", "listen", listener.Addr())
	if err := serve(ctx, listener, newSupervisor(slots, logger)); err != nil {
		logger.Error("serve 상태 서버 오류", "reason", err)
		return 1
	}
	return 0
}

func serve(ctx context.Context, listener net.Listener, supervisor *supervisor) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", supervisor.health)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	httpDone := make(chan error, 1)
	go func() { httpDone <- server.Serve(listener) }()
	modulesDone := make(chan struct{})
	go func() { defer close(modulesDone); supervisor.run(ctx) }()

	var err error
	select {
	case <-ctx.Done():
		_ = server.Close()
		err = <-httpDone
	case err = <-httpDone:
		_ = server.Close()
	}
	cancel()
	<-modulesDone
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// serveModules is the registry. A new feed only needs its config section
// and a module factory here. Shared transports are dependencies of feeds.
func serveModules(contents []byte, logger *slog.Logger) ([]*moduleSlot, string, error) {
	var settings struct {
		HealthAddress string `toml:"health_listen"`
	}
	if _, err := strictTable(contents, "serve", &settings); err != nil {
		return nil, "", errors.New("잘못된 TOML 또는 [serve] 설정")
	}
	if settings.HealthAddress == "" {
		settings.HealthAddress = "127.0.0.1:8792"
	}
	var doc map[string]any
	if err := toml.Unmarshal(contents, &doc); err != nil {
		return nil, "", errors.New("잘못된 TOML 설정")
	}
	enabled := func(section string) (bool, error) {
		raw, present := doc[section]
		if !present {
			return false, nil
		}
		table, ok := raw.(map[string]any)
		if !ok {
			return false, fmt.Errorf("[%s] 표가 아닙니다", section)
		}
		value, present := table["enabled"]
		if !present {
			return false, nil
		}
		on, ok := value.(bool)
		if !ok {
			return false, fmt.Errorf("[%s] enabled는 불리언이어야 합니다", section)
		}
		return on, nil
	}
	hub := newKISHub()
	books, booksErr := enabled("books")
	futures, _ := enabled("kis_futures")
	registry := map[string]func(context.Context, func()) error{
		"gatewayd": func(ctx context.Context, ready func()) error {
			return runGatewaydModule(ctx, contents, ready)
		},
		"kis_fills": func(ctx context.Context, ready func()) error {
			var settings struct {
				Enabled bool `toml:"enabled"`
			}
			if _, err := strictTable(contents, "kis_fills", &settings); err != nil {
				return err
			}
			// The fills configuration has no dependency on the independently
			// validated books settings, including their TOML field types.
			fillsDoc := make(map[string]any, len(doc))
			for key, value := range doc {
				if key != "books" {
					fillsDoc[key] = value
				}
			}
			fillsConfig, err := toml.Marshal(fillsDoc)
			if err != nil {
				return errors.New("잘못된 KIS 체결 설정")
			}
			return runKISFills(ctx, fillsConfig, hub, logger, ready)
		},
		"kis_quotes": func(ctx context.Context, ready func()) error {
			if booksErr != nil {
				return booksErr
			}
			return runKISQuotes(ctx, contents, hub, logger, futures, ready)
		},
		"kis_futures": func(ctx context.Context, ready func()) error {
			return runKISFutures(ctx, contents, hub, logger, books, ready)
		},
		"upbit": func(ctx context.Context, ready func()) error {
			settings, err := resolveUpbitSettings(contents)
			if err != nil {
				return err
			}
			return runUpbitRecorder(ctx, settings, logger, ready)
		},
		"toss_ws": func(ctx context.Context, ready func()) error {
			var input struct {
				KIS struct {
					Gatewayd gatewaydSettings `toml:"gatewayd"`
				} `toml:"kis"`
			}
			if err := toml.Unmarshal(contents, &input); err != nil {
				return errors.New("잘못된 Toss WS gatewayd 설정")
			}
			if err := validateGatewaydTableKeys(contents); err != nil {
				return err
			}
			settings, err := resolveTossWSSettings(contents, input.KIS.Gatewayd)
			if err != nil {
				return err
			}
			return runTossWS(ctx, settings, logger, ready)
		},
		"quotes": func(ctx context.Context, ready func()) error {
			return runTossQuoteFeed(ctx, contents, logger, ready)
		},
	}
	modules := map[string]Module{
		"alpaca": &streamModule{name: "alpaca", start: func(ctx context.Context, observe func(func() bool)) error {
			settings, err := resolveAlpacaSettings(contents)
			if err != nil {
				return err
			}
			module, err := newAlpaca(settings, logger)
			if err != nil {
				return err
			}
			observe(module.Health)
			return module.Run(ctx)
		}},
		"upbit_fills": &streamModule{name: "upbit_fills", start: func(ctx context.Context, observe func(func() bool)) error {
			settings, err := resolveUpbitFillsSettings(contents)
			if err != nil {
				return err
			}
			return runUpbitFills(ctx, settings.Directory, observe)
		}},
	}
	for name, start := range registry {
		modules[name] = &serviceModule{name: name, start: start}
	}
	names := make([]string, 0, len(modules))
	for name := range modules {
		names = append(names, name)
	}
	sort.Strings(names)
	slots := make([]*moduleSlot, 0, len(names)+1)
	needsKIS := false
	for _, name := range names {
		on, err := enabled(name)
		if name == "kis_quotes" {
			on = on || books || booksErr != nil
		}
		slot := &moduleSlot{name: name}
		switch {
		case err != nil:
			slot.Module = &serviceModule{name: name, start: func(context.Context, func()) error { return err }}
		case on:
			slot.Module = modules[name]
			needsKIS = needsKIS || name == "kis_fills" || name == "kis_quotes" || name == "kis_futures"
		}
		slots = append(slots, slot)
	}
	if needsKIS {
		interval, configErr := serveAlertInterval(contents)
		alerter := alert.FromEnv(logger, interval)
		for _, slot := range slots {
			switch slot.name {
			case "kis_fills", "kis_quotes", "kis_futures":
				if module, ok := slot.Module.(*serviceModule); ok {
					module.start = alertKISFailures(module.start, alerter)
				}
			}
		}
		slots = append(slots, &moduleSlot{name: "kis_session", Module: &serviceModule{
			name: "kis_session",
			start: alertKISFailures(func(ctx context.Context, ready func()) error {
				if configErr != nil {
					err := errors.Join(reader.ErrApprovalConfig, configErr)
					hub.finish(nil, err)
					return err
				}
				cfg, err := parseKISTransport(contents)
				if err != nil {
					err = errors.Join(reader.ErrApprovalConfig, err)
					hub.finish(nil, err)
					return err
				}
				return hub.run(ctx, cfg, logger, alerter, ready)
			}, alerter),
		}})
	}
	return slots, settings.HealthAddress, nil
}

func serveAlertInterval(contents []byte) (time.Duration, error) {
	var settings struct {
		Alerts struct {
			RateLimit string `toml:"rate_limit"`
		} `toml:"alerts"`
	}
	if err := toml.Unmarshal(contents, &settings); err != nil {
		return 0, errors.New("config: invalid alerts settings")
	}
	if settings.Alerts.RateLimit == "" {
		return alert.DefaultRateLimit, nil
	}
	interval, err := time.ParseDuration(settings.Alerts.RateLimit)
	if err != nil || interval <= 0 {
		return 0, errors.New("config: invalid alerts rate_limit")
	}
	return interval, nil
}

func alertKISFailures(start func(context.Context, func()) error, alerter alert.Alerter) func(context.Context, func()) error {
	return func(ctx context.Context, ready func()) error {
		err := runAttempt(func() error { return start(ctx, ready) })
		if ctx.Err() == nil {
			if errors.Is(err, errSharedKISStopped) {
				return err // The shared session reports the original failure.
			}
			if errors.Is(err, ws.ErrSessionOccupied) {
				alerter.Alert(ctx, "session-occupied", "fillwire KIS session occupied; operator action required")
			} else {
				exitCodeWithAlert(err, alerter)
			}
		}
		return err
	}
}

func runGatewaydModule(ctx context.Context, contents []byte, ready func()) error {
	var settings struct {
		Enabled        bool   `toml:"enabled"`
		Providers      string `toml:"providers"`
		EnsureInterval string `toml:"ensure_interval"`
		ListenAddress  string `toml:"listen"`
	}
	if _, err := strictTable(contents, "gatewayd", &settings); err != nil {
		return err
	}
	var input struct {
		Redis redisSettings `toml:"redis"`
	}
	if err := toml.Unmarshal(contents, &input); err != nil {
		return errors.New("잘못된 gatewayd Redis 설정")
	}
	args := []string{"-ensure-interval", settings.EnsureInterval}
	if settings.Providers != "" {
		args = append(args, "-providers", settings.Providers)
	}
	lookup := func(name string) string {
		switch {
		case name == "GATEWAYD_LISTEN_ADDR" && settings.ListenAddress != "":
			return settings.ListenAddress
		case name == "REDIS_URL" && input.Redis.URL != "":
			return input.Redis.URL
		default:
			return os.Getenv(name)
		}
	}
	if gatewayd.RunModule(ctx, args, lookup, os.Stderr, ready) != 0 {
		return errors.New("gatewayd 실행 실패")
	}
	return nil
}

func runTossQuoteFeed(ctx context.Context, contents []byte, logger *slog.Logger, ready func()) error {
	var input struct {
		KIS struct {
			Gatewayd gatewaydSettings `toml:"gatewayd"`
		} `toml:"kis"`
		Redis  redisSettings `toml:"redis"`
		Stream struct {
			Key string `toml:"key"`
		} `toml:"stream"`
	}
	if err := toml.Unmarshal(contents, &input); err != nil {
		return errors.New("잘못된 Toss 해외 시세 설정")
	}
	var cfg runtimeConfig
	cfg.KIS.Gatewayd, cfg.Redis, cfg.Stream.Key = input.KIS.Gatewayd, input.Redis, input.Stream.Key
	if cfg.KIS.Gatewayd.TokenHeader == "" {
		cfg.KIS.Gatewayd.TokenHeader = "Authorization"
	}
	if err := validateRedisURL(cfg.Redis.URL); err != nil {
		return err
	}
	cfg.quotes = resolveQuoteSettings(contents, cfg)
	if cfg.quotes.err != nil {
		return cfg.quotes.err
	}
	var err error
	if cfg.recording, err = resolveRecordingSettings(contents); err != nil {
		return err
	}
	lane := startQuoteLane(cfg, logger, runDependencies{})
	if lane.done == nil {
		return errors.New("Toss 해외 시세 모듈을 초기화하지 못했습니다")
	}
	ready()
	select {
	case <-lane.done:
		return errors.New("Toss 해외 시세 모듈이 종료되었습니다")
	case <-ctx.Done():
		lane.halt()
		<-lane.done
		return ctx.Err()
	}
}
