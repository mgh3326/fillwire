package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/mgh3326/fillwire/internal/binancedemo"
)

type binanceDemoFileConfig struct {
	Enabled    bool     `toml:"enabled"`
	Symbols    []string `toml:"symbols"`
	Directory  string   `toml:"directory"`
	UserStream *bool    `toml:"user_stream"`
}

func resolveBinanceDemoSettings(contents []byte) (binanceDemoFileConfig, error) {
	var cfg binanceDemoFileConfig
	if _, err := strictTable(contents, "binance_demo", &cfg); err != nil {
		return binanceDemoFileConfig{}, err
	}
	if cfg.Enabled && (len(cfg.Symbols) == 0 || strings.TrimSpace(cfg.Directory) == "") {
		return binanceDemoFileConfig{}, errors.New("binance_demo: 활성화하려면 symbols와 directory가 필요합니다")
	}
	return cfg, nil
}

func newBinanceDemo(settings binanceDemoFileConfig, logger *slog.Logger) (*binancedemo.Reader, error) {
	userStream := settings.UserStream == nil || *settings.UserStream
	cfg, err := binancedemo.FromEnvironment(binancedemo.Config{
		Symbols: append([]string(nil), settings.Symbols...), RecordingRoot: settings.Directory,
		EnableUserStream: userStream, Logger: logger.With("lane", "binance_demo"),
	})
	if err != nil {
		return nil, err
	}
	return binancedemo.New(cfg)
}

func runBinanceDemoCommand(args []string) error {
	fs := flag.NewFlagSet("binance-demo", flag.ContinueOnError)
	symbols := fs.String("symbols", "", "Binance Spot Demo symbols separated by commas")
	directory := fs.String("record-dir", "", "append-only recording root directory")
	noUser := fs.Bool("no-user-stream", false, "record market streams without subscribing to the user stream")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if os.Getenv("BINANCE_SPOT_DEMO_STREAMS_ENABLED") != "true" {
		return errors.New("binance demo: BINANCE_SPOT_DEMO_STREAMS_ENABLED must be exactly true")
	}
	if *symbols == "" || *directory == "" || fs.NArg() != 0 {
		return errors.New("usage: fillwire binance-demo --symbols BTCUSDT,ETHUSDT --record-dir <directory> [--no-user-stream]")
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("lane", "binance_demo")
	config := binancedemo.Config{
		Symbols: strings.Split(*symbols, ","), RecordingRoot: *directory,
		EnableUserStream: !*noUser,
		Logger:           logger,
	}
	var err error
	if config, err = binancedemo.FromEnvironment(config); err != nil {
		return err
	}
	reader, err := binancedemo.New(config)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err = reader.Run(ctx)
	health := reader.Health()
	logger.Info("Binance demo reader stopped", "market_frames", health.MarketFrames, "user_events", health.UserEvents,
		"malformed_frames", health.MalformedFrames, "subscribe_rejections", health.SubscribeRejects,
		"reconnects", health.Reconnects, "record_errors", health.RecordErrors)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
