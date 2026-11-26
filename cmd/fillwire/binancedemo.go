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
	config := binancedemo.Config{
		Symbols: strings.Split(*symbols, ","), RecordingRoot: *directory,
		EnableUserStream: !*noUser,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, nil)).With("lane", "binance_demo"),
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
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
