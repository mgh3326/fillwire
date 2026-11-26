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

	"github.com/mgh3326/fillwire/internal/recording"
	"github.com/mgh3326/fillwire/internal/upbitpriv"
)

type upbitFillsConfig struct {
	Enabled   bool   `toml:"enabled"`
	Directory string `toml:"directory"`
}

func runUpbitFillsCommand(args []string) error {
	flags := flag.NewFlagSet(upbitpriv.Name, flag.ContinueOnError)
	path := flags.String("config", "fillwire.toml", "path to fillwire TOML configuration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: fillwire upbit-fills [-config path]")
	}
	contents, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	var cfg upbitFillsConfig
	if _, err := strictTable(contents, "upbit_fills", &cfg); err != nil {
		return err
	}
	if !cfg.Enabled {
		slog.Info("Upbit fills recording disabled")
		return nil
	}
	if strings.TrimSpace(cfg.Directory) == "" {
		return errors.New("upbit_fills: 활성화하려면 directory가 필요합니다")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runUpbitFills(ctx, cfg.Directory)
}

func runUpbitFills(ctx context.Context, directory string) (runErr error) {
	var sink *recording.Sink
	reader, err := upbitpriv.New(upbitpriv.Config{
		AccessKey: os.Getenv("UPBIT_ACCESS_KEY"),
		SecretKey: os.Getenv("UPBIT_SECRET_KEY"),
		Append: func(record upbitpriv.Record) error {
			return sink.Append(upbitpriv.Name, "myOrder", record.ReceivedAt, record)
		},
	})
	if err != nil {
		return err
	}
	sink, err = recording.NewSink(directory, upbitpriv.Name)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, sink.Close()) }()
	err = reader.Run(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
