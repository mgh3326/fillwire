package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/mgh3326/fillwire/internal/alpaca"
)

const (
	alpacaPaperKeyEnv    = "ALPACA_PAPER_API_KEY"
	alpacaPaperSecretEnv = "ALPACA_PAPER_API_SECRET"
	alpacaDataKeyEnv     = "ALPACA_DATA_API_KEY_ID"
	alpacaDataSecretEnv  = "ALPACA_DATA_API_SECRET_KEY"
)

type alpacaFileConfig struct {
	Enabled   bool     `toml:"enabled"`
	Symbols   []string `toml:"symbols"`
	Directory string   `toml:"directory"`
}

func resolveAlpacaSettings(contents []byte) (alpacaFileConfig, error) {
	var cfg alpacaFileConfig
	present, err := strictTable(contents, "alpaca", &cfg)
	if err != nil {
		return alpacaFileConfig{}, err
	}
	if !present || !cfg.Enabled {
		return alpacaFileConfig{}, nil
	}
	if strings.TrimSpace(cfg.Directory) == "" || len(cfg.Symbols) == 0 {
		return alpacaFileConfig{}, errors.New("alpaca: 활성화하려면 directory와 symbols가 필요합니다")
	}
	return cfg, nil
}

func runAlpacaCLI(args []string) int {
	flags := flag.NewFlagSet("fillwire alpaca", flag.ContinueOnError)
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
	contents, err := os.ReadFile(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "alpaca 설정 파일을 읽지 못했습니다")
		return 2
	}
	settings, err := resolveAlpacaSettings(contents)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if !settings.Enabled {
		fmt.Fprintln(os.Stderr, "[alpaca] enabled = true 설정이 필요합니다")
		return 2
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	code, err := runAlpaca(ctx, settings, logger, func() {})
	if err != nil {
		logger.Error("Alpaca 기록 모듈이 중지되었습니다", "reason", err.Error())
	}
	return code
}

func runAlpaca(ctx context.Context, settings alpacaFileConfig, logger *slog.Logger, ready func()) (int, error) {
	module, err := alpaca.New(alpaca.Config{
		PaperKeyEnv: alpacaPaperKeyEnv, PaperSecretEnv: alpacaPaperSecretEnv,
		DataKeyEnv: alpacaDataKeyEnv, DataSecretEnv: alpacaDataSecretEnv,
		Symbols: settings.Symbols, Root: settings.Directory, Logger: logger,
	})
	if err != nil {
		return 2, err
	}
	ready()
	if err := module.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return 1, err
	}
	return 0, nil
}
