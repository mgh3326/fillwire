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
	"time"

	"github.com/mgh3326/fillwire/internal/kismockws"
	"github.com/mgh3326/fillwire/internal/reader"
	"github.com/mgh3326/go-kis/kis"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/pelletier/go-toml/v2"
	"github.com/redis/go-redis/v9"
)

type kisMockWSCommandConfig struct {
	KIS struct {
		Endpoint       string `toml:"endpoint"`
		AccountMode    string `toml:"account_mode"`
		ApprovalMode   string `toml:"approval_mode"`
		ApprovalMargin string `toml:"approval_refresh_margin"`
		HTSID          string `toml:"hts_id"`
		AppKeyEnv      string `toml:"app_key_env"`
		AppSecretEnv   string `toml:"app_secret_env"`
	} `toml:"kis"`
	Redis struct {
		URL string `toml:"url"`
	} `toml:"redis"`
	KISMockWS struct {
		Enabled            bool     `toml:"enabled"`
		Symbols            []string `toml:"symbols"`
		RecordingDirectory string   `toml:"recording_directory"`
		SubscribePace      string   `toml:"subscribe_pace"`
		FuturesNoticeTR    string   `toml:"futures_notice_tr"`
	} `toml:"kis_mock_ws"`
}

var errKISMockWSUsage = errors.New("kis-mock-ws: invalid command line")

func kisMockWSConfigError(message string) error {
	return fmt.Errorf("%w: kis-mock-ws: %s", reader.ErrApprovalConfig, message)
}

func kisMockWSCommandExitCode(err error) int {
	if err == nil || errors.Is(err, flag.ErrHelp) || errors.Is(err, context.Canceled) {
		return 0
	}
	if errors.Is(err, errKISMockWSUsage) {
		return 2
	}
	return reader.ProcessExitCode(err)
}

func runKISMockWSCommand(args []string) error {
	flags := flag.NewFlagSet("kis-mock-ws", flag.ContinueOnError)
	configPath := flags.String("config", "fillwire.toml", "path to fillwire TOML configuration")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("%w: %v", errKISMockWSUsage, err)
	}
	contents, err := os.ReadFile(*configPath)
	if err != nil {
		return fmt.Errorf("%w: read config: %v", reader.ErrApprovalConfig, err)
	}
	var cfg kisMockWSCommandConfig
	if err := toml.Unmarshal(contents, &cfg); err != nil {
		return kisMockWSConfigError("invalid TOML configuration")
	}
	if !cfg.KISMockWS.Enabled {
		return kisMockWSConfigError("disabled; set [kis_mock_ws].enabled = true")
	}
	if cfg.KIS.Endpoint != "mock" || cfg.KIS.AccountMode != "mock" {
		return kisMockWSConfigError("refusing non-mock KIS configuration")
	}
	if cfg.KIS.ApprovalMode != "" {
		return kisMockWSConfigError("this command currently requires the shared default approval mode")
	}
	if strings.TrimSpace(cfg.KIS.HTSID) == "" || strings.TrimSpace(cfg.Redis.URL) == "" || strings.TrimSpace(cfg.KISMockWS.RecordingDirectory) == "" {
		return kisMockWSConfigError("HTS ID, Redis URL and recording_directory are required")
	}
	if err := validateRedisURL(cfg.Redis.URL); err != nil {
		return fmt.Errorf("%w: kis-mock-ws: Redis URL refused by transport policy", reader.ErrApprovalConfig)
	}
	appKey, ok := os.LookupEnv(cfg.KIS.AppKeyEnv)
	if !ok || appKey == "" {
		return kisMockWSConfigError(fmt.Sprintf("required environment variable %s is unset", cfg.KIS.AppKeyEnv))
	}
	appSecret, ok := os.LookupEnv(cfg.KIS.AppSecretEnv)
	if !ok || appSecret == "" {
		return kisMockWSConfigError(fmt.Sprintf("required environment variable %s is unset", cfg.KIS.AppSecretEnv))
	}
	margin := time.Hour
	if strings.TrimSpace(cfg.KIS.ApprovalMargin) != "" {
		margin, err = time.ParseDuration(cfg.KIS.ApprovalMargin)
		if err != nil || margin <= 0 || margin > reader.MaxRefreshMargin {
			return kisMockWSConfigError("invalid approval_refresh_margin")
		}
	}
	pace := kismockws.SubscriptionPace
	if strings.TrimSpace(cfg.KISMockWS.SubscribePace) != "" {
		pace, err = time.ParseDuration(cfg.KISMockWS.SubscribePace)
		if err != nil || pace <= 0 {
			return kisMockWSConfigError("invalid subscribe_pace")
		}
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	redisOptions, err := redis.ParseURL(cfg.Redis.URL)
	if err != nil {
		return kisMockWSConfigError("invalid Redis URL")
	}
	redisOptions.PoolSize = 2
	redisClient := redis.NewClient(redisOptions)
	defer redisClient.Close()
	kisClient, err := kis.NewClient(kis.Config{Host: kis.HostVTS, AppKey: appKey, AppSecret: appSecret, RequestTimeout: reader.ApprovalIssueTimeout})
	if err != nil {
		return kisMockWSConfigError("invalid KIS mock client configuration")
	}
	approval, err := reader.NewApprovalProviderConfig(redisClient, kisApprovalIssuer{client: kisClient}, "", "mock", margin, logger)
	if err != nil {
		return err
	}
	ctx, stop := signalContext()
	defer stop()
	refreshDone := make(chan error, 1)
	go func() { refreshDone <- approval.RefreshLoop(ctx) }()
	module, err := kismockws.New(kismockws.Config{
		Endpoint: "mock", HTSID: cfg.KIS.HTSID, Symbols: cfg.KISMockWS.Symbols,
		RecordingDirectory: cfg.KISMockWS.RecordingDirectory, Approval: approval,
		Dialer: ws.NewDialer(), Logger: logger, SubscribePace: pace,
		FuturesNoticeTR: cfg.KISMockWS.FuturesNoticeTR,
	})
	if err != nil {
		stop()
		<-refreshDone
		return fmt.Errorf("%w: %v", reader.ErrApprovalConfig, err)
	}
	moduleErr := module.Run(ctx)
	stop()
	refreshErr := <-refreshDone
	return errors.Join(moduleErr, refreshErr)
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
