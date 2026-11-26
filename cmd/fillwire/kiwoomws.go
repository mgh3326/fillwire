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

	"github.com/mgh3326/fillwire/internal/kiwoomws"
	"github.com/mgh3326/fillwire/internal/recording"
)

type kiwoomAccountConfig struct {
	Enabled      bool     `toml:"enabled"`
	Symbols      []string `toml:"symbols"`
	Directory    string   `toml:"directory"`
	AppKeyEnv    string   `toml:"app_key_env"`
	AppSecretEnv string   `toml:"app_secret_env"`
}

func runKiwoomCommand(account kiwoomws.Account, args []string) error {
	name := kiwoomws.NameKR
	table := "kiwoom_mock_kr"
	if account == kiwoomws.AccountUS {
		name, table = kiwoomws.NameUS, "kiwoom_mock_us"
	}
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	path := flags.String("config", "fillwire.toml", "fillwire TOML 설정 파일")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("사용법: fillwire %s [-config 경로]", name)
	}
	contents, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	var krCfg, usCfg kiwoomAccountConfig
	if _, err := strictTable(contents, "kiwoom_mock_kr", &krCfg); err != nil {
		return err
	}
	if _, err := strictTable(contents, "kiwoom_mock_us", &usCfg); err != nil {
		return err
	}
	krSymbols, usSymbols := []string(nil), []string(nil)
	if krCfg.Enabled {
		krSymbols = krCfg.Symbols
	}
	if usCfg.Enabled {
		usSymbols = usCfg.Symbols
	}
	if err := kiwoomws.ValidateCombinedSymbols(krSymbols, usSymbols); err != nil {
		return err
	}
	selected := krCfg
	if account == kiwoomws.AccountUS {
		selected = usCfg
	}
	if !selected.Enabled {
		slog.Info("키움 모의 WebSocket 기록기가 비활성화되어 있습니다", "account", account)
		return nil
	}
	if strings.TrimSpace(selected.Directory) == "" {
		return errors.New("키움 모의 WebSocket을 켜려면 directory 설정이 필요합니다")
	}
	expectedKey, expectedSecret := "KIWOOM_MOCK_APP_KEY", "KIWOOM_MOCK_APP_SECRET"
	if account == kiwoomws.AccountUS {
		expectedKey, expectedSecret = "KIWOOM_MOCK_US_APP_KEY", "KIWOOM_MOCK_US_APP_SECRET"
	}
	if selected.AppKeyEnv != expectedKey || selected.AppSecretEnv != expectedSecret {
		return fmt.Errorf("[%s] app_key_env와 app_secret_env는 %s, %s여야 합니다", table, expectedKey, expectedSecret)
	}
	appKey, appSecret := os.Getenv(expectedKey), os.Getenv(expectedSecret)
	if strings.TrimSpace(appKey) == "" {
		return fmt.Errorf("환경 변수 %s가 설정되지 않았습니다", expectedKey)
	}
	if strings.TrimSpace(appSecret) == "" {
		return fmt.Errorf("환경 변수 %s가 설정되지 않았습니다", expectedSecret)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runKiwoom(ctx, account, selected, appKey, appSecret)
}

func runKiwoom(ctx context.Context, account kiwoomws.Account, cfg kiwoomAccountConfig, appKey, appSecret string) (runErr error) {
	stream := kiwoomws.NameKR
	if account == kiwoomws.AccountUS {
		stream = kiwoomws.NameUS
	}
	sink, err := recording.NewSink(cfg.Directory, stream)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, sink.Close()) }()
	reader, err := kiwoomws.New(kiwoomws.Config{
		Account: account,
		Symbols: cfg.Symbols,
		Token:   func(ctx context.Context) (string, error) { return kiwoomws.IssueMockToken(ctx, appKey, appSecret) },
		Append: func(frame kiwoomws.Frame) error {
			return sink.Append(stream, "frames", frame.ReceivedAt, frame)
		},
	})
	if err != nil {
		return err
	}
	return reader.Run(ctx)
}
