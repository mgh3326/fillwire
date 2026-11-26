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

	"github.com/mgh3326/fillwire/internal/nhplugws"
	"github.com/mgh3326/fillwire/internal/recording"
)

type nhplugWSFileConfig struct {
	Enabled      bool                    `toml:"enabled"`
	UserID       string                  `toml:"user_id"`
	Directory    string                  `toml:"directory"`
	AppKeyEnv    string                  `toml:"app_key_env"`
	AppSecretEnv string                  `toml:"app_secret_env"`
	Quotes       []nhplugws.Registration `toml:"quotes"`
}

type nhplugWSSettings struct {
	file      nhplugWSFileConfig
	appKey    string
	appSecret string
}

func resolveNHPlugWSSettings(contents []byte) (nhplugWSSettings, error) {
	var file nhplugWSFileConfig
	if _, err := strictTable(contents, "nhplug_ws", &file); err != nil {
		return nhplugWSSettings{}, err
	}
	if !file.Enabled {
		return nhplugWSSettings{file: file}, nil
	}
	if strings.TrimSpace(file.Directory) == "" {
		return nhplugWSSettings{}, errors.New("nhplug_ws: 활성화하려면 directory가 필요합니다")
	}
	if _, err := nhplugws.BuildRegistrations(file.UserID, file.Quotes); err != nil {
		return nhplugWSSettings{}, err
	}
	if strings.TrimSpace(file.AppKeyEnv) == "" {
		file.AppKeyEnv = "NHPLUG_APP_KEY"
	}
	if strings.TrimSpace(file.AppSecretEnv) == "" {
		file.AppSecretEnv = "NHPLUG_APP_SECRET"
	}
	appKey, ok := os.LookupEnv(file.AppKeyEnv)
	if !ok || appKey == "" {
		return nhplugWSSettings{}, errors.New("nhplug_ws: NHPLUG 앱 키 환경 변수가 설정되지 않았습니다")
	}
	appSecret, ok := os.LookupEnv(file.AppSecretEnv)
	if !ok || appSecret == "" {
		return nhplugWSSettings{}, errors.New("nhplug_ws: NHPLUG 앱 시크릿 환경 변수가 설정되지 않았습니다")
	}
	return nhplugWSSettings{file: file, appKey: appKey, appSecret: appSecret}, nil
}

func runNHPlugWSCommand(args []string) error {
	flags := flag.NewFlagSet(nhplugws.Name, flag.ContinueOnError)
	path := flags.String("config", "fillwire.toml", "path to fillwire TOML configuration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: fillwire nhplug-ws [-config path]")
	}
	contents, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	settings, err := resolveNHPlugWSSettings(contents)
	if err != nil {
		return err
	}
	if !settings.file.Enabled {
		slog.Info("NHPLUG WS 기록이 비활성화되어 있습니다")
		return nil
	}
	tokens, err := nhplugws.NewOAuthTokenSource(settings.appKey, settings.appSecret)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runNHPlugWS(ctx, settings.file, tokens.Token, tokens.Invalidate)
}

func runNHPlugWS(ctx context.Context, cfg nhplugWSFileConfig, token nhplugws.TokenSource, invalidate func(string)) (runErr error) {
	sink, err := recording.NewSink(cfg.Directory, nhplugws.Name)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, sink.Close()) }()
	reader, err := nhplugws.New(nhplugws.Config{
		Endpoint:        nhplugws.MockEndpoint,
		UserID:          cfg.UserID,
		Quotes:          cfg.Quotes,
		Token:           token,
		InvalidateToken: invalidate,
		Append: func(record nhplugws.Record) error {
			return sink.Append(nhplugws.Name, "frames", record.ReceivedAt, record)
		},
		Logger: slog.Default().With("lane", nhplugws.Name),
	})
	if err != nil {
		return err
	}
	err = reader.Run(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
