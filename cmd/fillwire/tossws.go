package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/gatewayd"
	"github.com/mgh3326/fillwire/internal/recording"
	"github.com/mgh3326/fillwire/internal/tokenfetch"
	"github.com/mgh3326/fillwire/internal/tossws"
)

const tossOrderStream = "toss-order"

type tossWSFileConfig struct {
	Enabled    bool   `toml:"enabled"`
	AccountSeq string `toml:"account_seq"`
	Directory  string `toml:"directory"`
}

type tossWSSettings struct {
	tossWSFileConfig
	fetchURL   string
	credential string
}

func resolveTossWSSettings(contents []byte, g gatewaydSettings) (tossWSSettings, error) {
	var f tossWSFileConfig
	if _, err := strictTable(contents, "toss_ws", &f); err != nil {
		return tossWSSettings{}, err
	}
	if !f.Enabled {
		return tossWSSettings{}, nil
	}
	if !tossws.ValidAccountSeq(f.AccountSeq) || strings.TrimSpace(f.Directory) == "" {
		return tossWSSettings{}, errors.New("toss_ws: 활성화하려면 숫자 문자열 account_seq와 directory가 필요합니다")
	}
	if g.TokenHeader == "" {
		g.TokenHeader = "Authorization"
	}
	base, err := tokenfetch.NormalizeURL(g.URL)
	if err != nil || !tokenfetch.CredentialTransportOK(base) ||
		strings.TrimSpace(g.TokenEnv) == "" || !strings.EqualFold(g.TokenHeader, "Authorization") {
		return tossWSSettings{}, errors.New("toss_ws: [kis.gatewayd]의 HTTPS 또는 루프백 url, token_env 및 Authorization token_header가 필요합니다")
	}
	credential, ok := os.LookupEnv(g.TokenEnv)
	if !ok || credential == "" {
		return tossWSSettings{}, errors.New("toss_ws: gatewayd 인증 환경 변수가 설정되지 않았습니다")
	}
	return tossWSSettings{tossWSFileConfig: f, fetchURL: base, credential: credential}, nil
}

func startTossWS(ctx context.Context, settings tossWSSettings, logger *slog.Logger) func() {
	if !settings.Enabled {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := runTossWS(ctx, settings, logger, nil); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("Toss WS reader stopped", "reason", err)
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func runTossWS(ctx context.Context, settings tossWSSettings, logger *slog.Logger, ready func()) error {
	logger = logger.With("lane", "toss_ws")
	client, err := tokenfetch.NewClient(settings.fetchURL, settings.credential)
	if err != nil {
		return errors.New("Toss WS gatewayd client setup failed")
	}
	sink, err := recording.NewSink(settings.Directory, tossOrderStream)
	if err != nil {
		return err
	}
	defer func() {
		if err := sink.Close(); err != nil {
			logger.Error("Toss WS JSONL recorder close failed", "reason", err)
		}
	}()
	reader, err := tossws.New(tossws.Config{
		AccountSeq: settings.AccountSeq,
		Tokens:     tokenfetch.NewHolder(client, string(gatewayd.ProviderToss), 2*time.Minute, false, nil),
		Append: func(r tossws.Record) error {
			return sink.Append(tossOrderStream, settings.AccountSeq, r.ReceivedAt, r)
		},
		Logger: logger,
	})
	if err != nil {
		return err
	}
	if ready != nil {
		ready()
	}
	return reader.Run(ctx)
}
