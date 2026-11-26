package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/gatewayd"
	"github.com/mgh3326/fillwire/internal/quote"
	"github.com/mgh3326/fillwire/internal/recording"
	"github.com/mgh3326/fillwire/internal/tokenfetch"
	"github.com/mgh3326/fillwire/internal/tossws"
)

const tossOrderStream = "toss-order"
const tossQuoteStream = "toss-quote"

type tossWSFileConfig struct {
	Enabled     bool   `toml:"enabled"`
	AccountSeq  string `toml:"account_seq"`
	Directory   string `toml:"directory"`
	SymbolsFile string `toml:"symbols_file"`
}

type tossWSSettings struct {
	tossWSFileConfig
	symbols    []quote.Symbol
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
	var symbols []quote.Symbol
	if strings.TrimSpace(f.SymbolsFile) != "" {
		loaded, loadErr := quote.LoadSymbols(f.SymbolsFile)
		if loadErr != nil {
			return tossWSSettings{}, errors.New("toss_ws: symbols_file 거부됨")
		}
		symbols = loaded
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
	return tossWSSettings{tossWSFileConfig: f, symbols: symbols, fetchURL: base, credential: credential}, nil
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
	orderSink, err := recording.NewSink(settings.Directory, tossOrderStream)
	if err != nil {
		return err
	}
	quoteSink, err := recording.NewSink(settings.Directory, tossQuoteStream)
	if err != nil {
		_ = orderSink.Close()
		return err
	}
	defer func() {
		if err := orderSink.Close(); err != nil {
			logger.Error("Toss WS order recorder close failed", "reason", err)
		}
		if err := quoteSink.Close(); err != nil {
			logger.Error("Toss WS quote recorder close failed", "reason", err)
		}
	}()
	reader, err := tossws.New(tossws.Config{
		AccountSeq: settings.AccountSeq,
		Symbols:    settings.symbols,
		Tokens:     tokenfetch.NewHolder(client, string(gatewayd.ProviderToss), 2*time.Minute, false, nil),
		Append: func(r tossws.Record) error {
			return orderSink.Append(tossOrderStream, settings.AccountSeq, r.ReceivedAt, r)
		},
		AppendQuote: func(r tossws.Record) error {
			return quoteSink.Append(tossQuoteStream, settings.AccountSeq, r.ReceivedAt, r)
		},
		Logger: logger,
	})
	if err != nil {
		return err
	}
	if ready != nil {
		ready()
	}
	err = reader.Run(ctx)
	// Reader.Run retries transport failures itself. Preserve its deliberate
	// authentication stops and subscription rejections under the supervisor.
	if ready != nil && err != nil && ctx.Err() == nil &&
		(!errors.Is(err, tossws.ErrRecord) || errors.Is(err, tossws.ErrAuthRevokedStop)) {
		return errors.Join(errModuleStopped, err)
	}
	return err
}
