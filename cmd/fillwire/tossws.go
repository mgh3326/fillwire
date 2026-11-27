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

// tossWSDeps is what a Reader attempt needs beyond its settings. Quote holds
// recorder health across supervisor restarts, so the owner must pass the same
// value to every attempt. The other fields replace production I/O for tests;
// nil means production.
type tossWSDeps struct {
	quote       *tossws.QuoteHealth
	dial        tossws.Dialer
	tokens      tossws.TokenSource
	appendQuote func(tossws.Record) error
}

func startTossWS(ctx context.Context, settings tossWSSettings, logger *slog.Logger) func() {
	if !settings.Enabled {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := runTossWS(ctx, settings, logger, nil, tossWSDeps{}); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("Toss WS reader stopped", "reason", err)
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func runTossWS(ctx context.Context, settings tossWSSettings, logger *slog.Logger, observe func(func() bool), deps tossWSDeps) error {
	logger = logger.With("lane", "toss_ws")
	tokens := deps.tokens
	if tokens == nil {
		client, err := tokenfetch.NewClient(settings.fetchURL, settings.credential)
		if err != nil {
			return errors.New("Toss WS gatewayd client setup failed")
		}
		tokens = tokenfetch.NewHolder(client, string(gatewayd.ProviderToss), 2*time.Minute, false, nil)
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
	appendQuote := deps.appendQuote
	if appendQuote == nil {
		appendQuote = func(r tossws.Record) error {
			return quoteSink.Append(tossQuoteStream, settings.AccountSeq, r.ReceivedAt, r)
		}
	}
	reader, err := tossws.New(tossws.Config{
		AccountSeq: settings.AccountSeq,
		Symbols:    settings.symbols,
		Tokens:     tokens,
		Append: func(r tossws.Record) error {
			return orderSink.Append(tossOrderStream, settings.AccountSeq, r.ReceivedAt, r)
		},
		AppendQuote: appendQuote,
		Dial:        deps.dial,
		QuoteHealth: deps.quote,
		Logger:      logger,
	})
	if err != nil {
		return err
	}
	if observe != nil {
		observe(reader.Health)
	}
	err = reader.Run(ctx)
	// Reader.Run retries transport failures itself. Preserve its deliberate
	// authentication stops and subscription rejections under the supervisor.
	if observe != nil && err != nil && ctx.Err() == nil && tossWSModuleStop(err) {
		return errors.Join(errModuleStopped, err)
	}
	return err
}

// tossWSModuleStop reports whether a reader.Run result is a deliberate stop
// the supervisor must not retry. A pure recording failure stays retryable,
// but a permanent rejection joined with ErrRecord is still a stop.
func tossWSModuleStop(err error) bool {
	for _, stop := range []error{
		tossws.ErrAccountNotFound, tossws.ErrRejected, tossws.ErrAck,
		tossws.ErrForbidden, tossws.ErrAuthRevokedStop,
	} {
		if errors.Is(err, stop) {
			return true
		}
	}
	return !errors.Is(err, tossws.ErrRecord)
}
