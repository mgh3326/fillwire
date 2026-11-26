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
	"time"

	"github.com/mgh3326/fillwire/internal/gatewayd"
	"github.com/mgh3326/fillwire/internal/quote"
	"github.com/mgh3326/fillwire/internal/recording"
	"github.com/mgh3326/fillwire/internal/tokenfetch"
	"github.com/mgh3326/fillwire/internal/tossws"
	"github.com/pelletier/go-toml/v2"
)

const tossOrderStream = "toss-order"

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
	logger = logger.With("lane", "toss_ws")
	client, err := tokenfetch.NewClient(settings.fetchURL, settings.credential)
	if err != nil {
		logger.Error("Toss WS gatewayd client setup failed")
		return func() {}
	}
	sink, err := recording.NewSink(settings.Directory, tossOrderStream)
	if err != nil {
		logger.Error("Toss WS JSONL recorder setup failed", "reason", err)
		return func() {}
	}
	reader, err := tossws.New(tossws.Config{
		AccountSeq: settings.AccountSeq,
		Symbols:    settings.symbols,
		Tokens:     tokenfetch.NewHolder(client, string(gatewayd.ProviderToss), 2*time.Minute, false, nil),
		Append: func(r tossws.Record) error {
			return sink.Append(tossOrderStream, settings.AccountSeq, r.ReceivedAt, r)
		},
		Logger: logger,
	})
	if err != nil {
		_ = sink.Close()
		logger.Error("Toss WS reader setup failed", "reason", err)
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := reader.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("Toss WS reader stopped", "reason", err)
		}
		if err := sink.Close(); err != nil {
			logger.Error("Toss WS JSONL recorder close failed", "reason", err)
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// runTossWSCommand runs only the Toss websocket stream for hosts that have not
// yet adopted the resident `serve` module registry.
func runTossWSCommand(args []string) (runErr error) {
	flags := flag.NewFlagSet("fillwire tossws", flag.ContinueOnError)
	configPath := flags.String("config", "fillwire.toml", "path to fillwire TOML configuration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: fillwire tossws [-config FILE]")
	}
	contents, err := os.ReadFile(*configPath)
	if err != nil {
		return errors.New("tossws: cannot read configuration")
	}
	var cfg fileConfig
	if err := toml.Unmarshal(contents, &cfg); err != nil {
		return errors.New("tossws: invalid TOML configuration")
	}
	if cfg.KIS.Gatewayd.TokenHeader == "" {
		cfg.KIS.Gatewayd.TokenHeader = "Authorization"
	}
	settings, err := resolveTossWSSettings(contents, cfg.KIS.Gatewayd)
	if err != nil {
		return err
	}
	if !settings.Enabled {
		return errors.New("tossws: [toss_ws] is disabled")
	}
	client, err := tokenfetch.NewClient(settings.fetchURL, settings.credential)
	if err != nil {
		return errors.New("tossws: gatewayd client setup failed")
	}
	sink, err := recording.NewSink(settings.Directory, tossOrderStream)
	if err != nil {
		return errors.New("tossws: JSONL recorder setup failed")
	}
	defer func() { runErr = errors.Join(runErr, sink.Close()) }()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("lane", "toss_ws")
	reader, err := tossws.New(tossws.Config{
		AccountSeq: settings.AccountSeq,
		Symbols:    settings.symbols,
		Tokens:     tokenfetch.NewHolder(client, string(gatewayd.ProviderToss), 2*time.Minute, false, nil),
		Append: func(r tossws.Record) error {
			return sink.Append(tossOrderStream, settings.AccountSeq, r.ReceivedAt, r)
		},
		Logger: logger,
	})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return reader.Run(ctx)
}
