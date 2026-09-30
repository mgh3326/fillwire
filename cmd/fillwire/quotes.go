package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/quote"
	"github.com/mgh3326/fillwire/internal/reader"
	"github.com/mgh3326/go-kis/kis"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/pelletier/go-toml/v2"
	"github.com/redis/go-redis/v9"
)

const (
	defaultQuoteMaxLen = 100000
	defaultQuoteBuffer = 1024
	// quoteStopTimeout bounds the wait for the quote socket's unsubscribe and
	// close at shutdown. go-kis bounds those writes at 5s.
	quoteStopTimeout = 7 * time.Second
	// quoteRedisPoolSize keeps the quote lane on its own small connection
	// pool, so quote writes can never occupy a connection the fills XADD or
	// consumer group needs.
	quoteRedisPoolSize = 4
	quoteRedisTimeout  = 2 * time.Second
)

// quoteFileConfig is decoded in a second pass over the same TOML, apart from
// fileConfig, so no [quotes] content can change how the fills settings load.
type quoteFileConfig struct {
	Quotes struct {
		Enabled      bool   `toml:"enabled"`
		Endpoint     string `toml:"endpoint"`
		AppKeyEnv    string `toml:"app_key_env"`
		AppSecretEnv string `toml:"app_secret_env"`
		SymbolsFile  string `toml:"symbols_file"`
		StreamKey    string `toml:"stream_key"`
		MaxLen       int64  `toml:"max_len"`
		Buffer       int    `toml:"buffer"`
	} `toml:"quotes"`
}

// quoteSettings is the resolved quote lane. When enabled is false the lane
// does not exist. When err is set the operator enabled it but the settings
// were rejected; the lane stays off and the fills pipeline runs unchanged.
type quoteSettings struct {
	enabled   bool
	err       error
	endpoint  string
	appKey    string
	appSecret string
	symbols   []string
	streamKey string
	maxLen    int64
	buffer    int
}

// resolveQuoteSettings never fails the fills configuration. It runs after the
// fills settings are complete and only reads them.
func resolveQuoteSettings(contents []byte, fills runtimeConfig) quoteSettings {
	var file quoteFileConfig
	if err := toml.Unmarshal(contents, &file); err != nil {
		// A type error inside [quotes] cannot tell us whether the operator
		// meant it on, so report it as a rejected lane.
		return quoteSettings{enabled: true, err: errors.New("quotes: invalid [quotes] TOML")}
	}
	q := file.Quotes
	if !q.Enabled {
		return quoteSettings{}
	}
	settings := quoteSettings{enabled: true}
	reject := func(reason string) quoteSettings {
		return quoteSettings{enabled: true, err: errors.New("quotes: " + reason)}
	}
	if q.Endpoint != "live" && q.Endpoint != "mock" {
		return reject("endpoint must be live or mock")
	}
	settings.endpoint = q.Endpoint
	if strings.TrimSpace(q.AppKeyEnv) == "" || strings.TrimSpace(q.AppSecretEnv) == "" {
		return reject("app_key_env and app_secret_env are required")
	}
	if q.AppKeyEnv == fills.KIS.AppKeyEnv || q.AppSecretEnv == fills.KIS.AppSecretEnv {
		return reject("quote credentials must use environment names distinct from [kis]")
	}
	var ok bool
	if settings.appKey, ok = os.LookupEnv(q.AppKeyEnv); !ok || settings.appKey == "" {
		return reject(fmt.Sprintf("required environment variable %s is unset", q.AppKeyEnv))
	}
	if settings.appSecret, ok = os.LookupEnv(q.AppSecretEnv); !ok || settings.appSecret == "" {
		return reject(fmt.Sprintf("required environment variable %s is unset", q.AppSecretEnv))
	}
	// One KIS app key admits exactly one websocket session (go-kis ws
	// package documentation; OPSP8996). A quote socket on the fills key would
	// be refused while fills holds the session, and would take the session
	// whenever fills is between reconnects, making fills exit 42.
	if settings.appKey == fills.appKey {
		return reject("quote app key must differ from the fills app key: one KIS app key admits one websocket session")
	}
	if strings.TrimSpace(q.SymbolsFile) == "" {
		return reject("symbols_file is required")
	}
	symbols, err := quote.LoadSymbols(q.SymbolsFile)
	if err != nil {
		return reject(err.Error())
	}
	settings.symbols = symbols
	settings.streamKey = q.StreamKey
	if settings.streamKey == "" {
		settings.streamKey = quote.DefaultStreamKey
	}
	if strings.TrimSpace(settings.streamKey) != settings.streamKey || settings.streamKey == fills.Stream.Key {
		return reject("stream_key must have no surrounding spaces and must differ from the fills stream key")
	}
	settings.maxLen = q.MaxLen
	if settings.maxLen == 0 {
		settings.maxLen = defaultQuoteMaxLen
	}
	settings.buffer = q.Buffer
	if settings.buffer == 0 {
		settings.buffer = defaultQuoteBuffer
	}
	if settings.maxLen < 0 || settings.buffer < 0 {
		return reject("max_len and buffer must be positive")
	}
	return settings
}

// quoteLane is a started quote reader. The zero value is an absent lane.
type quoteLane struct {
	stop context.CancelFunc
	done chan struct{}
}

// startQuoteLane starts the quote reader if it is enabled and valid. It
// shares nothing mutable with the fills pipeline: it builds its own Redis
// client, KIS REST client, approval provider, and dialer, and it never
// reports an error that could stop the process.
func startQuoteLane(cfg runtimeConfig, logger *slog.Logger, dependencies runDependencies) (lane quoteLane) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logger.Error("quote reader disabled: startup panic contained", "lane", "quotes")
			lane = quoteLane{}
		}
	}()
	settings := cfg.quotes
	if !settings.enabled {
		return quoteLane{}
	}
	logger = logger.With("lane", "quotes")
	if settings.err != nil {
		logger.Error("quote reader disabled: configuration rejected", "reason", settings.err.Error())
		return quoteLane{}
	}
	redisOptions, err := redis.ParseURL(cfg.Redis.URL)
	if err != nil {
		logger.Error("quote reader disabled: invalid Redis URL")
		return quoteLane{}
	}
	redisOptions.PoolSize = quoteRedisPoolSize
	redisOptions.ReadTimeout = quoteRedisTimeout
	redisOptions.WriteTimeout = quoteRedisTimeout
	redisClient := redis.NewClient(redisOptions)

	endpoint, host := ws.EndpointLive, kis.HostLive
	if settings.endpoint == "mock" {
		endpoint, host = ws.EndpointVTS, kis.HostVTS
	}
	approval := dependencies.quoteApproval
	if approval == nil {
		client, err := kis.NewClient(kis.Config{
			Host:           host,
			AppKey:         settings.appKey,
			AppSecret:      settings.appSecret,
			RequestTimeout: reader.ApprovalIssueTimeout,
		})
		if err != nil {
			_ = redisClient.Close()
			logger.Error("quote reader disabled: KIS REST client")
			return quoteLane{}
		}
		// In-process cache only. The fills approval cache in Redis belongs to
		// the fills app key and is never read or written by this lane.
		approval = ws.NewClientApprovalProvider(client)
	}
	dialer := dependencies.quoteDialer
	if dialer == nil {
		dialer = ws.NewDialer()
	}
	runner, err := quote.NewRunner(quote.Config{
		Endpoint:  endpoint,
		Symbols:   settings.symbols,
		Approval:  approval,
		Dialer:    dialer,
		Redis:     redisClient,
		StreamKey: settings.streamKey,
		MaxLen:    settings.maxLen,
		Buffer:    settings.buffer,
		Clock:     dependencies.quoteClock,
		Logger:    logger,
		RetryMin:  dependencies.quoteRetryMin,
		Backoff:   dependencies.quoteBackoff,
	})
	if err != nil {
		_ = redisClient.Close()
		logger.Error("quote reader disabled", "reason", err.Error())
		return quoteLane{}
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer redisClient.Close()
		runner.Run(ctx)
	}()
	logger.Info("quote reader started", "symbols", len(settings.symbols), "stream", settings.streamKey, "endpoint", settings.endpoint)
	return quoteLane{stop: stop, done: done}
}

// halt asks the lane to stop without waiting.
func (l quoteLane) halt() {
	if l.stop != nil {
		l.stop()
	}
}

// wait blocks until the lane has closed its socket or deadline passes.
func (l quoteLane) wait(deadline time.Time, logger *slog.Logger) {
	if l.done == nil {
		return
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-l.done:
	case <-timer.C:
		logger.Error("quote reader did not stop before shutdown timeout", "lane", "quotes")
	}
}
