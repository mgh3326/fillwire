package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/fillwire/internal/bookfeed"
	"github.com/mgh3326/fillwire/internal/kisquote"
	"github.com/mgh3326/fillwire/internal/kisread"
	"github.com/mgh3326/fillwire/internal/ratebudget"
	"github.com/pelletier/go-toml/v2"
	"github.com/redis/go-redis/v9"
)

const (
	kisQuoteRedisPoolSize = 2
	kisQuoteRedisTimeout  = time.Second
	kisQuoteDefaultLimit  = 41 // KIS 실시간 등록 상한의 보수값, [unverified]
	kisQuoteMaxLimit      = 500
)

// kisQuoteFileConfig is the [kis_quotes] table. It is decoded strictly, apart
// from fileConfig: an unknown key (for example a credential) rejects the lane.
type kisQuoteFileConfig struct {
	Enabled           bool     `toml:"enabled"`
	Symbols           []string `toml:"symbols"`
	RegistrationLimit int      `toml:"registration_limit"`
	MinHold           string   `toml:"min_hold"`
	FeedGap           string   `toml:"feed_gap"`
	MaxLen            int64    `toml:"max_len"`
	Buffer            int      `toml:"buffer"`
	AccessTokenEnv    string   `toml:"access_token_env"`
	AccessTokenSource string   `toml:"access_token_source"`
}

// kisQuoteSettings is the resolved promoted-quote lane. When enabled is false
// the lane does not exist. When err is set the operator enabled it but the
// settings were rejected; the lane stays off and the fills pipeline runs
// unchanged.
type kisQuoteSettings struct {
	enabled     bool
	err         error
	symbols     []string
	limit       int
	minHold     time.Duration
	feedGap     time.Duration
	maxLen      int64
	buffer      int
	tokenEnv    string
	tokenSource string
}

// resolveKisQuoteSettings never fails the fills configuration. It runs before
// the credential checks so enabling the lane also requires the KIS app key and
// secret its REST recovery sends.
func resolveKisQuoteSettings(contents []byte) kisQuoteSettings {
	reject := func(reason string) kisQuoteSettings {
		return kisQuoteSettings{enabled: true, err: errors.New("kis_quotes: " + reason)}
	}
	var document map[string]any
	if err := toml.Unmarshal(contents, &document); err != nil {
		return kisQuoteSettings{}
	}
	rawTable, present := document["kis_quotes"]
	if !present {
		return kisQuoteSettings{}
	}
	table, ok := rawTable.(map[string]any)
	if !ok {
		return reject("[kis_quotes] is not a table")
	}
	switch enabled := table["enabled"].(type) {
	case nil:
		return kisQuoteSettings{}
	case bool:
		if !enabled {
			return kisQuoteSettings{}
		}
	default:
		return reject("enabled must be true or false")
	}
	encoded, err := toml.Marshal(table)
	if err != nil {
		return reject("invalid [kis_quotes] table")
	}
	var q kisQuoteFileConfig
	decoder := toml.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&q); err != nil {
		return reject("invalid or unknown [kis_quotes] key")
	}
	settings := kisQuoteSettings{enabled: true}
	if len(q.Symbols) == 0 {
		return reject("symbols is required")
	}
	seen := make(map[string]bool, len(q.Symbols))
	for _, symbol := range q.Symbols {
		symbol = strings.TrimSpace(symbol)
		if !bookfeed.SymbolOK(symbol) {
			return reject("invalid symbol")
		}
		if seen[symbol] {
			return reject("duplicate symbol")
		}
		seen[symbol] = true
		settings.symbols = append(settings.symbols, symbol)
	}
	settings.limit = q.RegistrationLimit
	if settings.limit == 0 {
		settings.limit = kisQuoteDefaultLimit
	}
	if settings.limit < 1 || settings.limit > kisQuoteMaxLimit {
		return reject("registration_limit out of range")
	}
	hold := q.MinHold
	if strings.TrimSpace(hold) == "" {
		hold = "15m"
	}
	if settings.minHold, err = time.ParseDuration(hold); err != nil || settings.minHold <= 0 {
		return reject("invalid min_hold")
	}
	gap := q.FeedGap
	if strings.TrimSpace(gap) == "" {
		gap = "30s"
	}
	if settings.feedGap, err = time.ParseDuration(gap); err != nil || settings.feedGap <= 0 {
		return reject("invalid feed_gap")
	}
	settings.maxLen = q.MaxLen
	if settings.maxLen == 0 {
		settings.maxLen = kisquote.DefaultMaxLen
	}
	if settings.maxLen <= 0 {
		return reject("max_len must be positive")
	}
	settings.buffer = q.Buffer
	if settings.buffer == 0 {
		settings.buffer = kisquote.DefaultBuffer
	}
	if settings.buffer < 0 {
		return reject("buffer must be positive")
	}
	source := strings.TrimSpace(q.AccessTokenSource)
	if source != "" && source != "env" && source != "redis" {
		return reject("invalid access_token_source")
	}
	envSet := strings.TrimSpace(q.AccessTokenEnv) != ""
	if envSet == (source == "redis") {
		return reject("exactly one of access_token_env or access_token_source = \"redis\" is required")
	}
	settings.tokenEnv, settings.tokenSource = strings.TrimSpace(q.AccessTokenEnv), source
	return settings
}

// startKisQuotes builds the promoted-quote lane. A rejected or failed lane
// never stops the fills pipeline: it logs and returns nil.
// slots, when not nil, is the session budget the futures lane already reserved
// from; nil leaves the lane to build its own.
func startKisQuotes(ctx context.Context, cfg runtimeConfig, logger *slog.Logger, bookSymbols []string, slots *ratebudget.Slots) (*kisquote.Lane, func()) {
	settings := cfg.kisQuotes
	if !settings.enabled {
		return nil, func() {}
	}
	if settings.err != nil {
		logger.Error("KIS 승격 시세 레인 설정을 거부해 끕니다", "reason", settings.err.Error())
		return nil, func() {}
	}
	options, err := redis.ParseURL(cfg.Redis.URL)
	if err != nil {
		logger.Error("KIS 승격 시세 레인의 Redis URL 이 잘못되어 끕니다")
		return nil, func() {}
	}
	options.PoolSize = kisQuoteRedisPoolSize
	options.ReadTimeout = kisQuoteRedisTimeout
	options.WriteTimeout = kisQuoteRedisTimeout
	options.DialTimeout = kisQuoteRedisTimeout
	r := redis.NewClient(options)
	token, err := kisReadToken(ctx, cfg.KIS.Endpoint, cfg.appKey, settings.tokenEnv, settings.tokenSource, r)
	if err != nil {
		logger.Error("KIS 승격 시세 레인의 토큰 원본이 잘못되어 끕니다")
		_ = r.Close()
		return nil, func() {}
	}
	rest := &kisread.Client{
		Mock:      cfg.KIS.Endpoint == "mock",
		AppKey:    cfg.appKey,
		AppSecret: cfg.appSecret,
		Token:     token,
	}
	lane, err := kisquote.New(kisquote.Config{
		Symbols:           settings.symbols,
		RegistrationLimit: settings.limit,
		Fixed:             fixedSessionSlots(cfg),
		Slots:             slots,
		FixedSymbols:      bookSymbols,
		MinHold:           settings.minHold,
		FeedGap:           settings.feedGap,
		MaxLen:            settings.maxLen,
		Buffer:            settings.buffer,
		Redis:             r,
		REST:              rest,
		Logger:            logger,
	})
	if err != nil {
		logger.Error("KIS 승격 시세 레인을 열지 못해 끕니다", "reason", err.Error())
		_ = r.Close()
		return nil, func() {}
	}
	var once sync.Once
	return lane, func() {
		once.Do(func() {
			lane.Close()
			_ = r.Close()
		})
	}
}
