package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mgh3326/fillwire/internal/quote"
	"github.com/mgh3326/fillwire/internal/recording"
	"github.com/pelletier/go-toml/v2"
	"github.com/redis/go-redis/v9"
)

const (
	defaultQuoteMaxLen = 100000
	quoteStreamPrefix  = "quotes:"
	defaultQuoteBuffer = 1024
	// maxQuoteBuffer caps the tick buffer. An oversized value would be a
	// fatal allocation failure, which recover cannot contain and which would
	// take the fills pipeline down with the process.
	maxQuoteBuffer = 65536
	// quoteStopTimeout bounds the wait for the quote socket's close at
	// shutdown.
	quoteStopTimeout = 7 * time.Second
	// quoteRedisPoolSize keeps the quote lane on its own small connection
	// pool, so quote reads and writes can never occupy a connection the fills
	// XADD or consumer group needs.
	quoteRedisPoolSize = 4
	quoteRedisTimeout  = 2 * time.Second
)

// quoteFileConfig is the [quotes] table. It is decoded strictly, apart from
// fileConfig: an unknown key (for example a credential) rejects the lane.
type quoteFileConfig struct {
	Enabled     bool   `toml:"enabled"`
	Provider    string `toml:"provider"`
	SymbolsFile string `toml:"symbols_file"`
	TokenKey    string `toml:"token_key"`
	StreamKey   string `toml:"stream_key"`
	MaxLen      int64  `toml:"max_len"`
	Buffer      int    `toml:"buffer"`
}

// quoteSettings is the resolved quote lane. When enabled is false the lane
// does not exist. When err is set the operator enabled it but the settings
// were rejected; the lane stays off and the fills pipeline runs unchanged.
type quoteSettings struct {
	enabled   bool
	err       error
	symbols   []quote.Symbol
	tokenKey  string
	streamKey string
	maxLen    int64
	buffer    int
}

// resolveQuoteSettings never fails the fills configuration. It runs after the
// fills settings are complete and only reads them.
func resolveQuoteSettings(contents []byte, fills runtimeConfig) quoteSettings {
	reject := func(reason string) quoteSettings {
		return quoteSettings{enabled: true, err: errors.New("quotes: " + reason)}
	}
	var document map[string]any
	if err := toml.Unmarshal(contents, &document); err != nil {
		return quoteSettings{}
	}
	rawTable, present := document["quotes"]
	if !present {
		return quoteSettings{}
	}
	table, ok := rawTable.(map[string]any)
	if !ok {
		return reject("[quotes] is not a table")
	}
	switch enabled := table["enabled"].(type) {
	case nil:
		return quoteSettings{}
	case bool:
		if !enabled {
			return quoteSettings{}
		}
	default:
		return reject("enabled must be true or false")
	}
	encoded, err := toml.Marshal(table)
	if err != nil {
		return reject("invalid [quotes] table")
	}
	var q quoteFileConfig
	decoder := toml.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&q); err != nil {
		return reject("invalid or unknown [quotes] key")
	}
	if q.Provider != "toss" {
		return reject("provider must be toss")
	}
	settings := quoteSettings{enabled: true}
	if strings.TrimSpace(q.SymbolsFile) == "" {
		return reject("symbols_file is required")
	}
	symbols, err := quote.LoadSymbols(q.SymbolsFile)
	if err != nil {
		return reject(err.Error())
	}
	settings.symbols = symbols
	// The lane reads this one cached token and nothing else; it holds no
	// Toss client id or secret and can never issue or refresh a token.
	if !quote.ValidTokenKey(q.TokenKey) {
		return reject("token_key must be the cached token key toss:oauth:<16 hex>:access_token")
	}
	settings.tokenKey = q.TokenKey
	settings.streamKey = q.StreamKey
	if settings.streamKey == "" {
		settings.streamKey = quote.DefaultStreamKey
	}
	// The prefix keeps the quote lane out of every key the fills path owns:
	// its stream and the kis: and kis_mock: approval cache and lock keys.
	if !strings.HasPrefix(settings.streamKey, quoteStreamPrefix) || len(settings.streamKey) == len(quoteStreamPrefix) ||
		strings.TrimSpace(settings.streamKey) != settings.streamKey || settings.streamKey == fills.Stream.Key {
		return reject("stream_key must start with " + quoteStreamPrefix + " and must differ from the fills stream key")
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
	if settings.buffer > maxQuoteBuffer {
		return reject(fmt.Sprintf("buffer must be at most %d", maxQuoteBuffer))
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
// client, token reader, and dialer, and it never reports an error that could
// stop the process.
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

	token := dependencies.quoteToken
	if token == nil {
		cached, err := quote.NewCachedToken(redisClient, settings.tokenKey, dependencies.quoteClock)
		if err != nil {
			_ = redisClient.Close()
			logger.Error("quote reader disabled", "reason", err.Error())
			return quoteLane{}
		}
		token = cached
	}
	dialer := dependencies.quoteDialer
	if dialer == nil {
		dialer = quote.NewDialer()
	}
	var archive *recording.Sink
	var archiveQueue chan quoteArchiveEvent
	var archiveDrops atomic.Uint64
	var archiveLastWarning atomic.Int64
	var archiveErrors atomic.Uint64
	var archiveLastError atomic.Value
	pendingDrops := make(map[string]*atomic.Uint64, len(settings.symbols))
	for _, symbol := range settings.symbols {
		pendingDrops[quoteArchiveName(symbol)] = &atomic.Uint64{}
	}
	if cfg.recording.enabled {
		archive, err = recording.NewSink(cfg.recording.directory, "toss")
		if err != nil {
			_ = redisClient.Close()
			logger.Error("Toss 호가 파일 기록기를 시작하지 못했습니다", "reason", err.Error())
			return quoteLane{}
		}
		for _, symbol := range settings.symbols {
			if !recording.SafeName(quoteArchiveName(symbol)) {
				_ = archive.Close()
				_ = redisClient.Close()
				logger.Error("Toss 파일 이름이 잘못되었습니다", "symbol", symbol.Code)
				return quoteLane{}
			}
		}
		archiveQueue = make(chan quoteArchiveEvent, max(settings.buffer, defaultQuoteBuffer))
	}
	enqueueArchive := func(event quoteArchiveEvent) {
		if archiveQueue == nil {
			return
		}
		pending := pendingDrops[event.name]
		if dropped := pending.Swap(0); dropped > 0 {
			marker := tossArchiveRecord{Kind: "archive_gap", Market: event.value.Market, ReceivedAt: event.at.UTC(), Dropped: dropped}
			select {
			case archiveQueue <- quoteArchiveEvent{name: event.name, at: event.at, value: marker}:
			default:
				pending.Add(dropped + 1)
				archiveDrops.Add(1)
				now := time.Now().UnixNano()
				last := archiveLastWarning.Load()
				if now-last >= int64(30*time.Second) && archiveLastWarning.CompareAndSwap(last, now) {
					logger.Warn("Toss 호가 보존 큐 초과, 누락 구간을 기록합니다", "dropped", archiveDrops.Load())
				}
				return
			}
		}
		select {
		case archiveQueue <- event:
		default:
			pending.Add(1)
			archiveDrops.Add(1)
			now := time.Now().UnixNano()
			last := archiveLastWarning.Load()
			if now-last >= int64(30*time.Second) && archiveLastWarning.CompareAndSwap(last, now) {
				logger.Warn("Toss 호가 보존 큐 초과, 누락 구간을 기록합니다", "dropped", archiveDrops.Load())
			}
		}
	}
	runner, err := quote.NewRunner(quote.Config{
		Symbols:      settings.symbols,
		Token:        token,
		Dialer:       dialer,
		Redis:        redisClient,
		StreamKey:    settings.streamKey,
		MaxLen:       settings.maxLen,
		Buffer:       settings.buffer,
		Clock:        dependencies.quoteClock,
		Logger:       logger,
		RetryMin:     dependencies.quoteRetryMin,
		RetryMax:     dependencies.quoteRetryMax,
		PingInterval: dependencies.quotePing,
		OnTick: func(symbol quote.Symbol, tick quote.Tick, received time.Time) {
			if archiveQueue == nil {
				return
			}
			value := tossArchiveRecord{Kind: "quote", Market: symbol.Market, ReceivedAt: received.UTC(), Tick: &tick}
			enqueueArchive(quoteArchiveEvent{name: quoteArchiveName(symbol), at: received, value: value})
		},
	})
	if err != nil {
		_ = redisClient.Close()
		if archive != nil {
			_ = archive.Close()
		}
		logger.Error("quote reader disabled", "reason", err.Error())
		return quoteLane{}
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer redisClient.Close()
		if archive != nil {
			archiveDone := make(chan struct{})
			go func() {
				defer close(archiveDone)
				for event := range archiveQueue {
					if err := archive.Append("toss", event.name, event.at, event.value); err != nil {
						archiveErrors.Add(1)
						archiveLastError.Store(err.Error())
					}
				}
			}()
			runner.Run(ctx)
			for _, symbol := range settings.symbols {
				name := quoteArchiveName(symbol)
				if dropped := pendingDrops[name].Swap(0); dropped > 0 {
					at := time.Now().UTC()
					archiveQueue <- quoteArchiveEvent{name: name, at: at, value: tossArchiveRecord{Kind: "archive_gap", Market: symbol.Market, ReceivedAt: at, Dropped: dropped}}
				}
			}
			close(archiveQueue)
			<-archiveDone
			if dropped := archiveDrops.Load(); dropped > 0 {
				logger.Warn("Toss 호가 보존에서 누락 구간을 기록했습니다", "dropped", dropped)
			}
			if failed := archiveErrors.Load(); failed > 0 {
				logger.Error("Toss 호가 파일 기록 실패", "failed", failed, "reason", archiveLastError.Load())
			}
			if err := archive.Close(); err != nil {
				logger.Error("Toss 호가 파일 닫기에 실패했습니다", "reason", err.Error())
			}
			return
		}
		runner.Run(ctx)
	}()
	logger.Info("quote reader started", "provider", "toss", "symbols", len(settings.symbols), "stream", settings.streamKey)
	return quoteLane{stop: stop, done: done}
}

type quoteArchiveEvent struct {
	name  string
	at    time.Time
	value tossArchiveRecord
}

type tossArchiveRecord struct {
	Kind       string      `json:"kind"`
	Market     string      `json:"market"`
	ReceivedAt time.Time   `json:"received_at"`
	Tick       *quote.Tick `json:"tick,omitempty"`
	Dropped    uint64      `json:"dropped,omitempty"`
}

func quoteArchiveName(symbol quote.Symbol) string {
	code := strings.ReplaceAll(symbol.Code, ".", "_")
	return strings.ToUpper(symbol.Market) + "-" + code
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
