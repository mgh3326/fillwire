package main

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"sync"

	"github.com/mgh3326/fillwire/internal/kisfutures"
	"github.com/mgh3326/fillwire/internal/kisquote"
	"github.com/mgh3326/fillwire/internal/ratebudget"
	"github.com/pelletier/go-toml/v2"
	"github.com/redis/go-redis/v9"
)

// errKisFuturesIngest refuses the ingest switch at config load. auto_trader's
// research minute ingest accepts market kr and us only, so there is no path to
// research.kr_futures_candles_1m yet (hk 6430); the POST client follows it.
var errKisFuturesIngest = errors.New("config: [kis_futures] ingest must stay false: auto_trader has no futures minute ingest yet (hk 6430); bars go to Redis or JSONL only")

// kisFuturesFileConfig is the [kis_futures] table. It is decoded strictly,
// apart from fileConfig.
type kisFuturesFileConfig struct {
	Enabled      bool   `toml:"enabled"`
	Ingest       bool   `toml:"ingest"`
	CalendarFile string `toml:"calendar_file"`
	JSONLPath    string `toml:"jsonl_path"`
	MaxLen       int64  `toml:"max_len"`
}

// kisFuturesSettings is the resolved futures minute lane. When err is set the
// operator enabled it but the settings were rejected; the lane stays off and
// the fills pipeline runs unchanged.
type kisFuturesSettings struct {
	enabled      bool
	err          error
	calendarFile string
	jsonlPath    string
	maxLen       int64
}

// resolveKisFuturesSettings returns an error only for the ingest switch, which
// is refused whether or not the lane is enabled. Every other problem rejects
// the lane alone.
func resolveKisFuturesSettings(contents []byte) (kisFuturesSettings, error) {
	reject := func(reason string) (kisFuturesSettings, error) {
		return kisFuturesSettings{enabled: true, err: errors.New("kis_futures: " + reason)}, nil
	}
	var document map[string]any
	if err := toml.Unmarshal(contents, &document); err != nil {
		return kisFuturesSettings{}, nil // the file-wide decode reports syntax errors
	}
	rawTable, present := document["kis_futures"]
	if !present {
		return kisFuturesSettings{}, nil
	}
	table, ok := rawTable.(map[string]any)
	if !ok {
		return reject("[kis_futures] is not a table")
	}
	// Any spelling of the key counts: the decoder below would accept it.
	for key, value := range table {
		if strings.EqualFold(key, "ingest") && value != false {
			return kisFuturesSettings{}, errKisFuturesIngest
		}
	}
	switch enabled := table["enabled"].(type) {
	case nil:
		return kisFuturesSettings{}, nil
	case bool:
		if !enabled {
			return kisFuturesSettings{}, nil
		}
	default:
		return reject("enabled must be true or false")
	}
	encoded, err := toml.Marshal(table)
	if err != nil {
		return reject("invalid [kis_futures] table")
	}
	var f kisFuturesFileConfig
	decoder := toml.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&f); err != nil {
		return reject("invalid or unknown [kis_futures] key")
	}
	settings := kisFuturesSettings{
		enabled:      true,
		calendarFile: strings.TrimSpace(f.CalendarFile),
		jsonlPath:    strings.TrimSpace(f.JSONLPath),
		maxLen:       f.MaxLen,
	}
	if settings.calendarFile == "" {
		return reject("calendar_file is required")
	}
	if settings.maxLen == 0 {
		settings.maxLen = kisfutures.DefaultMaxLen
	}
	if settings.maxLen < 0 {
		return reject("max_len must be positive")
	}
	return settings, nil
}

// fixedSessionSlots are the registrations the session holds before any
// optional consumer: the execution notices and, when enabled, the book stream.
func fixedSessionSlots(cfg runtimeConfig) []string {
	fixed := []string{"fills"}
	if cfg.Books.Enabled {
		fixed = append(fixed, "books")
	}
	return fixed
}

// startKisFutures builds the futures minute lane and, with it, the session
// budget it shares with the promoted quote lane: the fixed slots and the
// safety margin first, then the futures registrations. A rejected or failed
// lane never stops the fills pipeline: it logs and returns nil, and then the
// quote lane budgets alone as before.
func startKisFutures(cfg runtimeConfig, logger *slog.Logger, dependencies runDependencies) (*kisfutures.Lane, *ratebudget.Slots, func()) {
	settings := cfg.kisFutures
	off := func(message string, args ...any) (*kisfutures.Lane, *ratebudget.Slots, func()) {
		logger.Error(message, args...)
		return nil, nil, func() {}
	}
	if !settings.enabled {
		return nil, nil, func() {}
	}
	if settings.err != nil {
		return off("KIS 선물 분봉 레인 설정을 거부해 끕니다", "reason", settings.err.Error())
	}
	// The official samples offer the futures trade streams on live only.
	if cfg.KIS.Endpoint != "live" {
		return off("KIS 선물 분봉 레인은 live 엔드포인트에서만 지원되어 끕니다")
	}
	calendar, err := kisfutures.LoadCalendar(settings.calendarFile)
	if err != nil {
		return off("KIS 선물 분봉 레인의 달력을 읽지 못해 끕니다", "reason", err.Error())
	}
	// The same ceiling as the quote lane: it is one session's limit.
	limit := kisQuoteDefaultLimit
	if cfg.kisQuotes.enabled && cfg.kisQuotes.err == nil {
		limit = cfg.kisQuotes.limit
	}
	slots, err := ratebudget.NewSlots(limit)
	if err == nil {
		err = kisquote.ReserveSession(slots, fixedSessionSlots(cfg))
	}
	if err != nil {
		return off("KIS 선물 분봉 레인의 구독 예산을 만들지 못해 끕니다", "reason", err.Error())
	}
	var sink kisfutures.Sink
	if settings.jsonlPath != "" {
		sink, err = kisfutures.NewJSONLSink(settings.jsonlPath)
	} else {
		options, parseErr := redis.ParseURL(cfg.Redis.URL)
		if parseErr != nil {
			return off("KIS 선물 분봉 레인의 Redis URL 이 잘못되어 끕니다")
		}
		options.PoolSize = kisQuoteRedisPoolSize
		options.ReadTimeout = kisQuoteRedisTimeout
		options.WriteTimeout = kisQuoteRedisTimeout
		options.DialTimeout = kisQuoteRedisTimeout
		sink, err = kisfutures.NewRedisSink(redis.NewClient(options), settings.maxLen)
	}
	if err != nil {
		return off("KIS 선물 분봉 레인의 출력을 열지 못해 끕니다", "reason", err.Error())
	}
	master := dependencies.futuresMaster
	if master == nil {
		master = kisfutures.HTTPMaster{}
	}
	lane, err := kisfutures.New(kisfutures.Config{
		Slots:    slots,
		Calendar: calendar,
		Master:   master,
		Sink:     sink,
		Now:      dependencies.futuresNow,
		Interval: dependencies.futuresInterval,
		Logger:   logger,
	})
	if err != nil {
		_ = sink.Close()
		return off("KIS 선물 분봉 레인을 열지 못해 끕니다", "reason", err.Error())
	}
	var once sync.Once
	return lane, slots, func() { once.Do(lane.Close) }
}
