package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/mgh3326/fillwire/internal/bookfeed"
	"github.com/mgh3326/fillwire/internal/kisread"
	"github.com/mgh3326/fillwire/internal/recording"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

type bookSettings struct {
	Enabled        bool   `toml:"enabled"`
	Symbol         string `toml:"symbol"`
	AccessTokenEnv string `toml:"access_token_env"`
}

func startBooks(ctx context.Context, cfg runtimeConfig, logger *slog.Logger) (func(ws.Event), func(), error) {
	if !cfg.Books.Enabled {
		return nil, func() {}, nil
	}
	if !bookfeed.SymbolOK(cfg.Books.Symbol) || cfg.Books.AccessTokenEnv == "" {
		return nil, nil, errors.New("books: symbol and access_token_env required")
	}
	options, err := redis.ParseURL(cfg.Redis.URL)
	if err != nil {
		return nil, nil, errors.New("books: invalid Redis URL")
	}
	options.PoolSize = 2
	options.ReadTimeout = time.Second
	options.WriteTimeout = time.Second
	options.DialTimeout = time.Second
	r := redis.NewClient(options)
	rest := &kisread.Client{Mock: cfg.KIS.Endpoint == "mock", AppKey: cfg.appKey, AppSecret: cfg.appSecret, Token: func() string { return os.Getenv(cfg.Books.AccessTokenEnv) }}
	runCtx, cancel := context.WithCancel(ctx)
	events := make(chan ws.Event, 32)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = bookfeed.Run(runCtx, r, rest, cfg.Books.Symbol, cfg.KIS.Endpoint == "mock", events, func(error) { logger.Warn("book producer could not publish a fresh snapshot") })
	}()
	var archive *recording.Sink
	var archiveEvents chan kisArchiveEvent
	var archiveDone chan struct{}
	var archiveDrops atomic.Uint64
	var archiveErrors atomic.Uint64
	var archiveLastError atomic.Value
	if cfg.recording.enabled {
		archive, err = recording.NewSink(cfg.recording.directory, "kis")
		if err != nil {
			cancel()
			<-done
			r.Close()
			return nil, nil, err
		}
		archiveEvents = make(chan kisArchiveEvent, 256)
		archiveDone = make(chan struct{})
		go func() {
			defer close(archiveDone)
			for event := range archiveEvents {
				if err := archive.Append("kis", cfg.Books.Symbol, event.at, event.record); err != nil {
					archiveErrors.Add(1)
					archiveLastError.Store(err.Error())
				}
			}
		}()
	}
	return func(e ws.Event) {
			if archiveEvents != nil {
				at := e.ReceivedAt
				if at.IsZero() {
					at = time.Now().UTC()
				}
				line, marshalErr := json.Marshal(struct {
					Kind       string    `json:"kind"`
					Market     string    `json:"market"`
					ReceivedAt time.Time `json:"received_at"`
					Raw        []byte    `json:"raw,omitempty"`
					Fields     []string  `json:"fields,omitempty"`
				}{"book", cfg.Books.Symbol, at, e.Raw, e.Fields})
				if marshalErr == nil {
					select {
					case archiveEvents <- kisArchiveEvent{at, json.RawMessage(line)}:
					default:
						archiveDrops.Add(1)
					}
				}
			}
			select {
			case events <- e:
			default:
			}
		}, func() {
			cancel()
			<-done
			r.Close()
			if archiveEvents != nil {
				close(archiveEvents)
				<-archiveDone
				if dropped := archiveDrops.Load(); dropped > 0 {
					logger.Warn("KIS 호가 보존 큐 초과", "dropped", dropped)
				}
				if failed := archiveErrors.Load(); failed > 0 {
					logger.Error("KIS 호가 파일 기록 실패", "failed", failed, "reason", archiveLastError.Load())
				}
			}
			if archive != nil {
				if err := archive.Close(); err != nil {
					logger.Error("KIS 호가 파일 닫기에 실패했습니다", "reason", err.Error())
				}
			}
		}, nil
}

type kisArchiveEvent struct {
	at     time.Time
	record json.RawMessage
}
