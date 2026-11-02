package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/mgh3326/fillwire/internal/bookfeed"
	"github.com/mgh3326/fillwire/internal/kisread"
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
	return func(e ws.Event) {
		select {
		case events <- e:
		default:
		}
	}, func() { cancel(); <-done; r.Close() }, nil
}
