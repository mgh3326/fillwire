package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/mgh3326/fillwire/internal/decode"
	"github.com/mgh3326/fillwire/internal/reader"
	"github.com/mgh3326/fillwire/internal/sink"
	"github.com/mgh3326/fillwire/internal/stream"
	"github.com/mgh3326/go-kis/kis"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

func main() {
	configPath := flag.String("config", "fillwire.toml", "path to fillwire TOML configuration")
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg, err := loadConfig(*configPath)
	if err != nil {
		logger.Error("startup configuration failed", "reason", err.Error())
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, logger); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("fillwire stopped", "reason", safeRuntimeError(err))
		os.Exit(reader.ProcessExitCode(err))
	}
}

func run(ctx context.Context, cfg runtimeConfig, logger *slog.Logger) error {
	redisOptions, err := redis.ParseURL(cfg.Redis.URL)
	if err != nil {
		return errors.New("startup: invalid Redis URL")
	}
	redisClient := redis.NewClient(redisOptions)
	defer redisClient.Close()

	kisHost := kis.HostLive
	if cfg.KIS.Endpoint == "mock" {
		kisHost = kis.HostVTS
	}
	kisClient, err := kis.NewClient(kis.Config{
		Host:           kisHost,
		AppKey:         cfg.appKey,
		AppSecret:      cfg.appSecret,
		RequestTimeout: cfg.timeout,
	})
	if err != nil {
		return errors.New("startup: KIS REST client configuration failed")
	}
	approval, err := reader.NewApprovalProvider(redisClient, ws.NewClientApprovalProvider(kisClient))
	if err != nil {
		return err
	}

	counters := decode.NewCounters()
	decoder := decode.New(decode.Config{
		AccountMode: cfg.KIS.AccountMode,
		Venue:       cfg.KIS.Venue,
		DupTrackMax: cfg.KIS.DupTrackMax,
		Counters:    counters,
		Logger:      logger,
	})
	queue, err := stream.New(redisClient, stream.Config{
		Key:          cfg.Stream.Key,
		MaxLen:       cfg.Stream.MaxLen,
		Group:        cfg.Stream.Group,
		Consumer:     cfg.Stream.Consumer,
		BatchSize:    cfg.Ingest.Batch,
		Block:        cfg.readBlock,
		ClaimMinIdle: cfg.claimMinIdle,
		Counters:     counters,
	})
	if err != nil {
		return err
	}
	ingestClient, err := sink.NewClient(sink.HTTPConfig{URL: cfg.Ingest.URL, Token: cfg.ingestToken, Timeout: cfg.timeout})
	if err != nil {
		return err
	}
	runner, err := sink.NewRunner(queue, ingestClient, sink.Config{
		RetryMin: cfg.retryMin,
		RetryMax: cfg.retryMax,
		Factor:   cfg.Retry.Factor,
		Logger:   logger,
		Counters: counters,
	})
	if err != nil {
		return err
	}

	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	events := make(chan ws.Event, cfg.Channel.Buffer)
	records := make(chan decode.Record, cfg.Channel.Buffer)
	errs := make(chan error, 3)
	var workers sync.WaitGroup

	workers.Add(1)
	go func() {
		defer workers.Done()
		defer close(events)
		if err := reader.New(reader.Config{
			Endpoint:    cfg.KIS.Endpoint,
			HTSID:       cfg.KIS.HTSID,
			Approval:    approval,
			Dialer:      ws.NewDialer(),
			EventBuffer: cfg.KIS.EventBuffer,
		}).Run(childCtx, events); err != nil {
			errs <- err
		}
	}()

	workers.Add(1)
	go func() {
		defer workers.Done()
		defer close(records)
		for event := range events {
			record, ok := decoder.Decode(event)
			if !ok {
				continue
			}
			select {
			case records <- record:
			case <-childCtx.Done():
				return
			}
		}
	}()

	workers.Add(1)
	go func() {
		defer workers.Done()
		for record := range records {
			if _, err := queue.Enqueue(childCtx, record); err != nil {
				errs <- err
				return
			}
		}
	}()

	workers.Add(1)
	go func() {
		defer workers.Done()
		if err := runner.Run(childCtx); err != nil && !errors.Is(err, context.Canceled) {
			errs <- err
		}
	}()

	select {
	case <-ctx.Done():
		cancel()
		workers.Wait()
		return nil
	case err := <-errs:
		cancel()
		workers.Wait()
		return err
	}
}

func safeRuntimeError(err error) string {
	text := err.Error()
	if containsSensitiveURL(text) {
		return "redacted startup or transport failure"
	}
	return text
}

func containsSensitiveURL(text string) bool {
	for index := 0; index+2 < len(text); index++ {
		if text[index:index+3] == "://" {
			return true
		}
	}
	return false
}
