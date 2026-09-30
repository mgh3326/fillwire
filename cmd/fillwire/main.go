package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/mgh3326/fillwire/internal/alert"
	"github.com/mgh3326/fillwire/internal/decode"
	"github.com/mgh3326/fillwire/internal/quote"
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
	alerter := alert.FromEnv(logger, alert.DefaultRateLimit)
	cfg, err := loadConfig(*configPath)
	if err != nil {
		logger.Error("startup configuration failed", "reason", err.Error())
		os.Exit(exitCodeWithAlert(reader.ErrApprovalConfig, alerter))
	}
	alerter.SetRateLimit(cfg.alertRateLimit)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, logger, alerter); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("fillwire stopped", "reason", safeRuntimeError(err))
		os.Exit(exitCodeWithAlert(err, alerter))
	}
}

func exitCodeWithAlert(err error, alerter alert.Alerter) int {
	code := reader.ProcessExitCode(err)
	if alerter != nil {
		if code == reader.ExitCodeConfiguration {
			alerter.Alert(context.Background(), "configuration", "fillwire configuration failure; operator action required")
		}
		if code == 1 {
			alerter.Alert(context.Background(), "transient", "fillwire transient failure; retry or restart required")
		}
	}
	return code
}

func run(ctx context.Context, cfg runtimeConfig, logger *slog.Logger, alerter alert.Alerter) error {
	return runWithDependencies(ctx, cfg, logger, runDependencies{alerter: alerter})
}

type runDependencies struct {
	approvalRedis    redis.UniversalClient
	approvalFallback ws.ApprovalKeyProvider
	dialer           ws.Dialer
	alerter          alert.Alerter

	// Quote lane test seams. They are never shared with the fills fields above.
	quoteToken    quote.TokenSource
	quoteDialer   quote.Dialer
	quoteClock    quote.Clock
	quoteRetryMin time.Duration
	quoteRetryMax time.Duration
	quotePing     time.Duration
}

type kisApprovalIssuer struct{ client *kis.Client }
type approvalFallbackIssuer struct{ provider ws.ApprovalKeyProvider }

func (i approvalFallbackIssuer) Issue(ctx context.Context) (string, error) {
	return i.provider.Reissue(ctx)
}
func (i kisApprovalIssuer) Issue(ctx context.Context) (string, error) {
	result, err := i.client.IssueApprovalKey(ctx)
	if err != nil || result.ApprovalKey == "" {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", reader.ErrApprovalUnavailable
	}
	return result.ApprovalKey, nil
}

func runWithDependencies(ctx context.Context, cfg runtimeConfig, logger *slog.Logger, dependencies runDependencies) error {
	redisOptions, err := redis.ParseURL(cfg.Redis.URL)
	if err != nil {
		return fmt.Errorf("%w: invalid Redis URL", reader.ErrApprovalConfig)
	}
	redisClient := redis.NewClient(redisOptions)
	defer redisClient.Close()
	approvalRedis := dependencies.approvalRedis
	if approvalRedis == nil {
		approvalRedis = redisClient
	}

	kisHost := kis.HostLive
	if cfg.KIS.Endpoint == "mock" {
		kisHost = kis.HostVTS
	}
	kisClient, err := kis.NewClient(kis.Config{
		Host:           kisHost,
		AppKey:         cfg.appKey,
		AppSecret:      cfg.appSecret,
		RequestTimeout: reader.ApprovalIssueTimeout,
	})
	if err != nil {
		return fmt.Errorf("%w: KIS REST client", reader.ErrApprovalConfig)
	}
	var issuer reader.ApprovalIssuer = kisApprovalIssuer{client: kisClient}
	if dependencies.approvalFallback != nil {
		issuer = approvalFallbackIssuer{dependencies.approvalFallback}
	}
	margin := cfg.refreshMargin
	if margin == 0 {
		margin = reader.DefaultRefreshMargin
	}
	approval, err := reader.NewApprovalProviderConfig(approvalRedis, issuer, cfg.KIS.ApprovalMode, cfg.KIS.AccountMode, margin, logger)
	if err != nil {
		return err
	}
	if dependencies.alerter != nil {
		approval.SetTransientAlert(func() {
			dependencies.alerter.Alert(context.Background(), "transient", "fillwire approval refresh failed; retrying while cached key remains valid")
		})
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
		Logger:       logger,
	})
	if err != nil {
		return err
	}
	ingestClient, err := sink.NewClient(sink.HTTPConfig{URL: cfg.Ingest.URL, Token: cfg.ingestToken, TokenHeader: cfg.Ingest.TokenHeader, Timeout: cfg.timeout})
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

	events := make(chan ws.Event, cfg.Channel.Buffer)
	records := make(chan decode.Record, cfg.Channel.Buffer)
	readerCtx, stopReader := context.WithCancel(context.Background())
	defer stopReader()
	refreshDone := make(chan error, 1)
	if cfg.KIS.ApprovalMode != reader.ApprovalModeCacheOnly {
		go func() {
			if err := approval.RefreshLoop(readerCtx); err != nil {
				refreshDone <- err
			}
		}()
	}
	runnerCtx, stopRunner := context.WithCancel(context.Background())
	defer stopRunner()
	pipeline := newIngressPipeline(ctx, events, records, decoder, queue)
	readerDone := make(chan error, 1)
	runnerDone := make(chan error, 1)
	dialer := dependencies.dialer
	if dialer == nil {
		dialer = ws.NewDialer()
	}

	go func() {
		defer close(events)
		readerDone <- reader.New(reader.Config{
			Endpoint:    cfg.KIS.Endpoint,
			HTSID:       cfg.KIS.HTSID,
			Approval:    approval,
			Dialer:      dialer,
			EventBuffer: cfg.KIS.EventBuffer,
			Logger:      logger,
		}).Run(readerCtx, events)
	}()
	ingressDone := pipeline.start()
	go func() {
		runnerDone <- runner.Run(runnerCtx)
	}()
	// The quote lane starts after the fills pipeline and never feeds the
	// select below: its failures cannot stop fillwire.
	quotes := startQuoteLane(cfg, logger, dependencies)
	defer quotes.halt()

	var (
		stopErr         error
		readerFinished  bool
		ingressFinished bool
		runnerFinished  bool
	)
	select {
	case <-ctx.Done():
	case stopErr = <-readerDone:
		readerFinished = true
		if stopErr == nil {
			stopErr = reader.ErrEventsClosed
		}
	case stopErr = <-ingressDone:
		ingressFinished = true
	case stopErr = <-runnerDone:
		runnerFinished = true
	case stopErr = <-refreshDone:
	}

	quotes.halt()
	quotesDeadline := time.Now().Add(quoteStopTimeout)

	// Stop the socket first. The bounded events and records channels remain
	// live until the separate drain context expires, so a received fill gets an
	// XADD opportunity even after SIGTERM has canceled the parent context.
	stopReader()
	stopRunner()
	if !readerFinished {
		if err := <-readerDone; err != nil && stopErr == nil {
			stopErr = err
		}
	}

	drainCtx, cancelDrain := context.WithTimeout(context.Background(), cfg.drainTimeout)
	pipeline.beginDrain(drainCtx)
	stopAtDrainDeadline := context.AfterFunc(drainCtx, pipeline.stop)
	if !ingressFinished {
		select {
		case err := <-ingressDone:
			ingressFinished = true
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && stopErr == nil {
				stopErr = err
			}
		case <-drainCtx.Done():
			pipeline.stop()
			remainingEvents, remainingRecords := len(events), len(records)
			remaining := uint64(remainingEvents + remainingRecords)
			counters.AddDrainDropped(remaining)
			logger.Error("shutdown stream drain expired", "events_remaining", remainingEvents, "records_remaining", remainingRecords)
		}
	}
	stopAtDrainDeadline()
	cancelDrain()
	pipeline.stop()

	if !runnerFinished {
		select {
		case err := <-runnerDone:
			if err != nil && !errors.Is(err, context.Canceled) && stopErr == nil {
				stopErr = err
			}
		case <-time.After(cfg.timeout):
			logger.Error("ingest runner did not stop before shutdown timeout")
		}
	}
	quotes.wait(quotesDeadline, logger)
	return stopErr
}

// ingressPipeline owns decode-to-XADD work. Its worker context is rooted in
// Background rather than the process signal context: shutdown swaps XADD to a
// bounded drain context before it can be canceled.
type ingressPipeline struct {
	shutdownCtx context.Context
	workerCtx   context.Context
	stopWorker  context.CancelFunc

	mu       sync.RWMutex
	drainCtx context.Context

	events  <-chan ws.Event
	records chan decode.Record
	decoder *decode.Decoder
	queue   *stream.Queue
}

func newIngressPipeline(shutdownCtx context.Context, events <-chan ws.Event, records chan decode.Record, decoder *decode.Decoder, queue *stream.Queue) *ingressPipeline {
	workerCtx, stopWorker := context.WithCancel(context.Background())
	return &ingressPipeline{
		shutdownCtx: shutdownCtx,
		workerCtx:   workerCtx,
		stopWorker:  stopWorker,
		events:      events,
		records:     records,
		decoder:     decoder,
		queue:       queue,
	}
}

func (p *ingressPipeline) start() <-chan error {
	done := make(chan error, 1)
	go func() { done <- p.run() }()
	return done
}

func (p *ingressPipeline) beginDrain(ctx context.Context) {
	p.mu.Lock()
	p.drainCtx = ctx
	p.mu.Unlock()
}

func (p *ingressPipeline) enqueueContext() context.Context {
	p.mu.RLock()
	ctx := p.drainCtx
	p.mu.RUnlock()
	if ctx != nil {
		return ctx
	}
	return p.workerCtx
}

func (p *ingressPipeline) stop() { p.stopWorker() }

func (p *ingressPipeline) run() error {
	if p.shutdownCtx == nil || p.decoder == nil || p.queue == nil {
		return errors.New("ingress: pipeline dependencies are required")
	}
	decoderDone := make(chan struct{})
	go func() {
		defer close(decoderDone)
		defer close(p.records)
		for {
			if p.workerCtx.Err() != nil {
				return
			}
			select {
			case <-p.workerCtx.Done():
				return
			case event, open := <-p.events:
				if !open {
					return
				}
				record, ok := p.decoder.Decode(event)
				if !ok {
					continue
				}
				if !sendRecord(p.workerCtx, p.records, record) {
					return
				}
			}
		}
	}()

	defer p.stop()
	for {
		if p.workerCtx.Err() != nil {
			<-decoderDone
			return p.workerCtx.Err()
		}
		select {
		case <-p.workerCtx.Done():
			<-decoderDone
			return p.workerCtx.Err()
		case record, open := <-p.records:
			if !open {
				<-decoderDone
				return nil
			}
			enqueueCtx := p.enqueueContext()
			if _, err := p.queue.Enqueue(enqueueCtx, record); err != nil {
				p.stop()
				<-decoderDone
				if enqueueCtx.Err() != nil {
					return enqueueCtx.Err()
				}
				return fmt.Errorf("ingress: XADD: %w", err)
			}
		}
	}
}

func sendRecord(ctx context.Context, records chan<- decode.Record, record decode.Record) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case records <- record:
		return true
	case <-ctx.Done():
		return false
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
