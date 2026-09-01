package gatewayd

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"time"
)

// ParseEnsureInterval accepts the optional autonomous refresh interval. A
// missing flag leaves the daemon HTTP-driven only.
func ParseEnsureInterval(args []string) (time.Duration, error) {
	flags := flag.NewFlagSet("gatewayd", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	intervalText := flags.String("ensure-interval", "", "")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return 0, errConfiguration
	}
	if *intervalText == "" {
		return 0, nil
	}
	interval, err := time.ParseDuration(*intervalText)
	if err != nil || interval <= 0 {
		return 0, errConfiguration
	}
	return interval, nil
}

// Run serves gatewayd until ctx is cancelled. Startup and periodic failures
// use fixed text so process output cannot reveal token-related material.
func Run(ctx context.Context, args []string, lookup func(string) string, stderr io.Writer) int {
	interval, err := ParseEnsureInterval(args)
	if err != nil {
		writeStartupError(stderr)
		return 1
	}
	config, err := ConfigFromEnv(lookup)
	if err != nil {
		writeStartupError(stderr)
		return 1
	}
	store, err := NewRedisClient(config.RedisURL)
	if err != nil {
		writeStartupError(stderr)
		return 1
	}
	ensurer, err := NewEnsureService(store, KISMockOAuthClient{Timeout: config.Timeout}, config)
	if err != nil {
		writeStartupError(stderr)
		return 1
	}
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		writeStartupError(stderr)
		return 1
	}
	logger := log.New(stderr, "", 0)
	server := &http.Server{
		Handler:           NewHandler(ensurer, logger),
		ErrorLog:          log.New(io.Discard, "", 0),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	if interval > 0 {
		go runPeriodicEnsure(ctx, ensurer, interval, logger)
	}
	select {
	case serveErr := <-serveResult:
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			writeStartupError(stderr)
			return 1
		}
		return 0
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownContext)
		<-serveResult
		return 0
	}
}

func runPeriodicEnsure(ctx context.Context, ensurer Ensurer, interval time.Duration, logger EventLogger) {
	ensureAndLog(ctx, ensurer, logger)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ensureAndLog(ctx, ensurer, logger)
		}
	}
}

func ensureAndLog(ctx context.Context, ensurer Ensurer, logger EventLogger) {
	state, err := ensurer.Ensure(ctx)
	if err != nil {
		logEvent(logger, "gatewayd: periodic token ensure failed")
		return
	}
	switch state {
	case EnsureStateFresh:
		logEvent(logger, "gatewayd: periodic token ensure fresh")
	case EnsureStateIssued:
		logEvent(logger, "gatewayd: periodic token ensure issued")
	default:
		logEvent(logger, "gatewayd: periodic token ensure failed")
	}
}

func writeStartupError(stderr io.Writer) {
	if stderr != nil {
		_, _ = io.WriteString(stderr, "gatewayd: startup_failed\n")
	}
}
