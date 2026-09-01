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

// RunOptions contains only daemon-local scheduling and the explicitly enabled
// token providers. The default remains kis-mock to make live activation an
// intentional deployment change.
type RunOptions struct {
	EnsureInterval time.Duration
	Providers      []TokenProvider
}

// ParseRunOptions accepts the optional autonomous refresh interval and closed
// provider list. A missing interval leaves the daemon HTTP-driven only.
func ParseRunOptions(args []string) (RunOptions, error) {
	flags := flag.NewFlagSet("gatewayd", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	intervalText := flags.String("ensure-interval", "", "")
	providersText := flags.String("providers", string(ProviderKISMock), "")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return RunOptions{}, errConfiguration
	}
	providers, err := ParseProviders(*providersText)
	if err != nil {
		return RunOptions{}, errConfiguration
	}
	options := RunOptions{Providers: providers}
	if *intervalText == "" {
		return options, nil
	}
	interval, err := time.ParseDuration(*intervalText)
	if err != nil || interval <= 0 {
		return RunOptions{}, errConfiguration
	}
	options.EnsureInterval = interval
	return options, nil
}

// ParseEnsureInterval remains available to callers that only need the legacy
// interval parser; it applies the same safe default provider selection.
func ParseEnsureInterval(args []string) (time.Duration, error) {
	options, err := ParseRunOptions(args)
	if err != nil {
		return 0, err
	}
	return options.EnsureInterval, nil
}

// Run serves gatewayd until ctx is cancelled. Startup and periodic failures
// use fixed text so process output cannot reveal token-related material.
func Run(ctx context.Context, args []string, lookup func(string) string, stderr io.Writer) int {
	options, err := ParseRunOptions(args)
	if err != nil {
		writeStartupError(stderr)
		return 1
	}
	config, err := ConfigFromEnvForProviders(lookup, options.Providers)
	if err != nil {
		writeStartupError(stderr)
		return 1
	}
	store, err := NewRedisClient(config.RedisURL)
	if err != nil {
		writeStartupError(stderr)
		return 1
	}
	ensurers := make(ProviderEnsurers, len(options.Providers))
	for _, provider := range options.Providers {
		providerConfig, found := config.providerConfig(provider)
		if !found {
			writeStartupError(stderr)
			return 1
		}
		ensurer, ensureErr := NewEnsureServiceForProvider(store, oauthIssuerFor(providerConfig, config.Timeout), providerConfig)
		if ensureErr != nil {
			writeStartupError(stderr)
			return 1
		}
		ensurers[provider] = ensurer
	}
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		writeStartupError(stderr)
		return 1
	}
	logger := log.New(stderr, "", 0)
	metrics := NewMetrics()
	server := &http.Server{
		Handler:           newHandler(ensurers, logger, metrics),
		ErrorLog:          log.New(io.Discard, "", 0),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	if options.EnsureInterval > 0 {
		go runPeriodicEnsureWithMetrics(ctx, ensurers, options.EnsureInterval, logger, metrics)
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

func oauthIssuerFor(config ProviderConfig, timeout time.Duration) OAuthIssuer {
	switch config.Provider {
	case ProviderKISMock:
		return KISMockOAuthClient{Timeout: timeout}
	case ProviderKISLive:
		return KISLiveOAuthClient{Timeout: timeout}
	case ProviderToss:
		return TossOAuthClient{BaseURL: config.BaseURL, Timeout: timeout}
	default:
		return nil
	}
}

func runPeriodicEnsure(ctx context.Context, ensurers ProviderEnsurers, interval time.Duration, logger EventLogger) {
	runPeriodicEnsureWithMetrics(ctx, ensurers, interval, logger, nil)
}

func runPeriodicEnsureWithMetrics(ctx context.Context, ensurers ProviderEnsurers, interval time.Duration, logger EventLogger, metrics *Metrics) {
	ensureConfiguredProvidersWithMetrics(ctx, ensurers, logger, metrics)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ensureConfiguredProvidersWithMetrics(ctx, ensurers, logger, metrics)
		}
	}
}

func ensureConfiguredProviders(ctx context.Context, ensurers ProviderEnsurers, logger EventLogger) {
	ensureConfiguredProvidersWithMetrics(ctx, ensurers, logger, nil)
}

func ensureConfiguredProvidersWithMetrics(ctx context.Context, ensurers ProviderEnsurers, logger EventLogger, metrics *Metrics) {
	// Ordered iteration makes periodic behavior deterministic and, more
	// importantly, limits it to the selected map entries.
	for _, provider := range orderedProviders() {
		if ensurer, configured := ensurers[provider]; configured && ensurer != nil {
			ensureAndLogForProvider(ctx, provider, ensurer, logger, metrics)
		}
	}
}

func ensureAndLog(ctx context.Context, ensurer Ensurer, logger EventLogger) {
	ensureAndLogForProvider(ctx, "", ensurer, logger, nil)
}

func ensureAndLogForProvider(ctx context.Context, provider TokenProvider, ensurer Ensurer, logger EventLogger, metrics *Metrics) {
	state, err := ensurer.Ensure(ctx)
	if err != nil {
		logEvent(logger, "gatewayd: periodic token ensure failed")
		metrics.recordEnsure(provider, "", true)
		return
	}
	switch state {
	case EnsureStateFresh:
		logEvent(logger, "gatewayd: periodic token ensure fresh")
		metrics.recordEnsure(provider, state, false)
	case EnsureStateIssued:
		logEvent(logger, "gatewayd: periodic token ensure issued")
		metrics.recordEnsure(provider, state, false)
	default:
		logEvent(logger, "gatewayd: periodic token ensure failed")
		metrics.recordEnsure(provider, "", true)
	}
}

func writeStartupError(stderr io.Writer) {
	if stderr != nil {
		_, _ = io.WriteString(stderr, "gatewayd: startup_failed\n")
	}
}
