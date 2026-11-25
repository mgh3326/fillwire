// Command market-tap is the shadow KIS REST minute-chart poller (hk 6344).
//
// It reads only the domestic (FHKST03010200, KRX J / NTX NX) and overseas
// (HHDFS76950200) minute GET transactions, computes closed bars with
// auto_trader's rules, and writes them to JSONL and/or Redis. It never writes
// the research tables (ingest is off) and never issues a KIS token: the token
// comes from Redis or gatewayd's fetch endpoint, read-only.
//
//	market-tap -enabled -targets targets.json -calendar calendar.json -out bars.jsonl -once
//
// The command is off unless -enabled is set. noorder_test.go fails if an order
// package, an order transaction or the go-kis broker client is linked in.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mgh3326/fillwire/internal/jsonx"
	"github.com/mgh3326/fillwire/internal/kisread"
	"github.com/mgh3326/fillwire/internal/marketminute"
	"github.com/mgh3326/fillwire/internal/netpolicy"
	"github.com/mgh3326/fillwire/internal/ratebudget"
	"github.com/mgh3326/fillwire/internal/tokenfetch"
	"github.com/redis/go-redis/v9"
)

const (
	usage = "usage: market-tap -enabled -targets FILE -calendar FILE (-out FILE | -redis URL) [options]\n"
	// ExitConfiguration matches fillwire's configuration refusal.
	ExitConfiguration = 78
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

type options struct {
	enabled    bool
	targets    string
	calendar   string
	out        string
	redisURL   string
	once       bool
	interval   time.Duration
	workers    int
	provider   string
	accountRef string
	host       string
	appKeyEnv  string
	appSecret  string

	tokenSource   string
	tokenKey      string
	tokenRedis    string
	gatewaydURL   string
	gatewaydToken string
	gatewaydName  string
	rate          float64
	burst         int
	endpointRate  float64
	endpointBurst int
	researchShare float64
}

func parseFlags(args []string) (*options, error) {
	opts := &options{}
	flags := flag.NewFlagSet("market-tap", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	flags.BoolVar(&opts.enabled, "enabled", false, "run the poller (default off)")
	flags.StringVar(&opts.targets, "targets", "", "target list JSON from auto_trader build_targets")
	flags.StringVar(&opts.calendar, "calendar", "", "exchange calendar JSON (closed days and US early closes)")
	flags.StringVar(&opts.out, "out", "", "append shadow rows to this JSONL file")
	flags.StringVar(&opts.redisURL, "redis", "", "write shadow rows to Redis Streams at this URL")
	flags.BoolVar(&opts.once, "once", false, "run one poll instead of the interval loop")
	flags.DurationVar(&opts.interval, "interval", 5*time.Minute, "delay between polls")
	flags.IntVar(&opts.workers, "workers", 4, "concurrent minute-chart reads")
	flags.StringVar(&opts.provider, "provider", "kis", "ratebudget provider name")
	flags.StringVar(&opts.accountRef, "account-ref", "live", "the one ratebudget account_ref for this broker")
	flags.StringVar(&opts.host, "host", marketminute.DefaultHost, "KIS REST host")
	flags.StringVar(&opts.appKeyEnv, "app-key-env", "KIS_APP_KEY", "environment variable holding the KIS app key")
	flags.StringVar(&opts.appSecret, "app-secret-env", "KIS_APP_SECRET", "environment variable holding the KIS app secret")
	flags.StringVar(&opts.tokenSource, "token-source", "redis", "redis (read kis:access_token) or gatewayd-fetch")
	flags.StringVar(&opts.tokenKey, "token-key", kisread.AccessTokenLiveKey, "Redis key of the cached access token")
	flags.StringVar(&opts.tokenRedis, "token-redis", "", "Redis URL holding the cached token when -token-source redis and no -redis sink is wanted")
	flags.StringVar(&opts.gatewaydURL, "gatewayd-url", "", "gatewayd base URL for -token-source gatewayd-fetch")
	flags.StringVar(&opts.gatewaydToken, "gatewayd-token-env", "", "environment variable holding the gatewayd credential")
	flags.StringVar(&opts.gatewaydName, "gatewayd-provider", "kis-live", "gatewayd provider: kis-live or kis-mock")
	flags.Float64Var(&opts.rate, "rate", 20, "provider-wide requests per second (KIS real limit)")
	flags.IntVar(&opts.burst, "burst", 20, "provider-wide bucket capacity")
	flags.Float64Var(&opts.endpointRate, "endpoint-rate", 20, "per-endpoint requests per second")
	flags.IntVar(&opts.endpointBurst, "endpoint-burst", 20, "per-endpoint bucket capacity")
	flags.Float64Var(&opts.researchShare, "research-share", 0.25, "fixed share of the provider bucket this process may use")
	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	return opts, nil
}

func run(args []string, stdout, stderr io.Writer) int {
	opts, err := parseFlags(args)
	if err != nil {
		fmt.Fprint(stderr, usage)
		return 2
	}
	if !opts.enabled {
		fmt.Fprintln(stdout, "market-tap: disabled (default off); nothing polled")
		return 0
	}
	if opts.targets == "" || opts.calendar == "" || (opts.out == "" && opts.redisURL == "") {
		fmt.Fprint(stderr, usage)
		return 2
	}
	if opts.interval <= 0 || opts.workers <= 0 {
		fmt.Fprintln(stderr, "market-tap: interval and workers must be positive")
		return 2
	}

	for _, raw := range []string{opts.redisURL, opts.tokenRedis} {
		if raw == "" {
			continue
		}
		if err := netpolicy.ValidateRedisURL(raw); err != nil {
			fmt.Fprintf(stderr, "market-tap: %v\n", err)
			return ExitConfiguration
		}
	}
	if err := marketminute.ValidateHost(opts.host); err != nil {
		fmt.Fprintf(stderr, "market-tap: %v\n", err)
		return ExitConfiguration
	}

	targets, err := loadTargets(opts.targets)
	if err != nil {
		fmt.Fprintf(stderr, "market-tap: %v\n", err)
		return ExitConfiguration
	}
	calendarData, err := os.ReadFile(opts.calendar)
	if err != nil {
		fmt.Fprintf(stderr, "market-tap: calendar unreadable\n")
		return ExitConfiguration
	}
	calendar, err := marketminute.LoadCalendar(calendarData)
	if err != nil {
		fmt.Fprintf(stderr, "market-tap: %v\n", err)
		return ExitConfiguration
	}
	limiter, err := newLimiter(opts)
	if err != nil {
		fmt.Fprintf(stderr, "market-tap: %v\n", err)
		return ExitConfiguration
	}
	tokens, reject, closeTokens, err := newTokenSource(opts)
	if err != nil {
		fmt.Fprintf(stderr, "market-tap: %v\n", err)
		return ExitConfiguration
	}
	defer closeTokens()

	client := &marketminute.Client{
		Token:     tokens,
		Reject:    reject,
		AppKey:    os.Getenv(opts.appKeyEnv),
		AppSecret: os.Getenv(opts.appSecret),
		Host:      opts.host,
	}
	if client.AppKey == "" || client.AppSecret == "" {
		fmt.Fprintln(stderr, "market-tap: KIS app key and secret environment variables are unset")
		return ExitConfiguration
	}

	sink, flush, closeSink, err := newSink(opts)
	if err != nil {
		fmt.Fprintf(stderr, "market-tap: %v\n", err)
		return ExitConfiguration
	}
	defer closeSink()
	poller, err := marketminute.NewPoller(marketminute.Config{
		Targets:    targets,
		Reader:     client,
		Limiter:    limiter,
		Sink:       sink,
		Calendar:   calendar,
		Provider:   opts.provider,
		AccountRef: opts.accountRef,
		Workers:    opts.workers,
	})
	if err != nil {
		fmt.Fprintf(stderr, "market-tap: %v\n", err)
		return ExitConfiguration
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := slog.New(slog.NewTextHandler(stderr, nil))
	for {
		report, err := poller.Run(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintf(stderr, "market-tap: poll failed: %v\n", err)
			return 1
		}
		if err := flush(); err != nil {
			fmt.Fprintf(stderr, "market-tap: sink flush failed\n")
			return 1
		}
		encoded, _ := json.Marshal(report)
		logger.Info("market-tap poll", "report", string(encoded))
		if opts.once || errors.Is(err, context.Canceled) {
			return 0
		}
		select {
		case <-ctx.Done():
			return 0
		case <-time.After(opts.interval):
		}
	}
}

type targetsFile struct {
	Targets []marketminute.Target `json:"targets"`
}

func loadTargets(path string) ([]marketminute.Target, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("target list unreadable")
	}
	var file targetsFile
	if err := jsonx.DecodeStrict(data, &file); err != nil {
		return nil, errors.New("target list is not valid JSON with a targets array")
	}
	if len(file.Targets) == 0 {
		return nil, errors.New("target list is empty")
	}
	return file.Targets, nil
}

func newLimiter(opts *options) (*ratebudget.Limiter, error) {
	global := ratebudget.Limit{Rate: opts.rate, Burst: opts.burst}
	endpoint := ratebudget.Limit{Rate: opts.endpointRate, Burst: opts.endpointBurst}
	domestic, _ := marketminute.Endpoint("KRX")
	overseas, _ := marketminute.Endpoint("NASD")
	return ratebudget.New(ratebudget.Config{
		Providers: []ratebudget.Provider{{
			Name:       opts.provider,
			AccountRef: opts.accountRef,
			Limit:      global,
			Endpoints: []ratebudget.Endpoint{
				{Name: domestic, Limit: endpoint},
				{Name: overseas, Limit: endpoint},
			},
		}},
		ResearchShare: opts.researchShare,
	})
}

func newSink(opts *options) (marketminute.Sink, func() error, func(), error) {
	var sinks marketminute.MultiSink
	var jsonl *marketminute.JSONLSink
	var file *os.File
	var client *redis.Client
	if opts.out != "" {
		opened, err := os.OpenFile(opts.out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, nil, nil, errors.New("JSONL output is not writable")
		}
		file = opened
		jsonl = marketminute.NewJSONLSink(file)
		sinks = append(sinks, jsonl)
	}
	if opts.redisURL != "" {
		parsed, err := redis.ParseURL(opts.redisURL)
		if err != nil {
			if file != nil {
				file.Close()
			}
			return nil, nil, nil, errors.New("redis URL is invalid")
		}
		client = redis.NewClient(parsed)
		sink, err := marketminute.NewRedisSink(client, marketminute.DefaultRedisMaxLen)
		if err != nil {
			if file != nil {
				file.Close()
			}
			return nil, nil, nil, err
		}
		sinks = append(sinks, sink)
	}
	flush := func() error {
		if jsonl != nil {
			return jsonl.Flush()
		}
		return nil
	}
	closeAll := func() {
		if client != nil {
			client.Close()
		}
		if file != nil {
			file.Close()
		}
	}
	return sinks, flush, closeAll, nil
}

func newTokenSource(opts *options) (marketminute.TokenSource, func(context.Context, string) error, func(), error) {
	noop := func() {}
	switch opts.tokenSource {
	case "redis":
		url := opts.tokenRedis
		if url == "" {
			url = opts.redisURL
		}
		if url == "" {
			return nil, nil, noop, errors.New("redis token source needs -token-redis (or -redis)")
		}
		parsed, err := redis.ParseURL(url)
		if err != nil {
			return nil, nil, noop, errors.New("redis URL is invalid")
		}
		client := redis.NewClient(parsed)
		cached, err := kisread.NewCachedToken(client, opts.tokenKey)
		if err != nil {
			client.Close()
			return nil, nil, noop, err
		}
		return func(ctx context.Context) (string, error) {
			token, err := cached.Token(ctx)
			if err != nil {
				return "", marketminute.ErrTokenUnavailable
			}
			return token, nil
		}, nil, func() { client.Close() }, nil
	case "gatewayd-fetch":
		if opts.gatewaydURL == "" {
			return nil, nil, noop, errors.New("gatewayd token source needs -gatewayd-url")
		}
		if opts.gatewaydToken == "" {
			return nil, nil, noop, errors.New("gatewayd token source needs -gatewayd-token-env")
		}
		credential := os.Getenv(opts.gatewaydToken)
		if credential == "" {
			return nil, nil, noop, errors.New("gatewayd credential environment variable is unset")
		}
		fetcher, err := tokenfetch.NewClient(opts.gatewaydURL, credential)
		if err != nil {
			return nil, nil, noop, errors.New("gatewayd URL is invalid")
		}
		if opts.gatewaydName != "kis-live" && opts.gatewaydName != "kis-mock" {
			return nil, nil, noop, errors.New("gatewayd provider must be kis-live or kis-mock")
		}
		holder := tokenfetch.NewHolder(fetcher, opts.gatewaydName, time.Minute, false, time.Now)
		return func(ctx context.Context) (string, error) {
				token, _, err := holder.Current(ctx)
				if err != nil {
					return "", marketminute.ErrTokenUnavailable
				}
				return token.Value(), nil
			}, func(ctx context.Context, rejected string) error {
				_, err := holder.Reissue(ctx, rejected)
				return err
			}, noop, nil
	default:
		return nil, nil, noop, errors.New("token source must be redis or gatewayd-fetch")
	}
}
