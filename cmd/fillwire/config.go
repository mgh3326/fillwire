package main

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/sink"
	"github.com/pelletier/go-toml/v2"
)

type fileConfig struct {
	KIS struct {
		Broker       string `toml:"broker"`
		Endpoint     string `toml:"endpoint"`
		AccountMode  string `toml:"account_mode"`
		Venue        string `toml:"venue"`
		HTSID        string `toml:"hts_id"`
		AppKeyEnv    string `toml:"app_key_env"`
		AppSecretEnv string `toml:"app_secret_env"`
		EventBuffer  int    `toml:"event_buffer"`
		DupTrackMax  int    `toml:"dup_track_max"`
	} `toml:"kis"`
	Redis struct {
		URL string `toml:"url"`
	} `toml:"redis"`
	Stream struct {
		Key          string `toml:"key"`
		MaxLen       int64  `toml:"max_len"`
		Group        string `toml:"consumer_group"`
		Consumer     string `toml:"consumer_name"`
		ClaimMinIdle string `toml:"claim_min_idle"`
		ReadBlock    string `toml:"read_block"`
	} `toml:"stream"`
	Ingest struct {
		URL      string `toml:"url"`
		TokenEnv string `toml:"token_env"`
		Batch    int64  `toml:"batch_size"`
		Timeout  string `toml:"timeout"`
	} `toml:"ingest"`
	Channel struct {
		Buffer       int    `toml:"buffer"`
		DrainTimeout string `toml:"drain_timeout"`
	} `toml:"channel"`
	Retry struct {
		Min    string  `toml:"min"`
		Max    string  `toml:"max"`
		Factor float64 `toml:"factor"`
	} `toml:"retry"`
}

type runtimeConfig struct {
	fileConfig
	claimMinIdle time.Duration
	readBlock    time.Duration
	timeout      time.Duration
	retryMin     time.Duration
	retryMax     time.Duration
	drainTimeout time.Duration
	ingestToken  string
	appKey       string
	appSecret    string
}

func loadConfig(path string) (runtimeConfig, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return runtimeConfig{}, fmt.Errorf("config: read file: %w", err)
	}
	var cfg runtimeConfig
	if err := toml.Unmarshal(contents, &cfg.fileConfig); err != nil {
		return runtimeConfig{}, errors.New("config: invalid TOML")
	}
	if strings.TrimSpace(cfg.KIS.Venue) == "" {
		cfg.KIS.Venue = "krx"
	}
	if strings.TrimSpace(cfg.Channel.DrainTimeout) == "" {
		cfg.Channel.DrainTimeout = "5s"
	}
	if err := cfg.validateStatic(); err != nil {
		return runtimeConfig{}, err
	}
	if cfg.claimMinIdle, err = time.ParseDuration(cfg.Stream.ClaimMinIdle); err != nil || cfg.claimMinIdle < 0 {
		return runtimeConfig{}, errors.New("config: invalid stream claim_min_idle")
	}
	if cfg.readBlock, err = time.ParseDuration(cfg.Stream.ReadBlock); err != nil || cfg.readBlock < 0 {
		return runtimeConfig{}, errors.New("config: invalid stream read_block")
	}
	if cfg.timeout, err = time.ParseDuration(cfg.Ingest.Timeout); err != nil || cfg.timeout <= 0 {
		return runtimeConfig{}, errors.New("config: invalid ingest timeout")
	}
	if cfg.drainTimeout, err = time.ParseDuration(cfg.Channel.DrainTimeout); err != nil || cfg.drainTimeout <= 0 {
		return runtimeConfig{}, errors.New("config: invalid channel drain_timeout")
	}
	if cfg.retryMin, err = time.ParseDuration(cfg.Retry.Min); err != nil || cfg.retryMin <= 0 {
		return runtimeConfig{}, errors.New("config: invalid retry min")
	}
	if cfg.retryMax, err = time.ParseDuration(cfg.Retry.Max); err != nil || cfg.retryMax < cfg.retryMin {
		return runtimeConfig{}, errors.New("config: invalid retry max")
	}
	if cfg.Retry.Factor < 1 {
		return runtimeConfig{}, errors.New("config: retry factor must be at least one")
	}
	var ok bool
	if cfg.ingestToken, ok = os.LookupEnv(cfg.Ingest.TokenEnv); !ok || cfg.ingestToken == "" {
		return runtimeConfig{}, fmt.Errorf("config: required environment variable %s is unset", cfg.Ingest.TokenEnv)
	}
	if cfg.appKey, ok = os.LookupEnv(cfg.KIS.AppKeyEnv); !ok || cfg.appKey == "" {
		return runtimeConfig{}, fmt.Errorf("config: required environment variable %s is unset", cfg.KIS.AppKeyEnv)
	}
	if cfg.appSecret, ok = os.LookupEnv(cfg.KIS.AppSecretEnv); !ok || cfg.appSecret == "" {
		return runtimeConfig{}, fmt.Errorf("config: required environment variable %s is unset", cfg.KIS.AppSecretEnv)
	}
	return cfg, nil
}

func (cfg runtimeConfig) validateStatic() error {
	if cfg.KIS.Broker != "kis" {
		return errors.New("config: broker must be kis")
	}
	if cfg.KIS.Endpoint != "live" && cfg.KIS.Endpoint != "mock" {
		return errors.New("config: KIS endpoint must be live or mock")
	}
	if cfg.KIS.AccountMode != cfg.KIS.Endpoint {
		return errors.New("config: account_mode must match KIS endpoint")
	}
	if strings.TrimSpace(cfg.KIS.Venue) == "" || strings.TrimSpace(cfg.KIS.HTSID) == "" {
		return errors.New("config: venue and HTS ID are required")
	}
	if strings.TrimSpace(cfg.KIS.AppKeyEnv) == "" || strings.TrimSpace(cfg.KIS.AppSecretEnv) == "" {
		return errors.New("config: KIS credential environment names are required")
	}
	if cfg.KIS.EventBuffer <= 0 || cfg.KIS.DupTrackMax <= 0 || cfg.Channel.Buffer <= 0 {
		return errors.New("config: buffer and duplicate tracking limits must be positive")
	}
	if strings.TrimSpace(cfg.Redis.URL) == "" || strings.TrimSpace(cfg.Stream.Key) == "" || strings.TrimSpace(cfg.Stream.Group) == "" || strings.TrimSpace(cfg.Stream.Consumer) == "" {
		return errors.New("config: Redis and stream fields are required")
	}
	if err := validateRedisURL(cfg.Redis.URL); err != nil {
		return err
	}
	if cfg.Stream.MaxLen <= 0 || cfg.Ingest.Batch < 1 || cfg.Ingest.Batch > 200 {
		return errors.New("config: max_len and batch_size are out of range")
	}
	if strings.TrimSpace(cfg.Ingest.URL) == "" || strings.TrimSpace(cfg.Ingest.TokenEnv) == "" {
		return errors.New("config: ingest URL and token environment name are required")
	}
	if err := sink.ValidateIngestURL(cfg.Ingest.URL); err != nil {
		return errors.New("config: ingest URL rejected")
	}
	return nil
}

func validateRedisURL(raw string) error {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Scheme == "" {
		return errors.New("config: Redis URL is invalid")
	}
	switch strings.ToLower(parsed.Scheme) {
	case "rediss":
		if parsed.Host != "" {
			return nil
		}
	case "redis":
		if redisLoopbackHost(parsed.Hostname()) {
			return nil
		}
	case "unix":
		if parsed.Path != "" || parsed.Opaque != "" {
			return nil
		}
	}
	return errors.New("config: Redis URL scheme is not permitted")
}

func redisLoopbackHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
