package main

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/alert"
	"github.com/mgh3326/fillwire/internal/bookfeed"
	"github.com/mgh3326/fillwire/internal/netpolicy"
	"github.com/mgh3326/fillwire/internal/reader"
	"github.com/mgh3326/fillwire/internal/sink"
	"github.com/pelletier/go-toml/v2"
)

type fileConfig struct {
	Books  bookSettings  `toml:"books"`
	KIS    kisSettings   `toml:"kis"`
	Redis  redisSettings `toml:"redis"`
	Stream struct {
		Key          string `toml:"key"`
		MaxLen       int64  `toml:"max_len"`
		Group        string `toml:"consumer_group"`
		Consumer     string `toml:"consumer_name"`
		ClaimMinIdle string `toml:"claim_min_idle"`
		ReadBlock    string `toml:"read_block"`
	} `toml:"stream"`
	Ingest struct {
		URL         string `toml:"url"`
		TokenEnv    string `toml:"token_env"`
		TokenHeader string `toml:"token_header"`
		Batch       int64  `toml:"batch_size"`
		Timeout     string `toml:"timeout"`
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
	Alerts struct {
		RateLimit string `toml:"rate_limit"`
	} `toml:"alerts"`
}

type kisSettings struct {
	Broker        string           `toml:"broker"`
	Endpoint      string           `toml:"endpoint"`
	AccountMode   string           `toml:"account_mode"`
	ApprovalMode  string           `toml:"approval_mode"`
	RefreshMargin string           `toml:"approval_refresh_margin"`
	Venue         string           `toml:"venue"`
	HTSID         string           `toml:"hts_id"`
	AppKeyEnv     string           `toml:"app_key_env"`
	AppSecretEnv  string           `toml:"app_secret_env"`
	EventBuffer   int              `toml:"event_buffer"`
	DupTrackMax   int              `toml:"dup_track_max"`
	Gatewayd      gatewaydSettings `toml:"gatewayd"`
}
type redisSettings struct {
	URL string `toml:"url"`
}

// gatewaydSettings configures the optional approval ensure client used when
// approval_mode = "gatewayd". token_env names an environment variable whose
// value is sent as the auth credential; token_header names the header it
// travels in. With token_env empty no credential header is sent, matching the
// loopback gatewayd deployment and the Python reference.
type gatewaydSettings struct {
	URL         string `toml:"url"`
	TokenEnv    string `toml:"token_env"`
	TokenHeader string `toml:"token_header"`
}

type runtimeConfig struct {
	fileConfig
	claimMinIdle   time.Duration
	readBlock      time.Duration
	timeout        time.Duration
	retryMin       time.Duration
	retryMax       time.Duration
	drainTimeout   time.Duration
	ingestToken    string
	appKey         string
	appSecret      string
	gatewaydToken  string
	refreshMargin  time.Duration
	alertRateLimit time.Duration
	quotes         quoteSettings
	kisQuotes      kisQuoteSettings
	kisFutures     kisFuturesSettings
	recording      recordingSettings
	upbit          upbitSettings
	tossWS         tossWSSettings
	reconcile      reconcileSettings
}

func loadConfig(path string) (runtimeConfig, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return runtimeConfig{}, fmt.Errorf("config: read file: %w", err)
	}
	return parseConfig(contents, true)
}

// parseConfig resolves the legacy side lanes only for the standalone run.
func parseConfig(contents []byte, includeSideLanes bool) (runtimeConfig, error) {
	var err error
	var cfg runtimeConfig
	if err := toml.Unmarshal(contents, &cfg.fileConfig); err != nil {
		return runtimeConfig{}, errors.New("config: invalid TOML")
	}
	if includeSideLanes {
		// The promoted KIS quote lane resolves before validation and before the
		// credential checks: its REST recovery sends the app key and secret, and
		// validateStatic and the env lookup must read the same settings.
		cfg.kisQuotes = resolveKisQuoteSettings(contents)
		if cfg.kisFutures, err = resolveKisFuturesSettings(contents); err != nil {
			return runtimeConfig{}, err
		}
	} else {
		cfg.Books = bookSettings{}
	}
	if err := validateGatewaydTableKeys(contents); err != nil {
		return runtimeConfig{}, err
	}
	if strings.TrimSpace(cfg.KIS.Venue) == "" {
		cfg.KIS.Venue = "krx"
	}
	if strings.TrimSpace(cfg.Channel.DrainTimeout) == "" {
		cfg.Channel.DrainTimeout = "5s"
	}
	if cfg.KIS.RefreshMargin == "" {
		cfg.KIS.RefreshMargin = "1h"
	}
	if cfg.Alerts.RateLimit == "" {
		cfg.Alerts.RateLimit = alert.DefaultRateLimit.String()
	}
	if cfg.Ingest.TokenHeader == "" {
		cfg.Ingest.TokenHeader = sink.DefaultIngestTokenHeader
	}
	if cfg.KIS.Gatewayd.TokenHeader == "" {
		cfg.KIS.Gatewayd.TokenHeader = "Authorization"
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
	if cfg.refreshMargin, err = time.ParseDuration(cfg.KIS.RefreshMargin); err != nil || cfg.refreshMargin <= 0 || cfg.refreshMargin > reader.MaxRefreshMargin {
		return runtimeConfig{}, errors.New("config: invalid KIS approval_refresh_margin")
	}
	if cfg.alertRateLimit, err = time.ParseDuration(cfg.Alerts.RateLimit); err != nil || cfg.alertRateLimit <= 0 {
		return runtimeConfig{}, errors.New("config: invalid alerts rate_limit")
	}
	var ok bool
	if cfg.ingestToken, ok = os.LookupEnv(cfg.Ingest.TokenEnv); !ok || cfg.ingestToken == "" {
		return runtimeConfig{}, fmt.Errorf("config: required environment variable %s is unset", cfg.Ingest.TokenEnv)
	}
	if cfg.kisCredentialsRequired() {
		if err := cfg.loadKISCredentials(); err != nil {
			return runtimeConfig{}, err
		}
	}
	if cfg.usesGatewayd() && strings.TrimSpace(cfg.KIS.Gatewayd.TokenEnv) != "" {
		if cfg.gatewaydToken, ok = os.LookupEnv(cfg.KIS.Gatewayd.TokenEnv); !ok || cfg.gatewaydToken == "" {
			return runtimeConfig{}, fmt.Errorf("config: required environment variable %s is unset", cfg.KIS.Gatewayd.TokenEnv)
		}
	}
	if includeSideLanes {
		// The quote lane resolves last and cannot fail this function.
		cfg.quotes = resolveQuoteSettings(contents, cfg)
		if cfg.recording, err = resolveRecordingSettings(contents); err != nil {
			return runtimeConfig{}, err
		}
		if cfg.upbit, err = resolveUpbitSettings(contents); err != nil {
			return runtimeConfig{}, err
		}
		if cfg.tossWS, err = resolveTossWSSettings(contents, cfg.KIS.Gatewayd); err != nil {
			return runtimeConfig{}, err
		}
	}
	if cfg.reconcile, err = resolveReconcileSettings(contents); err != nil {
		return runtimeConfig{}, err
	}
	return cfg, nil
}

// usesGatewayd reports whether the approval mode talks to gatewayd: the
// ensure-then-Redis mode or the fetch mode.
func (cfg runtimeConfig) usesGatewayd() bool {
	return cfg.KIS.ApprovalMode == reader.ApprovalModeGatewayd || cfg.KIS.ApprovalMode == reader.ApprovalModeGatewaydFetch
}

// kisCredentialsRequired reports whether an enabled lane reads the KIS app
// key and secret: approval_mode "" issues approval keys through the KIS REST
// client, and the books lane sends both on REST reads and derives the mock
// token-cache key from the app key. cache-only, gatewayd and gatewayd-fetch never issue, so
// with books off neither credential is needed; configured names are then
// optional and their values are never read.
func (cfg runtimeConfig) kisCredentialsRequired() bool {
	quotes := cfg.kisQuotes.enabled && cfg.kisQuotes.err == nil
	return cfg.KIS.ApprovalMode == "" || cfg.Books.Enabled || quotes
}

func (cfg *runtimeConfig) loadKISCredentials() error {
	var ok bool
	if cfg.appKey, ok = os.LookupEnv(cfg.KIS.AppKeyEnv); !ok || cfg.appKey == "" {
		return fmt.Errorf("config: required environment variable %s is unset", cfg.KIS.AppKeyEnv)
	}
	if cfg.appSecret, ok = os.LookupEnv(cfg.KIS.AppSecretEnv); !ok || cfg.appSecret == "" {
		return fmt.Errorf("config: required environment variable %s is unset", cfg.KIS.AppSecretEnv)
	}
	return nil
}

func (cfg runtimeConfig) validateKISConnection() error {
	if cfg.KIS.ApprovalMode != "" && cfg.KIS.ApprovalMode != reader.ApprovalModeCacheOnly && !cfg.usesGatewayd() {
		return errors.New("config: unsupported KIS approval_mode")
	}
	if cfg.KIS.ApprovalMode == reader.ApprovalModeGatewaydFetch {
		// The fetch client always sends "Authorization: Bearer <credential>"
		// and gatewayd answers 401 without it, so the credential is required.
		if strings.TrimSpace(cfg.KIS.Gatewayd.TokenEnv) == "" {
			return errors.New("config: KIS gatewayd-fetch requires [kis.gatewayd] token_env")
		}
		if !strings.EqualFold(cfg.KIS.Gatewayd.TokenHeader, "Authorization") {
			return errors.New("config: KIS gatewayd-fetch sends the credential as Authorization; token_header must be Authorization")
		}
	}
	if cfg.usesGatewayd() {
		normalized, err := reader.NormalizeGatewaydURL(cfg.KIS.Gatewayd.URL)
		if err != nil {
			return errors.New("config: invalid KIS gatewayd url")
		}
		if strings.TrimSpace(cfg.KIS.Gatewayd.TokenEnv) != "" {
			if err := sink.ValidateIngestTokenHeader(cfg.KIS.Gatewayd.TokenHeader); err != nil {
				return errors.New("config: invalid KIS gatewayd token_header")
			}
			// The token travels as a header on every ensure POST; over plain
			// http to a remote host it would be sent in cleartext.
			parsed, _ := url.Parse(normalized)
			if parsed.Scheme != "https" && !netpolicy.IsLoopbackHost(parsed.Hostname()) {
				return errors.New("config: KIS gatewayd credential requires https or a loopback url")
			}
		}
	}
	if cfg.KIS.Endpoint != "live" && cfg.KIS.Endpoint != "mock" {
		return errors.New("config: KIS endpoint must be live or mock")
	}
	if cfg.KIS.AccountMode != cfg.KIS.Endpoint {
		return errors.New("config: account_mode must match KIS endpoint")
	}
	return nil
}

func (cfg runtimeConfig) validateStatic() error {
	if cfg.KIS.Broker != "kis" {
		return errors.New("config: broker must be kis")
	}
	if err := cfg.validateKISConnection(); err != nil {
		return err
	}
	if strings.TrimSpace(cfg.KIS.Venue) == "" || strings.TrimSpace(cfg.KIS.HTSID) == "" {
		return errors.New("config: venue and HTS ID are required")
	}
	if cfg.kisCredentialsRequired() {
		if strings.TrimSpace(cfg.KIS.AppKeyEnv) == "" {
			return errors.New("config: KIS app_key_env is required")
		}
		if strings.TrimSpace(cfg.KIS.AppSecretEnv) == "" {
			return errors.New("config: KIS app_secret_env is required")
		}
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
	if err := sink.ValidateIngestTokenHeader(cfg.Ingest.TokenHeader); err != nil {
		return errors.New("config: invalid ingest token_header")
	}
	// The access token itself is read lazily on each REST call, since its
	// owner refreshes it; only the name is checked here. Exactly one token
	// source must be configured: access_token_env, or
	// access_token_source = "redis" for the shared token_keys.py cache key.
	if cfg.Books.Enabled {
		if !bookfeed.SymbolOK(cfg.Books.Symbol) {
			return errors.New("config: books symbol required")
		}
		source := strings.TrimSpace(cfg.Books.AccessTokenSource)
		if source != "" && source != "env" && source != "redis" {
			return errors.New("config: invalid books access_token_source")
		}
		envSet := strings.TrimSpace(cfg.Books.AccessTokenEnv) != ""
		if envSet == (source == "redis") {
			return errors.New("config: books requires exactly one of access_token_env or access_token_source = \"redis\"")
		}
	}
	return nil
}

// validateGatewaydTableKeys strict-decodes [kis.gatewayd] like the quotes,
// recording and reconcile tables: the file-wide unmarshal ignores unknown
// fields, so a misspelled key such as tokn_env would otherwise be dropped
// silently and the ensure call would go out without its credential header.
func validateGatewaydTableKeys(contents []byte) error {
	var document map[string]any
	if err := toml.Unmarshal(contents, &document); err != nil {
		return nil // the file-wide decode above already reported the syntax error
	}
	kisTable, ok := document["kis"].(map[string]any)
	if !ok {
		return nil
	}
	rawTable, present := kisTable["gatewayd"]
	if !present {
		return nil
	}
	table, ok := rawTable.(map[string]any)
	if !ok {
		return errors.New("config: invalid KIS gatewayd table")
	}
	encoded, err := toml.Marshal(table)
	if err != nil {
		return errors.New("config: invalid KIS gatewayd table")
	}
	var g gatewaydSettings
	decoder := toml.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&g); err != nil {
		return errors.New("config: invalid or unknown KIS gatewayd key")
	}
	return nil
}

func validateRedisURL(raw string) error {
	if err := netpolicy.ValidateRedisURL(raw); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	return nil
}
