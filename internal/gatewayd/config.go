// Package gatewayd provides a deliberately small, loopback-only OAuth token
// issuer. It has no order, command, or execution boundary.
package gatewayd

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

const defaultListenAddress = "127.0.0.1:8791"

var errConfiguration = errors.New("gateway configuration invalid")

// ProviderConfig is a credential scope whose provider and base URL are both
// validated against compile-time constants before any HTTP client is built.
// AppKey/AppSecret name the generic OAuth client credentials; for Toss they
// correspond to TOSS_API_CLIENT_ID/TOSS_API_CLIENT_SECRET.
type ProviderConfig struct {
	Provider  TokenProvider
	BaseURL   string
	AppKey    string
	AppSecret string
}

// Config holds process configuration. The BaseURL/AppKey/AppSecret fields are
// retained for the existing kis-mock-only constructor; new callers use
// ProviderConfigs. None of these values are rendered into output or logs.
type Config struct {
	BaseURL         string
	AppKey          string
	AppSecret       string
	RedisURL        string
	ListenAddress   string
	Timeout         time.Duration
	ProviderConfigs map[TokenProvider]ProviderConfig
	// FetchConsumers authorizes POST /v1/tokens/{provider}/fetch only. Empty
	// (the default) rejects every fetch with 401.
	FetchConsumers []FetchConsumer
}

// ConfigFromEnv preserves the safe historical default: only kis-mock is
// configured unless the process explicitly passes --providers.
func ConfigFromEnv(lookup func(string) string) (Config, error) {
	return ConfigFromEnvForProviders(lookup, []TokenProvider{ProviderKISMock})
}

// ConfigFromEnvForProviders loads credentials only for the explicitly chosen
// providers. The default CLI provider list is kis-mock, so live token issuance
// cannot be enabled by merely placing live credentials in the environment.
func ConfigFromEnvForProviders(lookup func(string) string, providers []TokenProvider) (Config, error) {
	if lookup == nil {
		lookup = func(string) string { return "" }
	}
	normalized, err := normalizedProviders(providers)
	if err != nil {
		return Config{}, errConfiguration
	}
	config := Config{
		RedisURL:        strings.TrimSpace(lookup("REDIS_URL")),
		ListenAddress:   strings.TrimSpace(lookup("GATEWAYD_LISTEN_ADDR")),
		Timeout:         10 * time.Second,
		ProviderConfigs: make(map[TokenProvider]ProviderConfig, len(normalized)),
	}
	if config.ListenAddress == "" {
		config.ListenAddress = defaultListenAddress
	}
	if config.RedisURL == "" || !loopbackAddress(config.ListenAddress) {
		return Config{}, errConfiguration
	}
	for _, provider := range normalized {
		providerConfig, configErr := providerConfigFromEnv(lookup, provider)
		if configErr != nil {
			return Config{}, errConfiguration
		}
		config.ProviderConfigs[provider] = providerConfig
	}
	consumers, err := parseFetchConsumers(lookup("GATEWAYD_FETCH_CONSUMERS"))
	if err != nil {
		return Config{}, errConfiguration
	}
	config.FetchConsumers = consumers
	// Keep the pre-existing constructor and its callers source compatible.
	if mockConfig, found := config.ProviderConfigs[ProviderKISMock]; found {
		config.BaseURL = mockConfig.BaseURL
		config.AppKey = mockConfig.AppKey
		config.AppSecret = mockConfig.AppSecret
	}
	return config, nil
}

func providerConfigFromEnv(lookup func(string) string, provider TokenProvider) (ProviderConfig, error) {
	baseURL, found := providerBaseURL(provider)
	if !found {
		return ProviderConfig{}, errConfiguration
	}
	var appKeyName, appSecretName string
	switch provider {
	case ProviderKISMock, ProviderKISMockApproval:
		appKeyName, appSecretName = "KIS_MOCK_APP_KEY", "KIS_MOCK_APP_SECRET"
	case ProviderKISLive, ProviderKISLiveApproval:
		appKeyName, appSecretName = "KIS_APP_KEY", "KIS_APP_SECRET"
	case ProviderToss:
		appKeyName, appSecretName = "TOSS_API_CLIENT_ID", "TOSS_API_CLIENT_SECRET"
	default:
		return ProviderConfig{}, errConfiguration
	}
	config := ProviderConfig{
		Provider:  provider,
		BaseURL:   baseURL,
		AppKey:    strings.TrimSpace(lookup(appKeyName)),
		AppSecret: strings.TrimSpace(lookup(appSecretName)),
	}
	if !validProviderConfig(config) {
		return ProviderConfig{}, errConfiguration
	}
	return config, nil
}

func validProviderConfig(config ProviderConfig) bool {
	return knownProvider(config.Provider) && validateProviderBaseURL(config.Provider, config.BaseURL) &&
		config.AppKey != "" && config.AppSecret != "" &&
		safeHeaderText(config.AppKey) && safeHeaderText(config.AppSecret)
}

func (config Config) providerConfig(provider TokenProvider) (ProviderConfig, bool) {
	if providerConfig, found := config.ProviderConfigs[provider]; found {
		return providerConfig, true
	}
	// Compatibility for direct kis-mock Config literals in focused tests and
	// internal callers predating ProviderConfigs.
	if provider == ProviderKISMock && config.BaseURL != "" {
		return ProviderConfig{
			Provider:  ProviderKISMock,
			BaseURL:   config.BaseURL,
			AppKey:    config.AppKey,
			AppSecret: config.AppSecret,
		}, true
	}
	return ProviderConfig{}, false
}

func safeHeaderText(value string) bool {
	return !strings.ContainsAny(value, "\r\n")
}

func loopbackAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return false
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 0 || portNumber > 65535 {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

const minFetchCredentialLength = 32

// FetchConsumer is one caller of /fetch: a bearer credential and the closed set
// of providers it may read. The credential is never rendered anywhere.
type FetchConsumer struct {
	Name       string
	Credential string
	Providers  []TokenProvider
}

// parseFetchConsumers reads GATEWAYD_FETCH_CONSUMERS, a JSON array of
// {"name","credential","providers"}. Blank means fetch is off. Duplicate names
// or credentials, short credentials, empty or unknown provider lists, and
// unknown fields are configuration errors (the error carries no value).
func parseFetchConsumers(raw string) ([]FetchConsumer, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var entries []struct {
		Name       string   `json:"name"`
		Credential string   `json:"credential"`
		Providers  []string `json:"providers"`
	}
	if err := decoder.Decode(&entries); err != nil || entries == nil {
		return nil, errConfiguration
	}
	var trailing json.RawMessage
	if decoder.Decode(&trailing) != io.EOF {
		return nil, errConfiguration
	}
	consumers := make([]FetchConsumer, 0, len(entries))
	names := make(map[string]struct{}, len(entries))
	credentials := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.Name == "" || len(entry.Credential) < minFetchCredentialLength ||
			!safeHeaderText(entry.Credential) || strings.ContainsAny(entry.Credential, " \t") ||
			len(entry.Providers) == 0 {
			return nil, errConfiguration
		}
		if _, duplicate := names[entry.Name]; duplicate {
			return nil, errConfiguration
		}
		if _, duplicate := credentials[entry.Credential]; duplicate {
			return nil, errConfiguration
		}
		names[entry.Name] = struct{}{}
		credentials[entry.Credential] = struct{}{}
		providers := make([]TokenProvider, 0, len(entry.Providers))
		seen := make(map[TokenProvider]struct{}, len(entry.Providers))
		for _, rawProvider := range entry.Providers {
			provider := TokenProvider(strings.TrimSpace(rawProvider))
			if !knownProvider(provider) {
				return nil, errConfiguration
			}
			if _, duplicate := seen[provider]; duplicate {
				return nil, errConfiguration
			}
			seen[provider] = struct{}{}
			providers = append(providers, provider)
		}
		consumers = append(consumers, FetchConsumer{Name: entry.Name, Credential: entry.Credential, Providers: providers})
	}
	return consumers, nil
}
