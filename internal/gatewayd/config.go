// Package gatewayd provides a deliberately small, loopback-only OAuth token
// issuer. It has no order, command, or execution boundary.
package gatewayd

import (
	"errors"
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
	case ProviderKISMock:
		appKeyName, appSecretName = "KIS_MOCK_APP_KEY", "KIS_MOCK_APP_SECRET"
	case ProviderKISLive:
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
