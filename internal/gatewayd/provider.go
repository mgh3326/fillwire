package gatewayd

import (
	"net/url"
	"strings"
	"time"

	"github.com/mgh3326/broker-edge/internal/kismockread"
)

// TokenProvider is the closed set of token issuers gatewayd may own. It is
// deliberately unrelated to order or execution provider selection.
type TokenProvider string

const (
	ProviderKISMock TokenProvider = "kis-mock"
	ProviderKISLive TokenProvider = "kis-live"
	ProviderToss    TokenProvider = "toss"
)

const (
	// KISLiveBaseURL is only used by the KIS live OAuth token endpoint. No
	// order endpoint is represented in gatewayd.
	KISLiveBaseURL = "https://openapi.koreainvestment.com:9443"
	TossBaseURL    = "https://openapi.tossinvest.com"

	tossTokenExpiryBuffer = 120 * time.Second
	tossLockWaitTimeout   = 5 * time.Second
	tossLockPollInterval  = 50 * time.Millisecond
)

func knownProvider(provider TokenProvider) bool {
	switch provider {
	case ProviderKISMock, ProviderKISLive, ProviderToss:
		return true
	default:
		return false
	}
}

func providerBaseURL(provider TokenProvider) (string, bool) {
	switch provider {
	case ProviderKISMock:
		return kismockread.MockBaseURL, true
	case ProviderKISLive:
		return KISLiveBaseURL, true
	case ProviderToss:
		return TossBaseURL, true
	default:
		return "", false
	}
}

func providerOAuthPath(provider TokenProvider) (string, bool) {
	switch provider {
	case ProviderKISMock, ProviderKISLive:
		return oauthTokenPath, true
	case ProviderToss:
		return tossOAuthTokenPath, true
	default:
		return "", false
	}
}

func providerTokenExpiryBuffer(provider TokenProvider) time.Duration {
	if provider == ProviderToss {
		return tossTokenExpiryBuffer
	}
	return tokenExpiryBuffer
}

func providerInitialLockWait(provider TokenProvider) time.Duration {
	if provider == ProviderToss {
		return 0
	}
	return lockInitialWait
}

func providerLockPollInterval(provider TokenProvider) time.Duration {
	if provider == ProviderToss {
		return tossLockPollInterval
	}
	return lockPollInterval
}

func providerLockWaitTimeout(provider TokenProvider) time.Duration {
	if provider == ProviderToss {
		return tossLockWaitTimeout
	}
	return lockWaitTimeout
}

func providerCacheTTL(provider TokenProvider, expiresIn int64) time.Duration {
	ttl := time.Duration(expiresIn) * time.Second
	if provider != ProviderToss {
		ttl += tokenExpiryBuffer
	}
	return ttl
}

// validateProviderBaseURL makes a provider's authority a compile-time choice.
// In particular, the KIS mock and live authorities cannot be exchanged.
func validateProviderBaseURL(provider TokenProvider, rawURL string) bool {
	expected, found := providerBaseURL(provider)
	if !found || rawURL != expected {
		return false
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed == nil || parsed.User != nil || parsed.Path != "" ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return false
	}
	expectedURL, err := url.Parse(expected)
	return err == nil && strings.EqualFold(parsed.Scheme, "https") &&
		strings.EqualFold(parsed.Host, expectedURL.Host)
}

// ParseProviders accepts the command-line comma list and rejects duplicates so
// one provider cannot be refreshed twice per interval by configuration error.
func ParseProviders(value string) ([]TokenProvider, error) {
	if strings.TrimSpace(value) == "" {
		return nil, errConfiguration
	}
	parts := strings.Split(value, ",")
	providers := make([]TokenProvider, 0, len(parts))
	seen := make(map[TokenProvider]struct{}, len(parts))
	for _, part := range parts {
		provider := TokenProvider(strings.TrimSpace(part))
		if !knownProvider(provider) {
			return nil, errConfiguration
		}
		if _, duplicate := seen[provider]; duplicate {
			return nil, errConfiguration
		}
		seen[provider] = struct{}{}
		providers = append(providers, provider)
	}
	return providers, nil
}

func normalizedProviders(providers []TokenProvider) ([]TokenProvider, error) {
	if len(providers) == 0 {
		return nil, errConfiguration
	}
	joined := make([]string, len(providers))
	for index, provider := range providers {
		joined[index] = string(provider)
	}
	return ParseProviders(strings.Join(joined, ","))
}

func orderedProviders() []TokenProvider {
	return []TokenProvider{ProviderKISMock, ProviderKISLive, ProviderToss}
}
