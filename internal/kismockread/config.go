package kismockread

import (
	"strings"
	"time"
)

// Config holds process-local configuration. Its values must never be rendered
// into logs, errors, or CLI JSON.
type Config struct {
	BaseURL   string
	AppKey    string
	AppSecret string
	AccountNo string
	RedisURL  string
	Timeout   time.Duration
}

// ConfigFromEnv loads only the explicitly documented environment names.
func ConfigFromEnv(lookup func(string) string) (Config, *SafeError) {
	config := Config{
		BaseURL:   MockBaseURL,
		AppKey:    strings.TrimSpace(lookup("KIS_MOCK_APP_KEY")),
		AppSecret: strings.TrimSpace(lookup("KIS_MOCK_APP_SECRET")),
		AccountNo: strings.TrimSpace(lookup("KIS_MOCK_ACCOUNT_NO")),
		RedisURL:  strings.TrimSpace(lookup("REDIS_URL")),
		Timeout:   10 * time.Second,
	}
	if config.AppKey == "" || config.AppSecret == "" || config.AccountNo == "" || config.RedisURL == "" {
		return Config{}, safeError(CodeConfigurationMissing)
	}
	if !safeHeaderText(config.AppKey) || !safeHeaderText(config.AppSecret) {
		return Config{}, safeError(CodeConfigurationMissing)
	}
	if _, _, err := splitAccountNo(config.AccountNo); err != nil {
		return Config{}, err
	}
	return config, nil
}

func splitAccountNo(accountNo string) (string, string, *SafeError) {
	cleaned := strings.TrimSpace(accountNo)
	if len(cleaned) == 8 {
		for _, character := range cleaned {
			if character < '0' || character > '9' {
				return "", "", safeError(CodeInvalidInput)
			}
		}
		// VTS credentials commonly provide CANO alone. The documented default
		// account product code for that canonical eight-digit form is 01.
		return cleaned, "01", nil
	}
	if len(cleaned) == 11 && cleaned[8] == '-' {
		cleaned = cleaned[:8] + cleaned[9:]
	}
	if len(cleaned) != 10 {
		return "", "", safeError(CodeInvalidInput)
	}
	for _, character := range cleaned {
		if character < '0' || character > '9' {
			return "", "", safeError(CodeInvalidInput)
		}
	}
	return cleaned[:8], cleaned[8:], nil
}

func safeHeaderText(value string) bool {
	return !strings.ContainsAny(value, "\r\n")
}
