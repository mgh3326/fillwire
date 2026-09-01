// Package gatewayd provides the deliberately small, loopback-only KIS mock
// token issuer. It has no order or intent boundary.
package gatewayd

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/broker-edge/internal/kismockread"
)

const defaultListenAddress = "127.0.0.1:8791"

var errConfiguration = errors.New("gateway configuration invalid")

// Config contains the only process configuration gatewayd accepts. BaseURL is
// fixed to the VTS authority rather than being configurable.
type Config struct {
	BaseURL       string
	AppKey        string
	AppSecret     string
	RedisURL      string
	ListenAddress string
	Timeout       time.Duration
}

// ConfigFromEnv loads only the documented mock issuer settings. An invalid
// public listener is rejected before a server can be opened.
func ConfigFromEnv(lookup func(string) string) (Config, error) {
	if lookup == nil {
		lookup = func(string) string { return "" }
	}
	config := Config{
		BaseURL:       kismockread.MockBaseURL,
		AppKey:        strings.TrimSpace(lookup("KIS_MOCK_APP_KEY")),
		AppSecret:     strings.TrimSpace(lookup("KIS_MOCK_APP_SECRET")),
		RedisURL:      strings.TrimSpace(lookup("REDIS_URL")),
		ListenAddress: strings.TrimSpace(lookup("GATEWAYD_LISTEN_ADDR")),
		Timeout:       10 * time.Second,
	}
	if config.ListenAddress == "" {
		config.ListenAddress = defaultListenAddress
	}
	if config.AppKey == "" || config.AppSecret == "" || config.RedisURL == "" ||
		!safeHeaderText(config.AppKey) || !safeHeaderText(config.AppSecret) ||
		!loopbackAddress(config.ListenAddress) {
		return Config{}, errConfiguration
	}
	return config, nil
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
