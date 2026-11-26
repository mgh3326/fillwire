// Package binancedemo records read-only Binance Spot Demo market and user streams.
package binancedemo

import (
	"errors"
	"fmt"
	"net/url"
)

const (
	MarketHost = "demo-stream.binance.com"
	UserHost   = "demo-ws-api.binance.com"
	MarketURL  = "wss://demo-stream.binance.com:9443/stream"
	UserURL    = "wss://demo-ws-api.binance.com/ws-api/v3"
)

type streamKind string

const (
	marketStream streamKind = "market"
	userStream   streamKind = "user"
)

var deniedHosts = map[string]struct{}{
	"stream.binance.com": {}, "data-stream.binance.vision": {},
	"ws-api.binance.com": {}, "fstream.binance.com": {},
	"stream.testnet.binance.vision": {}, "ws-api.testnet.binance.vision": {},
	"demo-fstream.binance.com": {}, "demo-api.binance.com": {},
	"demo-fapi.binance.com": {}, "api.binance.com": {},
}

// ValidateURL accepts only the two documented Spot Demo stream endpoints.
// Call it before every dial; it never performs network I/O.
func ValidateURL(raw string, kind string) error {
	var expectedHost string
	var paths map[string]struct{}
	switch streamKind(kind) {
	case marketStream:
		expectedHost = MarketHost
		paths = map[string]struct{}{"/ws": {}, "/stream": {}}
	case userStream:
		expectedHost = UserHost
		paths = map[string]struct{}{"/ws-api/v3": {}}
	default:
		return errors.New("binance demo: unknown stream kind")
	}
	u, err := url.Parse(raw)
	if err != nil || u == nil || u.Host == "" {
		return errors.New("binance demo: malformed stream URL")
	}
	host := u.Hostname()
	if _, denied := deniedHosts[host]; denied {
		return fmt.Errorf("binance demo: stream host %q is denied", host)
	}
	if host != expectedHost {
		return fmt.Errorf("binance demo: stream host %q is not the Spot Demo %s host", host, kind)
	}
	if u.Scheme != "wss" || (u.Port() != "" && u.Port() != "443" && u.Port() != "9443") {
		return errors.New("binance demo: stream requires wss on port 443 or 9443")
	}
	if u.User != nil || u.Fragment != "" {
		return errors.New("binance demo: stream URL userinfo and fragment are denied")
	}
	if _, ok := paths[u.Path]; !ok {
		return errors.New("binance demo: stream URL path is not allowed")
	}
	return nil
}
