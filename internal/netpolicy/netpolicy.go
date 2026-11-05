// Package netpolicy holds the transport rules every fillwire binary applies
// before it sends a credential: which hosts count as loopback, and which Redis
// URLs may be used without TLS. Keeping one copy means the runtime and the
// operator commands accept and reject exactly the same addresses.
package netpolicy

import (
	"errors"
	"net"
	"net/url"
	"strings"
)

// IsLoopbackHost reports whether host names this machine. It is
// case-insensitive, ignores surrounding space and a trailing root dot, and
// accepts "localhost" and any loopback IP literal.
func IsLoopbackHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Errors returned by ValidateRedisURL.
var (
	ErrRedisURLInvalid   = errors.New("Redis URL is invalid")
	ErrRedisURLPlaintext = errors.New("Redis URL scheme is not permitted")
)

// ValidateRedisURL accepts rediss:// with a host, redis:// only to a loopback
// host, and unix:// with a socket path. Anything else could send a cached
// credential over a remote plaintext connection.
func ValidateRedisURL(raw string) error {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Scheme == "" {
		return ErrRedisURLInvalid
	}
	switch strings.ToLower(parsed.Scheme) {
	case "rediss":
		if parsed.Host != "" {
			return nil
		}
	case "redis":
		if IsLoopbackHost(parsed.Hostname()) {
			return nil
		}
	case "unix":
		if parsed.Path != "" || parsed.Opaque != "" {
			return nil
		}
	}
	return ErrRedisURLPlaintext
}
