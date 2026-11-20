// Package tokenfetch is the consumer side of gatewayd's
// POST /v1/tokens/{provider}/fetch: the broker token comes back in the
// response and the caller keeps it in memory.
//
// The token value and the consumer credential never appear in an error, a
// log line or a String/%v rendering produced here. Errors are sentinels
// chosen by HTTP status; the URL, response body and net/http errors are
// never wrapped.
package tokenfetch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/netpolicy"
)

// Transport rules shared with the ensure client: a 5s request budget, a 3s
// connect bound, no proxy environment and no redirects.
const (
	RequestTimeout = 5 * time.Second
	DialTimeout    = 3 * time.Second
	maxBodyBytes   = 64 << 10
)

// Sources gatewayd reports for a returned token.
const (
	SourceMemory = "memory"
	SourceRedis  = "redis"
	SourceIssued = "issued"
)

// Typed failures. 401/403/404/400 are operator-side problems (credential,
// allowlist, provider name, request shape); ErrIssueUnavailable is gatewayd's
// 503 (lock contended or broker issue failed); ErrUnavailable is everything
// else (transport, timeout, other status, malformed answer). Callers never
// fall back to issuing locally on any of them.
var (
	ErrUnauthenticated     = errors.New("tokenfetch: gatewayd rejected the credential (401 unauthenticated)")
	ErrProviderNotAllowed  = errors.New("tokenfetch: provider not allowed for this credential (403 provider_not_allowed)")
	ErrUnknownProvider     = errors.New("tokenfetch: gatewayd does not know the provider (404 unknown_provider)")
	ErrBadRequest          = errors.New("tokenfetch: gatewayd rejected the request (400 bad_request)")
	ErrIssueUnavailable    = errors.New("tokenfetch: gatewayd could not issue a token (503 issue_unavailable)")
	ErrUnavailable         = errors.New("tokenfetch: gatewayd fetch unavailable")
	ErrRejectedTokenReturn = errors.New("tokenfetch: gatewayd returned the token the broker rejected")
	ErrConfig              = errors.New("tokenfetch: invalid configuration")
)

var (
	providerPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)
	sha256Pattern   = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// Fetcher asks gatewayd for the current token of one provider. A non-empty
// failedSHA256 names the token the broker just rejected.
type Fetcher interface {
	Fetch(ctx context.Context, provider, failedSHA256 string) (Token, error)
}

// Token is one fetched token. The value is reachable only through Value so
// that formatting a Token or logging it cannot leak it.
type Token struct {
	value     string
	ExpiresAt time.Time
	Source    string
}

// NewToken builds a Token; production code gets them from Client.Fetch.
func NewToken(value string, expiresAt time.Time, source string) Token {
	return Token{value: value, ExpiresAt: expiresAt, Source: source}
}

func (t Token) Value() string              { return t.value }
func (t Token) String() string             { return "tokenfetch.Token{redacted}" }
func (t Token) GoString() string           { return t.String() }
func (t Token) LogValue() slog.Value       { return slog.StringValue("redacted") }
func (t Token) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, t.String()) }

// Remaining is the life left at now.
func (t Token) Remaining(now time.Time) time.Duration { return t.ExpiresAt.Sub(now) }

// Hash is the lowercase hex SHA-256 gatewayd uses to name a token.
func Hash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// NormalizeURL applies the same rules as auto_trader token_issuance.py
// _validated_gatewayd_url: http(s) only, a hostname, and no credentials,
// query, or fragment. Trailing slashes are stripped.
func NormalizeURL(raw string) (string, error) {
	normalized := strings.TrimRight(strings.TrimSpace(raw), "/")
	parsed, err := url.Parse(normalized)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Hostname() == "" || parsed.User != nil ||
		parsed.ForceQuery || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", ErrConfig
	}
	return normalized, nil
}

// CredentialTransportOK reports whether a credential may travel to base:
// https, or a loopback host. base must already be normalized.
func CredentialTransportOK(base string) bool {
	parsed, err := url.Parse(base)
	if err != nil {
		return false
	}
	return parsed.Scheme == "https" || netpolicy.IsLoopbackHost(parsed.Hostname())
}

// Client implements Fetcher over HTTP.
type Client struct {
	base       string
	credential string
	http       *http.Client
	now        func() time.Time
}

// NewClient builds a fetch client. The credential is required and is sent as
// "Authorization: Bearer <credential>" (a leading "Bearer " in the given value
// is not doubled); it is refused over plain http to a
// non-loopback host.
func NewClient(rawurl, credential string) (*Client, error) {
	base, err := NormalizeURL(rawurl)
	// The ensure client sends the variable's value raw as the header value, so
	// the same variable may already read "Bearer <credential>". Send exactly
	// one "Bearer" either way.
	credential = strings.TrimSpace(credential)
	if credential == "Bearer" {
		credential = ""
	}
	credential = strings.TrimSpace(strings.TrimPrefix(credential, "Bearer "))
	if err != nil || credential == "" || !CredentialTransportOK(base) {
		return nil, ErrConfig
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = (&net.Dialer{Timeout: DialTimeout}).DialContext
	return &Client{
		base:       base,
		credential: credential,
		now:        time.Now,
		http: &http.Client{
			Timeout:       RequestTimeout,
			Transport:     transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

type fetchRequest struct {
	FailedTokenSHA256 string `json:"failed_token_sha256,omitempty"`
}

type fetchResponse struct {
	Provider    string `json:"provider"`
	Token       string `json:"token"`
	ExpiresAt   string `json:"expires_at"`
	TokenSHA256 string `json:"token_sha256"`
	Source      string `json:"source"`
}

// Fetch makes one POST. It never retries.
func (c *Client) Fetch(ctx context.Context, provider, failedSHA256 string) (Token, error) {
	if !providerPattern.MatchString(provider) || (failedSHA256 != "" && !sha256Pattern.MatchString(failedSHA256)) {
		return Token{}, ErrBadRequest
	}
	body, err := json.Marshal(fetchRequest{FailedTokenSHA256: failedSHA256})
	if err != nil {
		return Token{}, ErrUnavailable
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/v1/tokens/"+provider+"/fetch", bytes.NewReader(body))
	if err != nil {
		return Token{}, ErrUnavailable
	}
	req.Header.Set("Authorization", "Bearer "+c.credential)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	response, err := c.http.Do(req)
	if err != nil {
		// net/http errors embed the request URL; drop them.
		return Token{}, ErrUnavailable
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxBodyBytes))
	if err != nil {
		return Token{}, ErrUnavailable
	}
	switch response.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized:
		return Token{}, ErrUnauthenticated
	case http.StatusForbidden:
		return Token{}, ErrProviderNotAllowed
	case http.StatusNotFound:
		return Token{}, ErrUnknownProvider
	case http.StatusBadRequest:
		return Token{}, ErrBadRequest
	case http.StatusServiceUnavailable:
		return Token{}, ErrIssueUnavailable
	default:
		return Token{}, ErrUnavailable
	}
	var answer fetchResponse
	if json.Unmarshal(raw, &answer) != nil {
		return Token{}, ErrUnavailable
	}
	expiresAt, err := time.Parse(time.RFC3339, answer.ExpiresAt)
	if err != nil || answer.Provider != provider || answer.Token == "" ||
		answer.TokenSHA256 != Hash(answer.Token) || !validSource(answer.Source) ||
		!expiresAt.After(c.now()) {
		return Token{}, ErrUnavailable
	}
	if failedSHA256 != "" && answer.TokenSHA256 == failedSHA256 {
		return Token{}, ErrRejectedTokenReturn
	}
	return Token{value: answer.Token, ExpiresAt: expiresAt.UTC(), Source: answer.Source}, nil
}

func validSource(source string) bool {
	return source == SourceMemory || source == SourceRedis || source == SourceIssued
}
