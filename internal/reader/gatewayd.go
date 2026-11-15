package reader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// Gatewayd ensure timing matches auto_trader token_issuance.py _ENSURE_TIMEOUT:
// a 5s request budget with a 3s connect bound. The ensure call is a single
// bounded POST per provider call; there is no client-side retry.
const (
	gatewaydEnsureTimeout = 5 * time.Second
	gatewaydDialTimeout   = 3 * time.Second
)

var (
	errGatewaydConfig = errors.New("reader: gatewayd configuration invalid")
	errGatewaydEnsure = errors.New("reader: gatewayd ensure unavailable")
)

// GatewaydEnsurer asks gatewayd to ensure one provider's token or approval
// key exists in the shared Redis cache. A nil error is only an
// acknowledgement: the caller must prove publication by re-reading Redis.
type GatewaydEnsurer interface {
	Ensure(ctx context.Context, provider string, force bool) error
}

var gatewaydProviderPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)

// NormalizeGatewaydURL applies the same rules as auto_trader
// token_issuance.py _validated_gatewayd_url: http(s) only, a hostname, and no
// credentials, query, or fragment. Trailing slashes are stripped.
func NormalizeGatewaydURL(raw string) (string, error) {
	normalized := strings.TrimRight(strings.TrimSpace(raw), "/")
	parsed, err := url.Parse(normalized)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Hostname() == "" || parsed.User != nil ||
		parsed.ForceQuery || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errGatewaydConfig
	}
	return normalized, nil
}

// gatewaydClient POSTs /v1/tokens/{provider}/ensure. It never reads a token
// body: gatewayd's acknowledgement carries no credential material.
type gatewaydClient struct {
	base       string
	authHeader string
	authToken  string
	http       *http.Client
}

// NewGatewaydClient builds the ensure client. authHeader/authToken are
// optional: an empty authToken sends no credential header, matching the
// loopback gatewayd deployment. When authToken is set, authHeader must be a
// valid header name and is validated by the caller at config load.
func NewGatewaydClient(rawurl, authHeader, authToken string) (GatewaydEnsurer, error) {
	base, err := NormalizeGatewaydURL(rawurl)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// trust_env=False parity: ambient proxy variables are never honored.
	transport.Proxy = nil
	transport.DialContext = (&net.Dialer{Timeout: gatewaydDialTimeout}).DialContext
	return &gatewaydClient{
		base:       base,
		authHeader: authHeader,
		authToken:  authToken,
		http: &http.Client{
			Timeout:   gatewaydEnsureTimeout,
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (c *gatewaydClient) Ensure(ctx context.Context, provider string, force bool) error {
	if !gatewaydProviderPattern.MatchString(provider) {
		return errGatewaydConfig
	}
	endpoint := fmt.Sprintf("%s/v1/tokens/%s/ensure", c.base, provider)
	if force {
		endpoint += "?force=true"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return errGatewaydEnsure
	}
	if c.authToken != "" {
		req.Header.Set(c.authHeader, c.authToken)
	}
	response, err := c.http.Do(req)
	if err != nil {
		return errGatewaydEnsure
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return errGatewaydEnsure
	}
	return nil
}
