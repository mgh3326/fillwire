package kismockread

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var errRedirectBlocked = errors.New("redirect blocked")
var errPinnedRequestBlocked = errors.New("pinned request blocked")

// ValidatePinnedURL permits only the fixed HTTPS mock netloc. It is called
// when URLs are built and again immediately before a transport can send one.
func ValidatePinnedURL(requestURL *url.URL) *SafeError {
	if requestURL == nil ||
		!strings.EqualFold(requestURL.Scheme, "https") ||
		!strings.EqualFold(requestURL.Host, MockHost) ||
		requestURL.User != nil || requestURL.Fragment != "" {
		return safeError(CodeRequestBlocked)
	}
	return nil
}

type pinningTransport struct {
	base http.RoundTripper
}

func (transport pinningTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil || ValidatePinnedURL(request.URL) != nil {
		return nil, errPinnedRequestBlocked
	}
	return transport.base.RoundTrip(request)
}

// NewPinnedHTTPClient creates a client that refuses redirects and performs the
// final host/scheme check inside RoundTrip, directly before network transport.
func NewPinnedHTTPClient(base http.RoundTripper, timeout time.Duration) *http.Client {
	if base == nil {
		// A proxy could observe credentials while the URL itself remains pinned.
		// Use a clone so this process never inherits proxy routing from ambient
		// environment configuration.
		if defaultTransport, ok := http.DefaultTransport.(*http.Transport); ok {
			cloned := defaultTransport.Clone()
			cloned.Proxy = nil
			base = cloned
		} else {
			base = &http.Transport{Proxy: nil}
		}
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &http.Client{
		Transport: pinningTransport{base: base},
		Timeout:   timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return errRedirectBlocked
		},
	}
}
