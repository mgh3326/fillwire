package nhplugws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	OAuthEndpoint = "https://api.nhplug.com:8443/oauth2/token"
	tokenLeeway   = time.Minute
)

// OAuthTokenSource follows the NH SDK client-credentials approval exchange.
// It retains credentials and tokens only in memory and never includes either
// in returned errors.
type OAuthTokenSource struct {
	appKey    string
	appSecret string
	client    *http.Client
	mu        sync.Mutex
	token     string
	expiresAt time.Time
	now       func() time.Time
}

func NewOAuthTokenSource(appKey, appSecret string) (*OAuthTokenSource, error) {
	if strings.TrimSpace(appKey) == "" || strings.TrimSpace(appSecret) == "" {
		return nil, errors.New("NHPLUG_APP_KEY and NHPLUG_APP_SECRET are required")
	}
	return &OAuthTokenSource{
		appKey:    appKey,
		appSecret: appSecret,
		client:    &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		now:       time.Now,
	}, nil
}

func (s *OAuthTokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token != "" && s.now().Before(s.expiresAt.Add(-tokenLeeway)) {
		return s.token, nil
	}
	form := url.Values{
		"appkey":       {s.appKey},
		"appsecretkey": {s.appSecret},
		"grant_type":   {"client_credentials"},
		"scope":        {"oob"},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, OAuthEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", errors.New("NHPLUG OAuth request construction failed")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := s.client.Do(req)
	if err != nil {
		return "", errors.New("NHPLUG OAuth token request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("NHPLUG OAuth token request returned HTTP %d", resp.StatusCode)
	}
	var payload struct {
		AccessToken string          `json:"access_token"`
		ExpiresIn   json.RawMessage `json:"expires_in"`
	}
	limited := io.LimitReader(resp.Body, 1<<20)
	if err := json.NewDecoder(limited).Decode(&payload); err != nil || strings.TrimSpace(payload.AccessToken) == "" {
		return "", errors.New("NHPLUG OAuth response is invalid")
	}
	var seconds float64
	if len(payload.ExpiresIn) == 0 {
		seconds = 86400
	} else if err := json.Unmarshal(payload.ExpiresIn, &seconds); err != nil || seconds <= 0 {
		return "", errors.New("NHPLUG OAuth response expiry is invalid")
	}
	s.token = strings.TrimSpace(payload.AccessToken)
	s.expiresAt = s.now().Add(time.Duration(seconds * float64(time.Second)))
	return s.token, nil
}
