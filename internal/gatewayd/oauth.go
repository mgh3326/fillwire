package gatewayd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	oauthTokenPath      = "/oauth2/tokenP"
	tossOAuthTokenPath  = "/oauth2/token"
	oauthResponseLimit  = 128 * 1024
	maximumTokenSeconds = int64(7 * 24 * 60 * 60)
)

var (
	errOAuthFailed          = errors.New("oauth failed")
	errOAuthRedirectBlocked = errors.New("oauth redirect blocked")
	errOAuthPinningBlocked  = errors.New("oauth pinned request blocked")
)

// OAuthToken is the minimal safe result of a client-credentials exchange.
type OAuthToken struct {
	AccessToken string
	ExpiresIn   int64
}

// OAuthIssuer is isolated so the ensure state machine can be tested with fake
// issuers without putting a token in an HTTP response or log.
type OAuthIssuer interface {
	Issue(ctx context.Context, appKey, appSecret string) (OAuthToken, error)
}

// KISOAuthClient posts only to the compile-time authority pinned for its KIS
// provider. BaseURL is present solely to make cross-provider pin tests able to
// prove rejection before an HTTP transport is reached.
type KISOAuthClient struct {
	Provider  TokenProvider
	BaseURL   string
	Transport http.RoundTripper
	Timeout   time.Duration
}

func (client KISOAuthClient) Issue(ctx context.Context, appKey, appSecret string) (OAuthToken, error) {
	if (client.Provider != ProviderKISMock && client.Provider != ProviderKISLive) ||
		!validateProviderBaseURL(client.Provider, client.BaseURL) ||
		appKey == "" || appSecret == "" || !safeHeaderText(appKey) || !safeHeaderText(appSecret) {
		return OAuthToken{}, errOAuthFailed
	}
	body, err := json.Marshal(struct {
		GrantType string `json:"grant_type"`
		AppKey    string `json:"appkey"`
		AppSecret string `json:"appsecret"`
	}{
		GrantType: "client_credentials",
		AppKey:    appKey,
		AppSecret: appSecret,
	})
	if err != nil {
		return OAuthToken{}, errOAuthFailed
	}
	request, err := newPinnedOAuthRequest(ctx, client.Provider, client.BaseURL, bytes.NewReader(body))
	if err != nil {
		return OAuthToken{}, errOAuthFailed
	}
	request.Header.Set("content-type", "application/json")
	response, err := newPinnedOAuthClient(client.Transport, client.Timeout, client.Provider).Do(request)
	if err != nil || response == nil {
		return OAuthToken{}, errOAuthFailed
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return OAuthToken{}, errOAuthFailed
	}
	payload, err := readOAuthResponse(response.Body)
	if err != nil || !validIssuedToken(payload, providerTokenExpiryBuffer(client.Provider)) {
		return OAuthToken{}, errOAuthFailed
	}
	return payload, nil
}

// KISMockOAuthClient keeps the original mock-only API source compatible while
// delegating to the same provider-pinned implementation used by live KIS.
type KISMockOAuthClient struct {
	Transport http.RoundTripper
	Timeout   time.Duration
}

func (client KISMockOAuthClient) Issue(ctx context.Context, appKey, appSecret string) (OAuthToken, error) {
	return KISOAuthClient{
		Provider:  ProviderKISMock,
		BaseURL:   mustProviderBaseURL(ProviderKISMock),
		Transport: client.Transport,
		Timeout:   client.Timeout,
	}.Issue(ctx, appKey, appSecret)
}

// KISLiveOAuthClient is limited to KIS live OAuth token issuance. It has no
// order methods and cannot be pointed at the mock authority.
type KISLiveOAuthClient struct {
	Transport http.RoundTripper
	Timeout   time.Duration
}

func (client KISLiveOAuthClient) Issue(ctx context.Context, appKey, appSecret string) (OAuthToken, error) {
	return KISOAuthClient{
		Provider:  ProviderKISLive,
		BaseURL:   mustProviderBaseURL(ProviderKISLive),
		Transport: client.Transport,
		Timeout:   client.Timeout,
	}.Issue(ctx, appKey, appSecret)
}

// TossOAuthClient mirrors TossOAuthTokenManager._issue_token: form-encoded
// client_credentials to the one pinned OAuth path.
type TossOAuthClient struct {
	BaseURL   string
	Transport http.RoundTripper
	Timeout   time.Duration
}

func (client TossOAuthClient) Issue(ctx context.Context, clientID, clientSecret string) (OAuthToken, error) {
	baseURL := client.BaseURL
	if baseURL == "" {
		baseURL = mustProviderBaseURL(ProviderToss)
	}
	if !validateProviderBaseURL(ProviderToss, baseURL) ||
		clientID == "" || clientSecret == "" || !safeHeaderText(clientID) || !safeHeaderText(clientSecret) {
		return OAuthToken{}, errOAuthFailed
	}
	form := url.Values{
		"grant_type":    []string{"client_credentials"},
		"client_id":     []string{clientID},
		"client_secret": []string{clientSecret},
	}
	request, err := newPinnedOAuthRequest(ctx, ProviderToss, baseURL, strings.NewReader(form.Encode()))
	if err != nil {
		return OAuthToken{}, errOAuthFailed
	}
	request.Header.Set("content-type", "application/x-www-form-urlencoded")
	response, err := newPinnedOAuthClient(client.Transport, client.Timeout, ProviderToss).Do(request)
	if err != nil || response == nil {
		return OAuthToken{}, errOAuthFailed
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return OAuthToken{}, errOAuthFailed
	}
	payload, err := readOAuthResponse(response.Body)
	if err != nil || !validIssuedToken(payload, tossTokenExpiryBuffer) {
		return OAuthToken{}, errOAuthFailed
	}
	return payload, nil
}

func newPinnedOAuthRequest(ctx context.Context, provider TokenProvider, baseURL string, body io.Reader) (*http.Request, error) {
	if !validateProviderBaseURL(provider, baseURL) {
		return nil, errOAuthFailed
	}
	parsed, err := url.Parse(baseURL)
	path, found := providerOAuthPath(provider)
	if err != nil || !found {
		return nil, errOAuthFailed
	}
	parsed.Path = path
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	parsed.User = nil
	if !validatePinnedOAuthURL(provider, parsed) {
		return nil, errOAuthFailed
	}
	return http.NewRequestWithContext(ctx, http.MethodPost, parsed.String(), body)
}

func mustProviderBaseURL(provider TokenProvider) string {
	baseURL, found := providerBaseURL(provider)
	if !found {
		return ""
	}
	return baseURL
}

func validatePinnedOAuthURL(provider TokenProvider, requestURL *url.URL) bool {
	if requestURL == nil || requestURL.User != nil || requestURL.RawQuery != "" || requestURL.ForceQuery || requestURL.Fragment != "" {
		return false
	}
	expectedBase, found := providerBaseURL(provider)
	expectedPath, pathFound := providerOAuthPath(provider)
	if !found || !pathFound {
		return false
	}
	expectedURL, err := url.Parse(expectedBase)
	return err == nil && strings.EqualFold(requestURL.Scheme, "https") &&
		strings.EqualFold(requestURL.Host, expectedURL.Host) && requestURL.Path == expectedPath
}

type oauthPinningTransport struct {
	base     http.RoundTripper
	provider TokenProvider
}

func (transport oauthPinningTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil || request.Method != http.MethodPost || !validatePinnedOAuthURL(transport.provider, request.URL) {
		return nil, errOAuthPinningBlocked
	}
	return transport.base.RoundTrip(request)
}

func newPinnedOAuthClient(base http.RoundTripper, timeout time.Duration, provider TokenProvider) *http.Client {
	if base == nil {
		// Credentials must not be sent through an ambient proxy configuration.
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
		Transport: oauthPinningTransport{base: base, provider: provider},
		Timeout:   timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return errOAuthRedirectBlocked
		},
	}
}

func validIssuedToken(token OAuthToken, buffer time.Duration) bool {
	return token.AccessToken != "" && safeHeaderText(token.AccessToken) &&
		token.ExpiresIn > int64(buffer/time.Second) && token.ExpiresIn <= maximumTokenSeconds
}

func readOAuthResponse(body io.Reader) (OAuthToken, error) {
	encoded, err := io.ReadAll(io.LimitReader(body, oauthResponseLimit+1))
	if err != nil || len(encoded) > oauthResponseLimit {
		return OAuthToken{}, errOAuthFailed
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var response struct {
		AccessToken string      `json:"access_token"`
		ExpiresIn   json.Number `json:"expires_in"`
	}
	if err := decoder.Decode(&response); err != nil {
		return OAuthToken{}, errOAuthFailed
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return OAuthToken{}, errOAuthFailed
	}
	expiresIn, err := strconv.ParseInt(string(response.ExpiresIn), 10, 64)
	if err != nil {
		return OAuthToken{}, errOAuthFailed
	}
	return OAuthToken{AccessToken: response.AccessToken, ExpiresIn: expiresIn}, nil
}
