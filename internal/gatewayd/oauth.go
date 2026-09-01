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
	"time"

	"github.com/mgh3326/broker-edge/internal/kismockread"
)

const (
	oauthTokenPath      = "/oauth2/tokenP"
	oauthResponseLimit  = 128 * 1024
	maximumTokenSeconds = int64(7 * 24 * 60 * 60)
)

var errOAuthFailed = errors.New("mock oauth failed")

// OAuthToken is the minimal safe result of the VTS OAuth exchange.
type OAuthToken struct {
	AccessToken string
	ExpiresIn   int64
}

// OAuthIssuer is isolated so the ensure state machine can be tested with a
// fake VTS issuer without putting a token in an HTTP response or log.
type OAuthIssuer interface {
	Issue(ctx context.Context, appKey, appSecret string) (OAuthToken, error)
}

// KISMockOAuthClient sends the one permitted token request to the fixed VTS
// OAuth endpoint. It shares the read path's HTTPS pin and redirect refusal.
type KISMockOAuthClient struct {
	Transport http.RoundTripper
	Timeout   time.Duration
}

func (client KISMockOAuthClient) Issue(ctx context.Context, appKey, appSecret string) (OAuthToken, error) {
	if appKey == "" || appSecret == "" || !safeHeaderText(appKey) || !safeHeaderText(appSecret) {
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
	baseURL, err := url.Parse(kismockread.MockBaseURL)
	if err != nil {
		return OAuthToken{}, errOAuthFailed
	}
	requestURL := *baseURL
	requestURL.Path = oauthTokenPath
	requestURL.RawPath = ""
	requestURL.RawQuery = ""
	requestURL.Fragment = ""
	requestURL.User = nil
	if kismockread.ValidatePinnedURL(&requestURL) != nil {
		return OAuthToken{}, errOAuthFailed
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL.String(), bytes.NewReader(body))
	if err != nil {
		return OAuthToken{}, errOAuthFailed
	}
	request.Header.Set("content-type", "application/json")
	response, err := kismockread.NewPinnedHTTPClient(client.Transport, client.Timeout).Do(request)
	if err != nil || response == nil {
		return OAuthToken{}, errOAuthFailed
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return OAuthToken{}, errOAuthFailed
	}
	payload, err := readOAuthResponse(response.Body)
	if err != nil {
		return OAuthToken{}, errOAuthFailed
	}
	if payload.AccessToken == "" || !safeHeaderText(payload.AccessToken) ||
		payload.ExpiresIn <= int64(tokenExpiryBuffer/time.Second) || payload.ExpiresIn > maximumTokenSeconds {
		return OAuthToken{}, errOAuthFailed
	}
	return payload, nil
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
