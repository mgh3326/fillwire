package kisread

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// kisread is the non-owner side of the shared KIS token contract: it reads
// the access token auto_trader (or gatewayd) publishes in Redis and has no
// path that issues, refreshes, locks, writes, or deletes a token.

// ErrTokenUnavailable reports that the shared cache holds no usable KIS
// access token. The message never carries a token or payload value.
var ErrTokenUnavailable = errors.New("kisread: no usable cached KIS access token")

// tokenExpiryBuffer matches auto_trader RedisTokenManager._token_expiry_buffer:
// a token this close to expiry is treated as absent.
const tokenExpiryBuffer = 60 * time.Second

// AccessTokenLiveKey is the fixed live-mode key from
// auto_trader app/services/brokers/token_keys.py kis_live_token_keys().
const AccessTokenLiveKey = "kis:access_token"

// AccessTokenKey returns the token_keys.py cache key for the account mode.
// Live is the fixed kis:access_token; mock derives
// kis_mock:{lower netloc}:{sha256(app_key)[:16hex]}:access_token from the KIS
// base URL netloc and app key, identical to broker-edge
// kismockread.TokenCacheKey (pinned by the golden fixture in testdata).
func AccessTokenKey(mock bool, baseURL, appKey string) (string, error) {
	if !mock {
		return AccessTokenLiveKey, nil
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return "", errors.New("kisread: invalid KIS base URL for token key")
	}
	if appKey == "" {
		return "", errors.New("kisread: app key required for mock token key")
	}
	fingerprint := sha256.Sum256([]byte(appKey))
	return fmt.Sprintf("kis_mock:%s:%x:access_token", strings.ToLower(parsed.Host), fingerprint[:8]), nil
}

// CachedToken reads the KIS access token that auto_trader or gatewayd keeps
// in Redis. Each Token call performs exactly one GET; nothing is retained in
// process beyond the returned value.
type CachedToken struct {
	client redis.UniversalClient
	key    string
	now    func() time.Time
}

// NewCachedToken validates the Redis client and cache key.
func NewCachedToken(client redis.UniversalClient, key string) (*CachedToken, error) {
	if client == nil || key == "" {
		return nil, errors.New("kisread: token cache needs a Redis client and key")
	}
	return &CachedToken{client: client, key: key, now: time.Now}, nil
}

// Token returns the cached access token while it is outside the expiry
// buffer, or ErrTokenUnavailable when the entry is missing, malformed, or
// near expiry. Redis failures also surface as ErrTokenUnavailable so callers
// treat every cache problem identically.
func (c *CachedToken) Token(ctx context.Context) (string, error) {
	raw, err := c.client.Get(ctx, c.key).Result()
	if err != nil {
		return "", ErrTokenUnavailable
	}
	fields, valid := strictTokenFields(raw)
	if !valid {
		return "", ErrTokenUnavailable
	}
	// The Python writer (auto_trader redis_token_manager.save_token) stores
	// exactly access_token, expires_at, and created_at. Stay strict via an
	// allowlist rather than a field count: unknown keys are still rejected,
	// created_at is tolerated metadata.
	for name := range fields {
		if name != "access_token" && name != "expires_at" && name != "created_at" {
			return "", ErrTokenUnavailable
		}
	}
	accessTokenRaw, hasAccessToken := fields["access_token"]
	expiresAtRaw, hasExpiresAt := fields["expires_at"]
	if !hasAccessToken || !hasExpiresAt {
		return "", ErrTokenUnavailable
	}
	var accessToken string
	var expiresAt float64
	if json.Unmarshal(accessTokenRaw, &accessToken) != nil ||
		json.Unmarshal(expiresAtRaw, &expiresAt) != nil ||
		accessToken == "" ||
		strings.ContainsAny(accessToken, "\r\n") ||
		math.IsNaN(expiresAt) || math.IsInf(expiresAt, 0) {
		return "", ErrTokenUnavailable
	}
	now := c.now()
	nowSeconds := float64(now.Unix()) + float64(now.Nanosecond())/float64(time.Second)
	if !(nowSeconds < expiresAt-tokenExpiryBuffer.Seconds()) {
		return "", ErrTokenUnavailable
	}
	return accessToken, nil
}

// strictTokenFields rejects duplicate keys, non-object JSON, and trailing
// data. A plain map unmarshal would silently retain only the final duplicate
// value. Mirrors broker-edge kismockread strictTokenFields.
func strictTokenFields(raw string) (map[string]json.RawMessage, bool) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return nil, false
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		keyToken, err := decoder.Token()
		key, isString := keyToken.(string)
		if err != nil || !isString {
			return nil, false
		}
		if _, duplicate := fields[key]; duplicate {
			return nil, false
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return nil, false
		}
		fields[key] = value
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return nil, false
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, false
	}
	return fields, true
}
