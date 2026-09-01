package kismockread

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/url"
	"strings"
	"time"
)

const tokenExpiryBuffer = 60 * time.Second

// RedisGetter is intentionally the only cache capability the reader receives.
// It cannot issue, save, clear, or lock a token.
type RedisGetter interface {
	Get(ctx context.Context, key string) (value string, present bool, err error)
}

// TokenCacheKey reproduces the specified VTS namespace derivation: lowercase
// base URL netloc plus the first 16 hex characters of sha256(app_key).
func TokenCacheKey(baseURL, appKey string) (string, *SafeError) {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return "", safeError(CodeRequestBlocked)
	}
	fingerprint := sha256.Sum256([]byte(appKey))
	return fmt.Sprintf(
		"kis_mock:%s:%x:access_token",
		strings.ToLower(parsed.Host),
		fingerprint[:8],
	), nil
}

// LoadCachedToken accepts only the Python writer's JSON cache payload
// (access_token, expires_at, optional created_at) and never falls back to
// issuance when it is absent, malformed, or near expiry.
func LoadCachedToken(ctx context.Context, getter RedisGetter, key string, now time.Time) (string, *SafeError) {
	if getter == nil {
		return "", safeError(CodeTokenCacheUnavailable)
	}
	raw, present, err := getter.Get(ctx, key)
	if err != nil {
		return "", safeError(CodeTokenCacheUnavailable)
	}
	if !present {
		return "", safeError(CodeTokenMissing)
	}

	fields, valid := strictTokenFields(raw)
	if !valid {
		return "", safeError(CodeTokenInvalid)
	}
	// The Python writer (auto_trader redis_token_manager.save_token) stores
	// exactly access_token, expires_at, and created_at. Stay strict via an
	// allowlist rather than a field count: unknown keys are still rejected,
	// created_at is tolerated metadata.
	for key := range fields {
		if key != "access_token" && key != "expires_at" && key != "created_at" {
			return "", safeError(CodeTokenInvalid)
		}
	}
	accessTokenRaw, hasAccessToken := fields["access_token"]
	expiresAtRaw, hasExpiresAt := fields["expires_at"]
	if !hasAccessToken || !hasExpiresAt {
		return "", safeError(CodeTokenInvalid)
	}

	var accessToken string
	var expiresAt float64
	if json.Unmarshal(accessTokenRaw, &accessToken) != nil ||
		json.Unmarshal(expiresAtRaw, &expiresAt) != nil ||
		accessToken == "" ||
		!safeHeaderText(accessToken) ||
		math.IsNaN(expiresAt) || math.IsInf(expiresAt, 0) {
		return "", safeError(CodeTokenInvalid)
	}

	nowSeconds := float64(now.Unix()) + float64(now.Nanosecond())/float64(time.Second)
	if !(nowSeconds < expiresAt-tokenExpiryBuffer.Seconds()) {
		return "", safeError(CodeTokenExpired)
	}
	return accessToken, nil
}

// strictTokenFields rejects duplicate keys as well as non-object JSON. A plain
// map unmarshal would silently retain only the final duplicate value.
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
