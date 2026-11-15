package kisread

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// tokenKeyFixture mirrors auto_trader@e61849228
// tests/fixtures/broker_token_keys.json, copied verbatim into testdata with a
// _source provenance field. broker-edge kismockread.TokenCacheKey produces the
// same mock key from the same inputs.
type tokenKeyFixture struct {
	Inputs struct {
		KISMockNetloc string `json:"kis_mock_netloc"`
		KISMockAppKey string `json:"kis_mock_app_key"`
	} `json:"inputs"`
	Expected struct {
		KISLiveAccessToken string `json:"kis_live_access_token"`
		KISMockAccessToken string `json:"kis_mock_access_token"`
		KISMockLock        string `json:"kis_mock_lock"`
		KISLiveLock        string `json:"kis_live_lock"`
	} `json:"expected"`
}

func loadTokenKeyFixture(t *testing.T) tokenKeyFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/broker_token_keys.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture tokenKeyFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestAccessTokenKeyMatchesGoldenFixture(t *testing.T) {
	fixture := loadTokenKeyFixture(t)
	baseURL := "https://" + fixture.Inputs.KISMockNetloc
	live, err := AccessTokenKey(false, "https://openapi.koreainvestment.com:9443", "ignored")
	if err != nil || live != fixture.Expected.KISLiveAccessToken {
		t.Fatalf("live key = %q, %v; want %q", live, err, fixture.Expected.KISLiveAccessToken)
	}
	mock, err := AccessTokenKey(true, baseURL, fixture.Inputs.KISMockAppKey)
	if err != nil || mock != fixture.Expected.KISMockAccessToken {
		t.Fatalf("mock key = %q, %v; want %q", mock, err, fixture.Expected.KISMockAccessToken)
	}
	// The derivation must be stable under URL case and app-key bytes: the
	// fingerprint covers the exact app key, the netloc is lowercased.
	again, err := AccessTokenKey(true, "HTTPS://"+fixture.Inputs.KISMockNetloc+"/", fixture.Inputs.KISMockAppKey)
	if err != nil || again != mock {
		t.Fatalf("mock key not stable: %q vs %q (%v)", again, mock, err)
	}
	for _, bad := range []struct {
		baseURL string
		appKey  string
	}{
		{"not-a-url", fixture.Inputs.KISMockAppKey},
		{"https://user:pass@" + fixture.Inputs.KISMockNetloc, fixture.Inputs.KISMockAppKey},
		{baseURL, ""},
	} {
		if _, err := AccessTokenKey(true, bad.baseURL, bad.appKey); err == nil {
			t.Fatalf("AccessTokenKey(%q, %q) accepted", bad.baseURL, bad.appKey)
		}
	}
}

func tokenFixture(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return mini, client
}

func writeToken(t *testing.T, mini *miniredis.Miniredis, key string, payload any) {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	mini.Set(key, string(data))
}

func TestCachedTokenReadsValidPayload(t *testing.T) {
	fixture := loadTokenKeyFixture(t)
	mini, client := tokenFixture(t)
	key := fixture.Expected.KISMockAccessToken
	expiresAt := float64(time.Now().Add(time.Hour).Unix())
	writeToken(t, mini, key, map[string]any{
		"access_token": "synthetic-kis-access-token",
		"expires_at":   expiresAt,
		"created_at":   expiresAt - 3300,
	})
	mini.SetTTL(key, time.Hour)
	cached, err := NewCachedToken(client, key)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		token, err := cached.Token(context.Background())
		if err != nil || token != "synthetic-kis-access-token" {
			t.Fatalf("Token() = %q, %v", token, err)
		}
	}
	// Read-only contract: the entry and its TTL are untouched.
	if ttl := mini.TTL(key); ttl <= 0 {
		t.Fatalf("token TTL changed: %s", ttl)
	}
	if got, _ := mini.Get(key); !json.Valid([]byte(got)) {
		t.Fatal("token payload mutated")
	}
}

func TestCachedTokenRejectsUnusableEntries(t *testing.T) {
	fixture := loadTokenKeyFixture(t)
	key := fixture.Expected.KISLiveAccessToken
	now := time.Now()
	for _, tc := range []struct {
		name    string
		setup   func(*miniredis.Miniredis)
		wantErr error
	}{
		{"missing", func(m *miniredis.Miniredis) {}, ErrTokenUnavailable},
		{"empty string", func(m *miniredis.Miniredis) { m.Set(key, "") }, ErrTokenUnavailable},
		{"not json", func(m *miniredis.Miniredis) { m.Set(key, "plain-string") }, ErrTokenUnavailable},
		{"json array", func(m *miniredis.Miniredis) { m.Set(key, `["x"]`) }, ErrTokenUnavailable},
		{"missing access_token", func(m *miniredis.Miniredis) {
			writeToken(t, m, key, map[string]any{"expires_at": now.Add(time.Hour).Unix()})
		}, ErrTokenUnavailable},
		{"missing expires_at", func(m *miniredis.Miniredis) {
			writeToken(t, m, key, map[string]any{"access_token": "synthetic"})
		}, ErrTokenUnavailable},
		{"empty access_token", func(m *miniredis.Miniredis) {
			writeToken(t, m, key, map[string]any{"access_token": "", "expires_at": now.Add(time.Hour).Unix()})
		}, ErrTokenUnavailable},
		{"unknown field", func(m *miniredis.Miniredis) {
			writeToken(t, m, key, map[string]any{"access_token": "synthetic", "expires_at": now.Add(time.Hour).Unix(), "extra": 1})
		}, ErrTokenUnavailable},
		{"duplicate key", func(m *miniredis.Miniredis) {
			m.Set(key, `{"access_token":"synthetic","access_token":"other","expires_at":9999999999}`)
		}, ErrTokenUnavailable},
		{"trailing data", func(m *miniredis.Miniredis) {
			m.Set(key, `{"access_token":"synthetic","expires_at":9999999999} trailing`)
		}, ErrTokenUnavailable},
		{"expired", func(m *miniredis.Miniredis) {
			writeToken(t, m, key, map[string]any{"access_token": "synthetic", "expires_at": now.Add(-time.Hour).Unix()})
		}, ErrTokenUnavailable},
		{"inside expiry buffer", func(m *miniredis.Miniredis) {
			writeToken(t, m, key, map[string]any{"access_token": "synthetic", "expires_at": float64(now.Add(30 * time.Second).Unix())})
		}, ErrTokenUnavailable},
		{"header unsafe token", func(m *miniredis.Miniredis) {
			writeToken(t, m, key, map[string]any{"access_token": "bad\r\ntoken", "expires_at": now.Add(time.Hour).Unix()})
		}, ErrTokenUnavailable},
		{"non-numeric expires_at", func(m *miniredis.Miniredis) {
			m.Set(key, `{"access_token":"synthetic","expires_at":"soon"}`)
		}, ErrTokenUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mini, client := tokenFixture(t)
			tc.setup(mini)
			cached, err := NewCachedToken(client, key)
			if err != nil {
				t.Fatal(err)
			}
			token, err := cached.Token(context.Background())
			if token != "" || !errors.Is(err, tc.wantErr) {
				t.Fatalf("Token() = %q, %v; want empty + %v", token, err, tc.wantErr)
			}
		})
	}
}

func TestCachedTokenRedisErrorIsUnavailable(t *testing.T) {
	mini, client := tokenFixture(t)
	mini.SetError("ERR synthetic redis failure")
	cached, err := NewCachedToken(client, "kis:access_token")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cached.Token(context.Background()); !errors.Is(err, ErrTokenUnavailable) {
		t.Fatalf("redis error = %v, want ErrTokenUnavailable", err)
	}
}

func TestCachedTokenNeverWrites(t *testing.T) {
	mini, client := tokenFixture(t)
	key := "kis:access_token"
	expiresAt := float64(time.Now().Add(time.Hour).Unix())
	writeToken(t, mini, key, map[string]any{"access_token": "synthetic", "expires_at": expiresAt})
	before := mini.Dump()
	cached, err := NewCachedToken(client, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cached.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	if after := mini.Dump(); after != before {
		t.Fatal("token read mutated the Redis fixture")
	}
}

func TestCachedTokenErrorNeverContainsPayload(t *testing.T) {
	mini, client := tokenFixture(t)
	secret := "synthetic-secret-token-value"
	mini.Set("kis:access_token", fmt.Sprintf(`{"access_token":%q,"expires_at":%d}`, secret, time.Now().Add(-time.Hour).Unix()))
	cached, err := NewCachedToken(client, "kis:access_token")
	if err != nil {
		t.Fatal(err)
	}
	_, err = cached.Token(context.Background())
	if err == nil || err.Error() == "" {
		t.Fatal("expected error")
	}
	if got := err.Error(); got == secret || strings.Contains(got, secret) {
		t.Fatalf("error leaked token payload: %q", got)
	}
}
