package gatewayd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/broker-edge/internal/kismockread"
)

func TestEnsureReturnsFreshWithoutOAuthForEveryProvider(t *testing.T) {
	for _, provider := range orderedProviders() {
		t.Run(string(provider), func(t *testing.T) {
			now := testGatewayNow()
			store := newFakeRedisStore(func() time.Time { return now })
			issuer := &fakeOAuthIssuer{token: OAuthToken{AccessToken: "unused", ExpiresIn: 3600}}
			service := newTestEnsureServiceForProvider(t, store, issuer, now, provider)
			payload := testFreshPayload(t, provider, now)
			store.save(service.CacheKey, payload, 3660*time.Second)

			state, err := service.Ensure(context.Background())
			if err != nil {
				t.Fatalf("ensure: %v", err)
			}
			if state != EnsureStateFresh {
				t.Fatalf("state = %q, want %q", state, EnsureStateFresh)
			}
			if issuer.callCount() != 0 {
				t.Fatalf("OAuth calls = %d, want 0", issuer.callCount())
			}
			if len(store.savedRecords()) != 0 {
				t.Fatal("fresh cache unexpectedly wrote Redis")
			}
		})
	}
}

func TestEnsureIssuesProviderCompatiblePayloadForEveryProvider(t *testing.T) {
	for _, provider := range orderedProviders() {
		t.Run(string(provider), func(t *testing.T) {
			now := testGatewayNow()
			store := newFakeRedisStore(func() time.Time { return now })
			issuer := &fakeOAuthIssuer{token: OAuthToken{AccessToken: "opaque-issued-token", ExpiresIn: 3600}}
			service := newTestEnsureServiceForProvider(t, store, issuer, now, provider)

			state, err := service.Ensure(context.Background())
			if err != nil {
				t.Fatalf("ensure: %v", err)
			}
			if state != EnsureStateIssued {
				t.Fatalf("state = %q, want %q", state, EnsureStateIssued)
			}
			if issuer.callCount() != 1 {
				t.Fatalf("OAuth calls = %d, want 1", issuer.callCount())
			}
			records := store.savedRecords()
			if len(records) != 1 {
				t.Fatalf("SET calls = %d, want 1", len(records))
			}
			record := records[0]
			if record.key != service.CacheKey {
				t.Fatalf("cache key = %q, want %q", record.key, service.CacheKey)
			}
			if provider == ProviderToss {
				const wantBytes = `{"access_token":"opaque-issued-token","expires_at":2000003600}`
				if record.value != wantBytes {
					t.Fatalf("SET bytes = %q, want %q", record.value, wantBytes)
				}
				if record.ttl != 3600*time.Second {
					t.Fatalf("SET TTL = %v, want 3600s", record.ttl)
				}
				if !tossPythonCompatiblePayload(record.value) {
					t.Fatalf("SET payload is not a Python-compatible two-field Toss JSON object: %q", record.value)
				}
				loaded, fresh, loadErr := loadTossCachedToken(context.Background(), store, service.CacheKey, now)
				if loadErr != nil || !fresh || loaded != "opaque-issued-token" {
					t.Fatalf("Toss reader round trip = %q, %t, %v", loaded, fresh, loadErr)
				}
				locked := store.savedWhileLocked()
				if len(locked) != 1 || !locked[0] {
					t.Fatal("Toss replacement cache SET did not occur while the lock was held")
				}
				return
			}
			const wantBytes = `{"access_token":"opaque-issued-token","expires_at":2000003600,"created_at":2000000000}`
			if record.value != wantBytes {
				t.Fatalf("SET bytes = %q, want %q", record.value, wantBytes)
			}
			if record.ttl != 3660*time.Second {
				t.Fatalf("SET TTL = %v, want 3660s", record.ttl)
			}
			if !pythonCompatiblePayload(record.value) {
				t.Fatalf("SET payload is not a Python-compatible three-field KIS JSON object: %q", record.value)
			}
			loaded, loadErr := kismockread.LoadCachedToken(context.Background(), store, service.CacheKey, now)
			if loadErr != nil || loaded != "opaque-issued-token" {
				t.Fatalf("KIS reader round trip = %q, %v", loaded, loadErr)
			}
		})
	}
}

func TestEnsureConcurrentCallsCooperateThroughRedisLockForEveryProvider(t *testing.T) {
	for _, provider := range orderedProviders() {
		t.Run(string(provider), func(t *testing.T) {
			now := testGatewayNow()
			store := newFakeRedisStore(func() time.Time { return now })
			store.setNXHit = make(chan struct{})
			releaseOAuth := make(chan struct{})
			issuer := &fakeOAuthIssuer{
				token:   OAuthToken{AccessToken: "singleflight-token", ExpiresIn: 3600},
				started: make(chan struct{}, 2),
				release: releaseOAuth,
			}
			service := newTestEnsureServiceForProvider(t, store, issuer, now, provider)
			service.InitialLockWait = 10 * time.Millisecond
			service.LockPollEvery = 5 * time.Millisecond
			service.LockWaitTimeout = 100 * time.Millisecond

			type result struct {
				state EnsureState
				err   error
			}
			first := make(chan result, 1)
			second := make(chan result, 1)
			go func() {
				state, err := service.Ensure(context.Background())
				first <- result{state: state, err: err}
			}()
			select {
			case <-issuer.started:
			case <-time.After(time.Second):
				t.Fatal("first ensure did not reach fake OAuth")
			}
			go func() {
				state, err := service.Ensure(context.Background())
				second <- result{state: state, err: err}
			}()
			select {
			case <-store.setNXHit:
			case <-time.After(time.Second):
				t.Fatal("second ensure did not contend for the Redis lock")
			}
			if issuer.callCount() != 1 {
				t.Fatalf("OAuth calls while lock is held = %d, want 1", issuer.callCount())
			}
			close(releaseOAuth)
			firstResult := <-first
			secondResult := <-second
			if firstResult.err != nil || firstResult.state != EnsureStateIssued {
				t.Fatalf("first result = %#v", firstResult)
			}
			if secondResult.err != nil || secondResult.state != EnsureStateFresh {
				t.Fatalf("second result = %#v", secondResult)
			}
			// Observable mutant tripwire: removing SET NX cooperation makes both
			// concurrent calls cross fake OAuth and yields two calls.
			if issuer.callCount() != 1 {
				t.Fatalf("OAuth calls = %d, want exactly 1", issuer.callCount())
			}
			if store.lockAttempts() != 2 {
				t.Fatalf("SET NX attempts = %d, want 2", store.lockAttempts())
			}
		})
	}
}

func TestEnsureServiceDerivesProviderNamespaces(t *testing.T) {
	for _, provider := range orderedProviders() {
		t.Run(string(provider), func(t *testing.T) {
			service := newTestEnsureServiceForProvider(t, newFakeRedisStore(testGatewayNow), &fakeOAuthIssuer{}, testGatewayNow(), provider)
			switch provider {
			case ProviderKISMock:
				config := testProviderConfig(provider)
				wantCacheKey, err := kismockread.TokenCacheKey(config.BaseURL, config.AppKey)
				if err != nil {
					t.Fatal(err)
				}
				if service.CacheKey != wantCacheKey || service.LockKey != strings.TrimSuffix(wantCacheKey, ":access_token")+":token:lock" {
					t.Fatalf("keys = %q / %q", service.CacheKey, service.LockKey)
				}
			case ProviderKISLive:
				if service.CacheKey != "kis:access_token" || service.LockKey != "kis:token:lock" {
					t.Fatalf("live keys = %q / %q", service.CacheKey, service.LockKey)
				}
			case ProviderToss:
				config := testProviderConfig(provider)
				fingerprint := sha256.Sum256([]byte(config.AppKey))
				namespace := "toss:oauth:" + hex.EncodeToString(fingerprint[:8])
				if service.CacheKey != namespace+":access_token" || service.LockKey != namespace+":lock" {
					t.Fatalf("Toss keys = %q / %q", service.CacheKey, service.LockKey)
				}
			}
		})
	}
}

func TestTossPayloadFormatCannotDriftIntoKISFormat(t *testing.T) {
	now := testGatewayNow()
	tossPayload, err := tossTokenPayload("opaque-toss-token", 3600, now)
	if err != nil {
		t.Fatal(err)
	}
	const wantTossBytes = `{"access_token":"opaque-toss-token","expires_at":2000003600}`
	if tossPayload != wantTossBytes {
		t.Fatalf("Toss bytes = %q, want %q", tossPayload, wantTossBytes)
	}
	if !tossPythonCompatiblePayload(tossPayload) || pythonCompatiblePayload(tossPayload) {
		t.Fatal("Toss two-field payload was accepted as the KIS three-field format")
	}
	kisPayload, err := tokenPayload("opaque-kis-token", 3600, now)
	if err != nil {
		t.Fatal(err)
	}
	if !pythonCompatiblePayload(kisPayload) || tossPythonCompatiblePayload(kisPayload) {
		t.Fatal("KIS three-field payload was accepted as the Toss two-field format")
	}
}

func TestTossEnsureRejectsKISPayloadBeforeIssuingReplacement(t *testing.T) {
	now := testGatewayNow()
	store := newFakeRedisStore(func() time.Time { return now })
	issuer := &fakeOAuthIssuer{token: OAuthToken{AccessToken: "replacement-toss-token", ExpiresIn: 3600}}
	service := newTestEnsureServiceForProvider(t, store, issuer, now, ProviderToss)
	kisPayload, err := tokenPayload("wrong-shape", 3600, now)
	if err != nil {
		t.Fatal(err)
	}
	store.save(service.CacheKey, kisPayload, 3660*time.Second)

	state, err := service.Ensure(context.Background())
	if err != nil || state != EnsureStateIssued || issuer.callCount() != 1 {
		t.Fatalf("ensure = %q, %v; OAuth calls = %d", state, err, issuer.callCount())
	}
}

func TestEnsureServiceRejectsCrossPinnedKISConfig(t *testing.T) {
	for _, test := range []struct {
		name   string
		config ProviderConfig
	}{
		{
			name:   "live cannot use mock host",
			config: ProviderConfig{Provider: ProviderKISLive, BaseURL: kismockread.MockBaseURL, AppKey: "app-key", AppSecret: "app-secret"},
		},
		{
			name:   "mock cannot use live host",
			config: ProviderConfig{Provider: ProviderKISMock, BaseURL: KISLiveBaseURL, AppKey: "app-key", AppSecret: "app-secret"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewEnsureServiceForProvider(newFakeRedisStore(testGatewayNow), &fakeOAuthIssuer{}, test.config); err == nil {
				t.Fatal("cross-pinned KIS config was accepted")
			}
		})
	}
}

func testFreshPayload(t *testing.T, provider TokenProvider, now time.Time) string {
	t.Helper()
	if provider == ProviderToss {
		payload, err := tossTokenPayload("fresh-cache-token", 3600, now)
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	payload, err := tokenPayload("fresh-cache-token", 3600, now)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func pythonCompatiblePayload(value string) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(value), &fields) != nil || len(fields) != 3 {
		return false
	}
	for _, key := range []string{"access_token", "expires_at", "created_at"} {
		if _, found := fields[key]; !found {
			return false
		}
	}
	var token string
	var expiresAt, createdAt float64
	return json.Unmarshal(fields["access_token"], &token) == nil && token != "" &&
		json.Unmarshal(fields["expires_at"], &expiresAt) == nil &&
		json.Unmarshal(fields["created_at"], &createdAt) == nil &&
		expiresAt > createdAt
}

func tossPythonCompatiblePayload(value string) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(value), &fields) != nil || len(fields) != 2 {
		return false
	}
	accessToken, hasAccessToken := fields["access_token"]
	expiresAt, hasExpiresAt := fields["expires_at"]
	if !hasAccessToken || !hasExpiresAt {
		return false
	}
	var token string
	var expiry float64
	return json.Unmarshal(accessToken, &token) == nil && token != "" &&
		json.Unmarshal(expiresAt, &expiry) == nil
}
