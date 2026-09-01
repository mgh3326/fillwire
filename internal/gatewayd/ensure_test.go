package gatewayd

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/broker-edge/internal/kismockread"
)

func TestEnsureReturnsFreshWithoutOAuth(t *testing.T) {
	now := testGatewayNow()
	store := newFakeRedisStore(func() time.Time { return now })
	issuer := &fakeOAuthIssuer{token: OAuthToken{AccessToken: "unused", ExpiresIn: 3600}}
	service := newTestEnsureService(t, store, issuer, now)
	payload, err := tokenPayload("fresh-cache-token", 3600, now)
	if err != nil {
		t.Fatal(err)
	}
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
}

func TestEnsureIssuesFixedPythonCompatiblePayload(t *testing.T) {
	now := testGatewayNow()
	store := newFakeRedisStore(func() time.Time { return now })
	issuer := &fakeOAuthIssuer{token: OAuthToken{AccessToken: "opaque-issued-token", ExpiresIn: 3600}}
	service := newTestEnsureService(t, store, issuer, now)

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
	if record.key != service.CacheKey || record.ttl != 3660*time.Second {
		t.Fatalf("SET metadata = %#v", record)
	}
	const wantBytes = `{"access_token":"opaque-issued-token","expires_at":2000003600,"created_at":2000000000}`
	if record.value != wantBytes {
		t.Fatalf("SET bytes = %q, want %q", record.value, wantBytes)
	}
	if !pythonCompatiblePayload(record.value) {
		t.Fatalf("SET payload is not a Python-compatible three-field JSON object: %q", record.value)
	}
	loaded, loadErr := kismockread.LoadCachedToken(context.Background(), store, service.CacheKey, now)
	if loadErr != nil || loaded != "opaque-issued-token" {
		t.Fatalf("reader round trip = %q, %v", loaded, loadErr)
	}
}

func TestEnsureConcurrentCallsCooperateThroughRedisLock(t *testing.T) {
	now := testGatewayNow()
	store := newFakeRedisStore(func() time.Time { return now })
	store.setNXHit = make(chan struct{})
	releaseOAuth := make(chan struct{})
	issuer := &fakeOAuthIssuer{
		token:   OAuthToken{AccessToken: "singleflight-token", ExpiresIn: 3600},
		started: make(chan struct{}, 2),
		release: releaseOAuth,
	}
	service := newTestEnsureService(t, store, issuer, now)
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
	// This is deliberately an observable mutant tripwire: removing the SET NX
	// cooperation makes both concurrent calls cross fake OAuth and yields 2.
	if issuer.callCount() != 1 {
		t.Fatalf("OAuth calls = %d, want exactly 1", issuer.callCount())
	}
	if store.lockAttempts() != 2 {
		t.Fatalf("SET NX attempts = %d, want 2", store.lockAttempts())
	}
}

func TestEnsureServiceDerivesPythonNamespaceLock(t *testing.T) {
	service := newTestEnsureService(t, newFakeRedisStore(testGatewayNow), &fakeOAuthIssuer{}, testGatewayNow())
	wantCacheKey, err := kismockread.TokenCacheKey(testGatewayConfig().BaseURL, testGatewayConfig().AppKey)
	if err != nil {
		t.Fatal(err)
	}
	if service.CacheKey != wantCacheKey {
		t.Fatalf("cache key = %q, want %q", service.CacheKey, wantCacheKey)
	}
	wantLockKey := strings.TrimSuffix(wantCacheKey, ":access_token") + ":token:lock"
	if service.LockKey != wantLockKey {
		t.Fatalf("lock key = %q, want %q", service.LockKey, wantLockKey)
	}
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

func newTestEnsureService(t *testing.T, store RedisStore, issuer OAuthIssuer, now time.Time) *EnsureService {
	t.Helper()
	service, err := NewEnsureService(store, issuer, testGatewayConfig())
	if err != nil {
		t.Fatal(err)
	}
	service.Now = func() time.Time { return now }
	return service
}
