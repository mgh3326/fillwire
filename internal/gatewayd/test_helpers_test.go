package gatewayd

import (
	"context"
	"sync"
	"time"

	"github.com/mgh3326/broker-edge/internal/kismockread"
)

type storedValue struct {
	value     string
	expiresAt time.Time
}

type setRecord struct {
	key   string
	value string
	ttl   time.Duration
}

type fakeRedisStore struct {
	mu sync.Mutex

	now    func() time.Time
	values map[string]storedValue
	locks  map[string]storedValue

	getErr   error
	setErr   error
	setNXErr error

	sets       []setRecord
	setNXCalls int
	setNXHit   chan struct{}
}

func newFakeRedisStore(now func() time.Time) *fakeRedisStore {
	return &fakeRedisStore{
		now:    now,
		values: make(map[string]storedValue),
		locks:  make(map[string]storedValue),
	}
}

func (store *fakeRedisStore) Get(_ context.Context, key string) (string, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.getErr != nil {
		return "", false, store.getErr
	}
	value, found := store.values[key]
	if !found || (!value.expiresAt.IsZero() && !store.now().Before(value.expiresAt)) {
		delete(store.values, key)
		return "", false, nil
	}
	return value.value, true, nil
}

func (store *fakeRedisStore) Set(_ context.Context, key, value string, ttl time.Duration) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.setErr != nil {
		return store.setErr
	}
	store.values[key] = storedValue{value: value, expiresAt: store.now().Add(ttl)}
	store.sets = append(store.sets, setRecord{key: key, value: value, ttl: ttl})
	return nil
}

func (store *fakeRedisStore) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.setNXErr != nil {
		return false, store.setNXErr
	}
	store.setNXCalls++
	if store.setNXHit != nil && store.setNXCalls == 2 {
		close(store.setNXHit)
	}
	lock, found := store.locks[key]
	if found && (lock.expiresAt.IsZero() || store.now().Before(lock.expiresAt)) {
		return false, nil
	}
	store.locks[key] = storedValue{value: value, expiresAt: store.now().Add(ttl)}
	return true, nil
}

func (store *fakeRedisStore) CompareAndDelete(_ context.Context, key, value string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if lock, found := store.locks[key]; found && lock.value == value {
		delete(store.locks, key)
	}
	return nil
}

func (store *fakeRedisStore) save(key, value string, ttl time.Duration) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.values[key] = storedValue{value: value, expiresAt: store.now().Add(ttl)}
}

func (store *fakeRedisStore) savedRecords() []setRecord {
	store.mu.Lock()
	defer store.mu.Unlock()
	output := make([]setRecord, len(store.sets))
	copy(output, store.sets)
	return output
}

func (store *fakeRedisStore) lockAttempts() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.setNXCalls
}

type fakeOAuthIssuer struct {
	mu sync.Mutex

	token   OAuthToken
	err     error
	calls   int
	started chan struct{}
	release <-chan struct{}
}

func (issuer *fakeOAuthIssuer) Issue(ctx context.Context, _, _ string) (OAuthToken, error) {
	issuer.mu.Lock()
	issuer.calls++
	started := issuer.started
	release := issuer.release
	issuer.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if release != nil {
		select {
		case <-ctx.Done():
			return OAuthToken{}, ctx.Err()
		case <-release:
		}
	}
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	return issuer.token, issuer.err
}

func (issuer *fakeOAuthIssuer) callCount() int {
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	return issuer.calls
}

func testGatewayConfig() Config {
	return Config{
		BaseURL:   kismockread.MockBaseURL,
		AppKey:    "app-key-for-test",
		AppSecret: "app-secret-for-test",
		RedisURL:  "redis://127.0.0.1:6379/0",
		Timeout:   time.Second,
	}
}

func testGatewayNow() time.Time {
	return time.Unix(2_000_000_000, 0)
}
