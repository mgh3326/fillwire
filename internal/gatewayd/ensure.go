package gatewayd

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/kismockread"
)

const (
	tokenExpiryBuffer = 60 * time.Second
	lockTTL           = 30 * time.Second
	lockInitialWait   = 200 * time.Millisecond
	lockPollInterval  = 100 * time.Millisecond
	lockWaitTimeout   = 3 * time.Second

	// tossIssuePublishBudget is the lock lease a Toss replacement must still
	// have after the final cache check before issuance may start: one OAuth
	// round trip (bounded by the OAuth client timeout) plus the Redis
	// publish (bounded by redisDialTimeout). Toss permits a single valid
	// token per client, so minting a token the lease cannot carry through
	// publish would kill the old one without installing the new.
	tossIssuePublishBudget = 10*time.Second + redisDialTimeout
	// tossShutdownPublishBudget bounds a Toss replacement whose caller is
	// gone (shutdown): issue and publish may still finish, but only inside
	// this window — the daemon's graceful shutdown does not wait a whole
	// lock lease.
	tossShutdownPublishBudget = 5 * time.Second
)

var (
	errEnsureUnavailable = errors.New("token ensure unavailable")
	errLockWaitTimeout   = errors.New("token lock wait timeout")
)

// EnsureState is the closed status response vocabulary. Token material never
// belongs in this type.
type EnsureState string

const (
	EnsureStateFresh  EnsureState = "fresh"
	EnsureStateIssued EnsureState = "issued"
)

// Ensurer is the only capability exposed to the HTTP boundary.
type Ensurer interface {
	Ensure(ctx context.Context) (EnsureState, error)
}

// EnsureService coordinates Redis cache reuse, cooperative distributed locking,
// and one provider's OAuth issuance path.
type EnsureService struct {
	Provider  TokenProvider
	Store     RedisStore
	OAuth     OAuthIssuer
	CacheKey  string
	LockKey   string
	AppKey    string
	AppSecret string
	Now       func() time.Time

	InitialLockWait time.Duration
	LockPollEvery   time.Duration
	LockWaitTimeout time.Duration

	// MinIssueLease overrides the lock lease a Toss replacement must still
	// hold before issuance may start; ShutdownPublishBudget overrides the
	// bound applied once its caller is gone. Zero uses the package defaults.
	MinIssueLease         time.Duration
	ShutdownPublishBudget time.Duration
}

// NewEnsureService preserves the existing kis-mock-only constructor. New
// multi-provider code should use NewEnsureServiceForProvider explicitly.
func NewEnsureService(store RedisStore, oauth OAuthIssuer, config Config) (*EnsureService, error) {
	providerConfig, found := config.providerConfig(ProviderKISMock)
	if !found {
		return nil, errEnsureUnavailable
	}
	return NewEnsureServiceForProvider(store, oauth, providerConfig)
}

// NewEnsureServiceForProvider derives the Redis namespace used by that
// provider's existing Python consumer before constructing an issuer service.
func NewEnsureServiceForProvider(store RedisStore, oauth OAuthIssuer, config ProviderConfig) (*EnsureService, error) {
	if store == nil || oauth == nil || !validProviderConfig(config) {
		return nil, errEnsureUnavailable
	}
	cacheKey, lockKey, err := providerTokenKeys(config)
	if err != nil {
		return nil, errEnsureUnavailable
	}
	return &EnsureService{
		Provider:        config.Provider,
		Store:           store,
		OAuth:           oauth,
		CacheKey:        cacheKey,
		LockKey:         lockKey,
		AppKey:          config.AppKey,
		AppSecret:       config.AppSecret,
		Now:             time.Now,
		InitialLockWait: providerInitialLockWait(config.Provider),
		LockPollEvery:   providerLockPollInterval(config.Provider),
		LockWaitTimeout: providerLockWaitTimeout(config.Provider),
	}, nil
}

func providerTokenKeys(config ProviderConfig) (string, string, error) {
	switch config.Provider {
	case ProviderKISMock:
		cacheKey, err := kismockread.TokenCacheKey(config.BaseURL, config.AppKey)
		if err != nil {
			return "", "", errEnsureUnavailable
		}
		namespace, found := strings.CutSuffix(cacheKey, ":access_token")
		if !found || namespace == "" {
			return "", "", errEnsureUnavailable
		}
		return cacheKey, namespace + ":token:lock", nil
	case ProviderKISLive:
		// This exactly mirrors the default RedisTokenManager() namespace in
		// auto_trader: live KIS has no credential fingerprint in its key.
		return "kis:access_token", "kis:token:lock", nil
	case ProviderToss:
		fingerprint := sha256.Sum256([]byte(config.AppKey))
		namespace := "toss:oauth:" + hex.EncodeToString(fingerprint[:8])
		return namespace + ":access_token", namespace + ":lock", nil
	default:
		return "", "", errEnsureUnavailable
	}
}

// Ensure returns fresh if the shared cache already contains a reader-valid
// token, otherwise it takes the provider's cooperative distributed lock and
// issues exactly one replacement token.
func (service *EnsureService) Ensure(ctx context.Context) (EnsureState, error) {
	return service.ensure(ctx, false, "")
}

// EnsureReplacing replaces the token whose hash a consumer reported as
// rejected. It skips the pre-lock freshness short-circuit, but under the lock it
// re-reads the cache and issues nothing if a different usable token was
// published since the caller decided to replace (a peer already did the work).
// A contended lock still waits for a fresh token exactly as Ensure does.
func (service *EnsureService) EnsureReplacing(ctx context.Context, rejectedSHA string) (EnsureState, error) {
	return service.ensure(ctx, true, rejectedSHA)
}

// replacedSince reports whether the cache now holds a usable token whose hash
// differs from the rejected one.
func (service *EnsureService) replacedSince(ctx context.Context, rejectedSHA string) (bool, error) {
	raw, present, err := service.Store.Get(ctx, service.CacheKey)
	if err != nil {
		return false, errEnsureUnavailable
	}
	if !present {
		return false, nil
	}
	token, _, ok := parseCachedToken(service.Provider, raw)
	if !ok || tokenSHA256(token) == rejectedSHA {
		return false, nil
	}
	return service.hasFreshToken(ctx)
}

func (service *EnsureService) tokenKey() string {
	if service == nil {
		return ""
	}
	return service.CacheKey
}

func (service *EnsureService) ensure(ctx context.Context, force bool, rejectedSHA string) (EnsureState, error) {
	if service == nil || !knownProvider(service.Provider) || service.Store == nil || service.OAuth == nil ||
		service.CacheKey == "" || service.LockKey == "" || service.AppKey == "" || service.AppSecret == "" {
		return "", errEnsureUnavailable
	}
	if !force {
		fresh, err := service.hasFreshToken(ctx)
		if err != nil {
			return "", err
		}
		if fresh {
			return EnsureStateFresh, nil
		}
	}
	lockValue, err := newLockValue()
	if err != nil {
		return "", errEnsureUnavailable
	}
	tossReplacement := service.Provider == ProviderToss && rejectedSHA != ""
	lockStarted := time.Now()
	locked, err := service.Store.SetNX(ctx, service.LockKey, lockValue, lockTTL)
	if err != nil {
		return "", errEnsureUnavailable
	}
	if !locked {
		return service.waitForFreshToken(ctx)
	}
	defer service.releaseLock(lockValue)

	leaseEnd := lockStarted.Add(lockTTL)
	if tossReplacement {
		// Toss permits one valid token per client: a reissue invalidates the
		// previous token, so the rotation must settle — a peer's rechecked
		// token or our own publish — while this process still holds the lock.
		// Detach from the caller so a shutdown mid-rotation still finishes
		// the publish. The work is bounded by the remaining lease, and once
		// the caller is gone the tighter shutdown publish budget applies.
		deadline := leaseEnd
		if ctx.Err() != nil {
			if bounded := time.Now().Add(service.shutdownPublishBudget()); bounded.Before(deadline) {
				deadline = bounded
			}
		}
		detached, detachCancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
		defer detachCancel()
		stopShrink := context.AfterFunc(ctx, func() {
			time.AfterFunc(service.shutdownPublishBudget(), detachCancel)
		})
		defer stopShrink()
		ctx = detached
	}

	// A peer can populate the cache between the initial GET and SET NX. Check
	// again while owning the lock before contacting the provider.
	if !force {
		fresh, err := service.hasFreshToken(ctx)
		if err != nil {
			return "", err
		}
		if fresh {
			return EnsureStateFresh, nil
		}
	} else if rejectedSHA != "" {
		replaced, err := service.replacedSince(ctx, rejectedSHA)
		if err != nil {
			return "", err
		}
		if replaced {
			return EnsureStateFresh, nil
		}
	}
	if tossReplacement && time.Until(leaseEnd) < service.minIssueLease() {
		// The remaining lease cannot cover OAuth issuance plus the Redis
		// publish, and a minted Toss token kills the previous one. Refuse so
		// the caller retries under a fresh lease instead.
		return "", errEnsureUnavailable
	}
	issued, err := service.OAuth.Issue(ctx, service.AppKey, service.AppSecret)
	if err != nil || !validIssuedToken(issued, providerTokenExpiryBuffer(service.Provider)) {
		return "", errEnsureUnavailable
	}
	payload, err := service.payload(issued.AccessToken, issued.ExpiresIn, service.now())
	if err != nil {
		return "", errEnsureUnavailable
	}
	// Toss permits one valid token per client. A successful reissue invalidates
	// the previous token, so publish the replacement while still owning this
	// shared lock before any Python consumer can observe an unlocked stale key.
	ttl := providerCacheTTL(service.Provider, issued.ExpiresIn)
	if tossReplacement {
		err = service.Store.SetIfLockOwned(ctx, service.LockKey, lockValue, service.CacheKey, payload, ttl)
	} else {
		err = service.Store.Set(ctx, service.CacheKey, payload, ttl)
	}
	if err != nil {
		return "", errEnsureUnavailable
	}
	return EnsureStateIssued, nil
}

func (service *EnsureService) payload(accessToken string, expiresIn int64, now time.Time) (string, error) {
	switch service.Provider {
	case ProviderKISMock, ProviderKISLive:
		return tokenPayload(accessToken, expiresIn, now)
	case ProviderToss:
		return tossTokenPayload(accessToken, expiresIn, now)
	default:
		return "", errEnsureUnavailable
	}
}

func (service *EnsureService) hasFreshToken(ctx context.Context) (bool, error) {
	switch service.Provider {
	case ProviderKISMock, ProviderKISLive:
		_, loadErr := kismockread.LoadCachedToken(ctx, service.Store, service.CacheKey, service.now())
		if loadErr == nil {
			return true, nil
		}
		if loadErr.Code == kismockread.CodeTokenCacheUnavailable {
			return false, errEnsureUnavailable
		}
		// Missing, malformed, and near-expiry payloads are all replaced under
		// the distributed lock. The reader remains fail-closed until SET wins.
		return false, nil
	case ProviderToss:
		_, fresh, err := loadTossCachedToken(ctx, service.Store, service.CacheKey, service.now())
		if err != nil {
			return false, errEnsureUnavailable
		}
		return fresh, nil
	default:
		return false, errEnsureUnavailable
	}
}

func (service *EnsureService) waitForFreshToken(ctx context.Context) (EnsureState, error) {
	if !waitContext(ctx, service.initialLockWait()) {
		return "", errEnsureUnavailable
	}
	fresh, err := service.hasFreshToken(ctx)
	if err != nil {
		return "", err
	}
	if fresh {
		return EnsureStateFresh, nil
	}
	pollEvery := service.lockPollEvery()
	attempts := int((service.lockWaitTimeout() + pollEvery - 1) / pollEvery)
	if attempts < 1 {
		attempts = 1
	}
	for range attempts {
		if !waitContext(ctx, pollEvery) {
			return "", errEnsureUnavailable
		}
		fresh, err = service.hasFreshToken(ctx)
		if err != nil {
			return "", err
		}
		if fresh {
			return EnsureStateFresh, nil
		}
	}
	return "", errLockWaitTimeout
}

func (service *EnsureService) releaseLock(lockValue string) {
	releaseContext, cancel := context.WithTimeout(context.Background(), redisDialTimeout)
	defer cancel()
	_ = service.Store.CompareAndDelete(releaseContext, service.LockKey, lockValue)
}

func (service *EnsureService) now() time.Time {
	if service.Now == nil {
		return time.Now()
	}
	return service.Now()
}

func (service *EnsureService) initialLockWait() time.Duration {
	if service.InitialLockWait <= 0 {
		return providerInitialLockWait(service.Provider)
	}
	return service.InitialLockWait
}

func (service *EnsureService) lockPollEvery() time.Duration {
	if service.LockPollEvery <= 0 {
		return providerLockPollInterval(service.Provider)
	}
	return service.LockPollEvery
}

func (service *EnsureService) lockWaitTimeout() time.Duration {
	if service.LockWaitTimeout <= 0 {
		return providerLockWaitTimeout(service.Provider)
	}
	return service.LockWaitTimeout
}

func (service *EnsureService) minIssueLease() time.Duration {
	if service.MinIssueLease <= 0 {
		return tossIssuePublishBudget
	}
	return service.MinIssueLease
}

func (service *EnsureService) shutdownPublishBudget() time.Duration {
	if service.ShutdownPublishBudget <= 0 {
		return tossShutdownPublishBudget
	}
	return service.ShutdownPublishBudget
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func newLockValue() (string, error) {
	bytes := make([]byte, 18)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

type cachedTokenPayload struct {
	AccessToken string  `json:"access_token"`
	ExpiresAt   float64 `json:"expires_at"`
	CreatedAt   float64 `json:"created_at"`
}

// tokenPayload deliberately uses a struct rather than a map: its three Python
// KIS field names and byte order are stable and covered by a fixed-byte test.
func tokenPayload(accessToken string, expiresIn int64, now time.Time) (string, error) {
	createdAt := float64(now.UnixNano()) / float64(time.Second)
	payload, err := json.Marshal(cachedTokenPayload{
		AccessToken: accessToken,
		ExpiresAt:   createdAt + float64(expiresIn),
		CreatedAt:   createdAt,
	})
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

type tossCachedTokenPayload struct {
	AccessToken string  `json:"access_token"`
	ExpiresAt   float64 `json:"expires_at"`
}

// tossTokenPayload deliberately has no created_at field. This is the exact
// two-field shape written by TossOAuthTokenManager._cache_token in Python.
func tossTokenPayload(accessToken string, expiresIn int64, now time.Time) (string, error) {
	nowSeconds := float64(now.UnixNano()) / float64(time.Second)
	payload, err := json.Marshal(tossCachedTokenPayload{
		AccessToken: accessToken,
		ExpiresAt:   nowSeconds + float64(expiresIn),
	})
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func loadTossCachedToken(ctx context.Context, store RedisStore, key string, now time.Time) (string, bool, error) {
	if store == nil {
		return "", false, errEnsureUnavailable
	}
	raw, present, err := store.Get(ctx, key)
	if err != nil {
		return "", false, errEnsureUnavailable
	}
	if !present {
		return "", false, nil
	}
	fields, valid := strictJSONFields(raw)
	if !valid || len(fields) != 2 {
		return "", false, nil
	}
	accessTokenRaw, hasAccessToken := fields["access_token"]
	expiresAtRaw, hasExpiresAt := fields["expires_at"]
	if !hasAccessToken || !hasExpiresAt {
		return "", false, nil
	}
	var accessToken string
	var expiresAt float64
	if json.Unmarshal(accessTokenRaw, &accessToken) != nil ||
		json.Unmarshal(expiresAtRaw, &expiresAt) != nil ||
		accessToken == "" || !safeHeaderText(accessToken) ||
		math.IsNaN(expiresAt) || math.IsInf(expiresAt, 0) {
		return "", false, nil
	}
	nowSeconds := float64(now.UnixNano()) / float64(time.Second)
	if !(nowSeconds < expiresAt-tossTokenExpiryBuffer.Seconds()) {
		return "", false, nil
	}
	return accessToken, true, nil
}

// strictJSONFields rejects duplicate keys and trailing JSON so the cache
// protocol stays deterministic even if an untrusted Redis value is malformed.
func strictJSONFields(raw string) (map[string]json.RawMessage, bool) {
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
