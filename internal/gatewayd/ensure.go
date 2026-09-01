package gatewayd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/mgh3326/broker-edge/internal/kismockread"
)

const (
	tokenExpiryBuffer = 60 * time.Second
	lockTTL           = 30 * time.Second
	lockInitialWait   = 200 * time.Millisecond
	lockPollInterval  = 100 * time.Millisecond
	lockWaitTimeout   = 3 * time.Second
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
// and the single VTS OAuth issuance path.
type EnsureService struct {
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
}

// NewEnsureService derives the same token namespace as the existing Python
// writer and the Go reader. The lock key is therefore {namespace}:token:lock.
func NewEnsureService(store RedisStore, oauth OAuthIssuer, config Config) (*EnsureService, error) {
	if store == nil || oauth == nil || config.AppKey == "" || config.AppSecret == "" {
		return nil, errEnsureUnavailable
	}
	baseURL, parseErr := url.Parse(config.BaseURL)
	if parseErr != nil || kismockread.ValidatePinnedURL(baseURL) != nil {
		return nil, errEnsureUnavailable
	}
	cacheKey, keyErr := kismockread.TokenCacheKey(config.BaseURL, config.AppKey)
	if keyErr != nil {
		return nil, errEnsureUnavailable
	}
	namespace, found := strings.CutSuffix(cacheKey, ":access_token")
	if !found || namespace == "" {
		return nil, errEnsureUnavailable
	}
	return &EnsureService{
		Store:           store,
		OAuth:           oauth,
		CacheKey:        cacheKey,
		LockKey:         namespace + ":token:lock",
		AppKey:          config.AppKey,
		AppSecret:       config.AppSecret,
		Now:             time.Now,
		InitialLockWait: lockInitialWait,
		LockPollEvery:   lockPollInterval,
		LockWaitTimeout: lockWaitTimeout,
	}, nil
}

// Ensure returns fresh if the shared cache already contains a reader-valid
// token, otherwise it takes the Python-compatible distributed lock and issues
// exactly one replacement token.
func (service *EnsureService) Ensure(ctx context.Context) (EnsureState, error) {
	if service == nil || service.Store == nil || service.OAuth == nil || service.CacheKey == "" || service.LockKey == "" {
		return "", errEnsureUnavailable
	}
	fresh, err := service.hasFreshToken(ctx)
	if err != nil {
		return "", err
	}
	if fresh {
		return EnsureStateFresh, nil
	}
	lockValue, err := newLockValue()
	if err != nil {
		return "", errEnsureUnavailable
	}
	locked, err := service.Store.SetNX(ctx, service.LockKey, lockValue, lockTTL)
	if err != nil {
		return "", errEnsureUnavailable
	}
	if !locked {
		return service.waitForFreshToken(ctx)
	}
	defer service.releaseLock(lockValue)

	// A peer can populate the cache between the initial GET and SET NX. Check
	// again while owning the lock before contacting VTS.
	fresh, err = service.hasFreshToken(ctx)
	if err != nil {
		return "", err
	}
	if fresh {
		return EnsureStateFresh, nil
	}
	issued, err := service.OAuth.Issue(ctx, service.AppKey, service.AppSecret)
	if err != nil {
		return "", errEnsureUnavailable
	}
	if issued.AccessToken == "" || !safeHeaderText(issued.AccessToken) ||
		issued.ExpiresIn <= int64(tokenExpiryBuffer/time.Second) || issued.ExpiresIn > maximumTokenSeconds {
		return "", errEnsureUnavailable
	}
	now := service.now()
	payload, err := tokenPayload(issued.AccessToken, issued.ExpiresIn, now)
	if err != nil {
		return "", errEnsureUnavailable
	}
	ttl := time.Duration(issued.ExpiresIn)*time.Second + tokenExpiryBuffer
	if err := service.Store.Set(ctx, service.CacheKey, payload, ttl); err != nil {
		return "", errEnsureUnavailable
	}
	return EnsureStateIssued, nil
}

func (service *EnsureService) hasFreshToken(ctx context.Context) (bool, error) {
	_, loadErr := kismockread.LoadCachedToken(ctx, service.Store, service.CacheKey, service.now())
	if loadErr == nil {
		return true, nil
	}
	if loadErr.Code == kismockread.CodeTokenCacheUnavailable {
		return false, errEnsureUnavailable
	}
	// Missing, malformed, and near-expiry payloads are all replaced under the
	// distributed lock. The reader remains fail-closed until that SET succeeds.
	return false, nil
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
		return lockInitialWait
	}
	return service.InitialLockWait
}

func (service *EnsureService) lockPollEvery() time.Duration {
	if service.LockPollEvery <= 0 {
		return lockPollInterval
	}
	return service.LockPollEvery
}

func (service *EnsureService) lockWaitTimeout() time.Duration {
	if service.LockWaitTimeout <= 0 {
		return lockWaitTimeout
	}
	return service.LockWaitTimeout
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
// field names and their byte order are stable and covered by a fixed-byte test.
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
