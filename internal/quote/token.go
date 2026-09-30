package quote

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrTokenUnavailable reports that the shared cache holds no usable token.
var ErrTokenUnavailable = errors.New("quote: no usable cached Toss access token")

// tokenExpiryBuffer matches auto_trader TOKEN_EXPIRY_BUFFER_SECONDS: a token
// this close to expiry is treated as absent.
const tokenExpiryBuffer = 120 * time.Second

// tokenKeyPattern is the auto_trader TossOAuthTokenManager cache key:
// toss:oauth:{sha256(client_id)[:16]}:access_token.
var tokenKeyPattern = regexp.MustCompile(`^toss:oauth:[0-9a-f]{16}:access_token$`)

// ValidTokenKey reports whether key names a Toss access-token cache entry.
func ValidTokenKey(key string) bool { return tokenKeyPattern.MatchString(key) }

// TokenSource returns a usable access token or ErrTokenUnavailable.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

// Clock supplies time to the lane; tests inject one.
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

// CachedToken reads the Toss access token that auto_trader (or broker-edge
// gatewayd) keeps in Redis. It is the non-owner side of that contract: it
// issues one GET and has no path that writes, issues, refreshes, or
// force-reissues a token. Toss allows one valid token per client, so any
// issue from here would invalidate the token auto_trader orders with.
type CachedToken struct {
	client redis.UniversalClient
	key    string
	clock  Clock
}

// NewCachedToken validates key.
func NewCachedToken(client redis.UniversalClient, key string, clock Clock) (*CachedToken, error) {
	if client == nil || !ValidTokenKey(key) {
		return nil, errors.New("quote: token cache needs a Redis client and a toss:oauth:<16 hex>:access_token key")
	}
	if clock == nil {
		clock = systemClock{}
	}
	return &CachedToken{client: client, key: key, clock: clock}, nil
}

type cachedTokenPayload struct {
	AccessToken *string  `json:"access_token"`
	ExpiresAt   *float64 `json:"expires_at"`
}

// Token returns the cached token while it is outside the expiry buffer.
func (c *CachedToken) Token(ctx context.Context) (string, error) {
	raw, err := c.client.Get(ctx, c.key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", ErrTokenUnavailable
		}
		return "", errors.New("quote: token cache read failed")
	}
	var payload cachedTokenPayload
	if json.Unmarshal([]byte(raw), &payload) != nil || payload.AccessToken == nil || payload.ExpiresAt == nil || *payload.AccessToken == "" {
		return "", ErrTokenUnavailable
	}
	expiresAt := time.Unix(0, int64(*payload.ExpiresAt*float64(time.Second)))
	if !c.clock.Now().Before(expiresAt.Add(-tokenExpiryBuffer)) {
		return "", ErrTokenUnavailable
	}
	return *payload.AccessToken, nil
}
