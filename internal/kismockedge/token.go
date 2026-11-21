package kismockedge

import (
	"context"
	"time"

	"github.com/mgh3326/broker-edge/internal/kismockread"
)

// RedisCachedTokenLoader reuses kis-mock-read's only-token capability: Redis
// GET followed by strict payload and expiry validation.
type RedisCachedTokenLoader struct{}

func (RedisCachedTokenLoader) Load(ctx context.Context, config kismockread.Config) (string, string) {
	getter, err := kismockread.NewRedisGETClient(config.RedisURL)
	if err != nil {
		return "", string(err.Code)
	}
	key, err := kismockread.TokenCacheKey(config.BaseURL, config.AppKey)
	if err != nil {
		return "", string(err.Code)
	}
	token, err := kismockread.LoadCachedToken(ctx, getter, key, time.Now())
	if err != nil {
		return "", string(err.Code)
	}
	return token, ""
}
