package reader

import (
	"context"
	"errors"
	"log/slog"

	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

const approvalCacheKey = "kis:websocket:approval_key"

// ApprovalProvider reads the legacy approval-key cache but never writes it.
// On a cache miss, it delegates REST issuance to the supplied ws provider.
type ApprovalProvider struct {
	redis    redis.UniversalClient
	fallback ws.ApprovalKeyProvider
	logger   *slog.Logger
}

// NewApprovalProvider constructs the Redis-first approval provider.
func NewApprovalProvider(client redis.UniversalClient, fallback ws.ApprovalKeyProvider, loggers ...*slog.Logger) (*ApprovalProvider, error) {
	if client == nil || fallback == nil {
		return nil, errors.New("reader: redis client and approval fallback are required")
	}
	var logger *slog.Logger
	if len(loggers) > 0 {
		logger = loggers[0]
	}
	return &ApprovalProvider{redis: client, fallback: fallback, logger: logger}, nil
}

// ApprovalKey returns a cached key if present, without a REST call. Redis
// errors are surfaced rather than silently turning an outage into credential
// churn; only a true cache miss falls back to REST issuance.
func (p *ApprovalProvider) ApprovalKey(ctx context.Context) (string, error) {
	key, err := p.redis.Get(ctx, approvalCacheKey).Result()
	if err == nil && key != "" {
		return key, nil
	}
	if errors.Is(err, redis.Nil) || (err == nil && key == "") {
		if p.logger != nil {
			p.logger.Info("KIS approval key REST issuance attempted")
		}
		return p.fallback.ApprovalKey(ctx)
	}
	return "", err
}

// Reissue deliberately bypasses the cache and delegates REST reissuance.
func (p *ApprovalProvider) Reissue(ctx context.Context) (string, error) {
	if p.logger != nil {
		p.logger.Info("KIS approval key REST reissue attempted")
	}
	return p.fallback.Reissue(ctx)
}
