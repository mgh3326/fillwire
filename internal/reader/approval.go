package reader

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

const approvalCacheKey = "kis:websocket:approval_key"

const ApprovalModeCacheOnly = "cache-only"

// ErrCacheOnlyApprovalUnavailable is permanent until an operator restores the
// existing approval key in Redis. It is intentionally value-free so callers
// can safely map it to the fail-closed process exit code.
var ErrCacheOnlyApprovalUnavailable = errors.New("reader: cache-only approval unavailable")

// ApprovalProvider reads the legacy approval-key cache but never writes it.
// On a cache miss, it delegates REST issuance to the supplied ws provider.
type ApprovalProvider struct {
	redis    redis.UniversalClient
	fallback ws.ApprovalKeyProvider
	logger   *slog.Logger
	mode     string

	failureOnce sync.Once
	failureCh   chan struct{}
}

// NewApprovalProvider constructs the Redis-first approval provider.
func NewApprovalProvider(client redis.UniversalClient, fallback ws.ApprovalKeyProvider, loggers ...*slog.Logger) (*ApprovalProvider, error) {
	return NewApprovalProviderWithMode(client, fallback, "", loggers...)
}

// NewApprovalProviderWithMode constructs the Redis-first provider with the
// explicitly selected approval policy. The empty mode preserves the original
// Redis-first/REST-fallback behavior.
func NewApprovalProviderWithMode(client redis.UniversalClient, fallback ws.ApprovalKeyProvider, mode string, loggers ...*slog.Logger) (*ApprovalProvider, error) {
	if client == nil || fallback == nil {
		return nil, errors.New("reader: redis client and approval fallback are required")
	}
	if mode != "" && mode != ApprovalModeCacheOnly {
		return nil, errors.New("reader: unsupported approval mode")
	}
	var logger *slog.Logger
	if len(loggers) > 0 {
		logger = loggers[0]
	}
	return &ApprovalProvider{
		redis:     client,
		fallback:  fallback,
		logger:    logger,
		mode:      mode,
		failureCh: make(chan struct{}),
	}, nil
}

// ApprovalKey returns a cached key if present, without a REST call. Redis
// errors are surfaced rather than silently turning an outage into credential
// churn; only a true cache miss falls back to REST issuance.
func (p *ApprovalProvider) ApprovalKey(ctx context.Context) (string, error) {
	key, err := p.redis.Get(ctx, approvalCacheKey).Result()
	if err == nil && key != "" {
		return key, nil
	}
	if p.mode == ApprovalModeCacheOnly {
		p.markCacheOnlyFailure()
		return "", ErrCacheOnlyApprovalUnavailable
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
	if p.mode == ApprovalModeCacheOnly {
		p.markCacheOnlyFailure()
		return "", ErrCacheOnlyApprovalUnavailable
	}
	if p.logger != nil {
		p.logger.Info("KIS approval key REST reissue attempted")
	}
	return p.fallback.Reissue(ctx)
}

func (p *ApprovalProvider) markCacheOnlyFailure() {
	p.failureOnce.Do(func() { close(p.failureCh) })
}

// cacheOnlyFailureSignal and cacheOnlyFailure are intentionally private: the
// Reader owns the bounded shutdown protocol, while other providers retain the
// original ws.ApprovalKeyProvider contract.
func (p *ApprovalProvider) cacheOnlyFailureSignal() <-chan struct{} { return p.failureCh }

func (p *ApprovalProvider) cacheOnlyFailure() error {
	if p.mode != ApprovalModeCacheOnly {
		return nil
	}
	select {
	case <-p.failureCh:
		return ErrCacheOnlyApprovalUnavailable
	default:
		return nil
	}
}
