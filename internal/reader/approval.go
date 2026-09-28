package reader

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	mrand "math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

// These values match auto_trader approval_keys.py and constants.py at
// be0839808b35b7fcd4bbeb326ac0a84daf7ad96a.
const (
	ApprovalModeCacheOnly = "cache-only"
	ApprovalCacheTTL      = 23 * time.Hour
	ApprovalLockTTL       = 15 * time.Second
	ApprovalWait          = 12 * time.Second
	ApprovalPoll          = 250 * time.Millisecond
	ApprovalIssueTimeout  = 10 * time.Second
	DefaultRefreshMargin  = time.Hour
	publishDeadline       = 14 * time.Second
)

const approvalCacheKey = "kis:websocket:approval_key"
const cachedScript = `local value = redis.call('GET', KEYS[1]); if not value then return {false, -2} end; return {value, redis.call('PTTL', KEYS[1])}`

var (
	ErrCacheOnlyApprovalUnavailable = errors.New("reader: cache-only approval unavailable")
	ErrApprovalUnavailable          = errors.New("reader: approval issuance temporarily unavailable")
	ErrApprovalLockLost             = errors.New("reader: approval issuance lock ownership lost")
	ErrApprovalConfig               = errors.New("reader: approval configuration invalid")
)

// ApprovalIssuer must make exactly one REST request and honor ctx. It is called
// only while a Redis lock is owned. It must never retain an in-process key.
type ApprovalIssuer interface {
	Issue(context.Context) (string, error)
}
type issuerFunc func(context.Context) (string, error)

func (f issuerFunc) Issue(ctx context.Context) (string, error) { return f(ctx) }

type ApprovalMetrics struct {
	IssueCalls      atomic.Uint64
	LockAcquired    atomic.Uint64
	LockContended   atomic.Uint64
	LockWaitSuccess atomic.Uint64
	LockWaitFailed  atomic.Uint64
}

type ApprovalProvider struct {
	redis       redis.UniversalClient
	issuer      ApprovalIssuer
	logger      *slog.Logger
	mode        string
	accountMode string
	margin      time.Duration
	metrics     *ApprovalMetrics
	now         func() time.Time
	wait        func(context.Context, time.Duration) error

	mu          sync.Mutex
	lastKey     string
	failureOnce sync.Once
	failureCh   chan struct{}
	failureErr  error
}

// NewApprovalProvider retains the historical live-mode constructor for callers.
func NewApprovalProvider(client redis.UniversalClient, fallback ws.ApprovalKeyProvider, loggers ...*slog.Logger) (*ApprovalProvider, error) {
	return NewApprovalProviderWithMode(client, fallback, "", loggers...)
}

// NewApprovalProviderWithMode retains cache-only behavior. The fallback is
// adapted through Reissue so an upstream in-process cache cannot skip REST.
func NewApprovalProviderWithMode(client redis.UniversalClient, fallback ws.ApprovalKeyProvider, mode string, loggers ...*slog.Logger) (*ApprovalProvider, error) {
	if fallback == nil {
		return nil, ErrApprovalConfig
	}
	var logger *slog.Logger
	if len(loggers) > 0 {
		logger = loggers[0]
	}
	return NewApprovalProviderConfig(client, issuerFunc(fallback.Reissue), mode, "live", DefaultRefreshMargin, logger)
}

func NewApprovalProviderConfig(client redis.UniversalClient, issuer ApprovalIssuer, mode, accountMode string, margin time.Duration, logger *slog.Logger) (*ApprovalProvider, error) {
	if client == nil || issuer == nil || (mode != "" && mode != ApprovalModeCacheOnly) || (accountMode != "live" && accountMode != "mock") || margin <= 0 || margin >= ApprovalCacheTTL {
		return nil, ErrApprovalConfig
	}
	return &ApprovalProvider{redis: client, issuer: issuer, logger: logger, mode: mode, accountMode: accountMode, margin: margin, metrics: &ApprovalMetrics{}, now: time.Now, wait: realWait, failureCh: make(chan struct{})}, nil
}

func (p *ApprovalProvider) Metrics() *ApprovalMetrics { return p.metrics }
func (p *ApprovalProvider) cacheKey() string {
	if p.accountMode == "mock" {
		return "kis_mock:websocket:approval_key"
	}
	return approvalCacheKey
}
func (p *ApprovalProvider) lockKey() string { return p.cacheKey() + ":lock" }
func (p *ApprovalProvider) observe(name string, fields ...any) {
	if p.logger != nil {
		p.logger.Info(name, fields...)
	}
}

func (p *ApprovalProvider) cached(ctx context.Context) (string, time.Duration, error) {
	// Atomic value and TTL read prevents pairing an old value with a newly
	// published key's TTL at the refresh boundary.
	result, err := p.redis.Eval(ctx, cachedScript, []string{p.cacheKey()}).Slice()
	if err != nil {
		return "", 0, err
	}
	if len(result) != 2 {
		return "", 0, ErrApprovalUnavailable
	}
	key, _ := result[0].(string)
	ttlMS, ok := result[1].(int64)
	if !ok {
		return "", 0, ErrApprovalUnavailable
	}
	return key, time.Duration(ttlMS) * time.Millisecond, nil
}
func usable(key string, ttl, margin time.Duration) bool {
	return strings.TrimSpace(key) != "" && ttl > margin
}

func (p *ApprovalProvider) ApprovalKey(ctx context.Context) (string, error) {
	key, ttl, err := p.cached(ctx)
	if p.mode == ApprovalModeCacheOnly {
		if err != nil {
			return p.fail(err)
		}
		if strings.TrimSpace(key) == "" {
			p.markFailure(ErrCacheOnlyApprovalUnavailable)
			return "", ErrCacheOnlyApprovalUnavailable
		}
		p.remember(key)
		return key, nil
	}
	if err != nil {
		return p.fail(err)
	}
	if usable(key, ttl, p.margin) {
		p.remember(key)
		return key, nil
	}
	return p.singleFlight(ctx, false, "")
}
func (p *ApprovalProvider) Reissue(ctx context.Context) (string, error) {
	if p.mode == ApprovalModeCacheOnly {
		p.markFailure(ErrCacheOnlyApprovalUnavailable)
		return "", ErrCacheOnlyApprovalUnavailable
	}
	p.mu.Lock()
	rejected := p.lastKey
	p.mu.Unlock()
	return p.singleFlight(ctx, true, rejected)
}
func (p *ApprovalProvider) remember(key string) { p.mu.Lock(); p.lastKey = key; p.mu.Unlock() }
func (p *ApprovalProvider) fail(err error) (string, error) {
	// Third-party errors can contain request URLs or response bodies. Never
	// carry those values into process logs or alerts.
	wrapped := ErrApprovalUnavailable
	if errors.Is(err, ErrApprovalLockLost) {
		wrapped = ErrApprovalLockLost
	}
	p.markFailure(wrapped)
	return "", wrapped
}
func (p *ApprovalProvider) markFailure(err error) {
	p.failureOnce.Do(func() { p.mu.Lock(); p.failureErr = err; p.mu.Unlock(); close(p.failureCh) })
}
func (p *ApprovalProvider) cacheOnlyFailureSignal() <-chan struct{} { return p.failureCh }
func (p *ApprovalProvider) cacheOnlyFailure() error {
	select {
	case <-p.failureCh:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.failureErr
	default:
		return nil
	}
}

const publishScript = `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3]) else return nil end`
const invalidateScript = `if redis.call('GET', KEYS[1]) == ARGV[1] then redis.call('DEL', KEYS[2]); return 1 else return nil end`
const releaseScript = `if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end`

func (p *ApprovalProvider) singleFlight(ctx context.Context, force bool, rejected string) (string, error) {
	tokenBytes := make([]byte, 16)
	if _, err := crand.Read(tokenBytes); err != nil {
		return p.fail(err)
	}
	token := hex.EncodeToString(tokenBytes)
	acquired, err := p.redis.SetNX(ctx, p.lockKey(), token, ApprovalLockTTL).Result()
	if err != nil {
		return p.fail(err)
	}
	if !acquired {
		p.metrics.LockContended.Add(1)
		p.observe("approval_lock_contended", "account_mode", p.accountMode, "count", p.metrics.LockContended.Load())
		deadline := p.now().Add(ApprovalWait)
		for {
			key, ttl, err := p.cached(ctx)
			if err != nil {
				return p.fail(err)
			}
			if usable(key, ttl, p.margin) && (!force || (rejected != "" && key != rejected)) {
				p.metrics.LockWaitSuccess.Add(1)
				p.observe("approval_lock_wait_success", "account_mode", p.accountMode, "count", p.metrics.LockWaitSuccess.Load())
				p.remember(key)
				return key, nil
			}
			left := deadline.Sub(p.now())
			if left <= 0 {
				break
			}
			poll := ApprovalPoll + time.Duration(mrand.Int64N(int64(ApprovalPoll)+1))
			if poll > left {
				poll = left
			}
			if err := p.wait(ctx, poll); err != nil {
				return p.fail(err)
			}
		}
		p.metrics.LockWaitFailed.Add(1)
		p.observe("approval_lock_wait_failed", "account_mode", p.accountMode, "count", p.metrics.LockWaitFailed.Load())
		return p.fail(ErrApprovalUnavailable)
	}
	p.metrics.LockAcquired.Add(1)
	p.observe("approval_lock_acquired", "account_mode", p.accountMode, "count", p.metrics.LockAcquired.Load())
	// The 10-second HTTP request and all Redis writes share a 14-second deadline,
	// leaving a one-second guard before the 15-second lock can expire.
	issueCtx, cancel := context.WithTimeout(ctx, publishDeadline)
	defer cancel()
	defer func() {
		releaseCtx, c := context.WithTimeout(context.Background(), time.Second)
		defer c()
		_ = p.redis.Eval(releaseCtx, releaseScript, []string{p.lockKey()}, token).Err()
	}()
	key, ttl, err := p.cached(issueCtx)
	if err != nil {
		return p.fail(err)
	}
	if usable(key, ttl, p.margin) && (!force || (rejected != "" && key != rejected)) {
		p.remember(key)
		return key, nil
	}
	if force {
		n, err := p.redis.Eval(issueCtx, invalidateScript, []string{p.lockKey(), p.cacheKey()}, token).Int()
		if errors.Is(err, redis.Nil) {
			return p.fail(ErrApprovalLockLost)
		}
		if err != nil {
			return p.fail(err)
		}
		if n == 0 {
			return p.fail(ErrApprovalLockLost)
		}
	}
	httpCtx, httpCancel := context.WithTimeout(issueCtx, ApprovalIssueTimeout)
	defer httpCancel()
	p.metrics.IssueCalls.Add(1)
	p.observe("approval_rest_issue_call", "account_mode", p.accountMode, "count", p.metrics.IssueCalls.Load())
	issued, err := p.issuer.Issue(httpCtx)
	if err != nil {
		return p.fail(err)
	}
	if strings.TrimSpace(issued) == "" {
		return p.fail(ErrApprovalUnavailable)
	}
	if issueCtx.Err() != nil {
		return p.fail(issueCtx.Err())
	}
	result, err := p.redis.Eval(issueCtx, publishScript, []string{p.lockKey(), p.cacheKey()}, token, issued, int(ApprovalCacheTTL.Seconds())).Result()
	if errors.Is(err, redis.Nil) {
		return p.fail(ErrApprovalLockLost)
	}
	if err != nil {
		return p.fail(err)
	}
	if result == nil {
		return p.fail(ErrApprovalLockLost)
	}
	p.remember(issued)
	return issued, nil
}

func realWait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// RefreshLoop refreshes in the background even if no subscribe occurs. Redis
// PTTL is the expiry source, so wall-clock jumps cannot postpone renewal.
func (p *ApprovalProvider) RefreshLoop(ctx context.Context) error {
	if p.mode == ApprovalModeCacheOnly {
		<-ctx.Done()
		return nil
	}
	for {
		key, ttl, err := p.cached(ctx)
		if err != nil {
			_, e := p.fail(err)
			return e
		}
		if !usable(key, ttl, p.margin) {
			if _, err := p.singleFlight(ctx, false, ""); err != nil {
				return err
			}
			continue
		}
		delay := ttl - p.margin
		if delay > time.Minute {
			delay = time.Minute
		}
		if err := p.wait(ctx, delay); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			_, e := p.fail(err)
			return e
		}
	}
}
