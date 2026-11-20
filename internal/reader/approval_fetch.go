package reader

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mgh3326/fillwire/internal/tokenfetch"
)

// ApprovalModeGatewaydFetch asks gatewayd for the approval key itself
// (POST /v1/tokens/{provider}/fetch) and keeps it in memory. It reads no
// Redis key, takes no lock and never issues locally.
const ApprovalModeGatewaydFetch = "gatewayd-fetch"

// FetchApprovalProvider is the gatewayd-fetch consumer. The key lives in
// memory; gatewayd is asked again only when less than the refresh margin
// remains (on the next connect, as in gatewayd mode) or when KIS rejected the
// key, in which case the rejected key's hash travels with the request. A
// refresh failure while the held key still has life keeps the stream, and a
// failure with no live key is terminal. No failure falls back to issuing.
//
// A non-forced fetch inside the margin may return the same key: gatewayd
// reissues only under 60 s of life. That answer is adopted as success.
type FetchApprovalProvider struct {
	holder      *tokenfetch.Holder
	logger      *slog.Logger
	accountMode string
	metrics     *ApprovalMetrics

	mu          sync.Mutex
	lastKey     string
	onTransient func()
	failureOnce sync.Once
	failureCh   chan struct{}
	failureErr  error
}

// NewApprovalProviderGatewaydFetch builds the consumer for the account
// mode's approval provider (kis-live-approval or kis-mock-approval).
func NewApprovalProviderGatewaydFetch(fetcher tokenfetch.Fetcher, accountMode string, margin time.Duration, logger *slog.Logger) (*FetchApprovalProvider, error) {
	return newFetchApprovalProvider(fetcher, accountMode, margin, logger, time.Now)
}

func newFetchApprovalProvider(fetcher tokenfetch.Fetcher, accountMode string, margin time.Duration, logger *slog.Logger, now func() time.Time) (*FetchApprovalProvider, error) {
	if fetcher == nil || (accountMode != "live" && accountMode != "mock") || margin <= 0 || margin > MaxRefreshMargin {
		return nil, ErrApprovalConfig
	}
	provider := "kis-live-approval"
	if accountMode == "mock" {
		provider = "kis-mock-approval"
	}
	p := &FetchApprovalProvider{logger: logger, accountMode: accountMode, metrics: &ApprovalMetrics{}, failureCh: make(chan struct{})}
	p.holder = tokenfetch.NewHolder(observedFetcher{inner: fetcher, p: p}, provider, margin, now)
	return p, nil
}

// observedFetcher counts and logs each gatewayd call. It sees the hash of a
// rejected key, never a key.
type observedFetcher struct {
	inner tokenfetch.Fetcher
	p     *FetchApprovalProvider
}

func (o observedFetcher) Fetch(ctx context.Context, provider, failed string) (tokenfetch.Token, error) {
	n := o.p.metrics.FetchCalls.Add(1)
	if o.p.logger != nil {
		o.p.logger.Info("approval_gatewayd_fetch", "account_mode", o.p.accountMode, "force", failed != "", "count", n)
	}
	return o.inner.Fetch(ctx, provider, failed)
}

func (p *FetchApprovalProvider) Metrics() *ApprovalMetrics { return p.metrics }

// SetTransientAlert reports a refresh failure that the held key survived.
func (p *FetchApprovalProvider) SetTransientAlert(fn func()) { p.onTransient = fn }

func (p *FetchApprovalProvider) ApprovalKey(ctx context.Context) (string, error) {
	token, degraded, err := p.holder.Current(ctx)
	if err != nil {
		return p.terminal(ctx, err)
	}
	if degraded {
		if p.logger != nil {
			p.logger.Info("approval_refresh_retry", "account_mode", p.accountMode)
		}
		if p.onTransient != nil {
			p.onTransient()
		}
	}
	return p.present(token), nil
}

// Reissue is called after KIS rejected the key this process last presented.
func (p *FetchApprovalProvider) Reissue(ctx context.Context) (string, error) {
	p.mu.Lock()
	rejected := p.lastKey
	p.mu.Unlock()
	token, err := p.holder.Reissue(ctx, rejected)
	if err != nil {
		return p.terminal(ctx, err)
	}
	return p.present(token), nil
}

func (p *FetchApprovalProvider) present(token tokenfetch.Token) string {
	p.mu.Lock()
	p.lastKey = token.Value()
	p.mu.Unlock()
	return token.Value()
}

// terminal records a failure no live key survived. The wrapped error is a
// tokenfetch sentinel: it names the status class and never a key, URL or body.
func (p *FetchApprovalProvider) terminal(ctx context.Context, err error) (string, error) {
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	wrapped := fmt.Errorf("reader: approval fetch: %w", err)
	p.failureOnce.Do(func() {
		p.mu.Lock()
		p.failureErr = wrapped
		p.mu.Unlock()
		close(p.failureCh)
	})
	return "", wrapped
}

func (p *FetchApprovalProvider) cacheOnlyFailureSignal() <-chan struct{} { return p.failureCh }
func (p *FetchApprovalProvider) cacheOnlyFailure() error {
	select {
	case <-p.failureCh:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.failureErr
	default:
		return nil
	}
}
