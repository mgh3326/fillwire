package gatewayd

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

const approvalTTL = 23 * time.Hour
const approvalLockTTL = 15 * time.Second

func isApprovalProvider(p TokenProvider) bool {
	return p == ProviderKISLiveApproval || p == ProviderKISMockApproval
}

type approvalStore interface {
	RedisStore
	// TTL returns the key's remaining lifetime; negative when absent, so one
	// PTTL answers presence and freshness atomically.
	TTL(ctx context.Context, key string) (time.Duration, error)
	// Held reports whether key exists, used to observe the issuance lock.
	Held(ctx context.Context, key string) (bool, error)
}

type approvalService struct {
	store             approvalStore
	config            ProviderConfig
	issuer            OAuthIssuer
	cacheKey, lockKey string
}

func newApprovalService(store approvalStore, config ProviderConfig, timeout time.Duration) (*approvalService, error) {
	if store == nil || !isApprovalProvider(config.Provider) || !validProviderConfig(config) {
		return nil, errEnsureUnavailable
	}
	prefix := "kis:websocket:approval_key"
	if config.Provider == ProviderKISMockApproval {
		prefix = "kis_mock:websocket:approval_key"
	}
	return &approvalService{store: store, config: config, issuer: approvalIssuer{config: config, timeout: timeout}, cacheKey: prefix, lockKey: prefix + ":lock"}, nil
}

func (s *approvalService) cachedTTL(ctx context.Context) (time.Duration, error) {
	ttl, err := s.store.TTL(ctx, s.cacheKey)
	if err != nil {
		return 0, errEnsureUnavailable
	}
	return ttl, nil
}
func (s *approvalService) Ensure(ctx context.Context) (EnsureState, error) {
	return s.ensure(ctx, false)
}
func (s *approvalService) EnsureForce(ctx context.Context) (EnsureState, error) {
	return s.ensure(ctx, true)
}
func (s *approvalService) ensure(ctx context.Context, force bool) (EnsureState, error) {
	ttl0, err := s.cachedTTL(ctx)
	if err != nil {
		return "", err
	}
	if ttl0 > tokenExpiryBuffer && !force {
		return EnsureStateFresh, nil
	}
	lock, err := newLockValue()
	if err != nil {
		return "", errEnsureUnavailable
	}
	acquired, err := s.store.SetNX(ctx, s.lockKey, lock, approvalLockTTL)
	if err != nil {
		return "", errEnsureUnavailable
	}
	// Publish evidence is a TTL increase over the previously observed sample:
	// a republish SET resets the expiry to ~approvalTTL, while a record that
	// only decays never moves upward.
	prevTTL := ttl0
	if !acquired {
		// The wait outlives approvalLockTTL so a lock whose holder died before
		// releasing it expires before the waiter gives up.
		deadline := time.NewTimer(approvalLockTTL + 5*time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			held, err := s.store.Held(ctx, s.lockKey)
			if err != nil {
				return "", errEnsureUnavailable
			}
			ttl, err := s.cachedTTL(ctx)
			if err != nil {
				return "", err
			}
			if ttl > tokenExpiryBuffer && (!force || ttl > prevTTL) {
				return EnsureStateFresh, nil
			}
			if !held {
				// The holder is gone without evidence of a publish, so it
				// failed before replacing the key. Take over issuance instead
				// of accepting the previous record as a forced refresh.
				reacquired, err := s.store.SetNX(ctx, s.lockKey, lock, approvalLockTTL)
				if err != nil {
					return "", errEnsureUnavailable
				}
				if reacquired {
					break
				}
			}
			prevTTL = ttl
			select {
			case <-ctx.Done():
				return "", errEnsureUnavailable
			case <-deadline.C:
				// A fresh record at the deadline means a refresh completed
				// with a lingering lock; the published key is valid, so the
				// wait succeeded.
				ttl, err := s.cachedTTL(ctx)
				if err != nil {
					return "", err
				}
				if ttl > tokenExpiryBuffer {
					return EnsureStateFresh, nil
				}
				return "", errLockWaitTimeout
			case <-ticker.C:
			}
		}
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), redisDialTimeout)
		defer cancel()
		_ = s.store.CompareAndDelete(c, s.lockKey, lock)
	}()
	// A publish may have landed between the last Held() observation and our
	// SetNX (or between the first sample and a direct acquire); skip issuance
	// when the TTL moved up in that gap.
	ttl, err := s.cachedTTL(ctx)
	if err != nil {
		return "", err
	}
	if ttl > tokenExpiryBuffer && (!force || ttl > prevTTL) {
		return EnsureStateFresh, nil
	}
	issued, err := s.issuer.Issue(ctx, s.config.AppKey, s.config.AppSecret)
	if err != nil || strings.TrimSpace(issued.AccessToken) == "" {
		return "", errEnsureUnavailable
	}
	if err := s.store.Set(ctx, s.cacheKey, issued.AccessToken, approvalTTL); err != nil {
		return "", errEnsureUnavailable
	}
	return EnsureStateIssued, nil
}

type approvalIssuer struct {
	config    ProviderConfig
	timeout   time.Duration
	transport http.RoundTripper
}

func (a approvalIssuer) Issue(ctx context.Context, key, secret string) (OAuthToken, error) {
	body, err := json.Marshal(struct {
		Grant  string `json:"grant_type"`
		Key    string `json:"appkey"`
		Secret string `json:"secretkey"`
	}{"client_credentials", key, secret})
	if err != nil {
		return OAuthToken{}, errOAuthFailed
	}
	req, err := newPinnedOAuthRequest(ctx, a.config.Provider, a.config.BaseURL, bytes.NewReader(body))
	if err != nil {
		return OAuthToken{}, errOAuthFailed
	}
	req.Header.Set("content-type", "application/json")
	resp, err := newPinnedOAuthClient(a.transport, a.timeout, a.config.Provider).Do(req)
	if err != nil || resp == nil {
		return OAuthToken{}, errOAuthFailed
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return OAuthToken{}, errOAuthFailed
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, oauthResponseLimit+1))
	if err != nil || len(data) > oauthResponseLimit {
		return OAuthToken{}, errOAuthFailed
	}
	var result struct {
		Key string `json:"approval_key"`
	}
	if json.Unmarshal(data, &result) != nil || strings.TrimSpace(result.Key) == "" {
		return OAuthToken{}, errOAuthFailed
	}
	return OAuthToken{AccessToken: result.Key}, nil
}
