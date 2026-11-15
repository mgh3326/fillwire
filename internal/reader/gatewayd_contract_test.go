package reader

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/fillwire/internal/testutil"
	"github.com/redis/go-redis/v9"
)

// fakeEnsurer records ensure calls and can publish the shared cache key the
// way gatewayd would after acknowledging.
type fakeEnsurer struct {
	mu        sync.Mutex
	providers []string
	forces    []bool
	err       error
	onEnsure  func()
}

func (f *fakeEnsurer) Ensure(_ context.Context, provider string, force bool) error {
	f.mu.Lock()
	f.providers = append(f.providers, provider)
	f.forces = append(f.forces, force)
	f.mu.Unlock()
	if f.onEnsure != nil {
		f.onEnsure()
	}
	return f.err
}

func (f *fakeEnsurer) calls() (providers []string, forces []bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.providers...), append([]bool(nil), f.forces...)
}

func newGatewaydFixture(t *testing.T, accountMode string, ensurer GatewaydEnsurer) (*miniredis.Miniredis, *redis.Client, *ApprovalProvider) {
	t.Helper()
	mini, client := testutil.Redis(t)
	p, err := NewApprovalProviderGatewayd(client, ensurer, accountMode, DefaultRefreshMargin, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return mini, client, p
}

func TestGatewaydModeRejectsIssuerConstructors(t *testing.T) {
	_, client := testutil.Redis(t)
	issuer := issuerFunc(func(context.Context) (string, error) { return "issued", nil })
	ensurer := &fakeEnsurer{}
	if _, err := NewApprovalProviderConfig(client, issuer, ApprovalModeGatewayd, "live", DefaultRefreshMargin, nil); !errors.Is(err, ErrApprovalConfig) {
		t.Fatalf("config constructor accepted gatewayd mode with an issuer: %v", err)
	}
	if _, err := NewApprovalProviderGatewayd(client, nil, "live", DefaultRefreshMargin, nil); !errors.Is(err, ErrApprovalConfig) {
		t.Fatalf("gatewayd constructor accepted a nil ensurer: %v", err)
	}
	if _, err := newApprovalProvider(client, issuer, ensurer, ApprovalModeGatewayd, "live", DefaultRefreshMargin, nil); !errors.Is(err, ErrApprovalConfig) {
		t.Fatalf("gatewayd provider accepted an issuer: %v", err)
	}
	if _, err := newApprovalProvider(client, nil, ensurer, "", "live", DefaultRefreshMargin, nil); !errors.Is(err, ErrApprovalConfig) {
		t.Fatalf("self mode accepted a nil issuer: %v", err)
	}
	if _, err := newApprovalProvider(client, issuer, nil, "", "live", DefaultRefreshMargin, nil); err != nil {
		t.Fatalf("self mode rejected a valid issuer: %v", err)
	}
}

func TestGatewaydCacheHitNeverEnsuresOrLocks(t *testing.T) {
	for _, tc := range []struct{ mode, key, lock string }{
		{"live", "kis:websocket:approval_key", "kis:websocket:approval_key:lock"},
		{"mock", "kis_mock:websocket:approval_key", "kis_mock:websocket:approval_key:lock"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			ensurer := &fakeEnsurer{}
			mini, _, p := newGatewaydFixture(t, tc.mode, ensurer)
			mini.Set(tc.key, "cached-gatewayd-key")
			mini.SetTTL(tc.key, ApprovalCacheTTL)
			key, err := p.ApprovalKey(context.Background())
			if err != nil || key != "cached-gatewayd-key" {
				t.Fatalf("gatewayd cache hit = %q, %v", key, err)
			}
			providers, _ := ensurer.calls()
			if len(providers) != 0 {
				t.Fatalf("cache hit called ensure %d times", len(providers))
			}
			if mini.Exists(tc.lock) {
				t.Fatal("gatewayd mode created the issuance lock")
			}
			if p.Metrics().IssueCalls.Load() != 0 || p.Metrics().LockAcquired.Load() != 0 || p.Metrics().EnsureCalls.Load() != 0 {
				t.Fatalf("metrics issue=%d lock=%d ensure=%d, want all zero", p.Metrics().IssueCalls.Load(), p.Metrics().LockAcquired.Load(), p.Metrics().EnsureCalls.Load())
			}
		})
	}
}

func TestGatewaydCacheMissEnsuresThenReadsRedis(t *testing.T) {
	for _, tc := range []struct {
		mode     string
		key      string
		lock     string
		provider string
	}{
		{"live", "kis:websocket:approval_key", "kis:websocket:approval_key:lock", "kis-live-approval"},
		{"mock", "kis_mock:websocket:approval_key", "kis_mock:websocket:approval_key:lock", "kis-mock-approval"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			mini, client := testutil.Redis(t)
			ensurer := &fakeEnsurer{onEnsure: func() {
				// gatewayd publishes the shared key after acknowledging.
				mini.Set(tc.key, "gatewayd-published")
				mini.SetTTL(tc.key, ApprovalCacheTTL)
			}}
			p, err := NewApprovalProviderGatewayd(client, ensurer, tc.mode, DefaultRefreshMargin, nil)
			if err != nil {
				t.Fatal(err)
			}
			key, err := p.ApprovalKey(context.Background())
			if err != nil || key != "gatewayd-published" {
				t.Fatalf("gatewayd miss = %q, %v", key, err)
			}
			providers, forces := ensurer.calls()
			if len(providers) != 1 || providers[0] != tc.provider || forces[0] {
				t.Fatalf("ensure calls = %v forces=%v, want one %s non-force", providers, forces, tc.provider)
			}
			if mini.Exists(tc.lock) {
				t.Fatal("gatewayd mode created the issuance lock")
			}
			if p.Metrics().IssueCalls.Load() != 0 || p.Metrics().LockAcquired.Load() != 0 {
				t.Fatalf("issuer/lock metrics nonzero: issue=%d lock=%d", p.Metrics().IssueCalls.Load(), p.Metrics().LockAcquired.Load())
			}
			if value, _ := mini.Get(tc.key); value != "gatewayd-published" {
				t.Fatalf("cache = %q, want gatewayd-published", value)
			}
		})
	}
}

func TestGatewaydEnsureAckWithoutPublishIsBoundedTypedError(t *testing.T) {
	ensurer := &fakeEnsurer{} // acknowledges but never publishes
	_, _, p := newGatewaydFixture(t, "live", ensurer)
	now := time.Now()
	p.now = func() time.Time { return now }
	var waits []time.Duration
	p.wait = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		now = now.Add(d)
		return nil
	}
	key, err := p.ApprovalKey(context.Background())
	if key != "" || !errors.Is(err, ErrGatewaydApprovalUnavailable) {
		t.Fatalf("unpublished ensure = %q, %v, want ErrGatewaydApprovalUnavailable", key, err)
	}
	providers, _ := ensurer.calls()
	if len(providers) != 1 {
		t.Fatalf("ensure calls = %d, want 1", len(providers))
	}
	// 3s deadline at 50ms polls: the loop must stop near the bound, not hang.
	wantPolls := int(GatewaydPollTimeout / GatewaydPollInterval)
	if len(waits) < wantPolls-2 || len(waits) > wantPolls+2 {
		t.Fatalf("poll waits = %d, want about %d bounded polls", len(waits), wantPolls)
	}
	for _, d := range waits {
		if d != GatewaydPollInterval {
			t.Fatalf("poll interval = %s, want %s", d, GatewaydPollInterval)
		}
	}
	if p.Metrics().IssueCalls.Load() != 0 || p.Metrics().LockAcquired.Load() != 0 {
		t.Fatal("gatewayd mode issued or locked locally")
	}
	if ProcessExitCode(err) != 1 {
		t.Fatalf("exit code = %d, want transient 1", ProcessExitCode(err))
	}
}

func TestGatewaydForeignLockDoesNotBlock(t *testing.T) {
	mini, client := testutil.Redis(t)
	// A foreign issuance lock must be irrelevant in gatewayd mode.
	mini.Set("kis:websocket:approval_key:lock", "other-owner")
	mini.SetTTL("kis:websocket:approval_key:lock", ApprovalLockTTL)
	ensurer := &fakeEnsurer{onEnsure: func() {
		mini.Set("kis:websocket:approval_key", "despite-lock")
		mini.SetTTL("kis:websocket:approval_key", ApprovalCacheTTL)
	}}
	p, err := NewApprovalProviderGatewayd(client, ensurer, "live", DefaultRefreshMargin, nil)
	if err != nil {
		t.Fatal(err)
	}
	key, err := p.ApprovalKey(context.Background())
	if err != nil || key != "despite-lock" {
		t.Fatalf("foreign lock blocked gatewayd mode: %q, %v", key, err)
	}
	if lock, _ := mini.Get("kis:websocket:approval_key:lock"); lock != "other-owner" {
		t.Fatalf("gatewayd mode disturbed foreign lock: %q", lock)
	}
}

func TestGatewaydEnsureFailureIsTypedAndSanitized(t *testing.T) {
	ensurer := &fakeEnsurer{err: errors.New("dial tcp 127.0.0.1:8791: connection refused with secret-body")}
	_, _, p := newGatewaydFixture(t, "live", ensurer)
	key, err := p.ApprovalKey(context.Background())
	if key != "" || !errors.Is(err, ErrApprovalUnavailable) {
		t.Fatalf("ensure transport failure = %q, %v", key, err)
	}
	if errors.Is(err, ErrGatewaydApprovalUnavailable) {
		t.Fatal("transport failure mislabeled as unpublished ack")
	}
	if p.cacheOnlyFailure() == nil {
		t.Fatal("terminal failure not latched")
	}
}

func TestGatewaydFailedEnsureReusesStillLiveKey(t *testing.T) {
	ensurer := &fakeEnsurer{err: errors.New("gatewayd down")}
	mini, client := testutil.Redis(t)
	mini.Set("kis:websocket:approval_key", "still-live-inside-margin")
	mini.SetTTL("kis:websocket:approval_key", 30*time.Minute)
	p, err := NewApprovalProviderGatewayd(client, ensurer, "live", DefaultRefreshMargin, nil)
	if err != nil {
		t.Fatal(err)
	}
	key, err := p.ApprovalKey(context.Background())
	if err != nil || key != "still-live-inside-margin" {
		t.Fatalf("failed ensure with live key = %q, %v", key, err)
	}
	if p.cacheOnlyFailure() != nil {
		t.Fatal("recoverable ensure failure latched terminal")
	}
}

func TestGatewaydForcedReissueNeverReturnsRejectedKey(t *testing.T) {
	mini, client := testutil.Redis(t)
	mini.Set("kis:websocket:approval_key", "rejected-key")
	mini.SetTTL("kis:websocket:approval_key", ApprovalCacheTTL)
	var waits int
	ensurer := &fakeEnsurer{onEnsure: func() {
		// gatewayd republishes the rejected value first; only a later poll
		// sees the replacement.
	}}
	p, err := NewApprovalProviderGatewayd(client, ensurer, "live", DefaultRefreshMargin, nil)
	if err != nil {
		t.Fatal(err)
	}
	p.wait = func(_ context.Context, d time.Duration) error {
		waits++
		if waits == 3 {
			mini.Set("kis:websocket:approval_key", "forced-replacement")
			mini.SetTTL("kis:websocket:approval_key", ApprovalCacheTTL)
		}
		return nil
	}
	key, err := p.Reissue(context.Background())
	if err != nil || key != "forced-replacement" {
		t.Fatalf("forced reissue = %q, %v, want replacement not rejected", key, err)
	}
	providers, forces := ensurer.calls()
	if len(providers) != 1 || providers[0] != "kis-live-approval" || !forces[0] {
		t.Fatalf("reissue ensure = %v forces=%v, want one forced kis-live-approval", providers, forces)
	}
	if waits < 3 {
		t.Fatalf("reissue returned before replacement published: waits=%d", waits)
	}
	if mini.Exists("kis:websocket:approval_key:lock") {
		t.Fatal("forced reissue created the issuance lock")
	}
}

func TestGatewaydForcedReissueAckWithoutReplacementIsBoundedError(t *testing.T) {
	mini, client := testutil.Redis(t)
	mini.Set("kis_mock:websocket:approval_key", "rejected-mock")
	mini.SetTTL("kis_mock:websocket:approval_key", ApprovalCacheTTL)
	ensurer := &fakeEnsurer{} // ack only; the rejected key stays cached
	p, err := NewApprovalProviderGatewayd(client, ensurer, "mock", DefaultRefreshMargin, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	p.now = func() time.Time { return now }
	p.wait = func(_ context.Context, d time.Duration) error {
		now = now.Add(d)
		return nil
	}
	key, err := p.Reissue(context.Background())
	if key != "" || !errors.Is(err, ErrGatewaydApprovalUnavailable) {
		t.Fatalf("rejected key still cached = %q, %v, want bounded typed error", key, err)
	}
	providers, forces := ensurer.calls()
	if len(providers) != 1 || providers[0] != "kis-mock-approval" || !forces[0] {
		t.Fatalf("mock reissue ensure = %v forces=%v, want one forced kis-mock-approval", providers, forces)
	}
}

func TestGatewaydRefreshLoopIsInert(t *testing.T) {
	ensurer := &fakeEnsurer{}
	_, _, p := newGatewaydFixture(t, "live", ensurer)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.RefreshLoop(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("gatewayd refresh loop = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gatewayd refresh loop did not stop")
	}
	providers, _ := ensurer.calls()
	if len(providers) != 0 {
		t.Fatal("refresh loop called ensure")
	}
}

func TestGatewaydEnsureHTTPContract(t *testing.T) {
	var sawPath, sawQuery, sawAuth, sawMethod string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawMethod = r.Method
		sawPath = r.URL.Path
		sawQuery = r.URL.RawQuery
		sawAuth = r.Header.Get("X-Gatewayd-Token")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	ensurer, err := NewGatewaydClient(server.URL+"/", "X-Gatewayd-Token", "synthetic-gatewayd-token")
	if err != nil {
		t.Fatal(err)
	}
	if err := ensurer.Ensure(context.Background(), "kis-live-approval", true); err != nil {
		t.Fatal(err)
	}
	if sawMethod != http.MethodPost || sawPath != "/v1/tokens/kis-live-approval/ensure" || sawQuery != "force=true" {
		t.Fatalf("ensure request = %s %s?%s", sawMethod, sawPath, sawQuery)
	}
	if sawAuth != "synthetic-gatewayd-token" {
		t.Fatal("auth header not sent")
	}
}

func TestGatewaydEnsureHTTPFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
	}{
		{"unauthorized", http.StatusUnauthorized},
		{"server error", http.StatusServiceUnavailable},
		{"redirect not followed", http.StatusFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.status == http.StatusFound {
					w.Header().Set("Location", "/elsewhere")
				}
				w.WriteHeader(tc.status)
			}))
			defer server.Close()
			ensurer, err := NewGatewaydClient(server.URL, "", "")
			if err != nil {
				t.Fatal(err)
			}
			if err := ensurer.Ensure(context.Background(), "kis-mock-approval", false); err == nil {
				t.Fatalf("status %d accepted as ensure ack", tc.status)
			}
		})
	}
	// No auth header is sent when no token is configured.
	var sawAuth bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, sawAuth = r.Header["Authorization"]
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	ensurer, err := NewGatewaydClient(server.URL, "Authorization", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := ensurer.Ensure(context.Background(), "kis-live-approval", false); err != nil {
		t.Fatal(err)
	}
	if sawAuth {
		t.Fatal("empty token still sent an Authorization header")
	}
}

func TestGatewaydURLValidation(t *testing.T) {
	for _, raw := range []string{
		"http://127.0.0.1:8791",
		"https://gatewayd.tailnet.example:8791/base",
		"http://127.0.0.1:8791/",
	} {
		if normalized, err := NormalizeGatewaydURL(raw); err != nil || normalized == "" {
			t.Fatalf("NormalizeGatewaydURL(%q) = %q, %v", raw, normalized, err)
		}
	}
	for _, raw := range []string{
		"",
		"ftp://127.0.0.1:8791",
		"http://user:pass@127.0.0.1:8791",
		"http://127.0.0.1:8791?x=1",
		"http://127.0.0.1:8791#frag",
		"127.0.0.1:8791",
	} {
		if _, err := NormalizeGatewaydURL(raw); err == nil {
			t.Fatalf("NormalizeGatewaydURL(%q) accepted", raw)
		}
		if _, err := NewGatewaydClient(raw, "", ""); err == nil {
			t.Fatalf("NewGatewaydClient(%q) accepted", raw)
		}
	}
}
