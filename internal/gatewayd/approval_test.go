package gatewayd

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestApprovalProvidersAndGolden(t *testing.T) {
	var fixture struct {
		Expected map[string]string `json:"expected"`
	}
	if err := json.NewDecoder(strings.NewReader(approvalGoldenFixture)).Decode(&fixture); err != nil {
		t.Fatal(err)
	}
	opts, err := ParseRunOptions([]string{"--providers", "kis-live-approval,kis-mock-approval"})
	if err != nil || len(opts.Providers) != 2 {
		t.Fatalf("options: %#v %v", opts, err)
	}
	defaults, err := ParseRunOptions(nil)
	if err != nil || len(defaults.Providers) != 1 || defaults.Providers[0] != ProviderKISMock {
		t.Fatal("default changed")
	}
	for _, tc := range []struct {
		p    TokenProvider
		mode string
	}{{ProviderKISLiveApproval, "live"}, {ProviderKISMockApproval, "mock"}} {
		s, err := newApprovalService(newFakeRedisStore(testGatewayNow), testProviderConfig(tc.p), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if s.cacheKey != fixture.Expected["kis_websocket_"+tc.mode+"_approval_key"] || s.lockKey != fixture.Expected["kis_websocket_"+tc.mode+"_lock"] {
			t.Fatal("golden key mismatch")
		}
	}
}

func TestApprovalEnsureAndForce(t *testing.T) {
	for _, p := range []TokenProvider{ProviderKISLiveApproval, ProviderKISMockApproval} {
		t.Run(string(p), func(t *testing.T) {
			ctx := context.Background()
			now := testGatewayNow()
			store := newFakeRedisStore(func() time.Time { return now })
			s, err := newApprovalService(store, testProviderConfig(p), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			issuer := &fakeOAuthIssuer{token: OAuthToken{AccessToken: "approval-value"}}
			s.issuer = issuer
			state, err := s.Ensure(ctx)
			if err != nil || state != EnsureStateIssued {
				t.Fatalf("miss: %s %v", state, err)
			}
			records := store.savedRecords()
			if len(records) != 1 || records[0].key != s.cacheKey || records[0].value != "approval-value" || records[0].ttl != 23*time.Hour || !store.savedWhileLocked()[0] {
				t.Fatal("incorrect publication")
			}
			state, err = s.Ensure(ctx)
			if err != nil || state != EnsureStateFresh || issuer.callCount() != 1 {
				t.Fatal("fresh key reissued")
			}
			now = now.Add(23*time.Hour - tokenExpiryBuffer)
			state, err = s.Ensure(ctx)
			if err != nil || state != EnsureStateIssued || issuer.callCount() != 2 {
				t.Fatal("buffer not refreshed")
			}
			issuer.token.AccessToken = "replacement"
			var logs bytes.Buffer
			handler := NewHandler(ProviderEnsurers{p: s}, testLogger(&logs))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/tokens/"+string(p)+"/ensure?force=true", nil))
			value, present, _ := store.Get(ctx, s.cacheKey)
			if rec.Code != 200 || !present || value != "replacement" || issuer.callCount() != 3 {
				t.Fatal("force failed")
			}
			for _, secret := range []string{"approval-value", "replacement", s.config.AppKey, s.config.AppSecret} {
				if strings.Contains(logs.String(), secret) || strings.Contains(rec.Body.String(), secret) {
					t.Fatal("secret leaked")
				}
			}
			if len(store.savedRecords()) != 3 || !store.savedWhileLocked()[2] {
				t.Fatal("replacement was not a single locked SET")
			}
		})
	}
}

func TestApprovalConcurrentAndHTTP(t *testing.T) {
	store := newFakeRedisStore(testGatewayNow)
	store.setNXHit = make(chan struct{})
	s, _ := newApprovalService(store, testProviderConfig(ProviderKISLiveApproval), time.Second)
	started, release := make(chan struct{}, 1), make(chan struct{})
	issuer := &fakeOAuthIssuer{token: OAuthToken{AccessToken: "shared"}, started: started, release: release}
	s.issuer = issuer
	done := make(chan error, 2)
	go func() { _, err := s.Ensure(context.Background()); done <- err }()
	<-started
	go func() { _, err := s.Ensure(context.Background()); done <- err }()
	<-store.setNXHit
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if issuer.callCount() != 1 || len(store.savedRecords()) != 1 {
		t.Fatal("concurrent issuance")
	}
	for _, p := range []TokenProvider{ProviderKISLiveApproval, ProviderKISMockApproval} {
		config := testProviderConfig(p)
		issuer := approvalIssuer{config: config, timeout: time.Second, transport: approvalRoundTrip(func(r *http.Request) (*http.Response, error) {
			if r.URL.String() != config.BaseURL+"/oauth2/Approval" {
				t.Error("wrong endpoint")
			}
			var body map[string]string
			if json.NewDecoder(r.Body).Decode(&body) != nil || body["grant_type"] != "client_credentials" || body["appkey"] != config.AppKey || body["secretkey"] != config.AppSecret {
				t.Error("wrong body")
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"approval_key":"http-key"}`)), Header: make(http.Header)}, nil
		})}
		token, err := issuer.Issue(context.Background(), config.AppKey, config.AppSecret)
		if err != nil || token.AccessToken != "http-key" {
			t.Fatal("approval HTTP failed")
		}
	}
}

func TestApprovalForceWaitSameValue(t *testing.T) {
	store := newFakeRedisStore(testGatewayNow)
	s, _ := newApprovalService(store, testProviderConfig(ProviderKISLiveApproval), time.Second)
	store.save(s.cacheKey, "same-key", approvalTTL)
	// Another issuer (gatewayd or the Python manager) holds the lock and is
	// about to re-store the identical still-valid approval key. The SET
	// pushes the expiry out, which is the waiter's evidence a publish
	// happened.
	if _, err := store.SetNX(context.Background(), s.lockKey, "other-holder", approvalLockTTL); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		store.save(s.cacheKey, "same-key", approvalTTL+time.Second)
		_ = store.CompareAndDelete(context.Background(), s.lockKey, "other-holder")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state, err := s.EnsureForce(ctx)
	if err != nil || state != EnsureStateFresh {
		t.Fatalf("force wait = %s, %v", state, err)
	}
}

func TestApprovalForceWaitFailedHolder(t *testing.T) {
	store := newFakeRedisStore(testGatewayNow)
	s, _ := newApprovalService(store, testProviderConfig(ProviderKISLiveApproval), time.Second)
	store.save(s.cacheKey, "old-key", approvalTTL)
	// The holder fails before publishing, then releases the lock. The waiter
	// must not accept the old record as a forced refresh; it takes over and
	// issues a replacement itself.
	if _, err := store.SetNX(context.Background(), s.lockKey, "failed-holder", approvalLockTTL); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = store.CompareAndDelete(context.Background(), s.lockKey, "failed-holder")
	}()
	issuer := &fakeOAuthIssuer{token: OAuthToken{AccessToken: "self-issued"}}
	s.issuer = issuer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state, err := s.EnsureForce(ctx)
	if err != nil || state != EnsureStateIssued || issuer.callCount() != 1 {
		t.Fatalf("force wait = %s, %v, calls %d", state, err, issuer.callCount())
	}
}

func TestApprovalForceWaitDeadHolder(t *testing.T) {
	var mu sync.Mutex
	now := testGatewayNow()
	store := newFakeRedisStore(func() time.Time { mu.Lock(); defer mu.Unlock(); return now })
	s, _ := newApprovalService(store, testProviderConfig(ProviderKISLiveApproval), time.Second)
	// The holder died holding the lock before any publish could be observed;
	// the lock only disappears when its TTL expires, then the waiter takes
	// over issuance.
	store.save(s.cacheKey, "old-key", approvalTTL)
	if _, err := store.SetNX(context.Background(), s.lockKey, "dead-holder", approvalLockTTL); err != nil {
		t.Fatal(err)
	}
	issuer := &fakeOAuthIssuer{token: OAuthToken{AccessToken: "self-issued"}}
	s.issuer = issuer
	go func() {
		time.Sleep(300 * time.Millisecond)
		mu.Lock()
		now = now.Add(approvalLockTTL + time.Second)
		mu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state, err := s.EnsureForce(ctx)
	if err != nil || state != EnsureStateIssued || issuer.callCount() != 1 {
		t.Fatalf("force wait = %s, %v, calls %d", state, err, issuer.callCount())
	}
}

func TestApprovalForceWaitGapPublish(t *testing.T) {
	store := newFakeRedisStore(testGatewayNow)
	s, _ := newApprovalService(store, testProviderConfig(ProviderKISLiveApproval), time.Second)
	store.save(s.cacheKey, "old-key", approvalTTL)
	if _, err := store.SetNX(context.Background(), s.lockKey, "failed-holder", approvalLockTTL); err != nil {
		t.Fatal(err)
	}
	// On the takeover reacquire (the third SetNX), a co-tenant publish lands
	// in the Held→SetNX gap; the post-acquire TTL recheck must observe it and
	// skip issuance instead of overwriting the fresh record.
	store.setNXHook = func() {
		if store.setNXCalls == 2 {
			store.save(s.cacheKey, "gap-published", approvalTTL+time.Second)
		}
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = store.CompareAndDelete(context.Background(), s.lockKey, "failed-holder")
	}()
	issuer := &fakeOAuthIssuer{token: OAuthToken{AccessToken: "unneeded"}}
	s.issuer = issuer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state, err := s.EnsureForce(ctx)
	if err != nil || state != EnsureStateFresh || issuer.callCount() != 0 {
		t.Fatalf("force wait = %s, %v, calls %d", state, err, issuer.callCount())
	}
}

type approvalRoundTrip func(*http.Request) (*http.Response, error)

func (f approvalRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestApprovalForceKeepsOldValueDuringIssue(t *testing.T) {
	store := newFakeRedisStore(testGatewayNow)
	s, _ := newApprovalService(store, testProviderConfig(ProviderKISMockApproval), time.Second)
	store.save(s.cacheKey, "old", approvalTTL)
	started, release := make(chan struct{}, 1), make(chan struct{})
	s.issuer = &fakeOAuthIssuer{token: OAuthToken{AccessToken: "new"}, started: started, release: release}
	done := make(chan error, 1)
	go func() { _, err := s.EnsureForce(context.Background()); done <- err }()
	<-started
	value, present, err := store.Get(context.Background(), s.cacheKey)
	if err != nil || !present || value != "old" {
		close(release)
		<-done
		t.Fatal("old key disappeared during issuance")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	value, present, err = store.Get(context.Background(), s.cacheKey)
	if err != nil || !present || value != "new" {
		t.Fatal("replacement missing")
	}
}
