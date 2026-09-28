package reader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type httpIssuer struct {
	url    string
	client *http.Client
}

func (i httpIssuer) Issue(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, i.url+"/oauth2/Approval", strings.NewReader(`{"grant_type":"client_credentials","appkey":"fixture-app","secretkey":"fixture-secret"}`))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := i.client.Do(req)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return "", errors.New("synthetic KIS error")
	}
	var body struct {
		Key string `json:"approval_key"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		return "", err
	}
	return body.Key, nil
}
func newApprovalFixture(t *testing.T, mode string, handler http.HandlerFunc) (*miniredis.Miniredis, *redis.Client, *ApprovalProvider) {
	t.Helper()
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	provider, err := NewApprovalProviderConfig(client, httpIssuer{server.URL, server.Client()}, "", mode, DefaultRefreshMargin, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return mini, client, provider
}
func TestApprovalContractNamesTTLAndModes(t *testing.T) {
	for _, tc := range []struct{ mode, key, lock string }{{"live", "kis:websocket:approval_key", "kis:websocket:approval_key:lock"}, {"mock", "kis_mock:websocket:approval_key", "kis_mock:websocket:approval_key:lock"}} {
		t.Run(tc.mode, func(t *testing.T) {
			var calls atomic.Int32
			mini, client, p := newApprovalFixture(t, tc.mode, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/oauth2/Approval" || r.Method != "POST" {
					t.Errorf("approval request = %s %s", r.Method, r.URL.Path)
				}
				var body map[string]string
				_ = json.NewDecoder(r.Body).Decode(&body)
				if body["grant_type"] != "client_credentials" || body["appkey"] != "fixture-app" || body["secretkey"] != "fixture-secret" {
					t.Error("approval request contract changed")
				}
				fmt.Fprint(w, `{"approval_key":"fixture-key"}`)
			})
			if p.cacheKey() != tc.key || p.lockKey() != tc.lock {
				t.Fatalf("keys = %s %s, want %s %s", p.cacheKey(), p.lockKey(), tc.key, tc.lock)
			}
			key, err := p.ApprovalKey(context.Background())
			if err != nil || key != "fixture-key" {
				t.Fatalf("approval = %q, %v", key, err)
			}
			if got, _ := mini.Get(tc.key); got != "fixture-key" {
				t.Fatalf("raw Redis value = %q", got)
			}
			if ttl := mini.TTL(tc.key); ttl != 23*time.Hour {
				t.Fatalf("cache TTL = %s, want 23h", ttl)
			}
			if mini.Exists(tc.lock) {
				t.Fatal("lock was not released")
			}
			if calls.Load() != 1 || p.Metrics().IssueCalls.Load() != 1 {
				t.Fatalf("REST issue calls = %d, metric = %d, want 1", calls.Load(), p.Metrics().IssueCalls.Load())
			}
			if _, err := client.Get(context.Background(), tc.key).Result(); err != nil {
				t.Fatal(err)
			}
			key, err = p.ApprovalKey(context.Background())
			if err != nil || key != "fixture-key" || calls.Load() != 1 {
				t.Fatalf("cache hit = %q, %v; calls = %d", key, err, calls.Load())
			}
		})
	}
	if ApprovalLockTTL != 15*time.Second || ApprovalWait != 12*time.Second || ApprovalPoll != 250*time.Millisecond || ApprovalIssueTimeout != 10*time.Second || !(ApprovalIssueTimeout < publishDeadline && publishDeadline < ApprovalLockTTL) {
		t.Fatal("approval lock, wait, poll, or HTTP timeout drifted")
	}
}
func TestApprovalConcurrentIssuersOneRESTCall(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	var enteredOnce sync.Once
	mini, client, p1 := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		enteredOnce.Do(func() { close(entered) })
		<-release
		fmt.Fprint(w, `{"approval_key":"one-key"}`)
	})
	p2, err := NewApprovalProviderConfig(client, p1.issuer, "", "live", DefaultRefreshMargin, nil)
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		key string
		err error
	}
	ch := make(chan result, 2)
	go func() { key, err := p1.ApprovalKey(context.Background()); ch <- result{key, err} }()
	<-entered
	go func() { key, err := p2.ApprovalKey(context.Background()); ch <- result{key, err} }()
	deadline := time.Now().Add(2 * time.Second)
	for p2.Metrics().LockContended.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if ttl := mini.TTL(p1.lockKey()); ttl != 15*time.Second {
		t.Fatalf("lock TTL = %s, want 15s", ttl)
	}
	close(release)
	results := make([]result, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case result := <-ch:
			results = append(results, result)
		case <-time.After(3 * time.Second):
			t.Fatal("issuer did not complete")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("two concurrent issuers made %d REST calls, want exactly 1", calls.Load())
	}
	if p2.Metrics().LockContended.Load() != 1 {
		t.Fatal("second issuer did not contend on Redis lock")
	}
	for _, result := range results {
		if result.err != nil || result.key != "one-key" {
			t.Fatalf("issuer = %#v", result)
		}
	}
	if p2.Metrics().LockWaitSuccess.Load() != 1 {
		t.Fatal("contender did not reuse published key")
	}
}
func TestApprovalContenderTimeoutNeverIssues(t *testing.T) {
	mini, _, p := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) { t.Error("contender called approval REST") })
	mini.Set(p.lockKey(), "other-token")
	mini.SetTTL(p.lockKey(), ApprovalLockTTL)
	now := time.Now()
	start := now
	p.now = func() time.Time { return now }
	p.wait = func(ctx context.Context, d time.Duration) error {
		if d <= 0 || d > 2*ApprovalPoll {
			t.Errorf("contender poll = %s, want at most 0.5s", d)
		}
		now = now.Add(d)
		return nil
	}
	key, err := p.ApprovalKey(context.Background())
	if key != "" || !errors.Is(err, ErrApprovalUnavailable) || p.Metrics().IssueCalls.Load() != 0 || p.Metrics().LockWaitFailed.Load() != 1 {
		t.Fatalf("timed-out contender = %q, %v; REST=%d fail=%d", key, err, p.Metrics().IssueCalls.Load(), p.Metrics().LockWaitFailed.Load())
	}
	if elapsed := now.Sub(start); elapsed != ApprovalWait {
		t.Fatalf("contender wait = %s, want 12s", elapsed)
	}
}
func TestApprovalExpiredOwnershipCannotPublishOrReleaseOtherLock(t *testing.T) {
	var mini *miniredis.Miniredis
	var p *ApprovalProvider
	mini, _, p = newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) {
		mini.FastForward(ApprovalLockTTL)
		mini.Set(p.lockKey(), "new-owner")
		mini.SetTTL(p.lockKey(), ApprovalLockTTL)
		fmt.Fprint(w, `{"approval_key":"stale-key"}`)
	})
	key, err := p.ApprovalKey(context.Background())
	if key != "" || !errors.Is(err, ErrApprovalLockLost) {
		t.Fatalf("stale holder = %q, %v, want lock loss", key, err)
	}
	if mini.Exists(p.cacheKey()) {
		t.Fatal("stale holder published approval key")
	}
	if lock, _ := mini.Get(p.lockKey()); lock != "new-owner" {
		t.Fatalf("old holder released successor lock: %q", lock)
	}
}
func TestApprovalRefreshMarginAndTimer(t *testing.T) {
	for _, tc := range []struct {
		name      string
		ttl       time.Duration
		wantCalls int
	}{{"just before", time.Hour + time.Millisecond, 0}, {"at", time.Hour, 1}, {"after", time.Hour - time.Millisecond, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			mini, _, p := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) { calls.Add(1); fmt.Fprint(w, `{"approval_key":"fresh"}`) })
			mini.Set(p.cacheKey(), "old")
			mini.SetTTL(p.cacheKey(), ApprovalCacheTTL)
			fakeNow := time.Now()
			p.now = func() time.Time { return fakeNow }
			advance := ApprovalCacheTTL - tc.ttl
			mini.FastForward(advance)
			fakeNow = fakeNow.Add(advance)
			key, err := p.ApprovalKey(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if got := int(calls.Load()); got != tc.wantCalls {
				t.Fatalf("REST calls at TTL %s = %d, want %d", tc.ttl, got, tc.wantCalls)
			}
			if tc.wantCalls == 0 && key != "old" || tc.wantCalls == 1 && key != "fresh" {
				t.Fatalf("key at TTL %s = %q", tc.ttl, key)
			}
		})
	}
	var calls atomic.Int32
	mini, _, p := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, `{"approval_key":"timer-fresh"}`)
	})
	mini.Set(p.cacheKey(), "old")
	mini.SetTTL(p.cacheKey(), time.Hour+time.Minute)
	fakeNow := time.Now()
	p.now = func() time.Time { return fakeNow }
	p.wait = func(ctx context.Context, d time.Duration) error {
		mini.FastForward(d)
		fakeNow = fakeNow.Add(d)
		if calls.Load() > 0 {
			return context.Canceled
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.RefreshLoop(ctx) }()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if key, _ := mini.Get(p.cacheKey()); key == "timer-fresh" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timer did not stop")
	}
	if calls.Load() != 1 {
		t.Fatalf("timer REST calls = %d, want 1", calls.Load())
	}
	if key, _ := mini.Get(p.cacheKey()); key != "timer-fresh" {
		t.Fatalf("timer did not refresh cached key: %q", key)
	}
}
func TestApprovalTransientFailureAndIsolation(t *testing.T) {
	mini, client, p := newApprovalFixture(t, "mock", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) })
	mini.Set("kis:websocket:approval_key", "live-key")
	mini.SetTTL("kis:websocket:approval_key", ApprovalCacheTTL)
	key, err := p.ApprovalKey(context.Background())
	if key != "" || !errors.Is(err, ErrApprovalUnavailable) {
		t.Fatalf("mock failure = %q, %v", key, err)
	}
	if ProcessExitCode(err) != 1 {
		t.Fatalf("transient classified as config: exit=%d", ProcessExitCode(err))
	}
	if got, _ := client.Get(context.Background(), "kis:websocket:approval_key").Result(); got != "live-key" {
		t.Fatal("mock changed live cache")
	}
	if p.Metrics().IssueCalls.Load() != 1 {
		t.Fatal("issue metric missed 5xx call")
	}
}

func TestTimerAndResubscribeShareOneIssuance(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	mini, client, timerProvider := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		close(entered)
		<-release
		fmt.Fprint(w, `{"approval_key":"renewed"}`)
	})
	mini.Set(timerProvider.cacheKey(), "old")
	mini.SetTTL(timerProvider.cacheKey(), DefaultRefreshMargin)
	subscribeProvider, err := NewApprovalProviderConfig(client, timerProvider.issuer, "", "live", DefaultRefreshMargin, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	timerDone := make(chan error, 1)
	go func() { timerDone <- timerProvider.RefreshLoop(ctx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("timer did not issue")
	}
	subscribeDone := make(chan string, 1)
	go func() {
		key, err := subscribeProvider.ApprovalKey(context.Background())
		if err != nil {
			subscribeDone <- "error"
			return
		}
		subscribeDone <- key
	}()
	deadline := time.Now().Add(time.Second)
	for subscribeProvider.Metrics().LockContended.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if subscribeProvider.Metrics().LockContended.Load() != 1 {
		t.Fatal("resubscribe did not contend with timer")
	}
	close(release)
	select {
	case key := <-subscribeDone:
		if key != "renewed" {
			t.Fatalf("resubscribe key = %q, want renewed", key)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("resubscribe blocked")
	}
	cancel()
	select {
	case <-timerDone:
	case <-time.After(time.Second):
		t.Fatal("timer did not stop")
	}
	if calls.Load() != 1 {
		t.Fatalf("timer and resubscribe REST calls = %d, want 1", calls.Load())
	}
}

func TestApprovalIssueDeadlineAndSecretFreeObservations(t *testing.T) {
	var logs strings.Builder
	mini, _, p := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"approval_key":"secret-approval-value"}`)
	})
	p.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	base := p.issuer
	p.issuer = issuerFunc(func(ctx context.Context) (string, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > ApprovalIssueTimeout || time.Until(deadline) < ApprovalIssueTimeout-time.Second {
			t.Errorf("approval HTTP deadline = %v, want 10s", deadline)
		}
		return base.Issue(ctx)
	})
	if _, err := p.ApprovalKey(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := mini.Get(p.cacheKey()); got != "secret-approval-value" {
		t.Fatal("key was not cached")
	}
	for _, secret := range []string{"secret-approval-value", "fixture-app", "fixture-secret"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("approval observation leaked %s", secret)
		}
	}
	for _, field := range []string{"approval_rest_issue_call", "approval_lock_acquired", "account_mode", "count"} {
		if !strings.Contains(logs.String(), field) {
			t.Fatalf("approval observation missing %s", field)
		}
	}
}

type lockCommandHook struct {
	ch   chan []any
	lock string
}

func (h *lockCommandHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *lockCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "set" {
			args := cmd.Args()
			if len(args) > 1 && fmt.Sprint(args[1]) == h.lock {
				h.ch <- append([]any(nil), args...)
			}
		}
		return next(ctx, cmd)
	}
}
func (h *lockCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func TestApprovalLockUsesSetNXEXUniqueTokens(t *testing.T) {
	var calls atomic.Int32
	mini, client, p := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"approval_key":"key-%d"}`, calls.Add(1))
	})
	hook := &lockCommandHook{ch: make(chan []any, 2), lock: p.lockKey()}
	client.AddHook(hook)
	tokens := map[string]bool{}
	for i := 0; i < 2; i++ {
		if _, err := p.ApprovalKey(context.Background()); err != nil {
			t.Fatal(err)
		}
		args := <-hook.ch
		if len(args) != 6 || fmt.Sprint(args[0]) != "set" || fmt.Sprint(args[1]) != p.lockKey() || strings.ToLower(fmt.Sprint(args[3])) != "ex" || fmt.Sprint(args[4]) != "15" || strings.ToLower(fmt.Sprint(args[5])) != "nx" {
			t.Fatalf("lock acquisition command = %v, want SET key unique-token EX 15 NX", args)
		}
		token := fmt.Sprint(args[2])
		if token == "" || tokens[token] {
			t.Fatal("lock token was empty or reused")
		}
		tokens[token] = true
		mini.Del(p.cacheKey())
	}
	if calls.Load() != 2 {
		t.Fatalf("separate issuance calls = %d, want 2", calls.Load())
	}
}
