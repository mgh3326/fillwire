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

func TestApprovalRefreshCancellationIsClean(t *testing.T) {
	mini, _, p := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) { t.Error("unexpected approval request on shutdown") })
	mini.Set(p.cacheKey(), "cached")
	mini.SetTTL(p.cacheKey(), ApprovalCacheTTL)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.RefreshLoop(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("refresh cancellation = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("refresh did not stop")
	}
	if err := p.cacheOnlyFailure(); err != nil {
		t.Fatalf("shutdown raised approval failure: %v", err)
	}
}

func TestApprovalFailedRefreshKeepsLiveKeyAndRetries(t *testing.T) {
	var calls atomic.Int32
	var unavailable atomic.Bool
	unavailable.Store(true)
	mini, _, p := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if unavailable.Load() {
			w.WriteHeader(503)
			return
		}
		fmt.Fprint(w, `{"approval_key":"replacement"}`)
	})
	mini.Set(p.cacheKey(), "still-live")
	mini.SetTTL(p.cacheKey(), 30*time.Minute)
	now := time.Now()
	p.now = func() time.Time { return now }
	alerts := 0
	lastAlert := time.Time{}
	p.SetTransientAlert(func() {
		if lastAlert.IsZero() || now.Sub(lastAlert) >= 10*time.Minute {
			alerts++
			lastAlert = now
		}
	})
	key, err := p.ApprovalKey(context.Background())
	if err != nil || key != "still-live" || p.cacheOnlyFailure() != nil {
		t.Fatalf("resubscribe after 503 = %q, %v, terminal=%v; want live cache", key, err, p.cacheOnlyFailure())
	}
	if calls.Load() != 1 || alerts != 1 {
		t.Fatalf("initial REST calls=%d alerts=%d, want 1 each", calls.Load(), alerts)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	retries := []time.Duration{}
	p.wait = func(ctx context.Context, d time.Duration) error {
		if key, _ := mini.Get(p.cacheKey()); key == "replacement" {
			cancel()
			return ctx.Err()
		}
		retries = append(retries, d)
		mini.FastForward(d)
		now = now.Add(d)
		if calls.Load() >= 3 {
			unavailable.Store(false)
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- p.RefreshLoop(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("refresh loop = %v, want clean stop", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("refresh retry did not complete")
	}
	if calls.Load() != 4 {
		t.Fatalf("REST calls = %d, want initial failure, two retries, then success", calls.Load())
	}
	if len(retries) != 2 {
		t.Fatalf("retry waits = %v, want 2 bounded waits", retries)
	}
	for _, d := range retries {
		if d < refreshRetryMin || d > refreshRetryMax {
			t.Fatalf("retry wait = %s, want 1s..30s", d)
		}
	}
	if alerts != 1 {
		t.Fatalf("alerts within one 10m interval = %d, want 1", alerts)
	}
	if value, _ := mini.Get(p.cacheKey()); value != "replacement" {
		t.Fatalf("replacement cache = %q", value)
	}
	if ttl := mini.TTL(p.cacheKey()); ttl != ApprovalCacheTTL {
		t.Fatalf("replacement TTL = %s, want 23h", ttl)
	}
	if p.cacheOnlyFailure() != nil {
		t.Fatalf("recoverable refresh latched terminal failure: %v", p.cacheOnlyFailure())
	}
	now = now.Add(10 * time.Minute)
	unavailable.Store(true)
	mini.Set(p.cacheKey(), "still-live")
	mini.SetTTL(p.cacheKey(), 30*time.Minute)
	key, err = p.ApprovalKey(context.Background())
	if err != nil || key != "still-live" || alerts != 2 {
		t.Fatalf("next alert interval: key=%q err=%v alerts=%d, want live key and 2 alerts", key, err, alerts)
	}
}

func TestApprovalNoLiveKeyFailureIsTerminal(t *testing.T) {
	_, _, p := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) })
	if _, err := p.ApprovalKey(context.Background()); !errors.Is(err, ErrApprovalUnavailable) || p.cacheOnlyFailure() == nil {
		t.Fatalf("cold failure = %v terminal=%v, want transient terminal", err, p.cacheOnlyFailure())
	}
	mini2, _, p2 := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) })
	mini2.Set(p2.cacheKey(), "expired")
	mini2.SetTTL(p2.cacheKey(), time.Millisecond)
	mini2.FastForward(2 * time.Millisecond)
	if _, err := p2.ApprovalKey(context.Background()); !errors.Is(err, ErrApprovalUnavailable) || p2.cacheOnlyFailure() == nil {
		t.Fatalf("expired cache failure = %v terminal=%v, want transient terminal", err, p2.cacheOnlyFailure())
	}
}

func TestApprovalCancelMidIssueDoesNotLatch(t *testing.T) {
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer client.Close()
	entered := make(chan struct{})
	p, err := NewApprovalProviderConfig(client, issuerFunc(func(ctx context.Context) (string, error) { close(entered); <-ctx.Done(); return "", ctx.Err() }), "", "live", DefaultRefreshMargin, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.RefreshLoop(ctx) }()
	<-entered
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("cancel mid-issue = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("refresh did not stop")
	}
	if err := p.cacheOnlyFailure(); err != nil {
		t.Fatalf("canceled issue latched terminal failure: %v", err)
	}
	if mini.Exists(p.lockKey()) {
		t.Fatal("canceled holder kept lock")
	}
}

func TestApprovalContenderWaitsForSlowHolder(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	_, client, holder := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		close(entered)
		<-release
		fmt.Fprint(w, `{"approval_key":"issued"}`)
	})
	contender, err := NewApprovalProviderConfig(client, holder.issuer, "", "live", DefaultRefreshMargin, nil)
	if err != nil {
		t.Fatal(err)
	}
	holderDone := make(chan error, 1)
	go func() { _, err := holder.ApprovalKey(context.Background()); holderDone <- err }()
	<-entered
	time.AfterFunc(1500*time.Millisecond, func() { close(release) })
	key, err := contender.ApprovalKey(context.Background())
	if err != nil || key != "issued" || calls.Load() != 1 {
		t.Fatalf("slow-holder contender = %q, %v; REST calls=%d, want issued and one call", key, err, calls.Load())
	}
	if err := <-holderDone; err != nil {
		t.Fatal(err)
	}
}
func TestApprovalForcedReissueFailureClearsRejectedKey(t *testing.T) {
	mini, _, p := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) })
	mini.Set(p.cacheKey(), "rejected")
	mini.SetTTL(p.cacheKey(), ApprovalCacheTTL)
	if _, err := p.ApprovalKey(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Reissue(context.Background()); !errors.Is(err, ErrApprovalUnavailable) {
		t.Fatalf("forced issue failure = %v", err)
	}
	if mini.Exists(p.cacheKey()) {
		value, _ := mini.Get(p.cacheKey())
		t.Fatalf("rejected key %q still cached after failed forced reissue", value)
	}
}
func TestApprovalForcedContenderNeverReturnsRejectedKey(t *testing.T) {
	mini, _, p := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) { t.Error("forced contender issued outside lock") })
	mini.Set(p.cacheKey(), "rejected")
	mini.SetTTL(p.cacheKey(), ApprovalCacheTTL)
	if _, err := p.ApprovalKey(context.Background()); err != nil {
		t.Fatal(err)
	}
	mini.Set(p.lockKey(), "other-holder")
	mini.SetTTL(p.lockKey(), ApprovalLockTTL)
	time.AfterFunc(600*time.Millisecond, func() { mini.Set(p.cacheKey(), "replacement"); mini.SetTTL(p.cacheKey(), ApprovalCacheTTL) })
	key, err := p.Reissue(context.Background())
	if err != nil || key != "replacement" {
		t.Fatalf("forced contender = %q, %v, want replacement", key, err)
	}
	if p.Metrics().IssueCalls.Load() != 0 {
		t.Fatalf("contender REST calls=%d, want 0", p.Metrics().IssueCalls.Load())
	}
}
func TestApprovalRefreshRechecksAtMostOneMinute(t *testing.T) {
	mini, _, p := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) { t.Error("unexpected REST") })
	mini.Set(p.cacheKey(), "fresh")
	mini.SetTTL(p.cacheKey(), ApprovalCacheTTL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var waited time.Duration
	p.wait = func(_ context.Context, d time.Duration) error { waited = d; cancel(); return ctx.Err() }
	if err := p.RefreshLoop(ctx); err != nil {
		t.Fatal(err)
	}
	if waited <= 0 || waited > time.Minute {
		t.Fatalf("refresh wait = %s, want at most 1m", waited)
	}
}

type publishDeadlineHook struct{ lockAt, publishAt time.Time }

func (*publishDeadlineHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (*publishDeadlineHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *publishDeadlineHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "set" && len(cmd.Args()) == 6 && fmt.Sprint(cmd.Args()[5]) == "nx" {
			h.lockAt = time.Now()
		}
		if cmd.Name() == "eval" && strings.Contains(fmt.Sprint(cmd.Args()[1]), "'SET', KEYS[2]") {
			h.publishAt, _ = ctx.Deadline()
		}
		return next(ctx, cmd)
	}
}
func TestApprovalPublishDeadlineUnderLockTTL(t *testing.T) {
	_, client, p := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"approval_key":"issued"}`) })
	hook := &publishDeadlineHook{}
	client.AddHook(hook)
	if _, err := p.ApprovalKey(context.Background()); err != nil {
		t.Fatal(err)
	}
	if hook.lockAt.IsZero() || hook.publishAt.IsZero() || hook.publishAt.Sub(hook.lockAt) >= ApprovalLockTTL {
		t.Fatalf("publish deadline %s after lock attempt, want below 15s", hook.publishAt.Sub(hook.lockAt))
	}
}

func TestApprovalRefreshMarginConstructorBoundary(t *testing.T) {
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer client.Close()
	issuer := issuerFunc(func(context.Context) (string, error) { return "fixture", nil })
	if _, err := NewApprovalProviderConfig(client, issuer, "", "live", MaxRefreshMargin, nil); err != nil {
		t.Fatalf("2h margin rejected: %v", err)
	}
	if _, err := NewApprovalProviderConfig(client, issuer, "", "live", MaxRefreshMargin+time.Nanosecond, nil); !errors.Is(err, ErrApprovalConfig) {
		t.Fatalf("margin above 2h = %v, want config error", err)
	}
}

func TestApprovalRefreshRetriesRedisBlipWhileKnownKeyLive(t *testing.T) {
	var calls atomic.Int32
	mini, _, p := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, `{"approval_key":"replacement"}`)
	})
	mini.Set(p.cacheKey(), "old")
	mini.SetTTL(p.cacheKey(), ApprovalCacheTTL)
	now := time.Now()
	p.now = func() time.Time { return now }
	if key, err := p.ApprovalKey(context.Background()); err != nil || key != "old" {
		t.Fatalf("initial cache = %q, %v", key, err)
	}
	advance := ApprovalCacheTTL - 30*time.Minute
	mini.FastForward(advance)
	now = now.Add(advance)
	mini.SetError("ERR synthetic Redis blip")
	alerts := 0
	p.SetTransientAlert(func() { alerts++ })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waits := 0
	p.wait = func(ctx context.Context, d time.Duration) error {
		waits++
		if waits == 1 {
			if d < refreshRetryMin || d > refreshRetryMax {
				t.Errorf("Redis retry = %s, want 1s..30s", d)
			}
			mini.SetError("")
			mini.FastForward(d)
			now = now.Add(d)
			return nil
		}
		cancel()
		return ctx.Err()
	}
	done := make(chan error, 1)
	go func() { done <- p.RefreshLoop(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Redis blip ended stream: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Redis refresh did not recover")
	}
	if alerts != 1 || calls.Load() != 1 || p.cacheOnlyFailure() != nil {
		t.Fatalf("Redis recovery alerts=%d REST=%d terminal=%v, want 1,1,nil", alerts, calls.Load(), p.cacheOnlyFailure())
	}
	if value, _ := mini.Get(p.cacheKey()); value != "replacement" {
		t.Fatalf("Redis recovery cached %q, want replacement", value)
	}
}

func TestApprovalCanceledProviderCallDoesNotLatch(t *testing.T) {
	_, _, p := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) { t.Error("canceled call reached fake KIS") })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.ApprovalKey(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled provider call = %v, want context.Canceled", err)
	}
	if failure := p.cacheOnlyFailure(); failure != nil {
		t.Fatalf("canceled provider call latched terminal failure: %v", failure)
	}
}

func TestApprovalForcedReissueTracksPresentedKeyAcrossTimerRefresh(t *testing.T) {
	var calls atomic.Int32
	mini, _, p := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n == 1 {
			fmt.Fprint(w, `{"approval_key":"background-key"}`)
		} else {
			fmt.Fprint(w, `{"approval_key":"forced-replacement"}`)
		}
	})
	mini.Set(p.cacheKey(), "rejected-presented")
	mini.SetTTL(p.cacheKey(), ApprovalCacheTTL)
	if key, err := p.ApprovalKey(context.Background()); err != nil || key != "rejected-presented" {
		t.Fatalf("presented key = %q, %v", key, err)
	}
	mini.SetTTL(p.cacheKey(), 30*time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.wait = func(ctx context.Context, d time.Duration) error {
		if key, _ := mini.Get(p.cacheKey()); key == "background-key" {
			cancel()
			return ctx.Err()
		}
		return errors.New("unexpected wait before timer refresh")
	}
	if err := p.RefreshLoop(ctx); err != nil {
		t.Fatal(err)
	}
	mini.Set(p.cacheKey(), "rejected-presented")
	mini.SetTTL(p.cacheKey(), ApprovalCacheTTL)
	key, err := p.Reissue(context.Background())
	if err != nil || key != "forced-replacement" || calls.Load() != 2 {
		t.Fatalf("forced reissue after timer = %q, %v, REST calls=%d; rejected key must not return", key, err, calls.Load())
	}
}

func TestApprovalRefreshExpiredKeyFailureExitsTransient(t *testing.T) {
	mini, _, p := newApprovalFixture(t, "live", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) })
	mini.Set(p.cacheKey(), "expired")
	mini.SetTTL(p.cacheKey(), time.Millisecond)
	mini.FastForward(2 * time.Millisecond)
	err := p.RefreshLoop(context.Background())
	if !errors.Is(err, ErrApprovalUnavailable) || p.cacheOnlyFailure() == nil || ProcessExitCode(err) != 1 {
		t.Fatalf("expired-key refresh = %v, terminal=%v, exit=%d; want transient exit 1", err, p.cacheOnlyFailure(), ProcessExitCode(err))
	}
}
