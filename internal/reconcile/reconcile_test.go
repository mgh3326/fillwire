package reconcile

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const sentinelToken = "SENTINEL-reconcile-token-9f3a"

func newTestClient(t *testing.T, url string, dryRun bool, timeout time.Duration) *Client {
	t.Helper()
	c, err := NewClient(HTTPConfig{URL: url, Token: sentinelToken, TokenHeader: "X-Test-Token", DryRun: dryRun, Timeout: timeout})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestClientRequestBodyAndHeaders(t *testing.T) {
	var gotBody, gotToken, gotMethod, gotType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		gotBody, gotToken, gotMethod, gotType = string(raw), r.Header.Get("X-Test-Token"), r.Method, r.Header.Get("Content-Type")
		_, _ = io.WriteString(w, `{"status":"executed","deduped":false,"backfilled":0,"extra":1}`)
	}))
	defer srv.Close()
	res, err := newTestClient(t, srv.URL+"/trigger", true, time.Second).Trigger(context.Background(), "kr")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("raw request body: %s", gotBody)
	if gotBody != `{"market":"kr","dry_run":true,"reason":"reconnect"}` {
		t.Fatalf("body = %s", gotBody)
	}
	if gotMethod != http.MethodPost || gotToken != sentinelToken || gotType != "application/json" {
		t.Fatalf("method=%s token-ok=%v type=%s", gotMethod, gotToken == sentinelToken, gotType)
	}
	if res.Status != StatusExecuted {
		t.Fatalf("status = %q", res.Status)
	}
}

func TestClientCommitBody(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		_, _ = io.WriteString(w, `{"status":"executed","backfilled":3}`)
	}))
	defer srv.Close()
	res, err := newTestClient(t, srv.URL, false, time.Second).Trigger(context.Background(), "us")
	if err != nil || res.Backfilled != 3 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if gotBody != `{"market":"us","dry_run":false,"reason":"reconnect"}` {
		t.Fatalf("body = %s", gotBody)
	}
}

func TestClientOutcomes(t *testing.T) {
	var redirectTarget atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirectTarget.Add(1) }))
	defer target.Close()
	cases := []struct {
		name      string
		handler   http.HandlerFunc
		timeout   time.Duration
		wantErr   FailureClass // "" = success
		wantHTTP  int
		wantState string
	}{
		{"success", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"status":"executed","deduped":false}`)
		}, time.Second, "", 0, StatusExecuted},
		{"deduped is normal", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"status":"deduped","deduped":true}`)
		}, time.Second, "", 0, StatusDeduped},
		{"401", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(401) }, time.Second, FailHTTP, 401, ""},
		{"403", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(403) }, time.Second, FailHTTP, 403, ""},
		{"422", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(422) }, time.Second, FailHTTP, 422, ""},
		{"500", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) }, time.Second, FailHTTP, 500, ""},
		{"503", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }, time.Second, FailHTTP, 503, ""},
		{"timeout", func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body) // lets the server notice the client hang-up
			select {
			case <-r.Context().Done():
			case <-time.After(3 * time.Second):
			}
		}, 50 * time.Millisecond, FailTimeout, 0, ""},
		{"redirect not followed", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
		}, time.Second, FailRedirect, 307, ""},
		{"malformed body", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `not json`) }, time.Second, FailContract, 0, ""},
		{"empty body", func(w http.ResponseWriter, _ *http.Request) {}, time.Second, FailContract, 0, ""},
		{"status outside closed set", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{"status":"ok"}`) }, time.Second, FailContract, 0, ""},
		{"status missing", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{"deduped":false}`) }, time.Second, FailContract, 0, ""},
		{"status wrong type", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{"status":1}`) }, time.Second, FailContract, 0, ""},
		{"reported failed", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{"status":"failed"}`) }, time.Second, FailReported, 0, ""},
		{"oversized body", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"status":"executed","pad":"`+strings.Repeat("x", maxResponseBytes)+`"}`)
		}, time.Second, FailContract, 0, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			defer srv.Close()
			res, err := newTestClient(t, srv.URL, true, tc.timeout).Trigger(context.Background(), "kr")
			t.Logf("result=%+v err=%v", res, err)
			if tc.wantErr == "" {
				if err != nil || res.Status != tc.wantState {
					t.Fatalf("res=%+v err=%v", res, err)
				}
				return
			}
			var typed *Error
			if !errors.As(err, &typed) || typed.Class != tc.wantErr || typed.Status != tc.wantHTTP {
				t.Fatalf("err = %v, want class %s status %d", err, tc.wantErr, tc.wantHTTP)
			}
			if strings.Contains(err.Error(), sentinelToken) || strings.Contains(err.Error(), srv.URL) {
				t.Fatalf("error leaks token or URL: %v", err)
			}
		})
	}
	if redirectTarget.Load() != 0 {
		t.Fatal("redirect was followed")
	}
}

func TestClientTransportFailure(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	_, err := newTestClient(t, url, true, time.Second).Trigger(context.Background(), "kr")
	var typed *Error
	if !errors.As(err, &typed) || typed.Class != FailTransport {
		t.Fatalf("err = %v", err)
	}
	if strings.Contains(err.Error(), url) {
		t.Fatalf("error leaks URL: %v", err)
	}
}

type fakeCaller struct {
	mu       sync.Mutex
	calls    []string
	inFlight map[string]int
	maxIn    map[string]int
	release  chan struct{} // nil = return at once
	result   Result
	err      error
}

func newFakeCaller() *fakeCaller {
	return &fakeCaller{inFlight: map[string]int{}, maxIn: map[string]int{}, result: Result{Status: StatusExecuted}}
}

func (f *fakeCaller) Trigger(ctx context.Context, market string) (Result, error) {
	f.mu.Lock()
	f.calls = append(f.calls, market)
	f.inFlight[market]++
	if f.inFlight[market] > f.maxIn[market] {
		f.maxIn[market] = f.inFlight[market]
	}
	f.mu.Unlock()
	defer func() { f.mu.Lock(); f.inFlight[market]--; f.mu.Unlock() }()
	if f.release != nil {
		select {
		case <-f.release:
		case <-ctx.Done():
			return Result{}, &Error{Class: FailTimeout}
		}
	}
	return f.result, f.err
}

func (f *fakeCaller) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.calls) }

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSchedulerFireNeverBlocksAndCoalesces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	caller := newFakeCaller()
	caller.release = make(chan struct{})
	s := NewScheduler(ctx, SchedulerConfig{Caller: caller, Markets: []string{"kr", "us"}, RateLimit: time.Minute})

	returned := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			s.Fire("reconnect")
		}
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("Fire blocked behind a hung trigger")
	}
	waitFor(t, func() bool { return caller.count() == 2 })
	if got := s.Counters.Coalesced.Load(); got != 98 {
		t.Fatalf("coalesced = %d, want 98", got)
	}
	close(caller.release) // pending follow-up runs once per market, then it stops
	waitFor(t, func() bool { return caller.count() == 4 })
	time.Sleep(20 * time.Millisecond)
	cancel()
	s.Wait()
	caller.mu.Lock()
	defer caller.mu.Unlock()
	if len(caller.calls) != 4 || caller.maxIn["kr"] != 1 || caller.maxIn["us"] != 1 {
		t.Fatalf("calls=%v maxInFlight=%v, want 4 calls and at most 1 in flight per market", caller.calls, caller.maxIn)
	}
}

func TestSchedulerCancelAbortsHungTrigger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	caller := newFakeCaller()
	caller.release = make(chan struct{})
	var logs bytes.Buffer
	s := NewScheduler(ctx, SchedulerConfig{Caller: caller, Markets: []string{"kr"}, RateLimit: time.Minute, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	s.Fire("startup")
	waitFor(t, func() bool { return caller.count() == 1 })
	cancel()
	done := make(chan struct{})
	go func() { s.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after cancel")
	}
	s.Fire("reconnect")
	if caller.count() != 1 {
		t.Fatal("Fire after cancel started a request")
	}
	if strings.Contains(logs.String(), "failed") {
		t.Fatalf("shutdown abort logged as a failure: %s", logs.String())
	}
}

func TestSchedulerFailureLogRateLimitAndCounters(t *testing.T) {
	now := time.Unix(1000, 0)
	var logs bytes.Buffer
	caller := newFakeCaller()
	caller.err = &Error{Class: FailHTTP, Status: 500}
	s := NewScheduler(context.Background(), SchedulerConfig{
		Caller: caller, Markets: []string{"kr"}, RateLimit: 10 * time.Minute,
		Logger: slog.New(slog.NewTextHandler(&logs, nil)),
		Now:    func() time.Time { return now },
	})
	run := func() {
		s.Fire("reconnect")
		s.Wait()
	}
	for i := 0; i < 5; i++ {
		run()
	}
	if got := strings.Count(logs.String(), "reconcile trigger failed"); got != 1 {
		t.Fatalf("failure log lines = %d, want 1 per window:\n%s", got, logs.String())
	}
	if got := s.Counters.Failed.Load(); got != 5 {
		t.Fatalf("failed counter = %d, want 5", got)
	}
	now = now.Add(11 * time.Minute)
	run()
	if got := strings.Count(logs.String(), "reconcile trigger failed"); got != 2 || !strings.Contains(logs.String(), "suppressed_since_last_log=4") {
		t.Fatalf("after window: lines=%d logs:\n%s", got, logs.String())
	}
}

func TestSchedulerDedupedIsNotAFailure(t *testing.T) {
	var logs bytes.Buffer
	caller := newFakeCaller()
	caller.result = Result{Status: StatusDeduped, Deduped: true}
	s := NewScheduler(context.Background(), SchedulerConfig{Caller: caller, Markets: []string{"kr"}, RateLimit: time.Minute, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	s.Fire("startup")
	s.Wait()
	if s.Counters.Deduped.Load() != 1 || s.Counters.Failed.Load() != 0 || strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("deduped=%d failed=%d logs=%s", s.Counters.Deduped.Load(), s.Counters.Failed.Load(), logs.String())
	}
}

func TestSchedulerIntervalFires(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	caller := newFakeCaller()
	s := NewScheduler(ctx, SchedulerConfig{Caller: caller, Markets: []string{"kr"}, Interval: 5 * time.Millisecond, RateLimit: time.Minute})
	go s.Run(ctx)
	waitFor(t, func() bool { return caller.count() >= 3 })
	cancel()
	s.Wait()
}

func TestSchedulerIntervalZeroIsOff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	caller := newFakeCaller()
	s := NewScheduler(ctx, SchedulerConfig{Caller: caller, Markets: []string{"kr"}, RateLimit: time.Minute})
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run with interval 0 did not return")
	}
	if caller.count() != 0 {
		t.Fatal("interval 0 produced a call")
	}
}

func TestNilSchedulerIsSafe(t *testing.T) {
	var s *Scheduler
	s.Fire("x")
	s.Wait()
	s.Run(context.Background())
}

// End to end through the real client: the token must not appear in any log
// line, on success or on any failure class.
func TestTokenNeverLogged(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	for _, status := range []int{200, 401, 403, 422, 500} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if status == 200 {
				_, _ = io.WriteString(w, `{"status":"executed"}`)
				return
			}
			w.WriteHeader(status)
			_, _ = io.WriteString(w, "echo "+r.Header.Get("X-Test-Token"))
		}))
		s := NewScheduler(context.Background(), SchedulerConfig{Caller: newTestClient(t, srv.URL, true, time.Second), Markets: []string{"kr"}, RateLimit: 0, Logger: logger})
		s.Fire("reconnect")
		s.Wait()
		srv.Close()
	}
	if logs.Len() == 0 {
		t.Fatal("no log output to check")
	}
	if strings.Contains(logs.String(), sentinelToken) {
		t.Fatalf("token leaked into logs:\n%s", logs.String())
	}
}
