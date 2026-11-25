package ratebudget_test

import (
	"errors"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/ratebudget"
)

func highLimits() (ratebudget.Limit, ratebudget.Endpoint) {
	return ratebudget.Limit{Rate: 1000, Burst: 1000},
		ratebudget.Endpoint{Name: testQuote, Limit: ratebudget.Limit{Rate: 1000, Burst: 1000}}
}

func TestRetryAfterClosesTheEndpointForTheHeaderValue(t *testing.T) {
	clock := newTestClock()
	global, endpoint := highLimits()
	l := newLimiter(t, clock, 1, global, endpoint)
	k := quoteKey()

	if got := l.Observe(k, ratebudget.Outcome{StatusCode: 429, RetryAfter: 7 * time.Second}); got != 7*time.Second {
		t.Fatalf("Observe returned %s, want 7s", got)
	}
	r := mustRefuse(t, l.Acquire(k), ratebudget.CodeRateLimited)
	if r.RetryAfter != 7*time.Second {
		t.Fatalf("wait = %s, want 7s", r.RetryAfter)
	}
	clock.advance(6 * time.Second)
	r = mustRefuse(t, l.Acquire(k), ratebudget.CodeRateLimited)
	if r.RetryAfter != time.Second {
		t.Fatalf("wait after 6s = %s, want 1s", r.RetryAfter)
	}
	clock.advance(time.Second)
	mustAcquire(t, l, k)
}

func TestRetryAfterAbsentUsesBackoffSequence(t *testing.T) {
	clock := newTestClock()
	global, endpoint := highLimits()
	l := newLimiter(t, clock, 1, global, endpoint)
	k := quoteKey()

	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	for i, d := range want {
		if got := l.Observe(k, ratebudget.Outcome{StatusCode: 429}); got != d {
			t.Fatalf("attempt %d backoff = %s, want %s", i+1, got, d)
		}
		clock.advance(d)
	}
}

func TestServerAndTransportFailuresCloseTheEndpoint(t *testing.T) {
	clock := newTestClock()
	global, endpoint := highLimits()
	l := newLimiter(t, clock, 1, global, endpoint)
	k := quoteKey()

	if got := l.Observe(k, ratebudget.Outcome{StatusCode: 503}); got != time.Second {
		t.Fatalf("503 backoff = %s, want 1s", got)
	}
	mustRefuse(t, l.Acquire(k), ratebudget.CodeRateLimited)
	clock.advance(time.Second)
	mustAcquire(t, l, k)

	if got := l.Observe(k, ratebudget.Outcome{Err: errors.New("timeout")}); got != 2*time.Second {
		t.Fatalf("transport backoff = %s, want 2s", got)
	}
}

func TestRateLimitedOutcomeWithoutHTTPStatus(t *testing.T) {
	clock := newTestClock()
	global, endpoint := highLimits()
	l := newLimiter(t, clock, 1, global, endpoint)
	k := quoteKey()

	if got := l.Observe(k, ratebudget.Outcome{RateLimited: true}); got != time.Second {
		t.Fatalf("rate-limit backoff = %s, want 1s", got)
	}
	mustRefuse(t, l.Acquire(k), ratebudget.CodeRateLimited)
}

func TestOtherClientErrorsDoNotRetry(t *testing.T) {
	clock := newTestClock()
	l := newLimiter(t, clock, 1, ratebudget.Limit{Rate: 1, Burst: 1},
		ratebudget.Endpoint{Name: testQuote, Limit: ratebudget.Limit{Rate: 1, Burst: 1}})
	k := quoteKey()

	mustAcquire(t, l, k)
	if got := l.Observe(k, ratebudget.Outcome{StatusCode: 400}); got != 0 {
		t.Fatalf("400 cooldown = %s, want 0", got)
	}
	clock.advance(time.Second)
	mustAcquire(t, l, k)
}

func TestRetryAfterDoesNotTripTheCircuit(t *testing.T) {
	clock := newTestClock()
	global, endpoint := highLimits()
	l := newLimiter(t, clock, 1, global, endpoint)
	k := quoteKey()

	var closed time.Duration
	for range 5 {
		closed = l.Observe(k, ratebudget.Outcome{StatusCode: 429})
	}
	clock.advance(closed)
	mustAcquire(t, l, k)
}

func TestDefaultJitterStaysInsideTwentyPercent(t *testing.T) {
	clock := newTestClock()
	global, endpoint := highLimits()
	l, err := ratebudget.New(ratebudget.Config{
		Providers: []ratebudget.Provider{{
			Name:       testProvider,
			AccountRef: testAccount,
			Limit:      global,
			Endpoints:  []ratebudget.Endpoint{endpoint},
		}},
		ResearchShare: 1,
		Clock:         clock,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	k := quoteKey()
	var jittered bool
	for range 50 {
		l.Observe(k, ratebudget.Outcome{StatusCode: 400}) // reset the failure count
		got := l.Observe(k, ratebudget.Outcome{StatusCode: 500})
		if got < 800*time.Millisecond || got > 1200*time.Millisecond {
			t.Fatalf("jittered backoff %s outside [800ms,1200ms]", got)
		}
		if got != time.Second {
			jittered = true
		}
	}
	if !jittered {
		t.Fatal("zero Backoff.Jitter produced the unjittered 1s delay every time; the ±20% default is missing")
	}
}
