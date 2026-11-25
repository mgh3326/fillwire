package ratebudget_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/ratebudget"
)

const (
	testProvider = "kis"
	testAccount  = "live"
	testQuote    = "FHKST01010100|/uapi/domestic-stock/v1/quotations/inquire-price"
	testCandles  = "FHKST03010230|/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
	testDaily    = "FHPSUS01010000|/uapi/domestic-stock/v1/quotations/inquire-daily-price"
)

// testClock is the injectable clock: time only moves when a test advances it.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// identityJitter makes backoff assertions exact.
func identityJitter(d time.Duration) time.Duration { return d }

func quoteKey() ratebudget.Key {
	return ratebudget.Key{Provider: testProvider, AccountRef: testAccount, Endpoint: testQuote}
}

// newLimiter builds one provider with the given global and endpoint limits and
// a fixed research share. Tests that assert bucket arithmetic pass share 1.
func newLimiter(t *testing.T, clock ratebudget.Clock, share float64, global ratebudget.Limit, endpoints ...ratebudget.Endpoint) *ratebudget.Limiter {
	t.Helper()
	l, err := ratebudget.New(ratebudget.Config{
		Providers: []ratebudget.Provider{{
			Name:       testProvider,
			AccountRef: testAccount,
			Limit:      global,
			Endpoints:  endpoints,
		}},
		ResearchShare: share,
		Clock:         clock,
		Jitter:        identityJitter,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return l
}

func mustAcquire(t *testing.T, l *ratebudget.Limiter, k ratebudget.Key) {
	t.Helper()
	if err := l.Acquire(k); err != nil {
		t.Fatalf("Acquire(%s): %v", k.Endpoint, err)
	}
}

// mustRefuse asserts the refusal code and returns the refusal.
func mustRefuse(t *testing.T, err error, want ratebudget.Code) *ratebudget.Refusal {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s refusal, request passed", want)
	}
	if got := ratebudget.CodeOf(err); got != want {
		t.Fatalf("refusal code = %q, want %q (err %v)", got, want, err)
	}
	var r *ratebudget.Refusal
	if !errors.As(err, &r) {
		t.Fatalf("error %v is not a *ratebudget.Refusal", err)
	}
	return r
}

func near(got, want, tolerance time.Duration) bool {
	d := got - want
	if d < 0 {
		d = -d
	}
	return d <= tolerance
}
