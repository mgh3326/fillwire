package ratebudget_test

import (
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/ratebudget"
)

// tripEndpoint sends five 500 outcomes, the default circuit threshold.
func tripEndpoint(t *testing.T, l *ratebudget.Limiter, k ratebudget.Key) {
	t.Helper()
	for range 5 {
		l.Observe(k, ratebudget.Outcome{StatusCode: 500})
	}
}

func TestEndpointCircuitClosedOpenHalfOpenClosed(t *testing.T) {
	clock := newTestClock()
	global, endpoint := highLimits()
	l := newLimiter(t, clock, 1, global, endpoint)
	k := quoteKey()

	tripEndpoint(t, l, k)
	r := mustRefuse(t, l.Acquire(k), ratebudget.CodeCircuitOpen)
	if r.RetryAfter != 30*time.Second {
		t.Fatalf("open wait = %s, want 30s", r.RetryAfter)
	}

	clock.advance(29 * time.Second)
	r = mustRefuse(t, l.Acquire(k), ratebudget.CodeCircuitOpen)
	if r.RetryAfter != time.Second {
		t.Fatalf("wait before window end = %s, want 1s", r.RetryAfter)
	}

	clock.advance(time.Second)
	mustAcquire(t, l, k) // the single half-open probe

	r = mustRefuse(t, l.Acquire(k), ratebudget.CodeCircuitOpen)
	if r.RetryAfter != 0 {
		t.Fatalf("second probe wait = %s, want 0", r.RetryAfter)
	}

	if got := l.Observe(k, ratebudget.Outcome{StatusCode: 200}); got != 0 {
		t.Fatalf("success cooldown = %s, want 0", got)
	}
	mustAcquire(t, l, k) // closed again
}

func TestHalfOpenProbeFailureReopensTheCircuit(t *testing.T) {
	clock := newTestClock()
	global, endpoint := highLimits()
	l := newLimiter(t, clock, 1, global, endpoint)
	k := quoteKey()

	tripEndpoint(t, l, k)
	clock.advance(30 * time.Second)
	mustAcquire(t, l, k) // probe

	if got := l.Observe(k, ratebudget.Outcome{StatusCode: 500}); got != 30*time.Second {
		t.Fatalf("probe failure backoff = %s, want the 30s cap", got)
	}
	r := mustRefuse(t, l.Acquire(k), ratebudget.CodeCircuitOpen)
	if r.RetryAfter != 30*time.Second {
		t.Fatalf("reopened wait = %s, want 30s", r.RetryAfter)
	}
	clock.advance(30 * time.Second)
	mustAcquire(t, l, k)
}

func TestSuccessResetsConsecutiveFailures(t *testing.T) {
	clock := newTestClock()
	global, endpoint := highLimits()
	l := newLimiter(t, clock, 1, global, endpoint)
	k := quoteKey()

	for range 4 {
		l.Observe(k, ratebudget.Outcome{StatusCode: 500})
	}
	l.Observe(k, ratebudget.Outcome{StatusCode: 200})
	for range 4 {
		l.Observe(k, ratebudget.Outcome{StatusCode: 500})
	}
	// Eight failures total, but never five in a row.
	clock.advance(8 * time.Second)
	mustAcquire(t, l, k)
}

func TestLostProbeExpires(t *testing.T) {
	clock := newTestClock()
	global, endpoint := highLimits()
	l := newLimiter(t, clock, 1, global, endpoint)
	k := quoteKey()

	tripEndpoint(t, l, k)
	clock.advance(30 * time.Second)
	mustAcquire(t, l, k) // probe whose outcome is never reported

	mustRefuse(t, l.Acquire(k), ratebudget.CodeCircuitOpen)
	clock.advance(29 * time.Second)
	mustRefuse(t, l.Acquire(k), ratebudget.CodeCircuitOpen)
	clock.advance(time.Second) // one full open window after the probe started
	mustAcquire(t, l, k)
}

func TestProviderCircuitOpensOnEndpointMajority(t *testing.T) {
	clock := newTestClock()
	global, endpoint := highLimits()
	l := newLimiter(t, clock, 1, global,
		endpoint,
		ratebudget.Endpoint{Name: testCandles, Limit: endpoint.Limit},
		ratebudget.Endpoint{Name: testDaily, Limit: endpoint.Limit})

	quote := ratebudget.Key{Provider: testProvider, AccountRef: testAccount, Endpoint: testQuote}
	candles := ratebudget.Key{Provider: testProvider, AccountRef: testAccount, Endpoint: testCandles}
	daily := ratebudget.Key{Provider: testProvider, AccountRef: testAccount, Endpoint: testDaily}

	tripEndpoint(t, l, quote)
	tripEndpoint(t, l, candles)

	// Two of three endpoints are unhealthy, so the healthy one is refused by
	// the provider circuit.
	r := mustRefuse(t, l.Acquire(daily), ratebudget.CodeCircuitOpen)
	if r.RetryAfter != 30*time.Second {
		t.Fatalf("provider wait = %s, want 30s", r.RetryAfter)
	}

	clock.advance(30 * time.Second)
	mustAcquire(t, l, quote) // probe
	l.Observe(quote, ratebudget.Outcome{StatusCode: 200})
	// One of three is still unhealthy: no majority, so the healthy endpoint is
	// usable again.
	mustAcquire(t, l, daily)
}

func TestProviderProbeBypassesTheProviderCircuit(t *testing.T) {
	clock := newTestClock()
	global, endpoint := highLimits()
	l := newLimiter(t, clock, 1, global,
		endpoint,
		ratebudget.Endpoint{Name: testCandles, Limit: endpoint.Limit})

	quote := ratebudget.Key{Provider: testProvider, AccountRef: testAccount, Endpoint: testQuote}
	candles := ratebudget.Key{Provider: testProvider, AccountRef: testAccount, Endpoint: testCandles}

	tripEndpoint(t, l, quote)
	tripEndpoint(t, l, candles)
	clock.advance(30 * time.Second)

	// Both endpoints are open, so the provider is open, but each endpoint's
	// half-open probe must still get through.
	mustAcquire(t, l, quote)
	mustAcquire(t, l, candles)
}
