package ratebudget_test

import (
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/ratebudget"
)

// A probe is paid from the provider+account bucket like every other request.
// While a probe is due, the regular pass must leave it one token, so the regular
// poll cannot consume the probe's slot.
func TestRegularPassLeavesADueProbeItsSlot(t *testing.T) {
	clock := newTestClock()
	_, endpoint := highLimits()
	l := newLimiter(t, clock, 1, ratebudget.Limit{Rate: 1, Burst: 2},
		endpoint,
		ratebudget.Endpoint{Name: testCandles, Limit: endpoint.Limit})

	quote := quoteKey()
	candles := ratebudget.Key{Provider: testProvider, AccountRef: testAccount, Endpoint: testCandles}

	tripEndpoint(t, l, quote)
	clock.advance(30 * time.Second) // probe now due; provider bucket is full at 2

	mustAcquire(t, l, candles)                                    // regular pass: 2 -> 1, leaving the probe's token
	mustRefuse(t, l.Acquire(candles), ratebudget.CodeRateLimited) // would take the probe's slot
	mustAcquire(t, l, quote)                                      // the probe takes its reserved token
	mustRefuse(t, l.Acquire(candles), ratebudget.CodeRateLimited) // no reservation left; bucket is empty
}

// Probes and regular requests together never exceed the provider rate, and
// probes still run once per open window.
func TestProbePlusRegularNeverExceedsTheProviderRate(t *testing.T) {
	clock := newTestClock()
	_, endpoint := highLimits()
	const rate = 2.0
	const burst = 2
	l := newLimiter(t, clock, 1, ratebudget.Limit{Rate: rate, Burst: burst},
		endpoint,
		ratebudget.Endpoint{Name: testCandles, Limit: endpoint.Limit})

	quote := quoteKey()
	candles := ratebudget.Key{Provider: testProvider, AccountRef: testAccount, Endpoint: testCandles}
	tripEndpoint(t, l, quote)

	const seconds = 120
	const step = 50 * time.Millisecond
	granted, probes := 0, 0
	for elapsed := time.Duration(0); elapsed < seconds*time.Second; elapsed += step {
		clock.advance(step)
		if err := l.Acquire(candles); err == nil {
			granted++
		}
		if err := l.Acquire(quote); err == nil {
			granted++
			probes++
			l.Observe(quote, ratebudget.Outcome{StatusCode: 500}) // failed probe reopens the window
		}
	}

	limit := burst + rate*seconds
	if float64(granted) > limit+1 {
		t.Fatalf("granted %d requests in %ds, provider rate allows %.0f", granted, seconds, limit)
	}
	if probes < seconds/30-1 {
		t.Fatalf("probes granted = %d, want about one per 30s window", probes)
	}
}

// Probes stay bounded: the endpoint bucket still paces them, and only one probe
// is outstanding per window.
func TestProbesStayBoundedByTheEndpointBucket(t *testing.T) {
	clock := newTestClock()
	global, _ := highLimits()
	endpoint := ratebudget.Endpoint{Name: testQuote, Limit: ratebudget.Limit{Rate: 0.001, Burst: 1}}
	l := newLimiter(t, clock, 1, global, endpoint)
	quote := quoteKey()

	tripEndpoint(t, l, quote)
	clock.advance(30 * time.Second)
	mustAcquire(t, l, quote) // the first probe spends the endpoint's one token
	mustRefuse(t, l.Acquire(quote), ratebudget.CodeCircuitOpen)

	l.Observe(quote, ratebudget.Outcome{StatusCode: 500}) // probe fails, circuit reopens
	clock.advance(30 * time.Second)
	mustRefuse(t, l.Acquire(quote), ratebudget.CodeRateLimited)
}

// A due probe on an idle endpoint must not starve the regular pass when the
// provider bucket cannot hold a reserved token on top of one request. With
// burst 1 there is no room to reserve, so the regular pass still runs.
func TestIdleDueProbeDoesNotStarveTheRegularPassOnBurstOne(t *testing.T) {
	clock := newTestClock()
	_, endpoint := highLimits()
	l := newLimiter(t, clock, 1, ratebudget.Limit{Rate: 1, Burst: 1},
		endpoint,
		ratebudget.Endpoint{Name: testCandles, Limit: endpoint.Limit})

	candles := ratebudget.Key{Provider: testProvider, AccountRef: testAccount, Endpoint: testCandles}
	tripEndpoint(t, l, quoteKey())
	clock.advance(30 * time.Second) // quote's probe is due, but nobody probes it

	mustAcquire(t, l, candles)
	clock.advance(time.Second)
	mustAcquire(t, l, candles)
}
