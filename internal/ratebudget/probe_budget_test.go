package ratebudget_test

import (
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/ratebudget"
)

// slowGlobal is a provider bucket that refills far too slowly to matter within
// one test: it starts with one token and stays at zero after that one spend.
func slowGlobal() ratebudget.Limit {
	return ratebudget.Limit{Rate: 0.001, Burst: 1}
}

// A half-open probe must not spend the provider+account bucket. The regular
// poll's token is still there after the probe runs.
func TestProbeDoesNotSpendTheProviderBudget(t *testing.T) {
	clock := newTestClock()
	_, endpoint := highLimits()
	l := newLimiter(t, clock, 1, slowGlobal(),
		endpoint,
		ratebudget.Endpoint{Name: testCandles, Limit: endpoint.Limit})

	quote := quoteKey()
	candles := ratebudget.Key{Provider: testProvider, AccountRef: testAccount, Endpoint: testCandles}

	tripEndpoint(t, l, quote)
	clock.advance(30 * time.Second)

	mustAcquire(t, l, quote)   // half-open probe: pays from its own endpoint bucket
	mustAcquire(t, l, candles) // the provider token the probe did not take
}

// A regular poll that drained the provider budget must not starve a probe.
func TestRegularPollCannotStarveAProbe(t *testing.T) {
	clock := newTestClock()
	_, endpoint := highLimits()
	l := newLimiter(t, clock, 1, slowGlobal(),
		endpoint,
		ratebudget.Endpoint{Name: testCandles, Limit: endpoint.Limit})

	quote := quoteKey()
	candles := ratebudget.Key{Provider: testProvider, AccountRef: testAccount, Endpoint: testCandles}

	tripEndpoint(t, l, quote)
	clock.advance(30 * time.Second)

	mustAcquire(t, l, candles) // regular poll takes the only provider token
	mustAcquire(t, l, quote)   // probe still gets through
}

// Probes stay bounded: the endpoint bucket still paces them, and the single
// outstanding probe rule still holds.
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
