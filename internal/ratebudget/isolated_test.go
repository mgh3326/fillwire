package ratebudget_test

import (
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/ratebudget"
)

func TestIsolatedEndpointDoesNotVoteForTheProviderCircuit(t *testing.T) {
	fast := ratebudget.Limit{Rate: 1000, Burst: 100}
	key := func(endpoint string) ratebudget.Key {
		return ratebudget.Key{Provider: "kis", AccountRef: "live", Endpoint: endpoint}
	}
	const first, second, side = "TR1|/one", "TR2|/two", "TR3|/side"
	limiter, err := ratebudget.New(ratebudget.Config{
		Providers: []ratebudget.Provider{{
			Name: "kis", AccountRef: "live", Limit: fast,
			Endpoints: []ratebudget.Endpoint{
				{Name: first, Limit: fast},
				{Name: second, Limit: fast},
				{Name: side, Limit: fast, Isolated: true},
			},
		}},
		ResearchShare: 1,
		Clock:         newTestClock(),
		Jitter:        func(d time.Duration) time.Duration { return d },
	})
	if err != nil {
		t.Fatal(err)
	}

	// With the isolated circuit and one voter open, the other voter still runs:
	// a counted third endpoint would make that two of three.
	tripEndpoint(t, limiter, key(side))
	tripEndpoint(t, limiter, key(first))
	mustAcquire(t, limiter, key(second))

	// Both voters open holds the provider, the isolated endpoint included.
	tripEndpoint(t, limiter, key(second))
	limiter.Observe(key(side), ratebudget.Outcome{StatusCode: 200})
	mustRefuse(t, limiter.Acquire(key(side)), ratebudget.CodeCircuitOpen)
}
