package ratebudget_test

import (
	"errors"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/ratebudget"
)

func TestBucketsNeedBothTokens(t *testing.T) {
	clock := newTestClock()
	l := newLimiter(t, clock, 1, ratebudget.Limit{Rate: 100, Burst: 10},
		ratebudget.Endpoint{Name: testQuote, Limit: ratebudget.Limit{Rate: 2, Burst: 2}})
	k := quoteKey()

	mustAcquire(t, l, k)
	mustAcquire(t, l, k)
	r := mustRefuse(t, l.Acquire(k), ratebudget.CodeRateLimited)
	if !near(r.RetryAfter, 500*time.Millisecond, time.Millisecond) {
		t.Fatalf("endpoint wait = %s, want 500ms", r.RetryAfter)
	}

	clock.advance(250 * time.Millisecond)
	r = mustRefuse(t, l.Acquire(k), ratebudget.CodeRateLimited)
	if !near(r.RetryAfter, 250*time.Millisecond, time.Millisecond) {
		t.Fatalf("half-refilled wait = %s, want 250ms", r.RetryAfter)
	}

	clock.advance(250 * time.Millisecond)
	mustAcquire(t, l, k)
}

func TestGlobalBucketRefusesAndRefundsEndpoint(t *testing.T) {
	clock := newTestClock()
	// The provider bucket has one token; the endpoint has room but refills
	// slowly, so a double charge would still be visible a second later.
	l := newLimiter(t, clock, 1, ratebudget.Limit{Rate: 1, Burst: 1},
		ratebudget.Endpoint{Name: testQuote, Limit: ratebudget.Limit{Rate: 0.5, Burst: 2}})
	k := quoteKey()

	mustAcquire(t, l, k)

	// The endpoint token is taken, then the provider bucket refuses; the
	// endpoint token must come back.
	clock.advance(10 * time.Millisecond)
	r := mustRefuse(t, l.Acquire(k), ratebudget.CodeRateLimited)
	if !near(r.RetryAfter, 990*time.Millisecond, 2*time.Millisecond) {
		t.Fatalf("provider wait = %s, want 990ms", r.RetryAfter)
	}

	// Without the refund the endpoint keeps only the 0.5 token it refilled
	// since the failed attempt and refuses; with it, 1.5 tokens remain and
	// this request passes.
	clock.advance(1010 * time.Millisecond)
	mustAcquire(t, l, k)
}

func TestResearchShareScalesProviderBucket(t *testing.T) {
	clock := newTestClock()
	// Half of 10/s and half of burst 4 is 5/s and burst 2.
	l := newLimiter(t, clock, 0.5, ratebudget.Limit{Rate: 10, Burst: 4},
		ratebudget.Endpoint{Name: testQuote, Limit: ratebudget.Limit{Rate: 100, Burst: 10}})
	k := quoteKey()

	mustAcquire(t, l, k)
	mustAcquire(t, l, k)
	r := mustRefuse(t, l.Acquire(k), ratebudget.CodeRateLimited)
	if !near(r.RetryAfter, 200*time.Millisecond, 2*time.Millisecond) {
		t.Fatalf("shared bucket wait = %s, want 200ms", r.RetryAfter)
	}
	clock.advance(200 * time.Millisecond)
	mustAcquire(t, l, k)
}

func TestDefaultResearchShareIsQuarter(t *testing.T) {
	clock := newTestClock()
	l, err := ratebudget.New(ratebudget.Config{
		Providers: []ratebudget.Provider{{
			Name:       testProvider,
			AccountRef: testAccount,
			Limit:      ratebudget.Limit{Rate: 8, Burst: 4},
			Endpoints:  []ratebudget.Endpoint{{Name: testQuote, Limit: ratebudget.Limit{Rate: 100, Burst: 10}}},
		}},
		Clock:  clock,
		Jitter: identityJitter,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	k := quoteKey()
	mustAcquire(t, l, k)
	r := mustRefuse(t, l.Acquire(k), ratebudget.CodeRateLimited)
	if !near(r.RetryAfter, 500*time.Millisecond, 2*time.Millisecond) {
		t.Fatalf("default share wait = %s, want 500ms", r.RetryAfter)
	}
}

func TestRefusalCodesAreClosed(t *testing.T) {
	if ratebudget.CodeRateLimited != "rate_limited" ||
		ratebudget.CodeCircuitOpen != "circuit_open" ||
		ratebudget.CodeBudgetCut != "budget_cut" {
		t.Fatal("refusal codes changed")
	}
	if ratebudget.CodeOf(nil) != "" {
		t.Fatal("CodeOf(nil) is not empty")
	}
	if ratebudget.CodeOf(errors.New("other")) != "" {
		t.Fatal("CodeOf of a foreign error is not empty")
	}
	slots, err := ratebudget.NewSlots(0)
	if err != nil {
		t.Fatalf("NewSlots: %v", err)
	}
	_, err = slots.Admit("sym", 1)
	mustRefuse(t, err, ratebudget.CodeBudgetCut)
}

func TestConfigRefusesSecondKeyForOneProvider(t *testing.T) {
	limit := ratebudget.Limit{Rate: 1, Burst: 1}
	endpoint := ratebudget.Endpoint{Name: testQuote, Limit: limit}
	for _, tc := range []struct {
		name      string
		providers []ratebudget.Provider
	}{
		{
			name: "different account_ref",
			providers: []ratebudget.Provider{
				{Name: testProvider, AccountRef: "live", Limit: limit, Endpoints: []ratebudget.Endpoint{endpoint}},
				{Name: testProvider, AccountRef: "paper", Limit: limit, Endpoints: []ratebudget.Endpoint{endpoint}},
			},
		},
		{
			name: "same account_ref twice",
			providers: []ratebudget.Provider{
				{Name: testProvider, AccountRef: "live", Limit: limit, Endpoints: []ratebudget.Endpoint{endpoint}},
				{Name: testProvider, AccountRef: "live", Limit: limit, Endpoints: []ratebudget.Endpoint{endpoint}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ratebudget.New(ratebudget.Config{Providers: tc.providers, ResearchShare: 1})
			if !errors.Is(err, ratebudget.ErrConfig) {
				t.Fatalf("second key error = %v, want ErrConfig", err)
			}
		})
	}
}

func TestConfigRejectsInvalidInput(t *testing.T) {
	validLimit := ratebudget.Limit{Rate: 1, Burst: 1}
	validEndpoint := ratebudget.Endpoint{Name: testQuote, Limit: validLimit}
	validProvider := ratebudget.Provider{Name: testProvider, AccountRef: testAccount, Limit: validLimit, Endpoints: []ratebudget.Endpoint{validEndpoint}}

	cases := map[string]ratebudget.Config{
		"empty provider": {Providers: []ratebudget.Provider{{AccountRef: testAccount, Limit: validLimit, Endpoints: []ratebudget.Endpoint{validEndpoint}}}},
		"empty account":  {Providers: []ratebudget.Provider{{Name: testProvider, Limit: validLimit, Endpoints: []ratebudget.Endpoint{validEndpoint}}}},
		"zero rate":      {Providers: []ratebudget.Provider{{Name: testProvider, AccountRef: testAccount, Limit: ratebudget.Limit{Burst: 1}, Endpoints: []ratebudget.Endpoint{validEndpoint}}}},
		"zero burst":     {Providers: []ratebudget.Provider{{Name: testProvider, AccountRef: testAccount, Limit: ratebudget.Limit{Rate: 1}, Endpoints: []ratebudget.Endpoint{validEndpoint}}}},
		"endpoint without pipe": {
			Providers: []ratebudget.Provider{{Name: testProvider, AccountRef: testAccount, Limit: validLimit,
				Endpoints: []ratebudget.Endpoint{{Name: "FHKST01010100", Limit: validLimit}}}},
		},
		"endpoint without tr": {
			Providers: []ratebudget.Provider{{Name: testProvider, AccountRef: testAccount, Limit: validLimit,
				Endpoints: []ratebudget.Endpoint{{Name: "|/uapi/quotation", Limit: validLimit}}}},
		},
		"endpoint without path": {
			Providers: []ratebudget.Provider{{Name: testProvider, AccountRef: testAccount, Limit: validLimit,
				Endpoints: []ratebudget.Endpoint{{Name: "FHKST01010100|uapi/quotation", Limit: validLimit}}}},
		},
		"duplicate endpoint": {
			Providers: []ratebudget.Provider{{Name: testProvider, AccountRef: testAccount, Limit: validLimit,
				Endpoints: []ratebudget.Endpoint{validEndpoint, validEndpoint}}},
		},
		"share above one":   {Providers: []ratebudget.Provider{validProvider}, ResearchShare: 1.5},
		"negative share":    {Providers: []ratebudget.Provider{validProvider}, ResearchShare: -0.5},
		"negative failures": {Providers: []ratebudget.Provider{validProvider}, ResearchShare: 1, Circuit: ratebudget.Circuit{OpenAfter: -1}},
		"negative window":   {Providers: []ratebudget.Provider{validProvider}, ResearchShare: 1, Circuit: ratebudget.Circuit{OpenFor: -time.Second}},
		"base above max":    {Providers: []ratebudget.Provider{validProvider}, ResearchShare: 1, Backoff: ratebudget.Backoff{Base: 2 * time.Second, Max: time.Second}},
		"jitter above one":  {Providers: []ratebudget.Provider{validProvider}, ResearchShare: 1, Backoff: ratebudget.Backoff{Jitter: 1.5}},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ratebudget.New(cfg); !errors.Is(err, ratebudget.ErrConfig) {
				t.Fatalf("New error = %v, want ErrConfig", err)
			}
		})
	}
}

func TestUnknownKeysAreRefused(t *testing.T) {
	clock := newTestClock()
	l := newLimiter(t, clock, 1, ratebudget.Limit{Rate: 10, Burst: 10},
		ratebudget.Endpoint{Name: testQuote, Limit: ratebudget.Limit{Rate: 10, Burst: 10}})
	for _, k := range []ratebudget.Key{
		{Provider: "upbit", AccountRef: testAccount, Endpoint: testQuote},
		{Provider: testProvider, AccountRef: "paper", Endpoint: testQuote},
		{Provider: testProvider, AccountRef: testAccount, Endpoint: testCandles},
	} {
		if err := l.Acquire(k); !errors.Is(err, ratebudget.ErrUnknownKey) {
			t.Fatalf("Acquire(%+v) error = %v, want ErrUnknownKey", k, err)
		}
	}
}
