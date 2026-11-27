// Package ratebudget is the shared request budget for broker calls. It is
// pure scheduling: it imports no network, broker or database package. Callers
// acquire a request slot, run the call themselves, and report the outcome.
//
//	limiter, err := ratebudget.New(ratebudget.Config{Providers: []ratebudget.Provider{...}})
//	if err != nil { ... } // configuration is refused, not repaired
//	if err := limiter.Acquire(ratebudget.Key{Provider: "kis", AccountRef: "live", Endpoint: tr + "|" + path}); err != nil { ... }
//	retry := limiter.Observe(key, ratebudget.Outcome{StatusCode: 429, RetryAfter: 7 * time.Second})
//
// # Budgets
//
//   - Key: (provider, account_ref, endpoint); the endpoint is "TR_ID|path", the
//     same shape as auto_trader's kis_api_rate_limits.
//   - Hierarchical token buckets: one provider+account bucket, then one bucket
//     per endpoint below it. A request needs one token from both. A refusal
//     spends neither, and reports the longer of the two waits, so a retry at
//     that wait passes both layers.
//   - Research share: the provider+account bucket is scaled to Config.
//     ResearchShare (default 0.25) because a separate research process spends
//     the same broker limit. The share is fixed, not adaptive; endpoint buckets
//     are not scaled.
//   - One credential per provider: New refuses a second account_ref/key for the
//     same provider, and Acquire refuses a key whose account_ref is not the
//     configured one. Never rotate keys of one broker to escape its limit.
//   - Subscription slots: Slots reserves fixed consumers first, admits promoted
//     symbols into leftover slots and demotes by priority when capacity is
//     short.
//   - Coalescing: Group runs one call for identical request keys and shares its
//     result.
//   - Retry-After: a 429 closes the endpoint bucket for the header value; with
//     no header it uses jittered exponential backoff (1s→30s, ±20%).
//   - Circuits: Consecutive endpoint failures (default 5) open the endpoint for
//     30s, then allow one half-open probe. A provider is open while a majority
//     of its endpoints are unhealthy; an Isolated endpoint is not counted.
//
// A refusal is a *Refusal carrying one of the closed result codes
// (rate_limited, circuit_open, budget_cut) and the wait before the next
// attempt. There is no other refusal reason.
package ratebudget
