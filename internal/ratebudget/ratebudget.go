package ratebudget

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"sync"
	"time"
)

// Code is a closed refusal reason. A request either runs or is refused for one
// of these three reasons; there is no fourth.
type Code string

const (
	// CodeRateLimited means the provider, account or endpoint bucket has no
	// token left, or the endpoint is closed after a 429.
	CodeRateLimited Code = "rate_limited"
	// CodeCircuitOpen means the endpoint or provider circuit breaker is open.
	CodeCircuitOpen Code = "circuit_open"
	// CodeBudgetCut means no subscription slot is available for the request.
	CodeBudgetCut Code = "budget_cut"
)

// Refusal is returned by Acquire, Slots.Reserve and Slots.Admit. RetryAfter is
// the shortest wait that could let the request through; zero means "no wait
// helps, back off on your own".
type Refusal struct {
	Code       Code
	RetryAfter time.Duration
}

func (r *Refusal) Error() string {
	if r.RetryAfter > 0 {
		return fmt.Sprintf("ratebudget: %s (retry after %s)", r.Code, r.RetryAfter)
	}
	return "ratebudget: " + string(r.Code)
}

// CodeOf returns the refusal code of err, or "" when err is nil or is not a
// *Refusal.
func CodeOf(err error) Code {
	var r *Refusal
	if errors.As(err, &r) {
		return r.Code
	}
	return ""
}

var (
	// ErrConfig is wrapped by every configuration New and NewSlots reject.
	ErrConfig = errors.New("ratebudget: invalid configuration")
	// ErrUnknownKey is returned for a request key that New did not configure.
	ErrUnknownKey = errors.New("ratebudget: request key is not configured")
)

func configf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrConfig}, args...)...)
}

// Key identifies one request budget. Endpoint is "TR_ID|path".
type Key struct {
	Provider   string
	AccountRef string
	Endpoint   string
}

// Clock is the injectable time source. Now is the only operation the budget
// needs; callers do their own sleeping.
type Clock interface {
	Now() time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// Limit is a token bucket: Rate tokens per second, Burst tokens of capacity.
type Limit struct {
	Rate  float64
	Burst int
}

func (l Limit) validate(where string) error {
	if !(l.Rate > 0) {
		return configf("%s rate must be positive, got %v", where, l.Rate)
	}
	if l.Burst < 1 {
		return configf("%s burst must be at least 1, got %d", where, l.Burst)
	}
	return nil
}

// Endpoint is one broker endpoint with its own bucket. An Isolated endpoint's
// circuit does not count toward the provider circuit, so its failures never
// block the provider's other endpoints; it is still refused while the others
// hold the provider open.
type Endpoint struct {
	Name     string
	Limit    Limit
	Isolated bool
}

// Provider is one broker with exactly one account_ref. A second Provider with
// the same Name, or a second account_ref, is a configuration error: keys of one
// broker are never rotated to escape its limit.
type Provider struct {
	Name       string
	AccountRef string
	Limit      Limit
	Endpoints  []Endpoint
}

// Circuit configures the breakers. Zero values take the defaults 5 failures and
// a 30s open window.
type Circuit struct {
	OpenAfter int
	OpenFor   time.Duration
}

// Backoff configures retry delays. Zero values take the defaults 1s base, 30s
// cap and ±20% jitter. A deterministic delay uses Config.Jitter, because a zero
// Backoff.Jitter means the default jitter, not none.
type Backoff struct {
	Base   time.Duration
	Max    time.Duration
	Jitter float64
}

// Config is the whole limiter configuration. ResearchShare is the fixed share
// of every provider bucket this process may use; zero takes 0.25.
// Jitter, when set, replaces the random jitter with a deterministic function
// (tests); the default applies ±Backoff.Jitter uniformly.
type Config struct {
	Providers     []Provider
	ResearchShare float64
	Circuit       Circuit
	Backoff       Backoff
	Clock         Clock
	Jitter        func(time.Duration) time.Duration
}

const defaultResearchShare = 0.25

// Limiter hands out request slots for configured keys. It is safe for
// concurrent use.
type Limiter struct {
	clock   Clock
	circuit Circuit
	backoff Backoff
	jitter  func(time.Duration) time.Duration

	mu        sync.Mutex
	providers map[string]*providerState
}

type providerState struct {
	accountRef string
	global     *bucket
	openFor    time.Duration
	endpoints  map[string]*endpointState
	order      []*endpointState
}

type endpointState struct {
	bucket   *bucket
	breaker  *breaker
	failures int
	isolated bool
}

// New validates cfg and builds the limiter. It returns an error wrapping
// ErrConfig for every configuration fault, including a second key for one
// provider.
func New(cfg Config) (*Limiter, error) {
	share := cfg.ResearchShare
	if share == 0 {
		share = defaultResearchShare
	}
	if !(share > 0 && share <= 1) {
		return nil, configf("research share %v must be in (0,1]", share)
	}

	circuit := cfg.Circuit
	if circuit.OpenAfter == 0 {
		circuit.OpenAfter = 5
	}
	if circuit.OpenAfter < 0 {
		return nil, configf("circuit open_after %d must not be negative", circuit.OpenAfter)
	}
	if circuit.OpenFor == 0 {
		circuit.OpenFor = 30 * time.Second
	}
	if circuit.OpenFor < 0 {
		return nil, configf("circuit open_for %s must not be negative", circuit.OpenFor)
	}

	backoff := cfg.Backoff
	if backoff.Base == 0 {
		backoff.Base = time.Second
	}
	if backoff.Max == 0 {
		backoff.Max = 30 * time.Second
	}
	if backoff.Jitter == 0 {
		backoff.Jitter = 0.2
	}
	if backoff.Base < 0 || backoff.Max < 0 {
		return nil, configf("backoff base %s and max %s must not be negative", backoff.Base, backoff.Max)
	}
	if backoff.Base > backoff.Max {
		return nil, configf("backoff base %s exceeds max %s", backoff.Base, backoff.Max)
	}
	if math.IsNaN(backoff.Jitter) || backoff.Jitter < 0 || backoff.Jitter > 1 {
		return nil, configf("backoff jitter %v must be in [0,1]", backoff.Jitter)
	}

	clock := cfg.Clock
	if clock == nil {
		clock = wallClock{}
	}
	now := clock.Now()

	l := &Limiter{
		clock:     clock,
		circuit:   circuit,
		backoff:   backoff,
		jitter:    cfg.Jitter,
		providers: make(map[string]*providerState, len(cfg.Providers)),
	}
	for _, p := range cfg.Providers {
		if p.Name == "" {
			return nil, configf("provider name must not be empty")
		}
		if p.AccountRef == "" {
			return nil, configf("provider %q account_ref must not be empty", p.Name)
		}
		if _, dup := l.providers[p.Name]; dup {
			return nil, configf("provider %q is configured twice: one provider holds exactly one account_ref, and a second key is never used to escape a limit", p.Name)
		}
		if err := p.Limit.validate(fmt.Sprintf("provider %q", p.Name)); err != nil {
			return nil, err
		}
		ps := &providerState{
			accountRef: p.AccountRef,
			global:     newBucket(scaled(p.Limit, share), now),
			openFor:    circuit.OpenFor,
			endpoints:  make(map[string]*endpointState, len(p.Endpoints)),
		}
		for _, e := range p.Endpoints {
			if !validEndpointName(e.Name) {
				return nil, configf("provider %q endpoint %q must be TR_ID|path with a non-empty TR_ID and a path starting with /", p.Name, e.Name)
			}
			if _, dup := ps.endpoints[e.Name]; dup {
				return nil, configf("provider %q endpoint %q is configured twice", p.Name, e.Name)
			}
			if err := e.Limit.validate(fmt.Sprintf("provider %q endpoint %q", p.Name, e.Name)); err != nil {
				return nil, err
			}
			ep := &endpointState{
				bucket:   newBucket(e.Limit, now),
				breaker:  &breaker{openAfter: circuit.OpenAfter, openFor: circuit.OpenFor},
				isolated: e.Isolated,
			}
			ps.endpoints[e.Name] = ep
			ps.order = append(ps.order, ep)
		}
		l.providers[p.Name] = ps
	}
	return l, nil
}

func validEndpointName(name string) bool {
	tr, path, ok := strings.Cut(name, "|")
	return ok && tr != "" && strings.HasPrefix(path, "/") && !strings.Contains(path, "|")
}

// scaled applies the research share to a provider bucket. The endpoint buckets
// keep their configured limit.
func scaled(l Limit, share float64) Limit {
	if share == 1 {
		return l
	}
	burst := int(math.Floor(float64(l.Burst) * share))
	if burst < 1 {
		burst = 1
	}
	return Limit{Rate: l.Rate * share, Burst: burst}
}

func (l *Limiter) lookup(k Key) (*endpointState, *providerState, error) {
	ps, ok := l.providers[k.Provider]
	if !ok {
		return nil, nil, fmt.Errorf("%w: provider %q", ErrUnknownKey, k.Provider)
	}
	if k.AccountRef != ps.accountRef {
		return nil, nil, fmt.Errorf("%w: provider %q holds account_ref %q, not %q", ErrUnknownKey, k.Provider, ps.accountRef, k.AccountRef)
	}
	ep, ok := ps.endpoints[k.Endpoint]
	if !ok {
		return nil, nil, fmt.Errorf("%w: provider %q endpoint %q", ErrUnknownKey, k.Provider, k.Endpoint)
	}
	return ep, ps, nil
}

// Acquire takes one request slot for k. It returns nil when the request may
// run, or a *Refusal with one of the closed codes. A refusal spends no token:
// both buckets must be ready before either is spent, and the refusal's
// RetryAfter is the longer of the two waits.
func (l *Limiter) Acquire(k Key) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	ep, ps, err := l.lookup(k)
	if err != nil {
		return err
	}
	now := l.clock.Now()

	allowed, probe, wait := ep.breaker.gate(now)
	if !allowed {
		return &Refusal{Code: CodeCircuitOpen, RetryAfter: wait}
	}
	if !probe {
		if blocked, wait := ps.blocked(now); blocked {
			return &Refusal{Code: CodeCircuitOpen, RetryAfter: wait}
		}
	}
	// Every request pays from the provider+account bucket, the broker-facing
	// limit, probes included. A regular request must leave one token for each
	// probe that is due, so the regular pass cannot take a probe's slot; a probe
	// needs only the token itself. Each endpoint still pays its own bucket.
	endpointWait := ep.bucket.ready(now)
	hold := 0
	if !probe {
		hold = ps.probesDue(now)
	}
	providerWait := ps.global.readyHolding(now, hold)
	if wait := max(endpointWait, providerWait); wait > 0 {
		ep.breaker.abandonProbe()
		return &Refusal{Code: CodeRateLimited, RetryAfter: wait}
	}
	ep.bucket.take(now)
	ps.global.take(now)
	return nil
}

// Outcome is what the caller observed for one request.
//
// StatusCode is the HTTP status, or 0 when Err carries a transport failure.
// RateLimited marks a broker rate-limit response that is not an HTTP 429 (for
// example a KIS msg_cd). RetryAfter is the parsed Retry-After header.
type Outcome struct {
	StatusCode  int
	Err         error
	RetryAfter  time.Duration
	RateLimited bool
}

const statusTooManyRequests = 429

// Observe records the outcome for k and returns how long the endpoint stays
// closed. Zero means the endpoint may be used at once.
//
// A 429 (or RateLimited) closes the endpoint bucket for Retry-After, or for the
// jittered exponential delay when the header is absent, and does not count as a
// circuit failure. A transport error or 5xx adds a circuit failure and closes
// the endpoint for the same delay. Any other answer counts as a success and
// resets the endpoint. A key New did not configure is ignored and returns 0.
func (l *Limiter) Observe(k Key, o Outcome) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	ep, _, err := l.lookup(k)
	if err != nil {
		return 0
	}
	now := l.clock.Now()
	switch {
	case o.RateLimited || o.StatusCode == statusTooManyRequests:
		ep.failures++
		retry := o.RetryAfter
		if retry <= 0 {
			retry = l.retryDelay(ep.failures)
		}
		ep.bucket.close(now.Add(retry))
		ep.breaker.releaseProbe()
		return retry
	case o.Err != nil || o.StatusCode >= 500:
		ep.failures++
		retry := l.retryDelay(ep.failures)
		ep.bucket.close(now.Add(retry))
		ep.breaker.failure(now)
		return retry
	default:
		ep.failures = 0
		ep.breaker.success()
		return 0
	}
}

// retryDelay is the jittered exponential delay for the n-th consecutive
// failure: Base doubled per step, capped at Max.
func (l *Limiter) retryDelay(failures int) time.Duration {
	d := l.backoff.Base
	for i := 1; i < failures; i++ {
		if d >= l.backoff.Max {
			break
		}
		d *= 2
	}
	if d > l.backoff.Max {
		d = l.backoff.Max
	}
	return l.jittered(d)
}

func (l *Limiter) jittered(d time.Duration) time.Duration {
	if l.jitter != nil {
		return l.jitter(d)
	}
	f := l.backoff.Jitter
	if f <= 0 {
		return d
	}
	d = time.Duration(float64(d) * (1 + (rand.Float64()*2-1)*f))
	if d < 0 {
		d = 0
	}
	return d
}

// probesDue counts the endpoints whose half-open probe is due now, capped at
// burst-1. Each one holds one provider token back from the regular pass until
// its probe takes it. The cap is needed because the bucket never holds more
// than burst tokens: a larger hold would refuse every regular request forever,
// for example an idle endpoint left past its open window on a burst-1 provider.
func (p *providerState) probesDue(now time.Time) int {
	due := 0
	for _, ep := range p.order {
		if ep.breaker.probeDue(now) {
			due++
		}
	}
	return min(due, max(int(p.global.burst)-1, 0))
}

// blocked reports whether a majority of the provider's endpoints, isolated ones
// aside, are unhealthy. The wait is the longest endpoint window, at least one
// open window, because only a probe can close the provider.
func (p *providerState) blocked(now time.Time) (bool, time.Duration) {
	voters, tripped := 0, 0
	var wait time.Duration
	for _, ep := range p.order {
		if ep.isolated {
			continue
		}
		voters++
		if !ep.breaker.unhealthy() {
			continue
		}
		tripped++
		if w := ep.breaker.wait(now); w > wait {
			wait = w
		}
	}
	if 2*tripped <= voters {
		return false, 0
	}
	if wait <= 0 {
		wait = p.openFor
	}
	return true, wait
}
