package ratebudget

import (
	"math"
	"time"
)

// bucket is a token bucket that also carries an endpoint close window. Its
// zero-value time is never used: newBucket seeds Tokens full at the clock now.
type bucket struct {
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	closed time.Time
}

func newBucket(l Limit, now time.Time) *bucket {
	return &bucket{rate: l.Rate, burst: float64(l.Burst), tokens: float64(l.Burst), last: now}
}

func (b *bucket) refill(now time.Time) {
	if !now.After(b.last) {
		return
	}
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
}

// ready reports zero when a token is available and otherwise the wait until one
// is, without spending it. The wait is rounded up so an immediate retry does not
// spin on a fractional deficit. A close window is reported first. ready refills
// the bucket but never removes a token, so a caller can price both layers of a
// request before spending either.
func (b *bucket) ready(now time.Time) time.Duration {
	return b.readyHolding(now, 0)
}

// readyHolding is ready for a caller that must leave hold tokens in the bucket
// for others: it waits until hold+1 tokens are available.
func (b *bucket) readyHolding(now time.Time, hold int) time.Duration {
	if now.Before(b.closed) {
		return b.closed.Sub(now)
	}
	b.refill(now)
	if b.tokens >= float64(hold)+1 {
		return 0
	}
	need := (float64(hold) + 1 - b.tokens) / b.rate
	wait := time.Duration(math.Ceil(need * float64(time.Second)))
	if wait <= 0 {
		wait = time.Nanosecond
	}
	return wait
}

// take removes one token and reports whether it could. Callers use ready first;
// take is kept for the layer that spends after both layers are ready. On
// refusal the wait matches ready.
func (b *bucket) take(now time.Time) (bool, time.Duration) {
	if now.Before(b.closed) {
		return false, b.closed.Sub(now)
	}
	b.refill(now)
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	need := (1 - b.tokens) / b.rate
	wait := time.Duration(math.Ceil(need * float64(time.Second)))
	if wait <= 0 {
		wait = time.Nanosecond
	}
	return false, wait
}

// close shuts the bucket until at least until. An earlier close already on the
// bucket is extended, never shortened.
func (b *bucket) close(until time.Time) {
	if until.After(b.closed) {
		b.closed = until
	}
}
