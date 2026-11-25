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

// take removes one token and reports whether it could. On refusal the wait is
// the time until one token exists, rounded up so an immediate retry does not
// spin on a fractional deficit.
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

// refund gives back a token take removed. A caller uses it when a second layer
// refused the same request.
func (b *bucket) refund() {
	b.tokens++
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
}

// close shuts the bucket until at least until. An earlier close already on the
// bucket is extended, never shortened.
func (b *bucket) close(until time.Time) {
	if until.After(b.closed) {
		b.closed = until
	}
}
