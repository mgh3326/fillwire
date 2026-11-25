package ratebudget

import "time"

type breakerState int

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

// breaker is one circuit: closed, open for a window, then half-open with a
// single outstanding probe.
type breaker struct {
	openAfter int
	openFor   time.Duration

	state     breakerState
	failures  int
	openUntil time.Time
	probing   bool
	probeAt   time.Time
}

// gate reports whether a request may run and whether it is the half-open probe.
// It starts the open window's half-open phase lazily when the window elapsed,
// and reserves the single probe. A refusal carries the remaining open window,
// or zero while another probe is outstanding. A probe whose caller never
// reports an outcome expires after one open window, so a lost probe cannot
// wedge the circuit.
func (b *breaker) gate(now time.Time) (allowed, probe bool, wait time.Duration) {
	switch b.state {
	case breakerOpen:
		if now.Before(b.openUntil) {
			return false, false, b.openUntil.Sub(now)
		}
		b.state = breakerHalfOpen
		b.probing = false
		fallthrough
	case breakerHalfOpen:
		if b.probing && now.Sub(b.probeAt) < b.openFor {
			return false, false, 0
		}
		b.probing = true
		b.probeAt = now
		return true, true, 0
	default:
		return true, false, 0
	}
}

// abandonProbe releases a probe reserved by gate when the request was refused
// by a later layer and never ran.
func (b *breaker) abandonProbe() {
	if b.state == breakerHalfOpen {
		b.probing = false
	}
}

// success closes the circuit and clears the consecutive-failure count.
func (b *breaker) success() {
	b.state = breakerClosed
	b.failures = 0
	b.probing = false
	b.probeAt = time.Time{}
	b.openUntil = time.Time{}
}

// failure counts one failure. A closed circuit opens on the openAfter-th
// consecutive failure; a failed probe reopens the full window.
func (b *breaker) failure(now time.Time) {
	switch b.state {
	case breakerHalfOpen:
		b.state = breakerOpen
		b.openUntil = now.Add(b.openFor)
		b.probing = false
		b.failures = 0
	case breakerClosed:
		b.failures++
		if b.failures >= b.openAfter {
			b.state = breakerOpen
			b.openUntil = now.Add(b.openFor)
			b.failures = 0
		}
	}
}

// unhealthy reports whether the circuit is not closed. An open circuit whose
// window elapsed still counts until its probe succeeds, which is what the
// provider-level majority reads.
func (b *breaker) unhealthy() bool {
	return b.state != breakerClosed
}

// wait is the remaining open window, or zero outside an open window.
func (b *breaker) wait(now time.Time) time.Duration {
	if b.state == breakerOpen && now.Before(b.openUntil) {
		return b.openUntil.Sub(now)
	}
	return 0
}
