package decode

import "sync/atomic"

// Counters collects PR1 pipeline observations without exposing an HTTP
// endpoint. A later release can export this single snapshot type elsewhere.
type Counters struct {
	dropped        atomic.Uint64
	dupSuspect     atomic.Uint64
	xadded         atomic.Uint64
	xacked         atomic.Uint64
	xackShortfall  atomic.Uint64
	ingestFailures atomic.Uint64
	drainDropped   atomic.Uint64
}

// CounterSnapshot is a consistent-enough point-in-time view for diagnostics.
type CounterSnapshot struct {
	Dropped        uint64
	DupSuspect     uint64
	XAdded         uint64
	XAcked         uint64
	XAckShortfall  uint64
	IngestFailures uint64
	DrainDropped   uint64
}

// NewCounters allocates shared counters for the decoder, stream queue, and
// ingest runner.
func NewCounters() *Counters { return &Counters{} }

// Snapshot returns all pipeline counters without leaking credential material.
func (c *Counters) Snapshot() CounterSnapshot {
	if c == nil {
		return CounterSnapshot{}
	}
	return CounterSnapshot{
		Dropped:        c.dropped.Load(),
		DupSuspect:     c.dupSuspect.Load(),
		XAdded:         c.xadded.Load(),
		XAcked:         c.xacked.Load(),
		XAckShortfall:  c.xackShortfall.Load(),
		IngestFailures: c.ingestFailures.Load(),
		DrainDropped:   c.drainDropped.Load(),
	}
}

// IncDropped records a validation failure before Redis.
func (c *Counters) IncDropped() {
	if c != nil {
		c.dropped.Add(1)
	}
}

// IncDupSuspect records a repeated (order number, fill sequence) observation.
func (c *Counters) IncDupSuspect() {
	if c != nil {
		c.dupSuspect.Add(1)
	}
}

// IncXAdded records a successful Redis XADD.
func (c *Counters) IncXAdded() {
	if c != nil {
		c.xadded.Add(1)
	}
}

// AddXAcked records the count Redis confirmed as acknowledged.
func (c *Counters) AddXAcked(n uint64) {
	if c != nil && n > 0 {
		c.xacked.Add(n)
	}
}

// AddXAckShortfall records IDs that were already absent from the pending
// entries list when Redis processed a repeated XACK request.
func (c *Counters) AddXAckShortfall(n uint64) {
	if c != nil && n > 0 {
		c.xackShortfall.Add(n)
	}
}

// IncIngestFailure records a failed or untrustworthy ingest attempt.
func (c *Counters) IncIngestFailure() {
	if c != nil {
		c.ingestFailures.Add(1)
	}
}

// AddDrainDropped records records left behind only after the bounded shutdown
// drain expires. It is separate from validation drops.
func (c *Counters) AddDrainDropped(n uint64) {
	if c != nil && n > 0 {
		c.drainDropped.Add(n)
	}
}
