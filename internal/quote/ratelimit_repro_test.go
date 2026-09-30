package quote

// The two reproductions below are the Toss round-2 tester's own tests
// (t1050-verify-toss-r2, codex-sol; evidence-codex-toss-r2 in the builder job
// directory), adopted unchanged apart from this header. They measure actual
// Read returns and declaration writes, not enqueue time.

import (
	"bytes"
	"context"
	"testing"
	"time"
)

var r2RateFrame = []byte(`{"type":"error","error":{"code":"rate-limit-exceeded","message":"synthetic r2 refusal"}}`)

// Observe actual Read returns and declaration Write entries, not enqueue time.
type r2ObservedDialer struct {
	base   *fakeDialer
	reads  chan time.Time
	writes chan time.Time
}

func (d *r2ObservedDialer) Dial(ctx context.Context, endpoint, token string) (Transport, error) {
	c, err := d.base.Dial(ctx, endpoint, token)
	if err != nil {
		return nil, err
	}
	return &r2ObservedConn{Transport: c, owner: d}, nil
}

type r2ObservedConn struct {
	Transport
	owner *r2ObservedDialer
}

func (c *r2ObservedConn) Read(ctx context.Context) ([]byte, error) {
	raw, err := c.Transport.Read(ctx)
	if err == nil && bytes.Contains(raw, []byte("rate-limit-exceeded")) {
		c.owner.reads <- time.Now()
	}
	return raw, err
}

func (c *r2ObservedConn) Write(ctx context.Context, data []byte) error {
	if bytes.HasPrefix(data, []byte("[")) {
		c.owner.writes <- time.Now()
	}
	return c.Transport.Write(ctx, data)
}

func r2ObservedLane(t *testing.T) (*lane, *r2ObservedDialer, *fakeConn) {
	t.Helper()
	d := &r2ObservedDialer{reads: make(chan time.Time, 128), writes: make(chan time.Time, 128)}
	l := startLane(t, wednesdayMidday, ackAll, func(cfg *Config, l *lane) {
		krOnly(cfg, l)
		cfg.PingInterval = time.Hour
		d.base = l.dialer
		cfg.Dialer = d
	})
	c := <-l.dialer.dialed
	r2Time(t, d.writes, "initial declaration")
	return l, d, c
}

func r2Time(t *testing.T, ch <-chan time.Time, what string) time.Time {
	t.Helper()
	select {
	case at := <-ch:
		return at
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %s", what)
		return time.Time{}
	}
}

func TestR2EveryQueuedRateFrameExtendsCooldown(t *testing.T) {
	l, d, c := r2ObservedLane(t)
	c.push(r2RateFrame)
	first := r2Time(t, d.reads, "first rate frame read")
	waitFor(t, "first error handled", func() bool { return l.runner.Counters().ErrorFrames.Load() == 1 })
	time.Sleep(600 * time.Millisecond)
	c.push(r2RateFrame)
	second := r2Time(t, d.reads, "second rate frame read while declare waits")
	at := r2Time(t, d.writes, "next declaration")
	t.Logf("first receipt to next declaration=%s; latest receipt to next declaration=%s", at.Sub(first), at.Sub(second))
	if gap := at.Sub(second); gap < 950*time.Millisecond {
		t.Fatalf("next declaration only %s after latest received rate frame; want approximately 1s", gap)
	}
}

func TestR2RedeclareExhaustionPreservesCooldownAcrossReconnect(t *testing.T) {
	l, d, c := r2ObservedLane(t)
	for attempt := 0; attempt < maxRedeclares; attempt++ {
		c.push(r2RateFrame)
		receipt := r2Time(t, d.reads, "rate frame before allowed redeclaration")
		declaration := r2Time(t, d.writes, "allowed redeclaration")
		if gap := declaration.Sub(receipt); gap < 950*time.Millisecond {
			t.Fatalf("allowed redeclaration %d only waited %s", attempt, gap)
		}
	}
	c.push(r2RateFrame)
	receipt := r2Time(t, d.reads, "rate frame exhausting redeclarations")
	declaration := r2Time(t, d.writes, "initial declaration on replacement connection")
	gap := declaration.Sub(receipt)
	t.Logf("exhaustion receipt to reconnect declaration=%s; dials=%d", gap, l.dialer.dials())
	if !c.isClosed() || l.dialer.maxOpen.Load() != 1 {
		t.Fatal("replacement broke break-before-make / single-connection invariant")
	}
	if gap < 950*time.Millisecond {
		t.Fatalf("replacement connection declared %s after the exhausting rate frame; want approximately 1s", gap)
	}
}
