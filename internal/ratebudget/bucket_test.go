package ratebudget

import (
	"testing"
	"time"
)

func TestBucketTakeRefillAndRefund(t *testing.T) {
	now := time.Unix(0, 0)
	b := newBucket(Limit{Rate: 2, Burst: 2}, now)
	if ok, _ := b.take(now); !ok {
		t.Fatal("first token refused on a full bucket")
	}
	if ok, _ := b.take(now); !ok {
		t.Fatal("second token refused on a burst-2 bucket")
	}
	ok, wait := b.take(now)
	if ok {
		t.Fatal("empty bucket handed out a token")
	}
	if want := 500 * time.Millisecond; wait != want {
		t.Fatalf("wait = %s, want %s", wait, want)
	}
	// A fractional deficit is rounded up so a retry at the reported time
	// cannot find the bucket still empty.
	b = newBucket(Limit{Rate: 3, Burst: 1}, now)
	b.take(now)
	if ok, wait := b.take(now); ok || wait != 333333334*time.Nanosecond {
		t.Fatalf("fractional wait = %s (ok=%v), want 333333334ns", wait, ok)
	}
}

func TestBucketReadyDoesNotSpend(t *testing.T) {
	now := time.Unix(0, 0)
	b := newBucket(Limit{Rate: 2, Burst: 1}, now)
	if wait := b.ready(now); wait != 0 {
		t.Fatalf("full bucket ready = %s, want 0", wait)
	}
	if wait := b.ready(now); wait != 0 {
		t.Fatalf("second ready = %s, want 0 (ready must not spend)", wait)
	}
	b.take(now)
	want := 500 * time.Millisecond
	if wait := b.ready(now); wait != want {
		t.Fatalf("empty bucket ready = %s, want %s", wait, want)
	}
	if wait := b.ready(now); wait != want {
		t.Fatalf("repeated ready = %s, want %s (ready must not spend)", wait, want)
	}
}

func TestBucketRefillCapsAtBurst(t *testing.T) {
	now := time.Unix(0, 0)
	b := newBucket(Limit{Rate: 4, Burst: 1}, now)
	b.take(now)
	later := now.Add(10 * time.Second)
	if ok, _ := b.take(later); !ok {
		t.Fatal("refilled bucket refused")
	}
	if b.tokens != 0 {
		t.Fatalf("tokens = %v after take, want 0", b.tokens)
	}
}

func TestBucketCloseNeverShortens(t *testing.T) {
	now := time.Unix(0, 0)
	b := newBucket(Limit{Rate: 10, Burst: 1}, now)
	b.close(now.Add(time.Second))
	ok, wait := b.take(now.Add(400 * time.Millisecond))
	if ok || wait != 600*time.Millisecond {
		t.Fatalf("inside close = %s (ok=%v), want 600ms", wait, ok)
	}
	b.close(now.Add(500 * time.Millisecond))
	ok, wait = b.take(now.Add(500 * time.Millisecond))
	if ok || wait != 500*time.Millisecond {
		t.Fatalf("shortened close = %s (ok=%v), want 500ms", wait, ok)
	}
	if ok, _ := b.take(now.Add(time.Second)); !ok {
		t.Fatal("bucket still closed at the window end")
	}
}
