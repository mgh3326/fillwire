package ratebudget_test

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/ratebudget"
)

type callResult struct {
	value any
	err   error
}

// twoCallers starts two callers on the same key, lets the follower join while
// the leader's fn is still running, then releases it. The wait is the repo's
// goroutine-join convention (internal/gatewayd/fetch_test.go).
func twoCallers(t *testing.T, g *ratebudget.Group, key string, fn func() (any, error)) (callResult, callResult) {
	t.Helper()
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	leaderFn := func() (any, error) {
		once.Do(func() { close(started) })
		<-release
		return fn()
	}
	out := make(chan callResult, 2)
	go func() {
		v, err := g.Do(key, leaderFn)
		out <- callResult{value: v, err: err}
	}()
	<-started
	go func() {
		v, err := g.Do(key, leaderFn)
		out <- callResult{value: v, err: err}
	}()
	time.Sleep(100 * time.Millisecond) // let the follower join the in-flight call
	close(release)
	return <-out, <-out
}

func TestGroupCoalescesIdenticalRequests(t *testing.T) {
	var g ratebudget.Group
	var calls atomic.Int32
	a, b := twoCallers(t, &g, "FHKST01010100|/uapi/quotation?symbol=005930", func() (any, error) {
		calls.Add(1)
		return "body", nil
	})
	if n := calls.Load(); n != 1 {
		t.Fatalf("fn ran %d times, want 1", n)
	}
	for _, r := range []callResult{a, b} {
		if r.err != nil || r.value != "body" {
			t.Fatalf("shared result = %v, %v; want body, nil", r.value, r.err)
		}
	}
}

func TestGroupSharesTheError(t *testing.T) {
	var g ratebudget.Group
	want := errors.New("upstream refused")
	var calls atomic.Int32
	a, b := twoCallers(t, &g, "k", func() (any, error) {
		calls.Add(1)
		return nil, want
	})
	if n := calls.Load(); n != 1 {
		t.Fatalf("fn ran %d times, want 1", n)
	}
	for _, r := range []callResult{a, b} {
		if !errors.Is(r.err, want) {
			t.Fatalf("shared error = %v, want %v", r.err, want)
		}
	}
}

func TestGroupRunsAgainAfterCompletion(t *testing.T) {
	var g ratebudget.Group
	var calls atomic.Int32
	fn := func() (any, error) {
		calls.Add(1)
		return calls.Load(), nil
	}
	for want := int32(1); want <= 2; want++ {
		v, err := g.Do("k", fn)
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		if v != want {
			t.Fatalf("value = %v, want %d", v, want)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("fn ran %d times, want 2", calls.Load())
	}
}

func TestGroupCleansUpAfterGoexit(t *testing.T) {
	var g ratebudget.Group
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		g.Do("k", func() (any, error) {
			runtime.Goexit()
			return nil, nil
		})
	}()
	<-exited
	v, err := g.Do("k", func() (any, error) { return 11, nil })
	if err != nil || v != 11 {
		t.Fatalf("after Goexit: %v, %v; want 11, nil", v, err)
	}
}

func TestGroupStaysUsableAfterPanic(t *testing.T) {
	var g ratebudget.Group
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("panic did not reach the caller")
			}
		}()
		g.Do("k", func() (any, error) { panic("boom") })
	}()
	v, err := g.Do("k", func() (any, error) { return 7, nil })
	if err != nil || v != 7 {
		t.Fatalf("after panic: %v, %v; want 7, nil", v, err)
	}
}
