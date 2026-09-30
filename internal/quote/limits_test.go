package quote

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Tester reproduction 1 (toss-r1-blockers.md): a rate-limit-exceeded frame
// that arrives late must still hold the next declaration for about a second
// counted from the frame's receipt, not from the previous declaration.
func TestRunnerRateLimitCoolDownCountsFromFrameReceipt(t *testing.T) {
	var declarations atomic.Int32
	var mu sync.Mutex
	var frameSent, second time.Time
	l := startLane(t, wednesdayMidday, func(c *fakeConn, data []byte) {
		if !bytes.HasPrefix(data, []byte("[")) {
			return
		}
		switch declarations.Add(1) {
		case 1:
			go func() {
				time.Sleep(1200 * time.Millisecond) // later than the old 1s spacing
				mu.Lock()
				frameSent = time.Now()
				mu.Unlock()
				c.push(fixture(t, "error_rate_limit.json"))
			}()
		case 2:
			mu.Lock()
			second = time.Now()
			mu.Unlock()
			ackAll(c, data)
		default:
			ackAll(c, data)
		}
	}, nil)
	waitFor(t, "redeclaration", func() bool { return declarations.Load() >= 2 })
	mu.Lock()
	gap := second.Sub(frameSent)
	mu.Unlock()
	if gap < 950*time.Millisecond {
		t.Fatalf("redeclared %s after the rate-limit frame arrived, want at least about 1s", gap)
	}
	if l.dialer.dials() != 1 {
		t.Fatalf("rate limit caused a redial: %d", l.dialer.dials())
	}
}

// Tester reproduction 2 (toss-r1-blockers.md): A refused, then B refused,
// then the cache returns to A. A must never be dialled again; only a token
// outside the refused set is.
func TestRunnerNeverRedialsAnyRefusedToken(t *testing.T) {
	l := startLane(t, wednesdayMidday, ackAll, func(_ *Config, l *lane) {
		l.token.set("token-A", nil)
		l.dialer.refuse = func(token string) error {
			if token == "token-A" || token == "token-B" {
				return ErrUnauthorized
			}
			return nil
		}
	})
	waitFor(t, "A refused", func() bool { return l.dialer.dials() == 1 })
	l.token.set("token-B", nil)
	waitFor(t, "B refused", func() bool { return l.dialer.dials() == 2 })
	l.token.set("token-A", nil)
	reads := l.token.calls.Load()
	waitFor(t, "cache A re-read several times", func() bool { return l.token.calls.Load() >= reads+4 })
	if got := l.dialer.usedTokens(); len(got) != 2 {
		t.Fatalf("dials = %v; the refused token A was dialled again", got)
	}
	l.token.set("token-C", nil)
	conn := <-l.dialer.dialed
	for conn.token != "token-C" {
		conn = <-l.dialer.dialed
	}
	if got := l.dialer.usedTokens(); fmt.Sprint(got) != "[token-A token-B token-C]" {
		t.Fatalf("dial tokens = %v", got)
	}
	if l.runner.Counters().TokenRejected.Load() != 2 {
		t.Fatalf("token_rejected = %d, want 2", l.runner.Counters().TokenRejected.Load())
	}
}

func TestDeclareLimiterAdmitsAtMostFivePerRollingSecond(t *testing.T) {
	clock := newLaneClock(wednesdayMidday)
	limiter := newDeclareLimiter(clock)
	const callers = 12
	var mu sync.Mutex
	var admitted []time.Time
	var wg sync.WaitGroup
	for index := 0; index < callers; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := limiter.Wait(context.Background()); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			admitted = append(admitted, clock.Now())
			mu.Unlock()
		}()
	}
	wg.Wait()
	sort.Slice(admitted, func(i, j int) bool { return admitted[i].Before(admitted[j]) })
	if len(admitted) != callers {
		t.Fatalf("admitted %d of %d", len(admitted), callers)
	}
	for index := declareLimit; index < len(admitted); index++ {
		if gap := admitted[index].Sub(admitted[index-declareLimit]); gap < declareWindow {
			t.Fatalf("declarations %d and %d are %s apart: more than %d in one second", index-declareLimit, index, gap, declareLimit)
		}
	}
	// Twelve admissions at five per second need at least two full windows.
	if span := admitted[len(admitted)-1].Sub(admitted[0]); span < 2*declareWindow {
		t.Fatalf("12 declarations admitted within %s", span)
	}
}

func TestDeclareLimiterCoolDownAndCancel(t *testing.T) {
	clock := newLaneClock(wednesdayMidday)
	limiter := newDeclareLimiter(clock)
	limiter.coolDown(clock.Now().Add(300 * time.Millisecond))
	start := clock.Now()
	if err := limiter.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if waited := clock.Now().Sub(start); waited < 290*time.Millisecond {
		t.Fatalf("admitted %s into a 300ms cool-down", waited)
	}
	limiter.coolDown(clock.Now().Add(time.Hour))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := limiter.Wait(ctx); err == nil {
		t.Fatal("Wait returned during a cool-down despite cancellation")
	}
}

func TestRefusedTokensAreBoundedFingerprints(t *testing.T) {
	var refused refusedTokens
	for index := 0; index <= maxRefusedTokens; index++ {
		refused.add(fmt.Sprintf("secret-token-%d", index))
	}
	if len(refused.order) != maxRefusedTokens || len(refused.set) != maxRefusedTokens {
		t.Fatalf("refused set holds %d/%d entries, want %d", len(refused.order), len(refused.set), maxRefusedTokens)
	}
	if refused.contains("secret-token-0") {
		t.Fatal("oldest fingerprint not evicted")
	}
	if !refused.contains("secret-token-1") || !refused.contains(fmt.Sprintf("secret-token-%d", maxRefusedTokens)) {
		t.Fatal("recent fingerprints missing")
	}
	for _, entry := range refused.order {
		if strings.Contains(entry, "secret") || len(entry) != 64 {
			t.Fatalf("refused set stores %q; want a SHA-256 fingerprint only", entry)
		}
	}
	refused.add("secret-token-1") // duplicate: no growth, no eviction
	if len(refused.order) != maxRefusedTokens || !refused.contains("secret-token-2") {
		t.Fatal("duplicate add changed the set")
	}
}
