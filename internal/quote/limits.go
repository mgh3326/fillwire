package quote

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

const (
	// declareLimit and declareWindow enforce the Toss AsyncAPI limit of five
	// subscription declarations per second (connection channel, limits).
	declareLimit  = 5
	declareWindow = time.Second
	// rateLimitCoolDown is the AsyncAPI instruction after a
	// rate-limit-exceeded frame: wait about one second, then redeclare.
	// It is counted from the moment the frame was read.
	rateLimitCoolDown = time.Second
	// maxRefusedTokens bounds the refused-token memory.
	maxRefusedTokens = 32
)

// declareLimiter admits at most declareLimit declarations in any rolling
// declareWindow, and none before a cool-down set by a rate-limit frame. A
// slot is reserved under the lock before Wait returns, so concurrent callers
// can never exceed the limit.
type declareLimiter struct {
	clock     Clock
	mu        sync.Mutex
	admitted  []time.Time
	notBefore time.Time
}

func newDeclareLimiter(clock Clock) *declareLimiter { return &declareLimiter{clock: clock} }

// coolDown forbids any declaration before until.
func (l *declareLimiter) coolDown(until time.Time) {
	l.mu.Lock()
	if until.After(l.notBefore) {
		l.notBefore = until
	}
	l.mu.Unlock()
}

// Wait blocks until a declaration is allowed and reserves it.
func (l *declareLimiter) Wait(ctx context.Context) error {
	for {
		wait, ok := l.reserve()
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-l.clock.After(wait):
		}
	}
}

func (l *declareLimiter) reserve() (time.Duration, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock.Now()
	kept := l.admitted[:0]
	for _, at := range l.admitted {
		if now.Sub(at) < declareWindow {
			kept = append(kept, at)
		}
	}
	l.admitted = kept
	if now.Before(l.notBefore) {
		return l.notBefore.Sub(now), false
	}
	if len(l.admitted) >= declareLimit {
		return l.admitted[0].Add(declareWindow).Sub(now), false
	}
	l.admitted = append(l.admitted, now)
	return 0, true
}

// refusedTokens remembers fingerprints of tokens Toss refused with 401, so
// the lane never redials with any of them. It holds a SHA-256 fingerprint,
// never the token, and keeps at most maxRefusedTokens, dropping the oldest.
// A refused token never becomes valid again, and the configured cache key is
// fixed for the life of the process, so entries are never cleared otherwise.
type refusedTokens struct {
	order []string
	set   map[string]struct{}
}

func tokenFingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (r *refusedTokens) add(token string) {
	if r.set == nil {
		r.set = map[string]struct{}{}
	}
	fingerprint := tokenFingerprint(token)
	if _, ok := r.set[fingerprint]; ok {
		return
	}
	if len(r.order) >= maxRefusedTokens {
		delete(r.set, r.order[0])
		r.order = r.order[1:]
	}
	r.order = append(r.order, fingerprint)
	r.set[fingerprint] = struct{}{}
}

func (r *refusedTokens) contains(token string) bool {
	_, ok := r.set[tokenFingerprint(token)]
	return ok
}
