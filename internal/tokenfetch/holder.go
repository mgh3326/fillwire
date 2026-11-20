package tokenfetch

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Holder keeps one provider's token in memory. It asks gatewayd again only
// when less than margin remains or when the broker rejected the held token;
// in between it makes no call at all. Calls are serialized, so concurrent
// users share one fetch.
type Holder struct {
	fetcher  Fetcher
	provider string
	margin   time.Duration
	keepLive bool
	now      func() time.Time

	mu   sync.Mutex
	held Token
	has  bool
	// failed is the hash of a token the broker rejected and gatewayd has not
	// yet replaced. Every fetch carries it until one succeeds, so a failed
	// reissue is never retried as a plain refresh that could hand the
	// rejected token back.
	failed string
}

// NewHolder builds a Holder. now defaults to time.Now. keepLive is the
// refresh-failure policy: true keeps serving a held token that still has life
// when a refresh fails (the KIS stream outlives its key); false fails closed
// and returns the fetch error (the Toss lane must not dial after a gatewayd
// failure).
func NewHolder(fetcher Fetcher, provider string, margin time.Duration, keepLive bool, now func() time.Time) *Holder {
	if now == nil {
		now = time.Now
	}
	return &Holder{fetcher: fetcher, provider: provider, margin: margin, keepLive: keepLive, now: now}
}

// Current returns the held token while more than margin remains. Otherwise it
// fetches and adopts any unexpired answer; gatewayd may legitimately return
// the same token while it still has more than 60 s left. If that refresh
// fails and the holder keeps live tokens, a held token with life left is
// returned with degraded set so the caller can alert; otherwise the fetch
// error is returned. A deadline on ctx counts as a failure like any other;
// only cancellation (shutdown) never falls back.
func (h *Holder) Current(ctx context.Context) (token Token, degraded bool, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.has && h.held.Remaining(h.now()) > h.margin {
		return h.held, false, nil
	}
	fetched, err := h.fetcher.Fetch(ctx, h.provider, h.failed)
	if err == nil {
		h.adopt(fetched)
		return fetched, false, nil
	}
	if h.keepLive && !errors.Is(ctx.Err(), context.Canceled) && h.has && h.held.Remaining(h.now()) > 0 {
		return h.held, true, nil
	}
	return Token{}, false, err
}

// Reissue is for a token the broker rejected. It sends that token's hash, so
// gatewayd reissues if it is still current and otherwise answers with the
// newer token it already has; either answer is adopted. A held token equal to
// the rejected one is dropped first, so it is never served again, and the
// hash stays pending until a fetch succeeds. An empty rejected means nothing
// was presented: the held token (if any) is the one reported.
func (h *Holder) Reissue(ctx context.Context, rejected string) (Token, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if rejected == "" && h.has {
		rejected = h.held.value
	}
	if rejected != "" {
		h.failed = Hash(rejected)
		if h.has && h.held.value == rejected {
			h.held, h.has = Token{}, false
		}
	}
	fetched, err := h.fetcher.Fetch(ctx, h.provider, h.failed)
	if err != nil {
		return Token{}, err
	}
	h.adopt(fetched)
	return fetched, nil
}

func (h *Holder) adopt(token Token) {
	h.held, h.has, h.failed = token, true, ""
}
