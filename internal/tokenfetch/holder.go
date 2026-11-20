package tokenfetch

import (
	"context"
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
	now      func() time.Time

	mu   sync.Mutex
	held Token
	has  bool
}

// NewHolder builds a Holder. now defaults to time.Now.
func NewHolder(fetcher Fetcher, provider string, margin time.Duration, now func() time.Time) *Holder {
	if now == nil {
		now = time.Now
	}
	return &Holder{fetcher: fetcher, provider: provider, margin: margin, now: now}
}

// Current returns the held token while more than margin remains. Otherwise it
// fetches without a failed hash and adopts any unexpired answer; gatewayd may
// legitimately return the same token while it still has more than 60 s left.
// If that refresh fails while the held token still has life, the held token
// is returned with degraded set so the caller can alert; any other failure is
// returned as is.
func (h *Holder) Current(ctx context.Context) (token Token, degraded bool, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.has && h.held.Remaining(h.now()) > h.margin {
		return h.held, false, nil
	}
	fetched, err := h.fetcher.Fetch(ctx, h.provider, "")
	if err == nil {
		h.held, h.has = fetched, true
		return fetched, false, nil
	}
	if ctx.Err() == nil && h.has && h.held.Remaining(h.now()) > 0 {
		return h.held, true, nil
	}
	return Token{}, false, err
}

// Reissue is for a token the broker rejected. It sends that token's hash, so
// gatewayd reissues if it is still current and otherwise answers with the
// newer token it already has; either answer is adopted. A held token equal to
// the rejected one is dropped first, so it is never served again. An empty
// rejected means nothing was presented: the held token (if any) is the one
// reported.
func (h *Holder) Reissue(ctx context.Context, rejected string) (Token, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if rejected == "" && h.has {
		rejected = h.held.value
	}
	failed := ""
	if rejected != "" {
		failed = Hash(rejected)
		if h.has && h.held.value == rejected {
			h.held, h.has = Token{}, false
		}
	}
	fetched, err := h.fetcher.Fetch(ctx, h.provider, failed)
	if err != nil {
		return Token{}, err
	}
	h.held, h.has = fetched, true
	return fetched, nil
}
