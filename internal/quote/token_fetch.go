package quote

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mgh3326/fillwire/internal/tokenfetch"
)

// tossProvider is gatewayd's provider name for the Toss access token.
const tossProvider = "toss"

// reissueGap is the shortest time between two forced reissues the lane asks
// for. Toss keeps one valid token per client, so a handshake that keeps
// answering 401 must not turn into a reissue loop.
const reissueGap = time.Minute

// RejectionObserver is implemented by a TokenSource that wants to know which
// token Toss refused at the handshake (401). The runner calls it once per
// refusal, before it asks the source for the next token.
type RejectionObserver interface {
	TokenRejected(token string)
}

// FetchedToken is the gatewayd-fetch token source: it keeps the token in
// memory, asks gatewayd again only inside the expiry buffer, and after a 401
// asks with the refused token's hash so gatewayd reissues (or answers with
// the newer token it already holds). It reads no Redis key.
type FetchedToken struct {
	holder *tokenfetch.Holder
	clock  Clock

	mu         sync.Mutex
	rejected   string
	reissuedAt time.Time
}

// NewFetchedToken builds the source over fetcher.
func NewFetchedToken(fetcher tokenfetch.Fetcher, clock Clock) (*FetchedToken, error) {
	if fetcher == nil {
		return nil, fmt.Errorf("quote: token fetcher is required")
	}
	if clock == nil {
		clock = systemClock{}
	}
	return &FetchedToken{holder: tokenfetch.NewHolder(fetcher, tossProvider, tokenExpiryBuffer, false, clock.Now), clock: clock}, nil
}

// TokenRejected remembers the refused token until a replacement arrives.
func (s *FetchedToken) TokenRejected(token string) {
	s.mu.Lock()
	s.rejected = token
	s.mu.Unlock()
}

// Token returns the held token, or fetches. A refresh failure inside the
// expiry buffer fails closed even while the held token has life left: the
// runner must not dial after a gatewayd failure. Every failure matches
// ErrTokenUnavailable, so the runner keeps polling at its token cadence, and
// carries only the tokenfetch sentinel.
func (s *FetchedToken) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	rejected, last := s.rejected, s.reissuedAt
	s.mu.Unlock()
	if rejected != "" {
		if !last.IsZero() && s.clock.Now().Sub(last) < reissueGap {
			return "", fmt.Errorf("%w (gatewayd fetch): reissue throttled", ErrTokenUnavailable)
		}
		// The attempt counts, not only a success: a 503 or a timeout does not
		// show that gatewayd skipped the request, so retrying inside the gap
		// could reissue repeatedly. The rejection stays pending until a
		// replacement arrives.
		s.mu.Lock()
		s.reissuedAt = s.clock.Now()
		s.mu.Unlock()
		token, err := s.holder.Reissue(ctx, rejected)
		if err != nil {
			return "", fmt.Errorf("%w (gatewayd fetch): %w", ErrTokenUnavailable, err)
		}
		s.mu.Lock()
		s.rejected = ""
		s.mu.Unlock()
		return token.Value(), nil
	}
	token, _, err := s.holder.Current(ctx) // keepLive is off: err is never masked
	if err != nil {
		return "", fmt.Errorf("%w (gatewayd fetch): %w", ErrTokenUnavailable, err)
	}
	return token.Value(), nil
}
