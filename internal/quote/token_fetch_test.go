package quote

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/tokenfetch"
	"github.com/mgh3326/fillwire/internal/tokenfetch/fetchtest"
)

const fetchCredential = "consumer-credential-xyz"

var _ TokenSource = (*FetchedToken)(nil)
var _ RejectionObserver = (*FetchedToken)(nil)

// testClock adapts the shared fake clock to the lane's Clock.
type testClock struct{ *fetchtest.Clock }

func (c testClock) After(d time.Duration) <-chan time.Time {
	return time.After(min(d, time.Millisecond))
}

func newFetchedToken(t *testing.T) (*fetchtest.Server, *FetchedToken, testClock) {
	t.Helper()
	clock := testClock{&fetchtest.Clock{}}
	server := fetchtest.New(clock.Clock, fetchCredential)
	t.Cleanup(server.Close)
	client, err := tokenfetch.NewClient(server.URL, fetchCredential)
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewFetchedToken(client, clock)
	if err != nil {
		t.Fatal(err)
	}
	return server, source, clock
}

func TestFetchedTokenSteadyStateMakesNoHTTPCalls(t *testing.T) {
	server, source, clock := newFetchedToken(t)
	for i := 0; i < 8; i++ {
		token, err := source.Token(context.Background())
		if err != nil || token != "fake-toss-token-1" {
			t.Fatalf("Token #%d = %q, %v", i, token, err)
		}
		clock.Advance(time.Hour)
	}
	if calls := server.Calls(); len(calls) != 1 || calls[0].Provider != "toss" {
		t.Fatalf("calls = %+v", calls)
	}
}

func TestFetchedTokenRefreshesInsideTheExpiryBuffer(t *testing.T) {
	server, source, clock := newFetchedToken(t)
	first, _ := source.Token(context.Background())
	clock.Advance(24*time.Hour - 100*time.Second) // inside 120 s, outside gatewayd's 60 s
	same, err := source.Token(context.Background())
	if err != nil || same != first || len(server.Calls()) != 2 || server.Calls()[1].Failed != "" {
		t.Fatalf("at buffer = %q, %v, calls %+v", same, err, server.Calls())
	}
	clock.Advance(50 * time.Second) // under 60 s: gatewayd issues
	next, err := source.Token(context.Background())
	if err != nil || next == first {
		t.Fatalf("near expiry = %q, %v", next, err)
	}
}

func TestFetchedTokenReissuesWithTheRefusedHash(t *testing.T) {
	server, source, clock := newFetchedToken(t)
	refused, _ := source.Token(context.Background())
	source.TokenRejected(refused)
	next, err := source.Token(context.Background())
	if err != nil || next == refused {
		t.Fatalf("after 401 = %q, %v", next, err)
	}
	calls := server.Calls()
	if got := calls[len(calls)-1].Failed; got != tokenfetch.Hash(refused) {
		t.Fatalf("failed hash = %q", got)
	}
	// Another 401 inside the gap is throttled, then allowed.
	source.TokenRejected(next)
	if _, err := source.Token(context.Background()); !errors.Is(err, ErrTokenUnavailable) || len(server.Calls()) != len(calls) {
		t.Fatalf("throttled err = %v, calls %d -> %d", err, len(calls), len(server.Calls()))
	}
	clock.Advance(reissueGap)
	third, err := source.Token(context.Background())
	if err != nil || third == next {
		t.Fatalf("after gap = %q, %v", third, err)
	}
}

func TestFetchedTokenAdoptsTheStaleTokenAnswer(t *testing.T) {
	server, source, clock := newFetchedToken(t)
	refused, _ := source.Token(context.Background())
	server.Seed("toss", "newer-toss-token", clock.Now().Add(10*time.Hour))
	issued := server.Issued()
	source.TokenRejected(refused)
	got, err := source.Token(context.Background())
	if err != nil || got != "newer-toss-token" || server.Issued() != issued {
		t.Fatalf("got %q, %v, issued %d -> %d", got, err, issued, server.Issued())
	}
}

func TestFetchedTokenFailuresAreTypedAndKeepTheLanePolling(t *testing.T) {
	for _, tc := range []struct {
		name    string
		arrange func(*fetchtest.Server)
		want    error
	}{
		{"401", func(s *fetchtest.Server) { s.Credential = "rotated" }, tokenfetch.ErrUnauthenticated},
		{"403", func(s *fetchtest.Server) { s.Forbid("toss") }, tokenfetch.ErrProviderNotAllowed},
		{"503", func(s *fetchtest.Server) { s.FailWith(503, "") }, tokenfetch.ErrIssueUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, source, _ := newFetchedToken(t)
			tc.arrange(server)
			_, err := source.Token(context.Background())
			if !errors.Is(err, tc.want) || !errors.Is(err, ErrTokenUnavailable) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestFetchedTokenRefusedKeepsPendingUntilReplaced(t *testing.T) {
	server, source, _ := newFetchedToken(t)
	refused, _ := source.Token(context.Background())
	source.TokenRejected(refused)
	server.FailWith(503, "")
	if _, err := source.Token(context.Background()); !errors.Is(err, tokenfetch.ErrIssueUnavailable) {
		t.Fatalf("err = %v", err)
	}
	server.FailWith(0, "")
	next, err := source.Token(context.Background())
	if err != nil || next == refused {
		t.Fatalf("retry = %q, %v", next, err)
	}
	if calls := server.Calls(); calls[len(calls)-1].Failed != tokenfetch.Hash(refused) {
		t.Fatalf("retry lost the failed hash: %+v", calls[len(calls)-1])
	}
}

// scriptedFetcher hands out tokens for the lane clock and records hashes.
type scriptedFetcher struct {
	mu     sync.Mutex
	clock  Clock
	n      int
	failed []string
}

func (f *scriptedFetcher) Fetch(_ context.Context, provider string, failed string) (tokenfetch.Token, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = append(f.failed, failed)
	f.n++
	return tokenfetch.NewToken("scripted-toss-token-"+string(rune('0'+f.n)), f.clock.Now().Add(24*time.Hour), tokenfetch.SourceIssued), nil
}

func TestRunnerReissuesThroughGatewaydAfterA401AndNeverLogsTokens(t *testing.T) {
	var logs lockedBuffer
	fetcher := &scriptedFetcher{}
	l := startLane(t, wednesdayMidday, ackAll, func(cfg *Config, l *lane) {
		fetcher.clock = l.clock
		source, err := NewFetchedToken(fetcher, l.clock)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Token = source
		cfg.Logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		l.dialer.refuse = func(token string) error {
			if token == "scripted-toss-token-1" {
				return ErrUnauthorized
			}
			return nil
		}
	})
	conn := <-l.dialer.dialed
	if conn.token != "scripted-toss-token-2" {
		t.Fatalf("dial token = %q", conn.token)
	}
	if tokens := l.dialer.usedTokens(); len(tokens) != 2 || tokens[0] != "scripted-toss-token-1" {
		t.Fatalf("dial tokens = %v", tokens)
	}
	fetcher.mu.Lock()
	failed := append([]string(nil), fetcher.failed...)
	fetcher.mu.Unlock()
	if len(failed) != 2 || failed[0] != "" || failed[1] != tokenfetch.Hash("scripted-toss-token-1") {
		t.Fatalf("failed hashes = %q", failed)
	}
	if strings.Contains(logs.String(), "scripted-toss-token") {
		t.Fatalf("logs leak a token:\n%s", logs.String())
	}
	if l.runner.Counters().TokenRejected.Load() != 1 {
		t.Fatalf("token_rejected = %d", l.runner.Counters().TokenRejected.Load())
	}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestNewFetchedTokenRequiresAFetcher(t *testing.T) {
	if _, err := NewFetchedToken(nil, nil); err == nil {
		t.Fatal("nil fetcher accepted")
	}
}
