package tokenfetch_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/tokenfetch"
	"github.com/mgh3326/fillwire/internal/tokenfetch/fetchtest"
)

func newHolder(t *testing.T, margin time.Duration) (*fetchtest.Server, *tokenfetch.Holder, *fetchtest.Clock) {
	t.Helper()
	clock := &fetchtest.Clock{}
	server := fetchtest.New(clock, credential)
	t.Cleanup(server.Close)
	client, err := tokenfetch.NewClient(server.URL, credential)
	if err != nil {
		t.Fatal(err)
	}
	return server, tokenfetch.NewHolder(client, "kis-live-approval", margin, true, clock.Now), clock
}

func TestHolderSteadyStateMakesNoCalls(t *testing.T) {
	server, holder, clock := newHolder(t, time.Hour)
	for i := 0; i < 5; i++ {
		token, degraded, err := holder.Current(context.Background())
		if err != nil || degraded || token.Value() != "fake-kis-live-approval-token-1" {
			t.Fatalf("Current #%d = %v %v %v", i, token, degraded, err)
		}
		clock.Advance(time.Hour)
	}
	if got := len(server.Calls()); got != 1 {
		t.Fatalf("HTTP calls = %d, want 1", got)
	}
}

func TestHolderRefreshesAtMarginAndAdoptsTheSameToken(t *testing.T) {
	server, holder, clock := newHolder(t, time.Hour)
	first, _, _ := holder.Current(context.Background())
	clock.Advance(23*time.Hour + time.Minute) // 59 min left: inside the margin, outside gatewayd's 60 s rule
	second, degraded, err := holder.Current(context.Background())
	if err != nil || degraded || second.Value() != first.Value() || second.Source != tokenfetch.SourceMemory {
		t.Fatalf("at margin = %v %v %v", second, degraded, err)
	}
	if calls := server.Calls(); len(calls) != 2 || calls[1].Failed != "" {
		t.Fatalf("calls = %+v", calls)
	}
	clock.Advance(58*time.Minute + 30*time.Second) // under gatewayd's 60 s rule: a new token
	third, _, err := holder.Current(context.Background())
	if err != nil || third.Value() == first.Value() || third.Source != tokenfetch.SourceIssued {
		t.Fatalf("near expiry = %v %v", third, err)
	}
	if got := server.Issued(); got != 2 {
		t.Fatalf("issued = %d", got)
	}
}

func TestHolderReissueSendsTheRejectedHash(t *testing.T) {
	server, holder, _ := newHolder(t, time.Hour)
	first, _, _ := holder.Current(context.Background())
	next, err := holder.Reissue(context.Background(), first.Value())
	if err != nil || next.Value() == first.Value() {
		t.Fatalf("Reissue = %v %v", next, err)
	}
	calls := server.Calls()
	if got := calls[len(calls)-1].Failed; got != tokenfetch.Hash(first.Value()) {
		t.Fatalf("failed hash = %q", got)
	}
	// The held token is the new one; no further call.
	if again, _, _ := holder.Current(context.Background()); again.Value() != next.Value() || len(server.Calls()) != len(calls) {
		t.Fatalf("after reissue = %v, calls = %d", again, len(server.Calls()))
	}
}

func TestHolderAdoptsTheStaleTokenAnswer(t *testing.T) {
	server, holder, clock := newHolder(t, time.Hour)
	stale, _, _ := holder.Current(context.Background())
	// Another consumer already caused a reissue.
	server.Seed("kis-live-approval", "newer-token", clock.Now().Add(20*time.Hour))
	issued := server.Issued()
	got, err := holder.Reissue(context.Background(), stale.Value())
	if err != nil || got.Value() != "newer-token" || server.Issued() != issued {
		t.Fatalf("Reissue = %v %v issued %d->%d", got, err, issued, server.Issued())
	}
}

func TestHolderRefreshFailureKeepsALiveTokenOnly(t *testing.T) {
	server, holder, clock := newHolder(t, time.Hour)
	first, _, _ := holder.Current(context.Background())
	server.FailWith(503, "")
	clock.Advance(23*time.Hour + time.Minute)
	token, degraded, err := holder.Current(context.Background())
	if err != nil || !degraded || token.Value() != first.Value() {
		t.Fatalf("live = %v %v %v", token, degraded, err)
	}
	clock.Advance(2 * time.Hour) // expired
	if _, _, err := holder.Current(context.Background()); !errors.Is(err, tokenfetch.ErrIssueUnavailable) {
		t.Fatalf("expired err = %v", err)
	}
}

func TestHolderReissueFailureNeverServesTheRejectedToken(t *testing.T) {
	server, holder, _ := newHolder(t, time.Hour)
	first, _, _ := holder.Current(context.Background())
	server.FailWith(503, "")
	if _, err := holder.Reissue(context.Background(), first.Value()); !errors.Is(err, tokenfetch.ErrIssueUnavailable) {
		t.Fatalf("err = %v", err)
	}
	// Still failing: the rejected token must not come back as a "live" fallback.
	if _, _, err := holder.Current(context.Background()); !errors.Is(err, tokenfetch.ErrIssueUnavailable) {
		t.Fatalf("Current after failed reissue err = %v", err)
	}
}

func TestHolderKeepsTheRejectedHashPendingAfterAFailedReissue(t *testing.T) {
	server, holder, _ := newHolder(t, time.Hour)
	first, _, _ := holder.Current(context.Background())
	server.FailWith(503, "")
	if _, err := holder.Reissue(context.Background(), first.Value()); err == nil {
		t.Fatal("reissue unexpectedly succeeded")
	}
	server.FailWith(0, "")
	next, _, err := holder.Current(context.Background())
	if err != nil || next.Value() == first.Value() {
		t.Fatalf("Current after a failed reissue = %v, %v", next, err)
	}
	calls := server.Calls()
	if got := calls[len(calls)-1].Failed; got != tokenfetch.Hash(first.Value()) {
		t.Fatalf("the follow-up fetch lost the rejected hash: %q", got)
	}
	// Adopted: the next fetch is a plain one again.
	if _, _, err := holder.Current(context.Background()); err != nil || len(server.Calls()) != len(calls) {
		t.Fatalf("pending not cleared: %v, calls %d", err, len(server.Calls()))
	}
}

func TestHolderWithoutKeepLiveFailsClosed(t *testing.T) {
	clock := &fetchtest.Clock{}
	server := fetchtest.New(clock, credential)
	t.Cleanup(server.Close)
	client, _ := tokenfetch.NewClient(server.URL, credential)
	holder := tokenfetch.NewHolder(client, "toss", 2*time.Minute, false, clock.Now)
	if _, _, err := holder.Current(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(24*time.Hour - 90*time.Second)
	server.FailWith(503, "")
	if token, degraded, err := holder.Current(context.Background()); !errors.Is(err, tokenfetch.ErrIssueUnavailable) || degraded || token.Value() != "" {
		t.Fatalf("Current = %v %v %v", token, degraded, err)
	}
}
