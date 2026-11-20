package reader

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/tokenfetch"
	"github.com/mgh3326/fillwire/internal/tokenfetch/fetchtest"
	"github.com/mgh3326/go-kis/kis/ws"
)

var (
	_ ws.ApprovalKeyProvider  = (*FetchApprovalProvider)(nil)
	_ terminalApprovalFailure = (*FetchApprovalProvider)(nil)
)

const fetchCredential = "consumer-credential-xyz"

type fetchFixture struct {
	server *fetchtest.Server
	clock  *fetchtest.Clock
	p      *FetchApprovalProvider
	logs   *bytes.Buffer
	alerts atomic.Int32
}

func newFetchFixture(t *testing.T, accountMode string) *fetchFixture {
	t.Helper()
	f := &fetchFixture{clock: &fetchtest.Clock{}, logs: &bytes.Buffer{}}
	f.server = fetchtest.New(f.clock, fetchCredential)
	t.Cleanup(f.server.Close)
	client, err := tokenfetch.NewClient(f.server.URL, fetchCredential)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	f.p, err = newFetchApprovalProvider(client, accountMode, DefaultRefreshMargin, logger, f.clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	f.p.SetTransientAlert(func() { f.alerts.Add(1) })
	return f
}

func TestFetchApprovalSteadyStateMakesNoHTTPCalls(t *testing.T) {
	f := newFetchFixture(t, "live")
	for i := 0; i < 10; i++ {
		key, err := f.p.ApprovalKey(context.Background())
		if err != nil || key != "fake-kis-live-approval-token-1" {
			t.Fatalf("ApprovalKey #%d = %q, %v", i, key, err)
		}
		f.clock.Advance(time.Hour)
	}
	if got := len(f.server.Calls()); got != 1 {
		t.Fatalf("HTTP calls = %d, want 1 for ten reads", got)
	}
}

func TestFetchApprovalPicksTheProviderByAccountMode(t *testing.T) {
	f := newFetchFixture(t, "mock")
	if _, err := f.p.ApprovalKey(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls := f.server.Calls(); len(calls) != 1 || calls[0].Provider != "kis-mock-approval" {
		t.Fatalf("calls = %+v", calls)
	}
}

func TestFetchApprovalRefreshesAtMarginAndAdoptsTheSameKey(t *testing.T) {
	f := newFetchFixture(t, "live")
	first, _ := f.p.ApprovalKey(context.Background())
	f.clock.Advance(23*time.Hour + time.Minute) // 59 min left, margin is 1h
	got, err := f.p.ApprovalKey(context.Background())
	if err != nil || got != first {
		t.Fatalf("ApprovalKey at margin = %q, %v; gatewayd still has >60 s so the same key is correct", got, err)
	}
	if calls := f.server.Calls(); len(calls) != 2 || calls[1].Failed != "" {
		t.Fatalf("calls = %+v", calls)
	}
	if f.alerts.Load() != 0 || f.p.cacheOnlyFailure() != nil {
		t.Fatal("adopting the same key must not alert or fail")
	}
	f.clock.Advance(58*time.Minute + 30*time.Second)
	renewed, err := f.p.ApprovalKey(context.Background())
	if err != nil || renewed == first {
		t.Fatalf("under 60 s left gatewayd must issue: %q, %v", renewed, err)
	}
}

func TestFetchApprovalReissueSendsTheRejectedKeysHash(t *testing.T) {
	f := newFetchFixture(t, "live")
	rejected, _ := f.p.ApprovalKey(context.Background())
	next, err := f.p.Reissue(context.Background())
	if err != nil || next == rejected {
		t.Fatalf("Reissue = %q, %v", next, err)
	}
	calls := f.server.Calls()
	if got := calls[len(calls)-1].Failed; got != tokenfetch.Hash(rejected) {
		t.Fatalf("failed_token_sha256 = %q, want hash of the rejected key", got)
	}
	if again, _ := f.p.ApprovalKey(context.Background()); again != next || len(f.server.Calls()) != len(calls) {
		t.Fatalf("after reissue ApprovalKey = %q with %d calls", again, len(f.server.Calls()))
	}
}

func TestFetchApprovalAdoptsAStaleTokenAnswer(t *testing.T) {
	f := newFetchFixture(t, "live")
	rejected, _ := f.p.ApprovalKey(context.Background())
	f.server.Seed("kis-live-approval", "newer-key", f.clock.Now().Add(20*time.Hour))
	issued := f.server.Issued()
	got, err := f.p.Reissue(context.Background())
	if err != nil || got != "newer-key" || got == rejected || f.server.Issued() != issued {
		t.Fatalf("Reissue = %q, %v (issued %d -> %d)", got, err, issued, f.server.Issued())
	}
}

func TestFetchApprovalFailuresAreTypedTerminalAndNeverIssueLocally(t *testing.T) {
	for _, tc := range []struct {
		name    string
		arrange func(*fetchFixture)
		want    error
	}{
		{"401", func(f *fetchFixture) { f.server.Credential = "rotated" }, tokenfetch.ErrUnauthenticated},
		{"403", func(f *fetchFixture) { f.server.Forbid("kis-live-approval") }, tokenfetch.ErrProviderNotAllowed},
		{"503", func(f *fetchFixture) { f.server.FailWith(503, "") }, tokenfetch.ErrIssueUnavailable},
		{"404", func(f *fetchFixture) { f.server.FailWith(404, "") }, tokenfetch.ErrUnavailable},
	} {
		for _, forced := range []bool{false, true} {
			name := tc.name + map[bool]string{false: " read", true: " reissue"}[forced]
			t.Run(name, func(t *testing.T) {
				f := newFetchFixture(t, "live")
				if forced {
					if _, err := f.p.ApprovalKey(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				tc.arrange(f)
				var err error
				if forced {
					_, err = f.p.Reissue(context.Background())
				} else {
					_, err = f.p.ApprovalKey(context.Background())
				}
				if !errors.Is(err, tc.want) {
					t.Fatalf("err = %v, want %v", err, tc.want)
				}
				if !errors.Is(f.p.cacheOnlyFailure(), tc.want) {
					t.Fatalf("terminal failure = %v", f.p.cacheOnlyFailure())
				}
				select {
				case <-f.p.cacheOnlyFailureSignal():
				default:
					t.Fatal("failure signal not closed")
				}
			})
		}
	}
}

func TestFetchApprovalRefreshFailureKeepsALiveKeyOnly(t *testing.T) {
	f := newFetchFixture(t, "live")
	first, _ := f.p.ApprovalKey(context.Background())
	f.server.FailWith(503, "")
	f.clock.Advance(23*time.Hour + time.Minute)
	got, err := f.p.ApprovalKey(context.Background())
	if err != nil || got != first || f.alerts.Load() != 1 || f.p.cacheOnlyFailure() != nil {
		t.Fatalf("live key kept = %q, %v, alerts %d, terminal %v", got, err, f.alerts.Load(), f.p.cacheOnlyFailure())
	}
	f.clock.Advance(2 * time.Hour)
	if _, err := f.p.ApprovalKey(context.Background()); !errors.Is(err, tokenfetch.ErrIssueUnavailable) || f.p.cacheOnlyFailure() == nil {
		t.Fatalf("expired key err = %v", err)
	}
	// A KIS rejection never reuses the rejected key.
	g := newFetchFixture(t, "live")
	if _, err := g.p.ApprovalKey(context.Background()); err != nil {
		t.Fatal(err)
	}
	g.server.FailWith(503, "")
	if key, err := g.p.Reissue(context.Background()); err == nil || key != "" {
		t.Fatalf("failed reissue returned %q, %v", key, err)
	}
}

func TestFetchApprovalCancellationIsNotTerminal(t *testing.T) {
	f := newFetchFixture(t, "live")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.p.ApprovalKey(ctx); !errors.Is(err, context.Canceled) || f.p.cacheOnlyFailure() != nil {
		t.Fatalf("err = %v terminal %v", err, f.p.cacheOnlyFailure())
	}
}

func TestFetchApprovalNeverLogsTheKeyOrCredential(t *testing.T) {
	f := newFetchFixture(t, "live")
	ctx := context.Background()
	var errs []string
	note := func(err error) {
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	key, err := f.p.ApprovalKey(ctx)
	note(err)
	_, err = f.p.Reissue(ctx)
	note(err)
	f.clock.Advance(23*time.Hour + time.Minute)
	_, err = f.p.ApprovalKey(ctx)
	note(err)
	// 503 with a body that echoes the key and the credential.
	f.server.FailWith(503, `{"token":"`+key+`","credential":"`+fetchCredential+`"}`)
	_, err = f.p.Reissue(ctx)
	note(err)
	// A malformed 200 is covered in tokenfetch; a 401 here.
	f.server.FailWith(0, "")
	f.server.Credential = "rotated"
	_, err = f.p.Reissue(ctx)
	note(err)
	if len(errs) < 2 {
		t.Fatalf("expected failures to scan, got %v", errs)
	}
	output := f.logs.String() + strings.Join(errs, "\n")
	for _, secret := range []string{key, "fake-kis-live-approval-token", fetchCredential, f.server.URL} {
		if strings.Contains(output, secret) {
			t.Fatalf("output leaks %q:\n%s", secret, output)
		}
	}
	if !strings.Contains(f.logs.String(), "approval_gatewayd_fetch") {
		t.Fatal("fetch calls are not logged at all")
	}
}

func TestFetchApprovalConstructorRejectsBadInput(t *testing.T) {
	client, _ := tokenfetch.NewClient("http://127.0.0.1:1", "c")
	for name, build := range map[string]func() (*FetchApprovalProvider, error){
		"nil fetcher": func() (*FetchApprovalProvider, error) {
			return NewApprovalProviderGatewaydFetch(nil, "live", time.Hour, nil)
		},
		"bad account": func() (*FetchApprovalProvider, error) {
			return NewApprovalProviderGatewaydFetch(client, "paper", time.Hour, nil)
		},
		"zero margin": func() (*FetchApprovalProvider, error) {
			return NewApprovalProviderGatewaydFetch(client, "live", 0, nil)
		},
		"margin over 2h": func() (*FetchApprovalProvider, error) {
			return NewApprovalProviderGatewaydFetch(client, "live", 3*time.Hour, nil)
		},
	} {
		if _, err := build(); !errors.Is(err, ErrApprovalConfig) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}
