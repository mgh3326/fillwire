package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/reader"
	"github.com/mgh3326/go-kis/kis/ws"
)

func TestCommandHelp(t *testing.T) {
	for _, command := range []string{
		"fillwire", "run", "serve", "upbit", "upbit-fills", "alpaca", "nhplug-ws", "coverage", "bookrec", "edge-canary",
		"execmock", "execsim", "execupbit", "gatewayd", "kis-mock-edge", "kis-mock-read", "market-tap",
	} {
		t.Run(command, func(t *testing.T) {
			child := exec.Command(os.Args[0], "-test.run=^TestDispatchProcess$", "--", command, "--help")
			output, err := child.CombinedOutput()
			if err != nil || !(strings.Contains(strings.ToLower(string(output)), "usage") || strings.Contains(string(output), "사용법")) {
				t.Fatalf("fillwire %s --help: %v\n%s", command, err, output)
			}
		})
	}
}

func TestRootHelpPreservesDeploySmokeContract(t *testing.T) {
	for _, option := range []string{"--help", "-help", "-h"} {
		t.Run(option, func(t *testing.T) {
			child := exec.Command(os.Args[0], "-test.run=^TestDispatchProcess$", "--", option)
			output, err := child.CombinedOutput()
			if err != nil {
				t.Fatalf("fillwire %s: %v\n%s", option, err, output)
			}
			for _, want := range []string{"Usage of", "-config string", "serve"} {
				if !strings.Contains(string(output), want) {
					t.Errorf("fillwire %s missing %q:\n%s", option, want, output)
				}
			}
		})
	}
}

func TestDispatchProcess(t *testing.T) {
	if args := os.Args[1:]; len(args) >= 3 && args[1] == "--" {
		os.Exit(dispatch(args[2:]))
	}
}

func TestServeRegistryIsOptInAndHasNoOrderModules(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	defaults, address, err := serveModules(nil, logger)
	if err != nil || address != "127.0.0.1:8792" {
		t.Fatalf("default serve config: address=%q, err=%v", address, err)
	}
	var sections []string
	for _, slot := range defaults {
		sections = append(sections, slot.name)
		if slot.Module != nil {
			t.Errorf("%s enabled by default", slot.name)
		}
	}
	want := []string{"alpaca", "gatewayd", "kis_fills", "kis_futures", "kis_quotes", "quotes", "toss_ws", "upbit", "upbit_fills"}
	if !reflect.DeepEqual(sections, want) {
		t.Fatalf("serve registered %v, want %v", sections, want)
	}
	for _, section := range []string{"alpaca", "upbit_fills", "gatewayd", "kis_fills", "kis_futures", "kis_quotes", "books", "quotes", "toss_ws", "upbit", "execmock", "execsim", "execupbit", "kis-mock-edge"} {
		t.Run(section, func(t *testing.T) {
			slots, _, err := serveModules([]byte("["+section+"]\nenabled = true"), logger)
			if err != nil {
				t.Fatal(err)
			}
			var active []string
			for _, slot := range slots {
				if slot.Module != nil {
					active = append(active, slot.Module.Name())
				}
			}
			var expected []string
			switch section {
			case "kis_fills", "kis_futures", "kis_quotes":
				expected = []string{section, "kis_session"}
			case "books":
				expected = []string{"kis_quotes", "kis_session"}
			case "alpaca", "upbit_fills", "gatewayd", "quotes", "toss_ws", "upbit":
				expected = []string{section}
			}
			if !reflect.DeepEqual(active, expected) {
				t.Fatalf("active modules = %v, want %v", active, expected)
			}
		})
	}
}

func TestSupervisorRestartsOnlyFailedModule(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var stableStarts, flakyStarts atomic.Int32
	fail := make(chan struct{})
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	stableStarted := make(chan struct{})
	s := newSupervisor([]*moduleSlot{
		{name: "stable", Module: &serviceModule{name: "stable", start: func(ctx context.Context, ready func()) error {
			stableStarts.Add(1)
			ready()
			close(stableStarted)
			<-ctx.Done()
			return ctx.Err()
		}}},
		{name: "flaky", Module: &serviceModule{name: "flaky", start: func(ctx context.Context, ready func()) error {
			ready()
			if flakyStarts.Add(1) == 1 {
				close(firstStarted)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-fail:
					panic("fake crash")
				}
			}
			close(secondStarted)
			<-ctx.Done()
			return ctx.Err()
		}}},
		{name: "off"},
	}, logger)
	s.retryMin, s.retryMax = 25*time.Millisecond, 100*time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	done := make(chan struct{})
	go func() { defer close(done); s.run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	wait := func(started <-chan struct{}) {
		t.Helper()
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("module did not start")
		}
	}
	checkHealth := func(restarts uint64) {
		t.Helper()
		resp := httptest.NewRecorder()
		s.health(resp, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		var body struct {
			Modules map[string]moduleStatus
		}
		if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		want := map[string]moduleStatus{
			"stable": {State: "running"},
			"flaky":  {State: "running", Restarts: restarts},
			"off":    {State: "disabled"},
		}
		if resp.Code != http.StatusOK || !reflect.DeepEqual(body.Modules, want) {
			t.Fatalf("health HTTP %d: %+v, want 200 and %+v", resp.Code, body.Modules, want)
		}
	}
	wait(stableStarted)
	wait(firstStarted)
	checkHealth(0)
	close(fail)
	wait(secondStarted)
	checkHealth(1)
	if stableStarts.Load() != 1 || flakyStarts.Load() != 2 {
		t.Fatalf("stable starts=%d, flaky starts=%d", stableStarts.Load(), flakyStarts.Load())
	}
}

type fakeKISSession struct {
	subscribed     chan string
	unsubscribed   chan string
	unsubscribeErr error
	subscribeErr   error
}

func (f fakeKISSession) Subscribe(_ context.Context, tr, key string) error {
	if f.subscribed != nil {
		f.subscribed <- tr + ":" + key
	}
	return f.subscribeErr
}

func (f fakeKISSession) Unsubscribe(_ context.Context, tr, key string) error {
	if f.unsubscribed != nil {
		f.unsubscribed <- tr + ":" + key
	}
	return f.unsubscribeErr
}

func TestSharedKISFillAttachmentLeavesQuoteAndFuturesAttached(t *testing.T) {
	h := newKISHub()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	wire := fakeKISSession{subscribed: make(chan string, 1), unsubscribed: make(chan string, 1)}
	var quotes, futures int
	s := &kisSession{Session: wire, ctx: ctx, cancel: cancel,
		quotes: func(ws.Event) { quotes++ }, futures: func(ws.Event) { futures++ }}
	h.publish(s)
	var cfg runtimeConfig
	cfg.KIS.Endpoint, cfg.KIS.HTSID = "mock", "synthetic"
	out := make(chan ws.Event)
	ready := make(chan struct{})
	fillsCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- h.fills(cfg, func() { close(ready) })(fillsCtx, out, nil, nil); close(out) }()
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("fill subscription never became ready")
	}
	if got := <-wire.subscribed; got != ws.TRExecutionVTS+":synthetic" {
		t.Fatalf("fill subscription = %q", got)
	}
	delivered := make(chan struct{})
	go func() { s.onFill(ctx, ws.Event{TR: ws.TRExecutionVTS}); close(delivered) }()
	select {
	case got := <-out:
		if got.TR != ws.TRExecutionVTS {
			t.Fatalf("delivered TR = %s", got.TR)
		}
	case <-ctx.Done():
		t.Fatal("fill was not delivered")
	}
	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("fill subscription stop error = %v", err)
	}
	<-delivered
	if got := <-wire.unsubscribed; got != ws.TRExecutionVTS+":synthetic" {
		t.Fatalf("fill unsubscribe = %q", got)
	}
	s.onQuote(ws.Event{})
	s.onFutures(ws.Event{})
	if quotes != 1 || futures != 1 || ctx.Err() != nil {
		t.Fatalf("closing fills stopped shared socket or unrelated callbacks")
	}
}

func TestServeTossQuotesRejectsRemotePlaintextRedis(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := runTossQuoteFeed(context.Background(), []byte(`
[quotes]
enabled = true
[redis]
url = "redis://fixture:fixture@example.invalid:6379"
`), logger, func() { t.Fatal("unsafe module became ready") })
	if want := validateRedisURL("redis://fixture:fixture@example.invalid:6379"); err == nil || err.Error() != want.Error() {
		t.Fatalf("Toss quote startup returned %v, want %v", err, want)
	}
}

func TestSupervisorStopsKISTerminalErrors(t *testing.T) {
	for _, failure := range []error{ws.ErrSessionOccupied, reader.ErrApprovalConfig, reader.ErrCacheOnlyApprovalUnavailable} {
		t.Run(failure.Error(), func(t *testing.T) {
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			var attempts atomic.Int32
			slot := &moduleSlot{name: "kis_session", Module: &serviceModule{name: "kis_session", start: func(context.Context, func()) error {
				attempts.Add(1)
				return failure
			}}}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			s := newSupervisor([]*moduleSlot{slot}, logger)
			s.retryMin, s.retryMax = time.Millisecond, 2*time.Millisecond
			s.run(ctx)
			if got := slot.snapshot(); got != (moduleStatus{State: "failed"}) || attempts.Load() != 1 {
				t.Fatalf("state=%+v attempts=%d", got, attempts.Load())
			}
		})
	}
}

func TestKISRemovalKeepsSharedSocketForAbsentOrBookRegistration(t *testing.T) {
	for _, tc := range []struct {
		tr  string
		err error
	}{
		{ws.TRExecutionVTS, &ws.SubscribeError{}},
		{ws.TRQuoteBook, context.DeadlineExceeded},
		{ws.TRExecutionVTS, io.ErrClosedPipe},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		session := &kisSession{Session: fakeKISSession{unsubscribeErr: tc.err}, ctx: ctx, cancel: cancel}
		session.remove(tc.tr, "fixture")
		err := ctx.Err()
		cancel()
		if err != nil {
			t.Errorf("removal of %s after %v canceled shared socket", tc.tr, tc.err)
		}
	}
}

func TestKISOccupiedSubscribeCancelsGenerationAndKeepsReason(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &kisSession{Session: fakeKISSession{subscribeErr: ws.ErrSessionOccupied}, ctx: ctx, cancel: cancel}
	err := s.add(context.Background(), ws.TRExecutionVTS, "fixture")
	if !errors.Is(err, ws.ErrSessionOccupied) || !s.occupied.Load() || ctx.Err() == nil {
		t.Fatalf("occupied subscription: err=%v recorded=%v context=%v", err, s.occupied.Load(), ctx.Err())
	}
}

func TestKISBooksAttachWhenPromotedQuotesAreInvalid(t *testing.T) {
	t.Setenv("SERVE_TEST_APP_KEY", "fixture-key")
	t.Setenv("SERVE_TEST_APP_SECRET", "fixture-secret")
	t.Setenv("SERVE_TEST_BOOK_TOKEN", "")
	contents := []byte(`
[kis]
endpoint = "live"
app_key_env = "SERVE_TEST_APP_KEY"
app_secret_env = "SERVE_TEST_APP_SECRET"
[redis]
url = "redis://127.0.0.1:1"
[kis_quotes]
enabled = true
[books]
enabled = true
symbol = "005930"
access_token_env = "SERVE_TEST_BOOK_TOKEN"
`)
	if resolveKisQuoteSettings(contents).err == nil {
		t.Fatal("fixture should reject promoted quotes")
	}
	h := newKISHub()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	wire := fakeKISSession{subscribed: make(chan string, 1), unsubscribed: make(chan string, 1)}
	h.publish(&kisSession{Session: wire, ctx: ctx, cancel: cancel})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan error, 1)
	var ready atomic.Bool
	go func() { done <- runKISQuotes(ctx, contents, h, logger, false, func() { ready.Store(true) }) }()
	select {
	case got := <-wire.subscribed:
		if got != ws.TRQuoteBook+":005930" {
			t.Errorf("book subscription = %q", got)
		}
	case err := <-done:
		t.Fatalf("fixed books stopped with invalid promoted quotes: %v", err)
	case <-ctx.Done():
		t.Error("fixed books did not attach")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || ready.Load() {
		t.Fatalf("result = %v, combined quote module ready = %v", err, ready.Load())
	}
}

func TestServeKISFailureAlerts(t *testing.T) {
	ctx := context.Background()
	fake := &failingAlerter{}
	for _, failure := range []error{reader.ErrApprovalConfig, errors.New("fixture transient"), ws.ErrSessionOccupied, errSharedKISStopped} {
		run := alertKISFailures(func(context.Context, func()) error { return failure }, fake)
		if err := run(ctx, func() {}); !errors.Is(err, failure) {
			t.Fatalf("alert wrapper lost original failure: %v", err)
		}
	}
	if want := []string{"configuration", "transient", "session-occupied"}; !reflect.DeepEqual(fake.classes, want) {
		t.Fatalf("alerts = %v, want %v", fake.classes, want)
	}
}
