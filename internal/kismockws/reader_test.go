package kismockws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/recording"
	"github.com/mgh3326/go-kis/kis/ws"
)

type unusedApproval struct{}

func (unusedApproval) ApprovalKey(context.Context) (string, error) { return "test-key", nil }
func (unusedApproval) Reissue(context.Context) (string, error)     { return "test-key", nil }

func TestModuleRejectsLiveEndpointBeforeDial(t *testing.T) {
	called := false
	module, err := New(Config{
		Endpoint: "live", HTSID: "fixture-hts", Symbols: []string{"005930"},
		RecordingDirectory: t.TempDir(), Approval: unusedApproval{},
		Dialer: ws.DialerFunc(func(context.Context, string) (ws.Transport, error) {
			called = true
			return nil, nil
		}),
	})
	if err == nil || module != nil {
		t.Fatalf("New(live) = (%v, %v), want refusal", module, err)
	}
	if called {
		t.Fatal("live endpoint dialed before mock endpoint refusal")
	}
}

type ackTransport struct {
	mu     sync.Mutex
	reads  chan []byte
	closed chan struct{}
	once   sync.Once
	trs    []string
}

func newAckTransport() *ackTransport {
	return &ackTransport{reads: make(chan []byte, 32), closed: make(chan struct{})}
}

func (t *ackTransport) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, io.EOF
	case message := <-t.reads:
		return message, nil
	}
}

func (t *ackTransport) Write(_ context.Context, data []byte) error {
	var request struct {
		Body struct {
			Input struct {
				TR string `json:"tr_id"`
			} `json:"input"`
		} `json:"body"`
	}
	if err := json.Unmarshal(data, &request); err != nil {
		return err
	}
	t.mu.Lock()
	t.trs = append(t.trs, request.Body.Input.TR)
	t.mu.Unlock()
	t.reads <- []byte(`{"header":{"tr_id":"` + request.Body.Input.TR + `"},"body":{"rt_cd":"0","msg_cd":"MCA00000","msg1":"SUBSCRIBE SUCCESS","output":{}}}`)
	return nil
}

func (*ackTransport) WriteControl(context.Context, int, []byte) error { return nil }
func (t *ackTransport) Close() error {
	t.once.Do(func() { close(t.closed) })
	return nil
}

func TestModuleUsesVTSAndMockTRRegistrations(t *testing.T) {
	transport := newAckTransport()
	var endpoint string
	ctx, cancel := context.WithCancel(context.Background())
	module, err := New(Config{
		Endpoint: "mock", HTSID: "fixture-hts", Symbols: []string{"005930"},
		RecordingDirectory: t.TempDir(), Approval: unusedApproval{}, SubscribePace: time.Nanosecond,
		Dialer: ws.DialerFunc(func(_ context.Context, got string) (ws.Transport, error) {
			endpoint = got
			return transport, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- module.Run(ctx) }()
	deadline := time.After(2 * time.Second)
	for {
		transport.mu.Lock()
		count := len(transport.trs)
		transport.mu.Unlock()
		if count == 5 { // H0STCNI9 plus two quotes and two notices
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("mock subscriptions did not complete")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if endpoint != ws.EndpointVTS {
		t.Fatalf("dial endpoint = %q, want VTS %q", endpoint, ws.EndpointVTS)
	}
	transport.mu.Lock()
	got := slices.Clone(transport.trs)
	transport.mu.Unlock()
	for _, tr := range []string{"H0STCNI9", "H0STCNT0", "H0STASP0", TROverseasNotice, "H0IFCNI0"} {
		if !slices.Contains(got, tr) {
			t.Errorf("subscriptions %v missing %s", got, tr)
		}
	}
	if slices.Contains(got, "H0IFCNI9") {
		t.Errorf("default subscriptions unexpectedly contain H0IFCNI9: %v", got)
	}
}

func TestModuleUsesConfiguredFuturesNoticeTR(t *testing.T) {
	transport := newAckTransport()
	ctx, cancel := context.WithCancel(context.Background())
	module, err := New(Config{
		Endpoint: "mock", HTSID: "fixture-hts", Symbols: []string{"005930"},
		FuturesNoticeTR: TRFuturesNotice, RecordingDirectory: t.TempDir(),
		Approval: unusedApproval{}, SubscribePace: time.Nanosecond,
		Dialer: ws.DialerFunc(func(_ context.Context, _ string) (ws.Transport, error) { return transport, nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- module.Run(ctx) }()
	deadline := time.After(2 * time.Second)
	for {
		transport.mu.Lock()
		count := len(transport.trs)
		transport.mu.Unlock()
		if count == 5 {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("configured mock subscriptions did not complete")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	transport.mu.Lock()
	got := slices.Clone(transport.trs)
	transport.mu.Unlock()
	if !slices.Contains(got, TRFuturesNotice) || slices.Contains(got, TRFuturesNoticeAlt) {
		t.Fatalf("subscriptions = %v, want configured TR %s only", got, TRFuturesNotice)
	}
}

func TestModuleReturnsRecordingErrorAfterAppendFailure(t *testing.T) {
	transport := newAckTransport()
	module, err := New(Config{
		Endpoint: "mock", HTSID: "fixture-hts", Symbols: []string{"005930"},
		RecordingDirectory: t.TempDir(), Approval: unusedApproval{}, SubscribePace: time.Nanosecond,
		Dialer: ws.DialerFunc(func(context.Context, string) (ws.Transport, error) { return transport, nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	module.appendRecord = func(string, time.Time, any) error { return errors.New("disk full") }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- module.Run(ctx) }()
	deadline := time.After(2 * time.Second)
	for {
		transport.mu.Lock()
		count := len(transport.trs)
		transport.mu.Unlock()
		if count == 5 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("mock subscriptions did not complete")
		case <-time.After(time.Millisecond):
		}
	}
	fields := make([]string, 46)
	fields[0], fields[1] = "005930", "100203"
	fields[2], fields[12], fields[13], fields[21] = "100", "1", "1", "1"
	transport.reads <- []byte("0|H0STCNT0|001|" + strings.Join(fields, "^"))
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "disk full") {
			t.Fatalf("Run error = %v, want append failure", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after recording append failure")
	}
}

func TestModuleEnforcesSessionRegistrationLimit(t *testing.T) {
	symbols := make([]string, 20)
	for i := range symbols {
		symbols[i] = fmt.Sprintf("%06d", i+1)
	}
	_, err := New(Config{
		Endpoint: "mock", HTSID: "fixture-hts", Symbols: symbols,
		RecordingDirectory: t.TempDir(), Approval: unusedApproval{},
	})
	if err == nil {
		t.Fatal("20 symbols exceed the per-session registration budget")
	}
}

func TestModuleCanSelectLegacyFuturesTR(t *testing.T) {
	transport := newAckTransport()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	module, err := New(Config{
		Endpoint: "mock", HTSID: "fixture-hts", Symbols: []string{"005930"},
		FuturesNoticeTR: TRFuturesNotice, RecordingDirectory: t.TempDir(), Approval: unusedApproval{}, SubscribePace: time.Nanosecond,
		Dialer: ws.DialerFunc(func(context.Context, string) (ws.Transport, error) { return transport, nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- module.Run(ctx) }()
	deadline := time.After(2 * time.Second)
	for {
		transport.mu.Lock()
		count := len(transport.trs)
		got := slices.Clone(transport.trs)
		transport.mu.Unlock()
		if count == 5 {
			if !slices.Contains(got, TRFuturesNotice) {
				t.Fatalf("subscriptions %v lack configurable futures TR %s", got, TRFuturesNotice)
			}
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("mock subscriptions did not complete")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestModuleRejectsUnknownFuturesNoticeTR(t *testing.T) {
	_, err := New(Config{
		Endpoint: "mock", HTSID: "fixture-hts", Symbols: []string{"005930"},
		FuturesNoticeTR: "H0IFCNI7", RecordingDirectory: t.TempDir(), Approval: unusedApproval{},
	})
	if err == nil {
		t.Fatal("unknown futures notice TR accepted")
	}
}

func TestMalformedEventsAreCountedAndDropped(t *testing.T) {
	root := t.TempDir()
	sink, err := recording.NewSink(root, Name)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	module := &Module{}
	module.recordQuote(sink, ws.Event{TR: "H0STCNT0", Raw: []byte("malformed")}, func() {})
	module.recordNotice(sink, ws.Event{TR: TROverseasNotice, Fields: []string{"short"}, Raw: []byte("0|" + TROverseasNotice + "|001|ciphertext")}, func() {})
	if got := module.Health().MalformedFrames; got != 2 {
		t.Fatalf("malformed frame count = %d, want 2", got)
	}
	entries, err := filepath.Glob(filepath.Join(root, Name, "*", "*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || filepath.Base(entries[0]) != malformedRecording+".jsonl" {
		t.Fatalf("malformed notice recording = %v, want only malformed-notices", entries)
	}
}

type retrySession struct {
	calls int
	err   error
}

func (s *retrySession) Subscribe(context.Context, string, string) error {
	s.calls++
	if s.calls == 1 {
		return s.err
	}
	return nil
}
func (*retrySession) Unsubscribe(context.Context, string, string) error { return nil }

func TestSubscribeWithRetryRetriesTransientAndStopsOnRejection(t *testing.T) {
	transient := &retrySession{err: fmt.Errorf("temporary transport error")}
	if err := subscribeWithRetry(context.Background(), transient, "H0STCNT0", "005930"); err != nil {
		t.Fatal(err)
	}
	if transient.calls != 2 {
		t.Fatalf("transient subscribe calls = %d, want 2", transient.calls)
	}
	permanent := &retrySession{err: &ws.SubscribeError{TR: "H0STCNT0", Msg1: "invalid registration"}}
	err := subscribeWithRetry(context.Background(), permanent, "H0STCNT0", "005930")
	if !errors.Is(err, ws.ErrSubscribeFailed) || permanent.calls != 1 {
		t.Fatalf("permanent rejection = %v after %d calls", err, permanent.calls)
	}
}

type cancelWaitSession struct{ started chan struct{} }

func (s cancelWaitSession) Subscribe(ctx context.Context, _, _ string) error {
	close(s.started)
	<-ctx.Done()
	return ctx.Err()
}
func (cancelWaitSession) Unsubscribe(context.Context, string, string) error { return nil }

func TestSubscribeRegistrationTreatsShutdownAsClean(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	session := cancelWaitSession{started: make(chan struct{})}
	result := make(chan struct {
		err      error
		canceled bool
	}, 1)
	go func() {
		err, canceled := subscribeRegistration(ctx, session, "H0STCNT0", "005930")
		result <- struct {
			err      error
			canceled bool
		}{err, canceled}
	}()
	<-session.started
	cancel()
	got := <-result
	if got.err != nil || !got.canceled {
		t.Fatalf("shutdown subscription result = (%v, %v), want (nil, true)", got.err, got.canceled)
	}
}

func TestSlowQuoteWriterDropsOverflowAndDrains(t *testing.T) {
	transport := newAckTransport()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	module, err := New(Config{
		Endpoint: "mock", HTSID: "fixture-hts", Symbols: []string{"005930"},
		RecordingDirectory: t.TempDir(), Approval: unusedApproval{}, SubscribePace: time.Nanosecond,
		Dialer: ws.DialerFunc(func(context.Context, string) (ws.Transport, error) { return transport, nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	quoteStarted, releaseQuote, noticeWritten := make(chan struct{}), make(chan struct{}), make(chan struct{})
	quotesWritten := 0
	module.appendRecord = func(stream string, _ time.Time, _ any) error {
		if stream != quoteRecordingName {
			close(noticeWritten)
			return nil
		}
		quotesWritten++
		if quotesWritten == 1 {
			close(quoteStarted)
			<-releaseQuote
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- module.Run(ctx) }()
	defer func() {
		cancel()
		close(releaseQuote)
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
		if !t.Failed() && quotesWritten != 33 {
			t.Errorf("recorded %d quotes, want one in flight plus 32 queued", quotesWritten)
		}
	}()
	for {
		transport.mu.Lock()
		count := len(transport.trs)
		transport.mu.Unlock()
		if count == 5 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("subscriptions did not complete")
		case <-time.After(time.Millisecond):
		}
	}
	fields := make([]string, 46)
	fields[0], fields[1] = "005930", "100203"
	fields[2], fields[12], fields[13], fields[21] = "100", "1", "1", "1"
	frame := []byte("0|H0STCNT0|001|" + strings.Join(fields, "^"))
	transport.reads <- frame
	select {
	case <-quoteStarted:
	case <-ctx.Done():
		t.Fatal("quote writer did not start")
	}
	for i := 0; i < 34; i++ {
		select {
		case transport.reads <- frame:
		case <-ctx.Done():
			t.Fatal("blocked quote writer stalled the reader")
		}
	}
	select {
	case transport.reads <- []byte("0|H0IFCNI0|001|malformed"):
	case <-ctx.Done():
		t.Fatal("could not deliver notice after quote burst")
	}
	select {
	case <-noticeWritten:
	case <-ctx.Done():
		t.Fatal("blocked quote writer stalled notice processing")
	}
}
