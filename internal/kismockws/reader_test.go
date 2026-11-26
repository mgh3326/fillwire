package kismockws

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"slices"
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
	for _, tr := range []string{"H0STCNI9", "H0STCNT0", "H0STASP0", TROverseasNotice, TRFuturesNotice} {
		if !slices.Contains(got, tr) {
			t.Errorf("subscriptions %v missing %s", got, tr)
		}
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

func TestMalformedEventsAreCountedAndDropped(t *testing.T) {
	root := t.TempDir()
	sink, err := recording.NewSink(root, Name)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	module := &Module{}
	module.recordQuote(sink, ws.Event{TR: "H0STCNT0", Raw: []byte("malformed")})
	module.recordNotice(sink, ws.Event{TR: TROverseasNotice, Fields: []string{"short"}})
	if got := module.Health().MalformedFrames; got != 2 {
		t.Fatalf("malformed frame count = %d, want 2", got)
	}
	entries, err := filepath.Glob(filepath.Join(root, Name, "*", "*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("malformed frames were recorded: %v", entries)
	}
}
