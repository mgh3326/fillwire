package reader

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

type reconnectFallback struct{ reissueCalls atomic.Int32 }

func (*reconnectFallback) ApprovalKey(context.Context) (string, error) {
	return "synthetic-rest-key", nil
}

func (p *reconnectFallback) Reissue(context.Context) (string, error) {
	p.reissueCalls.Add(1)
	return "synthetic-rest-reissue", nil
}

type observedCacheOnlyProvider struct {
	*ApprovalProvider
	reissueCalls atomic.Int32
}

func (p *observedCacheOnlyProvider) Reissue(ctx context.Context) (string, error) {
	p.reissueCalls.Add(1)
	return p.ApprovalProvider.Reissue(ctx)
}

type reconnectTransport struct {
	in         chan []byte
	closed     chan struct{}
	onWrite    func(*reconnectTransport, []byte)
	subscribed chan struct{}
	writes     atomic.Int32
	once       sync.Once
}

func newReconnectTransport(onWrite func(*reconnectTransport, []byte)) *reconnectTransport {
	return &reconnectTransport{
		in:         make(chan []byte, 8),
		closed:     make(chan struct{}),
		onWrite:    onWrite,
		subscribed: make(chan struct{}),
	}
}

func (t *reconnectTransport) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, io.EOF
	case frame := <-t.in:
		return frame, nil
	}
}

func (t *reconnectTransport) Write(_ context.Context, frame []byte) error {
	t.writes.Add(1)
	if t.onWrite != nil {
		t.onWrite(t, frame)
	}
	return nil
}

func (*reconnectTransport) WriteControl(context.Context, int, []byte) error { return nil }

func (t *reconnectTransport) Close() error {
	t.once.Do(func() { close(t.closed) })
	return nil
}

func (t *reconnectTransport) drop() { _ = t.Close() }

func (t *reconnectTransport) push(frame string) {
	select {
	case t.in <- []byte(frame):
	case <-t.closed:
	}
}

func reconnectACK(t *reconnectTransport, raw []byte, msgCD, msg1 string) {
	var request struct {
		Body struct {
			Input struct {
				TRID string `json:"tr_id"`
			} `json:"input"`
		} `json:"body"`
	}
	if json.Unmarshal(raw, &request) != nil {
		return
	}
	if msgCD == "" {
		msgCD = "MCA00000"
	}
	t.push(`{"header":{"tr_id":"` + request.Body.Input.TRID + `"},"body":{"rt_cd":"0","msg_cd":"` + msgCD + `","msg1":"` + msg1 + `","output":{}}}`)
	if msgCD == "MCA00000" {
		select {
		case <-t.subscribed:
		default:
			close(t.subscribed)
		}
	}
}

type pacedClock struct{ now time.Time }

func (c pacedClock) Now() time.Time { return c.now }

func (c pacedClock) After(delay time.Duration) <-chan time.Time {
	result := make(chan time.Time, 1)
	time.AfterFunc(delay, func() { result <- c.now.Add(delay) })
	return result
}

func TestCacheOnlyReconnectOPSP0011TerminatesReader(t *testing.T) {
	mini := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	if err := mini.Set(approvalCacheKey, "cached-synthetic-key"); err != nil {
		t.Fatal(err)
	}
	fallback := &reconnectFallback{}
	baseProvider, err := NewApprovalProviderWithMode(redisClient, fallback, ApprovalModeCacheOnly)
	if err != nil {
		t.Fatal(err)
	}
	provider := &observedCacheOnlyProvider{ApprovalProvider: baseProvider}
	first := newReconnectTransport(func(transport *reconnectTransport, raw []byte) {
		reconnectACK(transport, raw, "", "SUBSCRIBE SUCCESS")
	})
	rejectionSeen := make(chan struct{}, 1)
	second := newReconnectTransport(func(transport *reconnectTransport, raw []byte) {
		reconnectACK(transport, raw, "OPSP0011", "approval rejected")
		select {
		case rejectionSeen <- struct{}{}:
		default:
		}
	})
	var dials atomic.Int32
	cfg := Config{
		Endpoint:    "live",
		HTSID:       "HTS_EXAMPLE",
		Approval:    provider,
		EventBuffer: 1,
		Backoff:     ws.BackoffConfig{Min: 10 * time.Millisecond, Max: 10 * time.Millisecond, Factor: 1, Jitter: -1},
		Clock:       pacedClock{now: time.Date(2026, time.September, 9, 21, 0, 0, 0, time.FixedZone("KST", 9*60*60))},
		Dialer: ws.DialerFunc(func(context.Context, string) (ws.Transport, error) {
			switch dials.Add(1) {
			case 1:
				return first, nil
			case 2:
				return second, nil
			default:
				return nil, errors.New("bounded synthetic reconnect dial")
			}
		}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- New(cfg).Run(ctx, make(chan ws.Event, 1)) }()
	select {
	case <-first.subscribed:
	case <-time.After(time.Second):
		t.Fatal("initial synthetic subscription did not complete")
	}
	time.Sleep(time.Millisecond)
	first.drop()
	select {
	case <-rejectionSeen:
	case <-time.After(time.Second):
		t.Fatal("synthetic OPSP0011 ACK was not observed")
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrCacheOnlyApprovalUnavailable) {
			t.Fatalf("Reader.Run error = %v, want named cache-only failure", err)
		}
		if got := ProcessExitCode(err); got != ExitCodeCacheOnlyApprovalUnavailable {
			t.Fatalf("process exit code = %d, want %d", got, ExitCodeCacheOnlyApprovalUnavailable)
		}
	case <-time.After(2 * time.Second):
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		t.Fatal("Reader.Run stayed alive after cache-only reconnect failure")
	}
	if got := provider.reissueCalls.Load(); got != 1 {
		t.Fatalf("go-kis Reissue calls observed = %d, want 1", got)
	}
	if got := fallback.reissueCalls.Load(); got != 0 {
		t.Fatalf("REST reissue calls = %d, want 0", got)
	}
	if got := dials.Load(); got != 2 {
		t.Fatalf("KIS dials = %d, want initial plus one reconnect", got)
	}
	if got := second.writes.Load(); got < 1 {
		t.Fatalf("reconnect subscribe writes = %d, want at least 1", got)
	}
	if got, err := mini.Get(approvalCacheKey); err != nil || got != "cached-synthetic-key" {
		t.Fatalf("cached approval after reconnect failure = %q, %v", got, err)
	}
}
