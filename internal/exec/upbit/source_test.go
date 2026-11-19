package upbit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestSourceReconnectsAndDecodesBooks drives the real reconnect loop against a
// scripted websocket server: a valid frame, a frame this model refuses, then a
// disconnect. The source must report the book, the decode error, and the book
// from the reconnected session.
func TestSourceReconnectsAndDecodesBooks(t *testing.T) {
	var connections atomic.Int32
	accepted := make(chan int32, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "")
		n := connections.Add(1)
		accepted <- n
		if _, _, err := c.Read(r.Context()); err != nil {
			return
		}
		switch n {
		case 1:
			_ = c.Write(r.Context(), websocket.MessageText, []byte(sampleFrame))
			_ = c.Write(r.Context(), websocket.MessageText, []byte(`{"type":"orderbook","code":"KRW-XRP","timestamp":1,"orderbook_units":[]}`))
			_ = c.Close(websocket.StatusNormalClosure, "forced disconnect")
		case 2:
			frame := strings.Replace(sampleFrame, `"ask_price":3000.0`, `"ask_price":3010.0`, 1)
			_ = c.Write(r.Context(), websocket.MessageText, []byte(frame))
			<-r.Context().Done()
		}
	}))
	defer server.Close()

	events := make(chan SourceEvent, 8)
	src, err := NewSource(SourceConfig{
		Market:   "KRW-XRP",
		Root:     t.TempDir(),
		Dialer:   dialFunc(func(ctx context.Context, _ string) (*websocket.Conn, error) { return dial(ctx, server.URL) }),
		Clock:    fastClock{},
		RetryMin: time.Millisecond,
		RetryMax: time.Millisecond,
	}, func(ev SourceEvent) { events <- ev })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- src.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	want := []struct {
		kind string
		ask  int64
	}{{EventBook, 3000}, {EventDecodeError, 0}, {EventGap, 0}, {EventBook, 3010}}
	for i, w := range want {
		select {
		case ev := <-events:
			if ev.Kind != w.kind {
				t.Fatalf("event %d kind = %q, want %q (%+v)", i, ev.Kind, w.kind, ev)
			}
			if ev.Kind == EventBook && (ev.Book.AskPriceKRW != w.ask || ev.Book.Market != "KRW-XRP") {
				t.Fatalf("event %d book = %+v", i, ev.Book)
			}
			if ev.Kind == EventDecodeError && !strings.Contains(ev.Reason, "no orderbook_units") {
				t.Fatalf("decode error reason = %q", ev.Reason)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("event %d not emitted; connections=%d", i, connections.Load())
		}
	}
	if got := connections.Load(); got < 2 {
		t.Fatalf("source did not reconnect; connections=%d", got)
	}
}

func TestNewSourceRefusesUnusableConfig(t *testing.T) {
	emit := func(SourceEvent) {}
	for name, cfg := range map[string]SourceConfig{
		"unknown market": {Market: "KRW-DOGE", Root: t.TempDir()},
		"no root":        {Market: "KRW-XRP"},
	} {
		if _, err := NewSource(cfg, emit); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := NewSource(SourceConfig{Market: "KRW-XRP", Root: t.TempDir()}, nil); err == nil {
		t.Error("nil emit accepted")
	}
}

type dialFunc func(context.Context, string) (*websocket.Conn, error)

func (f dialFunc) Dial(ctx context.Context, endpoint string) (*websocket.Conn, error) {
	return f(ctx, endpoint)
}

func dial(ctx context.Context, serverURL string) (*websocket.Conn, error) {
	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(serverURL, "http"), nil)
	return c, err
}

type fastClock struct{}

func (fastClock) Now() time.Time { return time.Now() }
func (fastClock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
