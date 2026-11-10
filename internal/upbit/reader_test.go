package upbit

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time  { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) set(t time.Time) { c.mu.Lock(); c.now = t; c.mu.Unlock() }
func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Millisecond):
		return nil
	}
}
func TestReaderWritesFramesGapAndRotatesUTC(t *testing.T) {
	day1 := time.Date(2026, 1, 1, 23, 59, 59, 0, time.UTC)
	day2 := day1.Add(2 * time.Second)
	clock := &fakeClock{now: day1}
	accepted := make(chan int, 2)
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "")
		n := int(connections.Add(1))
		accepted <- n
		_, request, readErr := c.Read(r.Context())
		var subscription []map[string]any
		if n == 1 && (readErr != nil || json.Unmarshal(request, &subscription) != nil || len(subscription) < 4) {
			t.Errorf("invalid subscription: %s (%v)", request, readErr)
		} else if n == 1 {
			orderbook, _ := subscription[1]["codes"].([]any)
			trade, _ := subscription[2]["codes"].([]any)
			if len(orderbook) != 1 || orderbook[0] != "KRW-BTC.15" || len(trade) != 1 || trade[0] != "KRW-BTC" {
				t.Errorf("subscription codes=%v", subscription)
			}
		}
		if n == 1 {
			_ = c.Write(r.Context(), websocket.MessageText, []byte(`{"type":"orderbook","code":"KRW-BTC","timestamp":1000,"orderbook_units":[]}`))
			clock.set(day2)
			_ = c.Write(r.Context(), websocket.MessageText, []byte(`{"type":"trade","code":"KRW-BTC","timestamp":2000,"sequential_id":42}`))
			_ = c.Close(websocket.StatusNormalClosure, "forced disconnect")
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http")
	root := t.TempDir()
	reader, err := New(Config{Markets: []string{"KRW-BTC"}, Root: root, Endpoint: endpoint, Clock: clock, RetryMin: time.Millisecond, RetryMax: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- reader.Run(ctx) }()
	select {
	case n := <-accepted:
		if n != 1 {
			t.Fatalf("first connection=%d", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("websocket not accepted")
	}
	select {
	case n := <-accepted:
		if n != 2 {
			t.Fatalf("second connection=%d", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not reconnect after disconnect")
	}
	cancel()
	<-done
	oldPath := root + "/upbit/2026-01-01/KRW-BTC.jsonl.gz"
	file, err := os.Open(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	var records []Record
	dec := json.NewDecoder(gz)
	for dec.More() {
		var rec Record
		if err := dec.Decode(&rec); err != nil {
			t.Fatal(err)
		}
		records = append(records, rec)
	}
	_ = gz.Close()
	_ = file.Close()
	if len(records) != 1 || records[0].Kind != "connected" {
		t.Fatalf("rotated records=%+v", records)
	}
	b, err := os.ReadFile(root + "/upbit/2026-01-02/KRW-BTC.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{`"kind":"orderbook"`, `"kind":"trade"`, `"kind":"gap"`} {
		if !strings.Contains(string(b), kind) {
			t.Fatalf("missing %s: %s", kind, b)
		}
	}
	if !strings.Contains(string(b), `"sequential_id":42`) {
		t.Fatalf("trade sequence missing: %s", b)
	}
}
