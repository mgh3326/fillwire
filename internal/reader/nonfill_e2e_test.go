package reader_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/fillwire/internal/decode"
	fillreader "github.com/mgh3326/fillwire/internal/reader"
	"github.com/mgh3326/fillwire/internal/sink"
	"github.com/mgh3326/fillwire/internal/stream"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

// h0stcni0 renders a real-shape 26-field H0STCNI0 frame. The order is for 5
// shares at 71,500; qty/price/hour/rctf/rfus/cntg/acpt vary per notice.
func h0stcni0(rctf, qty, price, hour, rfus, cntg, acpt string) string {
	fields := []string{
		"HTS_EXAMPLE", "00000000", "A123456789", "0000000000", "02", rctf, "00",
		"0", "005930", qty, price, hour, rfus, cntg, acpt, "00000", "5", "",
		"0", "1", "Y", "", "10", "", "EXAMPLE", "71500",
	}
	return "0|H0STCNI0|1|" + strings.Join(fields, "^")
}

// TestNonFillNoticesNeverReachIngestEndToEnd drives frames through the go-kis
// parser, the decoder, the Redis stream and the sink runner into a fake
// ingest. Only the CNTG_YN=2 fill may arrive, carrying the fill columns.
func TestNonFillNoticesNeverReachIngestEndToEnd(t *testing.T) {
	frames := []string{
		h0stcni0("0", "5", "71500", "091256", "0", "1", "1"), // order accept
		h0stcni0("1", "5", "71400", "091500", "0", "1", "2"), // modify confirm
		h0stcni0("0", "5", "71500", "091600", "1", "1", "1"), // refused
		h0stcni0("2", "5", "71500", "091700", "0", "1", "2"), // cancel confirm
		h0stcni0("0", "5", "71500", "091800", "0", "7", "1"), // unknown CNTG_YN
		h0stcni0("0", "3", "71200", "093015", "0", "2", "2"), // the fill
	}

	mini := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer redisClient.Close()
	counters := decode.NewCounters()
	queue, err := stream.New(redisClient, stream.Config{
		Key: "fills:kis", MaxLen: 100, Group: "fillwire-ingest", Consumer: "test-consumer",
		BatchSize: 200, Block: time.Millisecond, Counters: counters,
	})
	if err != nil {
		t.Fatal(err)
	}

	type posted struct {
		OrderID  string `json:"broker_order_id"`
		Qty      string `json:"filled_qty"`
		Price    string `json:"filled_price"`
		FilledAt string `json:"filled_at"`
	}
	var mu sync.Mutex
	var received []posted
	requestSeen := make(chan struct{}, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		var envelope struct {
			Fills []posted `json:"fills"`
		}
		_ = json.Unmarshal(body, &envelope)
		mu.Lock()
		received = append(received, envelope.Fills...)
		mu.Unlock()
		results := make([]string, len(envelope.Fills))
		for i := range results {
			results[i] = `{"status":"inserted","row_id":1,"reason":null}`
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"source":"fillwire","source_run_id":null,"received":1,"accepted":1,"rejected":0,"results":[` + strings.Join(results, ",") + `]}`))
		requestSeen <- struct{}{}
	}))
	defer server.Close()
	ingestClient, err := sink.NewClient(sink.HTTPConfig{URL: server.URL, Token: "test-token", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := sink.NewRunner(queue, ingestClient, sink.Config{RetryMin: time.Millisecond, RetryMax: 5 * time.Millisecond, Factor: 2, Counters: counters})
	if err != nil {
		t.Fatal(err)
	}

	transport := newFakeTransport(successfulSubscribe)
	events := make(chan ws.Event, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readerDone := make(chan error, 1)
	go func() { readerDone <- fillreader.New(readerConfig(transport, &fakeApproval{})).Run(ctx, events) }()
	select {
	case <-transport.subscribed:
	case <-time.After(time.Second):
		t.Fatal("reader did not subscribe")
	}

	decoder := decode.New(decode.Config{AccountMode: "live", Venue: "krx", DupTrackMax: 10, Counters: counters})
	for _, frame := range frames {
		transport.push(frame)
		var event ws.Event
		select {
		case event = <-events:
		case <-time.After(time.Second):
			t.Fatal("reader did not drain execution event")
		}
		if event.Execution == nil {
			t.Fatalf("go-kis did not parse %q as an execution", frame)
		}
		if record, ok := decoder.Decode(event); ok {
			if _, err := queue.Enqueue(ctx, record); err != nil {
				t.Fatal(err)
			}
		}
	}

	runnerDone := make(chan error, 1)
	go func() { runnerDone <- runner.Run(ctx) }()
	select {
	case <-requestSeen:
	case <-time.After(time.Second):
		t.Fatal("ingest request not received")
	}
	waitPending(t, redisClient, "fills:kis", "fillwire-ingest", 0)
	cancel()
	if err := <-readerDone; err != nil {
		t.Fatalf("reader returned %v", err)
	}
	if err := <-runnerDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("runner returned %v, want context cancellation", err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := posted{OrderID: "A123456789", Qty: "3", Price: "71200", FilledAt: "2026-09-07T09:30:15+09:00"}
	if len(received) != 1 || received[0] != want {
		t.Fatalf("ingest received %+v, want exactly [%+v]", received, want)
	}
	snapshot := counters.Snapshot()
	if snapshot.XAdded != 1 || snapshot.NonFillOrder != 2 || snapshot.NonFillReject != 1 || snapshot.NonFillCancel != 1 || snapshot.NonFillUnknown != 1 {
		t.Fatalf("counters = %+v, want XAdded 1, order 2, reject 1, cancel 1, unknown 1", snapshot)
	}
}
