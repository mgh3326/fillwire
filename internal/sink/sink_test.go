package sink_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/fillwire/internal/decode"
	"github.com/mgh3326/fillwire/internal/sink"
	"github.com/mgh3326/fillwire/internal/stream"
	"github.com/redis/go-redis/v9"
)

func TestT2Ingest5xxRetry(t *testing.T) {
	client, queue, counters := testQueue(t, "consumer", 1, 0)
	if _, err := queue.Enqueue(context.Background(), fixtureRecord(1)); err != nil {
		t.Fatal(err)
	}
	first := make(chan struct{}, 1)
	allowSuccess := make(chan struct{})
	var releaseSuccessOnce sync.Once
	releaseSuccess := func() { releaseSuccessOnce.Do(func() { close(allowSuccess) }) }
	t.Cleanup(releaseSuccess)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			first <- struct{}{}
			return
		}
		<-allowSuccess
		_, _ = w.Write([]byte(`{"source":"fillwire","source_run_id":null,"received":1,"accepted":1,"rejected":0,"results":[{"status":"inserted","row_id":1,"reason":null}]}`))
	}))
	defer server.Close()
	runner := testRunner(t, queue, server.URL, counters)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("first ingest attempt did not return 500")
	}
	waitPending(t, client, 1)
	releaseSuccess()
	waitPending(t, client, 0)
	if got := calls.Load(); got < 2 {
		t.Fatalf("ingest calls = %d, want retry", got)
	}
	if got := counters.Snapshot().IngestFailures; got < 1 {
		t.Fatalf("ingest failure counter = %d, want at least 1", got)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("runner returned %v", err)
	}
}

func TestT3RestartAutoClaim(t *testing.T) {
	client, queueA, counters := testQueue(t, "consumer-a", 1, 0)
	if err := queueA.EnsureGroup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := queueA.Enqueue(context.Background(), fixtureRecord(1)); err != nil {
		t.Fatal(err)
	}
	read, err := queueA.ReadNew(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(read) != 1 {
		t.Fatalf("consumer A read %d messages, want 1", len(read))
	}
	waitPending(t, client, 1)

	queueB, err := stream.New(client, stream.Config{
		Key: "fills:kis", MaxLen: 100, Group: "fillwire-ingest", Consumer: "consumer-b",
		BatchSize: 1, Block: time.Millisecond, ClaimMinIdle: 0, Counters: counters,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed := make(chan struct{}, 1)
	handlerFailure := &handlerFailure{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			handlerFailure.set(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if !strings.Contains(string(body), "ORDER-001") {
			handlerFailure.set(fmt.Errorf("claimed request body does not contain pending order"))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"source":"fillwire","source_run_id":null,"received":1,"accepted":1,"rejected":0,"results":[{"status":"inserted","row_id":1,"reason":null}]}`))
		claimed <- struct{}{}
	}))
	defer server.Close()
	runner := testRunner(t, queueB, server.URL, counters)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	select {
	case <-claimed:
	case <-time.After(time.Second):
		t.Fatal("consumer B did not auto-claim consumer A pending message")
	}
	handlerFailure.assert(t)
	waitPending(t, client, 0)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("runner returned %v", err)
	}
}

func TestT4DuplicateRedeliveryIdempotency(t *testing.T) {
	client, queue, counters := testQueue(t, "consumer", 1, 0)
	first := fixtureRecord(1)
	second := first
	second.DupSuspect = true
	second.DupObservationCount = 2
	second.RawPayloadJSON.DupSuspect = true
	second.RawPayloadJSON.DupObservationCount = 2
	if _, err := queue.Enqueue(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var observed []struct {
		FillSeq       int64  `json:"fill_seq"`
		BrokerOrderID string `json:"broker_order_id"`
	}
	var call atomic.Int32
	handlerFailure := &handlerFailure{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var envelope struct {
			Fills []struct {
				FillSeq       int64  `json:"fill_seq"`
				BrokerOrderID string `json:"broker_order_id"`
			} `json:"fills"`
		}
		if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil {
			handlerFailure.set(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		mu.Lock()
		observed = append(observed, envelope.Fills[0])
		mu.Unlock()
		status := "inserted"
		if call.Add(1) == 2 {
			status = "unchanged"
		}
		_, _ = w.Write([]byte(`{"source":"fillwire","source_run_id":null,"received":1,"accepted":1,"rejected":0,"results":[{"status":"` + status + `","row_id":1,"reason":null}]}`))
	}))
	defer server.Close()
	runner := testRunner(t, queue, server.URL, counters)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for call.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	waitPending(t, client, 0)
	mu.Lock()
	got := append([]struct {
		FillSeq       int64  `json:"fill_seq"`
		BrokerOrderID string `json:"broker_order_id"`
	}{}, observed...)
	mu.Unlock()
	if len(got) != 2 || got[0] != got[1] {
		t.Fatalf("idempotency fields = %#v, want identical two requests", got)
	}
	handlerFailure.assert(t)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("runner returned %v", err)
	}
}

func TestT5PartialRejectedLeavesOnlyRejectedPending(t *testing.T) {
	client, queue, counters := testQueue(t, "consumer", 3, 0)
	for index := 1; index <= 3; index++ {
		if _, err := queue.Enqueue(context.Background(), fixtureRecord(index)); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := client.XRange(context.Background(), "fills:kis", "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"source":"fillwire","source_run_id":null,"received":3,"accepted":2,"rejected":1,"results":[{"status":"inserted","row_id":1,"reason":null},{"status":"rejected","row_id":null,"reason":"invalid record"},{"status":"inserted","row_id":3,"reason":null}]}`))
	}))
	defer server.Close()
	runner := testRunner(t, queue, server.URL, counters)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	waitPending(t, client, 1)
	pending, err := client.XPendingExt(context.Background(), &redis.XPendingExtArgs{Stream: "fills:kis", Group: "fillwire-ingest", Start: "-", End: "+", Count: 10}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].ID != entries[1].ID {
		t.Fatalf("pending = %+v, want only second stream ID %s", pending, entries[1].ID)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("runner returned %v", err)
	}
}

func TestT6ResponseLengthMismatchAcksNothing(t *testing.T) {
	client, queue, counters := testQueue(t, "consumer", 3, 0)
	for index := 1; index <= 3; index++ {
		if _, err := queue.Enqueue(context.Background(), fixtureRecord(index)); err != nil {
			t.Fatal(err)
		}
	}
	first := make(chan struct{}, 1)
	allowCorrectResponse := make(chan struct{})
	var releaseCorrectResponseOnce sync.Once
	releaseCorrectResponse := func() { releaseCorrectResponseOnce.Do(func() { close(allowCorrectResponse) }) }
	t.Cleanup(releaseCorrectResponse)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			_, _ = w.Write([]byte(`{"source":"fillwire","source_run_id":null,"received":3,"accepted":2,"rejected":0,"results":[{"status":"inserted","row_id":1,"reason":null},{"status":"inserted","row_id":2,"reason":null}]}`))
			first <- struct{}{}
			return
		}
		<-allowCorrectResponse
		_, _ = w.Write([]byte(`{"source":"fillwire","source_run_id":null,"received":3,"accepted":3,"rejected":0,"results":[{"status":"inserted","row_id":1,"reason":null},{"status":"inserted","row_id":2,"reason":null},{"status":"inserted","row_id":3,"reason":null}]}`))
	}))
	defer server.Close()
	runner := testRunner(t, queue, server.URL, counters)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("mismatched response was not received")
	}
	waitPending(t, client, 3)
	releaseCorrectResponse()
	waitPending(t, client, 0)
	if got := counters.Snapshot().IngestFailures; got < 1 {
		t.Fatalf("ingest failure counter = %d, want mismatch counted", got)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("runner returned %v", err)
	}
}

func TestT21PartialXAckIsIdempotent(t *testing.T) {
	client, queue, counters := testQueue(t, "consumer", 2, 0)
	if err := queue.EnsureGroup(context.Background()); err != nil {
		t.Fatal(err)
	}
	for index := 1; index <= 2; index++ {
		if _, err := queue.Enqueue(context.Background(), fixtureRecord(index)); err != nil {
			t.Fatal(err)
		}
	}
	messages, err := queue.ReadNew(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 {
		t.Fatalf("group messages = %d, want 2", len(messages))
	}
	if acked, err := client.XAck(context.Background(), "fills:kis", "fillwire-ingest", messages[0].ID).Result(); err != nil || acked != 1 {
		t.Fatalf("pre-ack = %d, %v; want 1, nil", acked, err)
	}

	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		posts.Add(1)
		_, _ = w.Write([]byte(`{"source":"fillwire","source_run_id":null,"received":2,"accepted":2,"rejected":0,"results":[{"status":"inserted","row_id":1,"reason":null},{"status":"inserted","row_id":2,"reason":null}]}`))
	}))
	defer server.Close()
	runner := testRunner(t, queue, server.URL, counters)
	if err := runner.DeliverOnce(context.Background(), messages); err != nil {
		t.Fatalf("DeliverOnce with one already-acknowledged ID = %v, want nil", err)
	}
	time.Sleep(3 * time.Millisecond)
	if got := posts.Load(); got != 1 {
		t.Fatalf("POST count = %d, want 1 without retry loop", got)
	}
	if got := counters.Snapshot().XAckShortfall; got != 1 {
		t.Fatalf("XACK shortfall counter = %d, want 1", got)
	}
	waitPending(t, client, 0)
}

func TestT12BatchNeverExceeds200(t *testing.T) {
	client, queue, counters := testQueue(t, "consumer", 200, 0)
	for index := 1; index <= 250; index++ {
		if _, err := queue.Enqueue(context.Background(), fixtureRecord(index)); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	var batchSizes []int
	handlerFailure := &handlerFailure{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var envelope struct {
			Fills []json.RawMessage `json:"fills"`
		}
		if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil {
			handlerFailure.set(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		mu.Lock()
		batchSizes = append(batchSizes, len(envelope.Fills))
		mu.Unlock()
		results := make([]string, len(envelope.Fills))
		for index := range results {
			results[index] = fmt.Sprintf(`{"status":"inserted","row_id":%d,"reason":null}`, index+1)
		}
		_, _ = w.Write([]byte(`{"source":"fillwire","source_run_id":null,"received":` + fmt.Sprint(len(results)) + `,"accepted":` + fmt.Sprint(len(results)) + `,"rejected":0,"results":[` + join(results) + `]}`))
	}))
	defer server.Close()
	runner := testRunner(t, queue, server.URL, counters)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		count := len(batchSizes)
		mu.Unlock()
		if count >= 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	waitPending(t, client, 0)
	mu.Lock()
	got := append([]int(nil), batchSizes...)
	mu.Unlock()
	if len(got) != 2 || got[0] != 200 || got[1] != 50 {
		t.Fatalf("batch sizes = %v, want [200 50]", got)
	}
	for _, size := range got {
		if size > 200 {
			t.Fatalf("POST fills length = %d, exceeds 200", size)
		}
	}
	handlerFailure.assert(t)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("runner returned %v", err)
	}
}

func TestT16DupSuspectIsNotTopLevelIngestField(t *testing.T) {
	client, queue, counters := testQueue(t, "consumer", 1, 0)
	record := fixtureRecord(1)
	record.DupSuspect = true
	record.DupObservationCount = 2
	record.RawPayloadJSON.DupSuspect = true
	record.RawPayloadJSON.DupObservationCount = 2
	if _, err := queue.Enqueue(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	checked := make(chan struct{}, 1)
	handlerFailure := &handlerFailure{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var envelope struct {
			Fills []map[string]json.RawMessage `json:"fills"`
		}
		if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil {
			handlerFailure.set(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if len(envelope.Fills) != 1 {
			handlerFailure.set(fmt.Errorf("fills length = %d", len(envelope.Fills)))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fill := envelope.Fills[0]
		if _, exists := fill["dup_suspect"]; exists {
			handlerFailure.set(errors.New("dup_suspect was sent as forbidden top-level ingest field"))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		allowed := map[string]bool{
			"broker": true, "account_mode": true, "venue": true, "instrument_type": true, "symbol": true,
			"raw_symbol": true, "side": true, "broker_order_id": true, "fill_seq": true, "filled_qty": true,
			"filled_price": true, "filled_notional": true, "fee_amount": true, "fee_currency": true,
			"filled_at": true, "currency": true, "correlation_id": true, "raw_payload_json": true,
		}
		for key := range fill {
			if !allowed[key] {
				handlerFailure.set(fmt.Errorf("forbidden top-level field %q", key))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(fill["raw_payload_json"], &raw); err != nil {
			handlerFailure.set(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if _, exists := raw["dup_suspect"]; !exists {
			handlerFailure.set(errors.New("duplicate hint missing from raw_payload_json"))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"source":"fillwire","source_run_id":null,"received":1,"accepted":1,"rejected":0,"results":[{"status":"inserted","row_id":1,"reason":null}]}`))
		checked <- struct{}{}
	}))
	defer server.Close()
	runner := testRunner(t, queue, server.URL, counters)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	select {
	case <-checked:
	case <-time.After(time.Second):
		t.Fatal("ingest request was not checked")
	}
	handlerFailure.assert(t)
	waitPending(t, client, 0)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("runner returned %v", err)
	}
}

func testQueue(t *testing.T, consumer string, batchSize int64, claimMinIdle time.Duration) (*redis.Client, *stream.Queue, *decode.Counters) {
	t.Helper()
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	counters := decode.NewCounters()
	queue, err := stream.New(client, stream.Config{
		Key: "fills:kis", MaxLen: 1000, Group: "fillwire-ingest", Consumer: consumer,
		BatchSize: batchSize, Block: time.Millisecond, ClaimMinIdle: claimMinIdle, Counters: counters,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client, queue, counters
}

func testRunner(t *testing.T, queue *stream.Queue, url string, counters *decode.Counters) *sink.Runner {
	t.Helper()
	client, err := sink.NewClient(sink.HTTPConfig{URL: url, Token: "test-token", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := sink.NewRunner(queue, client, sink.Config{RetryMin: time.Millisecond, RetryMax: 5 * time.Millisecond, Factor: 2, Counters: counters})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func fixtureRecord(index int) decode.Record {
	orderID := fmt.Sprintf("ORDER-%03d", index)
	fields := []string{"HTS_EXAMPLE", "00000000", orderID, "0000000000", "02", "00", "00", "00", "005930", "3", "71200", "093015", "0", "2"}
	return decode.Record{
		Broker: "kis", AccountMode: "live", Venue: "krx", InstrumentType: "equity_kr",
		Symbol: "005930", RawSymbol: "005930", Side: "buy", BrokerOrderID: orderID,
		FillSeq: int64(1000 + index), FilledQty: "3", FilledPrice: "71200", FeeCurrency: "KRW",
		FilledAt: time.Date(2026, time.September, 7, 9, 30, 15, 0, time.FixedZone("KST", 9*60*60)), Currency: "KRW",
		RawPayloadJSON: decode.RawPayload{TR: "H0STCNI0", Fields: fields, ReceivedAt: time.Date(2026, time.September, 7, 9, 30, 20, 0, time.FixedZone("KST", 9*60*60))},
	}
}

func waitPending(t *testing.T, client *redis.Client, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pending, err := client.XPending(context.Background(), "fills:kis", "fillwire-ingest").Result()
		if err == nil && pending.Count == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	pending, err := client.XPending(context.Background(), "fills:kis", "fillwire-ingest").Result()
	t.Fatalf("pending = %+v, %v; want %d", pending, err, want)
}

func join(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for _, part := range parts[1:] {
		result += "," + part
	}
	return result
}

// handlerFailure moves assertions out of httptest handler goroutines. Calling
// Fatal there invokes Goexit only in the handler and can leave Server.Close
// waiting forever after an earlier assertion failure.
type handlerFailure struct {
	mu  sync.Mutex
	err error
}

func (f *handlerFailure) set(err error) {
	if err == nil {
		return
	}
	f.mu.Lock()
	if f.err == nil {
		f.err = err
	}
	f.mu.Unlock()
}

func (f *handlerFailure) assert(t *testing.T) {
	t.Helper()
	f.mu.Lock()
	err := f.err
	f.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}
