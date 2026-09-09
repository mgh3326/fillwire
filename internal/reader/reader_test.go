package reader_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
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

const kisExecutionFrame = "0|H0STCNI0|1|HTS_EXAMPLE^00000000^A123456789^0000000000^02^00^00^00^005930^3^71200^093015^0^2"

type fakeApproval struct {
	approvalCalls atomic.Int32
	reissueCalls  atomic.Int32
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func (c fixedClock) After(delay time.Duration) <-chan time.Time {
	result := make(chan time.Time, 1)
	result <- c.now.Add(delay)
	return result
}

func (p *fakeApproval) ApprovalKey(context.Context) (string, error) {
	p.approvalCalls.Add(1)
	return "fixture", nil
}

func (p *fakeApproval) Reissue(context.Context) (string, error) {
	p.reissueCalls.Add(1)
	return "fixture-reissued", nil
}

type fakeTransport struct {
	in         chan []byte
	closed     chan struct{}
	once       sync.Once
	writes     atomic.Int32
	onWrite    func(*fakeTransport, []byte)
	subscribed chan struct{}
}

func newFakeTransport(onWrite func(*fakeTransport, []byte)) *fakeTransport {
	return &fakeTransport{
		in:         make(chan []byte, 16),
		closed:     make(chan struct{}),
		onWrite:    onWrite,
		subscribed: make(chan struct{}),
	}
}

func (t *fakeTransport) Read(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, io.EOF
	case frame := <-t.in:
		return frame, nil
	}
}

func (t *fakeTransport) Write(_ context.Context, frame []byte) error {
	t.writes.Add(1)
	if t.onWrite != nil {
		t.onWrite(t, frame)
	}
	return nil
}

func (t *fakeTransport) WriteControl(context.Context, int, []byte) error { return nil }

func (t *fakeTransport) Close() error {
	t.once.Do(func() { close(t.closed) })
	return nil
}

func (t *fakeTransport) drop() { _ = t.Close() }

func (t *fakeTransport) push(frame string) {
	select {
	case t.in <- []byte(frame):
	case <-t.closed:
	}
}

func successfulSubscribe(t *fakeTransport, raw []byte) {
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
	t.push(`{"header":{"tr_id":"` + request.Body.Input.TRID + `"},"body":{"rt_cd":"0","msg_cd":"MCA00000","msg1":"SUBSCRIBE SUCCESS","output":{}}}`)
	select {
	case <-t.subscribed:
	default:
		close(t.subscribed)
	}
}

func occupiedSubscribe(t *fakeTransport, raw []byte) {
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
	t.push(`{"header":{"tr_id":"` + request.Body.Input.TRID + `"},"body":{"rt_cd":"1","msg_cd":"OPSP8996","msg1":"session occupied","output":{}}}`)
}

func readerConfig(transport *fakeTransport, approval ws.ApprovalKeyProvider) fillreader.Config {
	return fillreader.Config{
		Endpoint: "live",
		HTSID:    "HTS_EXAMPLE",
		Approval: approval,
		Dialer: ws.DialerFunc(func(context.Context, string) (ws.Transport, error) {
			return transport, nil
		}),
		EventBuffer: 1,
		Clock:       fixedClock{now: time.Date(2026, time.September, 7, 9, 30, 20, 0, time.FixedZone("KST", 9*60*60))},
	}
}

func TestT1NormalPipeline(t *testing.T) {
	mini := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer redisClient.Close()
	counters := decode.NewCounters()
	queue, err := stream.New(redisClient, stream.Config{
		Key:          "fills:kis",
		MaxLen:       100,
		Group:        "fillwire-ingest",
		Consumer:     "test-consumer",
		BatchSize:    200,
		Block:        time.Millisecond,
		ClaimMinIdle: 0,
		Counters:     counters,
	})
	if err != nil {
		t.Fatal(err)
	}

	requestSeen := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			t.Errorf("method = %s", request.Method)
		}
		if got := request.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("authorization = %q", got)
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		const expected = `{"fills":[{"broker":"kis","account_mode":"live","venue":"krx","instrument_type":"equity_kr","symbol":"005930","raw_symbol":"005930","side":"buy","broker_order_id":"A123456789","fill_seq":2023882045,"filled_qty":"3","filled_price":"71200","filled_notional":null,"fee_amount":null,"fee_currency":"KRW","filled_at":"2026-09-07T09:30:15+09:00","currency":"KRW","correlation_id":null,"raw_payload_json":{"tr":"H0STCNI0","fields":["HTS_EXAMPLE","00000000","A123456789","0000000000","02","00","00","00","005930","3","71200","093015","0","2"],"received_at":"2026-09-07T09:30:20+09:00"}}],"source":"fillwire","source_run_id":null}`
		assertJSONEqual(t, []byte(expected), body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"source":"fillwire","source_run_id":null,"received":1,"accepted":1,"rejected":0,"results":[{"status":"inserted","row_id":123,"reason":null}]}`))
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
	approval := &fakeApproval{}
	events := make(chan ws.Event, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readerDone := make(chan error, 1)
	go func() { readerDone <- fillreader.New(readerConfig(transport, approval)).Run(ctx, events) }()
	select {
	case <-transport.subscribed:
	case <-time.After(time.Second):
		t.Fatal("reader did not subscribe")
	}
	transport.push(kisExecutionFrame)
	var event ws.Event
	select {
	case event = <-events:
	case <-time.After(time.Second):
		t.Fatal("reader did not drain execution event")
	}
	decoder := decode.New(decode.Config{AccountMode: "live", Venue: "krx", DupTrackMax: 10, Counters: counters})
	record, ok := decoder.Decode(event)
	if !ok {
		t.Fatal("valid execution was dropped")
	}
	if _, err := queue.Enqueue(ctx, record); err != nil {
		t.Fatal(err)
	}

	runnerDone := make(chan error, 1)
	go func() { runnerDone <- runner.Run(ctx) }()
	select {
	case <-requestSeen:
	case <-time.After(time.Second):
		t.Fatal("ingest request not received")
	}
	waitPending(t, redisClient, "fills:kis", "fillwire-ingest", 0)
	if snapshot := counters.Snapshot(); snapshot.XAdded != 1 || snapshot.XAcked != 1 {
		t.Fatalf("counters = %+v, want one XADD and one XACK", snapshot)
	}
	cancel()
	if err := <-readerDone; err != nil {
		t.Fatalf("reader returned %v", err)
	}
	if err := <-runnerDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("runner returned %v, want context cancellation", err)
	}
}

func TestT7SessionOccupied(t *testing.T) {
	transport := newFakeTransport(occupiedSubscribe)
	logger, logs := readerJSONLogger()
	cfg := readerConfig(transport, &fakeApproval{})
	cfg.Logger = logger
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := fillreader.New(cfg).Run(ctx, make(chan ws.Event, 1))
	if !errors.Is(err, ws.ErrSessionOccupied) {
		t.Fatalf("error = %v, want ErrSessionOccupied", err)
	}
	if got := fillreader.ProcessExitCode(err); got != fillreader.ExitCodeSessionOccupied {
		t.Fatalf("exit code = %d, want %d", got, fillreader.ExitCodeSessionOccupied)
	}
	if got := transport.writes.Load(); got != 1 {
		t.Fatalf("subscribe writes = %d, want 1 (no retry)", got)
	}
	if got := readerLogMessageCount(t, logs.String(), "KIS websocket initial subscription active"); got != 0 {
		t.Fatalf("initial subscription events = %d, want 0 after failed subscription", got)
	}
}

func TestT18ReconnectSessionOccupiedStopsReader(t *testing.T) {
	first := newFakeTransport(successfulSubscribe)
	second := newFakeTransport(occupiedSubscribe)
	approval := &fakeApproval{}
	var dials atomic.Int32
	cfg := readerConfig(first, approval)
	cfg.Dialer = ws.DialerFunc(func(context.Context, string) (ws.Transport, error) {
		switch dials.Add(1) {
		case 1:
			return first, nil
		case 2:
			return second, nil
		default:
			return nil, errors.New("unexpected additional KIS dial")
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	events := make(chan ws.Event, 1)
	done := make(chan error, 1)
	go func() { done <- fillreader.New(cfg).Run(ctx, events) }()
	select {
	case <-first.subscribed:
	case <-time.After(time.Second):
		t.Fatal("initial KIS subscription did not complete")
	}
	first.push(kisExecutionFrame)
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("initial execution was not delivered")
	}
	first.drop()

	select {
	case err := <-done:
		if !errors.Is(err, ws.ErrSessionOccupied) {
			t.Fatalf("Reader.Run error = %v, want reconnect ErrSessionOccupied", err)
		}
		if got := fillreader.ProcessExitCode(err); got != fillreader.ExitCodeSessionOccupied {
			t.Fatalf("exit code = %d, want %d", got, fillreader.ExitCodeSessionOccupied)
		}
	case <-time.After(time.Second):
		t.Fatal("Reader.Run stayed alive after reconnect OPSP8996")
	}
	if got := dials.Load(); got != 2 {
		t.Fatalf("KIS dials = %d, want initial plus one reconnect", got)
	}
	if got := second.writes.Load(); got != 1 {
		t.Fatalf("reconnect subscribe writes = %d, want 1", got)
	}
}

func TestT25WebsocketLifecycleObservations(t *testing.T) {
	first := newFakeTransport(successfulSubscribe)
	releaseSecond := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSecond) }) }
	t.Cleanup(release)
	second := newFakeTransport(func(transport *fakeTransport, raw []byte) {
		<-releaseSecond
		successfulSubscribe(transport, raw)
	})
	approval := &fakeApproval{}
	logger, logs := readerJSONLogger()
	var dials atomic.Int32
	cfg := readerConfig(first, approval)
	cfg.Logger = logger
	cfg.Dialer = ws.DialerFunc(func(context.Context, string) (ws.Transport, error) {
		switch dials.Add(1) {
		case 1:
			return first, nil
		case 2:
			return second, nil
		default:
			return nil, errors.New("unexpected additional KIS dial")
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fillreader.New(cfg).Run(ctx, make(chan ws.Event, 1)) }()
	select {
	case <-first.subscribed:
	case <-time.After(time.Second):
		t.Fatal("initial KIS subscription did not complete")
	}
	waitReaderLogMessage(t, logs, "KIS websocket initial subscription active", 1)
	first.drop()
	select {
	case <-second.subscribed:
		t.Fatal("reconnect subscription completed before its release")
	case <-time.After(time.Millisecond * 10):
	}
	waitReaderLogMessage(t, logs, "KIS websocket reconnect attempted", 1)
	if got := readerLogMessageCount(t, logs.String(), "KIS websocket reconnect succeeded"); got != 0 {
		t.Fatalf("reconnect success events before resubscribe = %d, want 0", got)
	}
	release()
	select {
	case <-second.subscribed:
	case <-time.After(time.Second):
		t.Fatal("reconnect KIS subscription did not complete")
	}
	waitReaderLogMessage(t, logs, "KIS websocket reconnect succeeded", 1)

	if got := readerLogMessageCount(t, logs.String(), "KIS websocket initial subscription active"); got != 1 {
		t.Fatalf("initial subscription events = %d, want 1", got)
	}
	if got := readerLogMessageCount(t, logs.String(), "KIS websocket reconnect attempted"); got != 1 {
		t.Fatalf("reconnect attempt events = %d, want 1", got)
	}
	if got := readerLogMessageCount(t, logs.String(), "KIS websocket reconnect succeeded"); got != 1 {
		t.Fatalf("reconnect success events = %d, want 1", got)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("reader returned %v", err)
	}
	if got := dials.Load(); got != 2 {
		t.Fatalf("KIS dials = %d, want initial plus one reconnect", got)
	}
}

func TestT26WebsocketReconnectAttemptObservedWhenDialFails(t *testing.T) {
	first := newFakeTransport(successfulSubscribe)
	approval := &fakeApproval{}
	logger, logs := readerJSONLogger()
	redialEntered := make(chan struct{}, 1)
	var dials atomic.Int32
	cfg := readerConfig(first, approval)
	cfg.Logger = logger
	cfg.Dialer = ws.DialerFunc(func(ctx context.Context, _ string) (ws.Transport, error) {
		switch dials.Add(1) {
		case 1:
			return first, nil
		case 2:
			redialEntered <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		default:
			return nil, errors.New("unexpected additional KIS dial")
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fillreader.New(cfg).Run(ctx, make(chan ws.Event, 1)) }()
	select {
	case <-first.subscribed:
	case <-time.After(time.Second):
		t.Fatal("initial KIS subscription did not complete")
	}
	waitReaderLogMessage(t, logs, "KIS websocket initial subscription active", 1)
	first.drop()
	select {
	case <-redialEntered:
	case <-time.After(time.Second):
		t.Fatal("failed reconnect dial was not attempted")
	}
	waitReaderLogMessage(t, logs, "KIS websocket reconnect attempted", 1)
	if got := readerLogMessageCount(t, logs.String(), "KIS websocket reconnect succeeded"); got != 0 {
		t.Fatalf("reconnect success events after failed dial = %d, want 0", got)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("reader returned %v", err)
	}
	if got := dials.Load(); got != 2 {
		t.Fatalf("KIS dials = %d, want initial plus failed reconnect", got)
	}
}

func TestT10Backpressure(t *testing.T) {
	transport := newFakeTransport(successfulSubscribe)
	out := make(chan ws.Event, 1)
	out <- ws.Event{TR: "already-buffered"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fillreader.New(readerConfig(transport, &fakeApproval{})).Run(ctx, out) }()
	select {
	case <-transport.subscribed:
	case <-time.After(time.Second):
		t.Fatal("reader did not subscribe")
	}
	transport.push(kisExecutionFrame)
	time.Sleep(25 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("reader returned while output was full: %v", err)
	default:
	}
	<-out // release the bounded channel rather than dropping the event
	select {
	case event := <-out:
		if !bytes.Equal(event.Raw, []byte(kisExecutionFrame)) {
			t.Fatalf("delivered raw event = %q", event.Raw)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked event was not delivered after capacity was freed")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("reader returned %v", err)
	}
}

func TestT11ApprovalProvider(t *testing.T) {
	mini := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer redisClient.Close()
	fallback := &fakeApproval{}
	logger, logs := readerJSONLogger()
	provider, err := fillreader.NewApprovalProvider(redisClient, fallback, logger)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := redisClient.Set(ctx, "kis:websocket:approval_key", "cached-fixture", 0).Err(); err != nil {
		t.Fatal(err)
	}
	key, err := provider.ApprovalKey(ctx)
	if err != nil || key != "cached-fixture" {
		t.Fatalf("cached approval = %q, %v", key, err)
	}
	if got := fallback.approvalCalls.Load(); got != 0 {
		t.Fatalf("REST fallback calls with cache = %d, want 0", got)
	}
	if got := readerLogMessageCount(t, logs.String(), "KIS approval key REST issuance attempted"); got != 0 {
		t.Fatalf("REST issuance events with cache = %d, want 0", got)
	}
	if err := redisClient.Del(ctx, "kis:websocket:approval_key").Err(); err != nil {
		t.Fatal(err)
	}
	key, err = provider.ApprovalKey(ctx)
	if err != nil || key != "fixture" {
		t.Fatalf("fallback approval = %q, %v", key, err)
	}
	if got := fallback.approvalCalls.Load(); got != 1 {
		t.Fatalf("REST fallback calls after miss = %d, want 1", got)
	}
	if got := readerLogMessageCount(t, logs.String(), "KIS approval key REST issuance attempted"); got != 1 {
		t.Fatalf("REST issuance events after miss = %d, want 1", got)
	}
	if err := redisClient.Set(ctx, "kis:websocket:approval_key", "", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.ApprovalKey(ctx); err != nil {
		t.Fatal(err)
	}
	if got := fallback.approvalCalls.Load(); got != 2 {
		t.Fatalf("REST fallback calls after empty key = %d, want 2", got)
	}
	if got := readerLogMessageCount(t, logs.String(), "KIS approval key REST issuance attempted"); got != 2 {
		t.Fatalf("REST issuance events after empty key = %d, want 2", got)
	}
	if _, err := provider.Reissue(ctx); err != nil {
		t.Fatal(err)
	}
	if got := fallback.reissueCalls.Load(); got != 1 {
		t.Fatalf("reissue calls = %d, want 1", got)
	}
	if got := readerLogMessageCount(t, logs.String(), "KIS approval key REST reissue attempted"); got != 1 {
		t.Fatalf("REST reissue events = %d, want 1", got)
	}
	mini.Close()
	if _, err := provider.ApprovalKey(ctx); err == nil {
		t.Fatal("Redis error = nil, want surfaced error")
	}
	if got := fallback.approvalCalls.Load(); got != 2 {
		t.Fatalf("REST fallback calls after Redis error = %d, want 2", got)
	}
	for _, record := range readerLogRecords(t, logs.String()) {
		if len(record) != 3 {
			t.Errorf("approval observation fields = %#v, want standard fields only", record)
		}
	}
	for _, forbidden := range []string{"cached-fixture", "fixture-reissued", "kis:websocket:approval_key", "redis"} {
		if strings.Contains(logs.String(), forbidden) {
			t.Errorf("approval observation log contains forbidden %q: %s", forbidden, logs.String())
		}
	}
}

type logCapture struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (c *logCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data.Write(p)
}

func (c *logCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data.String()
}

func readerJSONLogger() (*slog.Logger, *logCapture) {
	logs := &logCapture{}
	return slog.New(slog.NewJSONHandler(logs, nil)), logs
}

func readerLogRecords(t *testing.T, raw string) []map[string]any {
	t.Helper()
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	lines := strings.Split(strings.TrimSpace(raw), "\n")
	records := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("invalid JSON log record %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

func readerLogMessageCount(t *testing.T, raw, message string) int {
	t.Helper()
	count := 0
	for _, record := range readerLogRecords(t, raw) {
		if record["msg"] == message {
			count++
		}
	}
	return count
}

func waitReaderLogMessage(t *testing.T, logs *logCapture, message string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := readerLogMessageCount(t, logs.String(), message); got >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("log message %q count = %d, want at least %d; logs=%q", message, readerLogMessageCount(t, logs.String(), message), want, logs.String())
}

func assertJSONEqual(t *testing.T, want, got []byte) {
	t.Helper()
	var wantValue any
	var gotValue any
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("bad expected JSON: %v", err)
	}
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("bad actual JSON: %v", err)
	}
	if !reflect.DeepEqual(wantValue, gotValue) {
		t.Fatalf("request JSON mismatch\nwant: %s\n got: %s", want, got)
	}
}

func waitPending(t *testing.T, client *redis.Client, key, group string, want int64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		pending, err := client.XPending(context.Background(), key, group).Result()
		if err == nil && pending.Count == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	pending, err := client.XPending(context.Background(), key, group).Result()
	t.Fatalf("pending = %+v, %v; want %d", pending, err, want)
}
