package bookfeed

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/fillwire/internal/kisread"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

func fixture(t *testing.T) Snapshot {
	t.Helper()
	f, err := os.Open("testdata/stream.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	if !scan.Scan() {
		t.Fatal("empty fixture")
	}
	var row struct {
		Values map[string]any `json:"values"`
	}
	if err := json.Unmarshal(scan.Bytes(), &row); err != nil {
		t.Fatal(err)
	}
	s, err := Decode(row.Values)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRecordedStreamSnapshotRoundTrip(t *testing.T) {
	m := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer r.Close()
	s := fixture(t)
	if err := Publish(context.Background(), r, s); err != nil {
		t.Fatal(err)
	}
	got, ok, err := Latest(context.Background(), r, s.Symbol)
	if err != nil || !ok || !got.At().Equal(s.At()) || got.Bids != s.Bids || got.Asks != s.Asks {
		t.Fatalf("snapshot %+v %v %v", got, ok, err)
	}
	if _, ok, err := Latest(context.Background(), r, "000660"); err != nil || ok {
		t.Fatal("read another symbol's book")
	}
	values := s.Values()
	values["bid_price_1"] = "broken"
	if _, err := Decode(values); err == nil {
		t.Fatal("accepted malformed level")
	}
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func asking(t *testing.T, calls *atomic.Int64) *kisread.Client {
	t.Helper()
	s := fixture(t)
	out := map[string]string{"aspr_acpt_hour": time.Now().In(KST).Format("150405")}
	for i := range 5 {
		n := string(rune('1' + i))
		v := s.Values()
		out["bidp"+n] = v["bid_price_"+n].(string)
		out["bidp_rsqn"+n] = v["bid_qty_"+n].(string)
		out["askp"+n] = v["ask_price_"+n].(string)
		out["askp_rsqn"+n] = v["ask_qty_"+n].(string)
	}
	body, _ := json.Marshal(map[string]any{"rt_cd": "0", "output1": out})
	return &kisread.Client{Mock: true, AppKey: "fixture", AppSecret: "fixture", Token: func() string { return "fixture" }, HTTP: &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if r.Method != "GET" || r.Header.Get("tr_id") != "FHKST01010200" || r.URL.Query().Get("FID_INPUT_ISCD") != "005930" || r.URL.Path != "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn" {
			t.Error("unexpected quotation request")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(body))), Header: make(http.Header)}, nil
	})}}
}

func TestMockRESTFallbackPublishesAfterWebsocketUnavailable(t *testing.T) {
	m := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer r.Close()
	var calls atomic.Int64
	rest := asking(t, &calls)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, r, rest, "005930", true, make(chan ws.Event), func(err error) { t.Error(err) })
	}()
	deadline := time.After(4 * time.Second)
	for {
		rows, err := r.XLen(ctx, "book:kis:005930").Result()
		if err != nil {
			t.Fatal(err)
		}
		if rows > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("no fallback snapshot")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	snapshot, ok, err := Latest(context.Background(), r, "005930")
	if err != nil || !ok || snapshot.Source != "rest" || calls.Load() != 1 {
		t.Fatalf("fallback %+v %v calls %d", snapshot, err, calls.Load())
	}
}

func TestWebsocketSnapshotKeepsFiveLevelsAndExchangeClock(t *testing.T) {
	data, err := os.ReadFile("../exec/sim/testdata/sim.evidence.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(data), "\n")
	var frame struct {
		Raw  string `json:"raw"`
		AtNS int64  `json:"at_ns"`
	}
	if err := json.Unmarshal([]byte(lines[1]), &frame); err != nil {
		t.Fatal(err)
	}
	snapshots, err := FromEvent(ws.Event{Raw: []byte(frame.Raw), ReceivedAt: time.Unix(0, frame.AtNS)})
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("snapshots %+v %v", snapshots, err)
	}
	s := snapshots[0]
	if s.Source != "ws" || len(s.Bids) != 5 || s.Bids[0].Price != 70000 || s.Asks[4].Price != 70600 || s.Exchange.In(KST).Format("150405") != "090000" {
		t.Fatalf("snapshot %+v", s)
	}
}

func TestRecentWebsocketArrivalSuppressesFallbackWithStaleExchangeClock(t *testing.T) {
	data, err := os.ReadFile("../exec/sim/testdata/sim.evidence.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	var frame struct {
		Raw string `json:"raw"`
	}
	if err := json.Unmarshal([]byte(strings.Split(string(data), "\n")[1]), &frame); err != nil {
		t.Fatal(err)
	}
	received := time.Now()
	parts := strings.SplitN(frame.Raw, "|", 4)
	fields := strings.Split(parts[3], "^")
	fields[1] = received.Add(-time.Minute).In(KST).Format("150405")
	parts[3] = strings.Join(fields, "^")
	event := ws.Event{Raw: []byte(strings.Join(parts, "|")), ReceivedAt: received}
	m := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer r.Close()
	var calls atomic.Int64
	rest := asking(t, &calls)
	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan ws.Event, 1)
	events <- event
	done := make(chan error, 1)
	go func() { done <- Run(ctx, r, rest, "005930", true, events, func(err error) { t.Error(err) }) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	// Observe a successfully published websocket book before testing the first
	// fallback tick. The exchange clock stays old; arrival is recent.
	deadline := time.After(time.Second)
	for {
		snapshot, ok, err := Latest(ctx, r, "005930")
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if snapshot.Source != "ws" || received.Sub(snapshot.At()) < 30*time.Second {
				t.Fatalf("expected a stale exchange clock on a fresh ws arrival: %+v", snapshot)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("websocket snapshot not published")
		case <-time.After(10 * time.Millisecond):
		}
	}
	<-time.After(2200 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("REST fallback calls=%d, want 0 after a recent websocket arrival", calls.Load())
	}
	snapshot, ok, err := Latest(ctx, r, "005930")
	if err != nil || !ok || snapshot.Source != "ws" || !snapshot.At().Before(received.Add(-30*time.Second)) {
		t.Fatalf("exchange staleness was hidden: %+v %v", snapshot, err)
	}
}
