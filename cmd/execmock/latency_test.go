package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/fillwire/internal/bookfeed"
	"github.com/mgh3326/fillwire/internal/exec"
	"github.com/mgh3326/fillwire/internal/exec/e1"
	"github.com/mgh3326/fillwire/internal/exec/exectest"
	"github.com/redis/go-redis/v9"
)

// fakeClock moves only when the test says so, and carries no monotonic reading.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

type stubGateway struct {
	clock  *fakeClock
	answer map[string]exec.Outcome
	err    error
	sends  []exec.Child
}

func (g *stubGateway) Send(c exec.Child) (exec.Outcome, error) {
	g.sends = append(g.sends, c)
	g.clock.advance(40 * time.Millisecond) // the request takes time
	if g.err != nil {
		return exec.Outcome{}, g.err
	}
	return g.answer[c.Key], nil
}
func (g *stubGateway) Query(string) (exec.View, error) { return exec.View{}, nil }

func parseLines(t *testing.T, out string) []latencyBody {
	t.Helper()
	var lines []latencyBody
	for _, raw := range strings.Split(strings.TrimSpace(out), "\n") {
		var w struct {
			Latency latencyBody `json:"latency"`
		}
		if err := json.Unmarshal([]byte(raw), &w); err != nil {
			t.Fatalf("not a latency line %q: %v", raw, err)
		}
		lines = append(lines, w.Latency)
	}
	return lines
}

func TestLatencyLineCarriesFirstObservationsAndDeltas(t *testing.T) {
	t0 := time.Date(2026, 11, 2, 9, 30, 0, 0, time.UTC)
	clock := &fakeClock{t: t0}
	noticeAt := t0.Add(3500 * time.Millisecond)
	var askedOrder string
	var askedSince time.Time
	tr := newLatencyTracker(clock.now, func(_ context.Context, id string, since time.Time) (time.Time, bool, error) {
		askedOrder, askedSince = id, since
		return noticeAt, true, nil
	})
	gw := &stubGateway{clock: clock, answer: map[string]exec.Outcome{
		"k1": {Key: "k1", Status: exec.StatusAccepted, BrokerOrderID: "0000123"},
		"k2": {Key: "k2", Status: exec.StatusAccepted},
	}}
	g := tr.Gateway(gw)
	clock.advance(time.Second) // t0+1s: POST of k1
	if _, err := g.Send(exec.Child{Action: exec.ActSubmit, IntentID: "i1", Key: "k1", Qty: 3, Price: 70000}); err != nil {
		t.Fatal(err)
	}
	clock.advance(time.Second) // t0+2.04s: POST of k2, a cancel with no fills
	if _, err := g.Send(exec.Child{Action: exec.ActCancel, IntentID: "i1", Key: "k2", TargetKey: "k1"}); err != nil {
		t.Fatal(err)
	}

	snap := func(fills int) ledgerRead {
		return func(context.Context) (e1.FillsSnapshot, error) {
			o := e1.LedgerOrder{BrokerOrderID: "123", Fills: make([]e1.LedgerFill, fills)}
			return e1.FillsSnapshot{IntentID: "i1", Orders: []e1.LedgerOrder{o}}, nil
		}
	}
	filled := func(n int64) brokerRead {
		return func(context.Context, exec.BrokerOrder) (brokerView, error) { return brokerView{Filled: n}, nil }
	}
	order := exec.BrokerOrder{BrokerOrderID: "123", Key: "k1"}
	ctx := context.Background()

	clock.advance(time.Second)
	tr.Ledger(snap(0))(ctx) // order listed, no fill yet: no stamp
	tr.Broker(filled(0))(ctx, order)
	clock.advance(time.Second) // t0+4.04s
	tr.Ledger(snap(1))(ctx)    // first E1 fill
	clock.advance(150 * time.Millisecond)
	tr.Broker(filled(2))(ctx, order) // first VTS fill, t0+4.19s
	clock.advance(time.Second)
	tr.Ledger(snap(2))(ctx) // later observations never move a stamp
	tr.Broker(filled(2))(ctx, order)

	var out bytes.Buffer
	if err := tr.Flush(&out); err != nil {
		t.Fatal(err)
	}
	lines := parseLines(t, out.String())
	if len(lines) != 2 {
		t.Fatalf("want one line per child, got %d: %s", len(lines), out.String())
	}
	if askedOrder != "123" || !askedSince.Equal(t0.Add(time.Second)) {
		t.Fatalf("notice lookup for %q since %v", askedOrder, askedSince)
	}
	l := lines[0]
	wantPost := t0.Add(time.Second)
	if l.ChildKey != "k1" || l.Action != exec.ActSubmit || l.Status != exec.StatusAccepted || l.BrokerOrderID != "123" || l.IntentID != "i1" {
		t.Fatalf("identity: %+v", l)
	}
	if !l.Post.Wall.Equal(wantPost) || *l.Post.MonoNS != 1000*int64(time.Millisecond) {
		t.Fatalf("post stamp %+v", l.Post)
	}
	if l.KISNotice == nil || !l.KISNotice.Wall.Equal(noticeAt) || l.KISNotice.MonoNS != nil {
		t.Fatalf("kis notice stamp %+v (another process: wall only)", l.KISNotice)
	}
	if !l.E1Fills.Wall.Equal(t0.Add(4080*time.Millisecond)) || !l.VTSReadback.Wall.Equal(t0.Add(4230*time.Millisecond)) {
		t.Fatalf("first observations moved: e1 %v vts %v", l.E1Fills.Wall, l.VTSReadback.Wall)
	}
	wantDeltas := map[string]int64{"post_to_kis_notice": 2500, "post_to_e1_fills": 3080, "post_to_vts_readback": 3230, "kis_notice_to_e1_fills": 580, "kis_notice_to_vts_readback": 730}
	var gotDeltas map[string]int64
	raw, _ := json.Marshal(l.DeltasNS)
	json.Unmarshal(raw, &gotDeltas)
	if len(gotDeltas) != len(wantDeltas) {
		t.Fatalf("deltas %v", gotDeltas)
	}
	for k, ms := range wantDeltas {
		if gotDeltas[k] != ms*int64(time.Millisecond) {
			t.Fatalf("delta %s = %dns, want %dms (all: %v)", k, gotDeltas[k], ms, gotDeltas)
		}
	}
	c := lines[1]
	if c.ChildKey != "k2" || c.Action != exec.ActCancel || c.KISNotice != nil || c.E1Fills != nil || c.VTSReadback != nil || c.DeltasNS != (deltasNS{}) {
		t.Fatalf("cancel child must carry only its POST time: %+v", c)
	}
	if !strings.Contains(out.String(), `"kis_notice":null`) || !strings.Contains(out.String(), `"e1_fills":null`) {
		t.Fatalf("missing evidence must print as null: %s", out.String())
	}
}

func TestLatencyNoticeFailureOrUncertainSendStillReports(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 11, 2, 9, 30, 0, 0, time.UTC)}
	tr := newLatencyTracker(clock.now, func(context.Context, string, time.Time) (time.Time, bool, error) {
		return time.Time{}, false, errors.New("redis://user:secret@host: connection refused")
	})
	gw := &stubGateway{clock: clock, answer: map[string]exec.Outcome{"k1": {Key: "k1", Status: exec.StatusAccepted, BrokerOrderID: "9"}}}
	g := tr.Gateway(gw)
	g.Send(exec.Child{Action: exec.ActSubmit, IntentID: "i1", Key: "k1"})
	gw.err = errors.New("timeout")
	if _, err := g.Send(exec.Child{Action: exec.ActSubmit, IntentID: "i1", Key: "k2"}); err == nil {
		t.Fatal("the wrapper must return the gateway's error unchanged")
	}
	var out bytes.Buffer
	if err := tr.Flush(&out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "secret") {
		t.Fatalf("redis error text leaked: %s", out.String())
	}
	lines := parseLines(t, out.String())
	if len(lines) != 2 || lines[0].KISNoticeError == "" || lines[0].KISNotice != nil {
		t.Fatalf("lookup failure must be reported, not fatal: %+v", lines)
	}
	if lines[1].Status != exec.StatusUncertain || lines[1].KISNoticeError != "" {
		t.Fatalf("a send with no broker order is never looked up: %+v", lines[1])
	}
}

func TestStreamNoticeReadFindsEarliestReceiveTime(t *testing.T) {
	mr := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer r.Close()
	base := time.Date(2026, 11, 2, 9, 30, 0, 0, time.UTC)
	add := func(id, order string, received time.Time) {
		payload, _ := json.Marshal(map[string]any{"broker_order_id": order, "raw_payload_json": map[string]any{"tr": "H0STCNI9", "received_at": received}})
		if err := r.XAdd(context.Background(), &redis.XAddArgs{Stream: "fills:kis", ID: id, Values: []any{"payload", string(payload), "dup_suspect", "false"}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	ms := strconv.FormatInt(base.UnixMilli(), 10)
	add("1-1", "0000123", base.Add(-time.Hour))      // yesterday's run, outside the scan window
	add(ms+"-1", "0000999", base.Add(time.Second))   // another order
	add(ms+"-2", "0000123", base.Add(5*time.Second)) // second partial
	add(ms+"-3", "0000123", base.Add(2*time.Second)) // first notice, enqueued later
	add(ms+"-4", "bad", time.Time{})                 // no receive time

	read := streamNoticeRead(r, "fills:kis")
	got, ok, err := read(context.Background(), "123", base)
	if err != nil || !ok || !got.Equal(base.Add(2*time.Second)) {
		t.Fatalf("got %v %v %v", got, ok, err)
	}
	if _, ok, err := read(context.Background(), "555", base); ok || err != nil {
		t.Fatalf("absent order: %v %v", ok, err)
	}
	if _, ok, err := streamNoticeRead(r, "fills:none")(context.Background(), "123", base); ok || err != nil {
		t.Fatalf("absent stream: %v %v", ok, err)
	}
}

func TestStreamNoticeReadPagesPastOnePage(t *testing.T) {
	mr := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer r.Close()
	base := time.Date(2026, 11, 2, 9, 30, 0, 0, time.UTC)
	ms := base.UnixMilli()
	for i := range noticePage + 10 {
		order, at := "0000999", base
		if i == noticePage+5 {
			order, at = "0000123", base.Add(time.Second)
		}
		payload, _ := json.Marshal(map[string]any{"broker_order_id": order, "raw_payload_json": map[string]any{"received_at": at}})
		if err := r.XAdd(context.Background(), &redis.XAddArgs{Stream: "fills:kis", ID: strconv.FormatInt(ms, 10) + "-" + strconv.Itoa(i), Values: []any{"payload", string(payload)}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	got, ok, err := streamNoticeRead(r, "fills:kis")(context.Background(), "123", base)
	if err != nil || !ok || !got.Equal(base.Add(time.Second)) {
		t.Fatalf("entry beyond the first page: %v %v %v", got, ok, err)
	}
}

func TestNextStreamID(t *testing.T) {
	if id, err := nextStreamID("1700000000000-41"); err != nil || id != "1700000000000-42" {
		t.Fatal(id, err)
	}
	if _, err := nextStreamID("junk"); err == nil {
		t.Fatal("junk id accepted")
	}
}

// The tracker must not change what execmock sends or when: the same ladder run
// with and without it reaches the gateway with the identical child sequence.
func TestLatencyTrackerDoesNotChangeTheOrderPath(t *testing.T) {
	run := func(withTracker bool) ([]exec.Child, string) {
		in := liveIntent(t)
		in.LimitPriceKRW = 71000
		start := in.ExpiresAt.Add(-35 * time.Second)
		gateway := exectest.New(in)
		e, kill := liveExecutor(t, in, gateway, true)
		seconds := []int{0, 5, 10, 15, 20, 25, 30, 31}
		ticks := make(chan time.Time, len(seconds))
		for range seconds {
			ticks <- time.Time{}
		}
		current := start
		index := 0
		ledger := func(context.Context) (e1.FillsSnapshot, error) {
			orders, err := e.BrokerOrders()
			if err != nil {
				return e1.FillsSnapshot{}, err
			}
			s := e1.FillsSnapshot{IntentID: in.IntentID, Orders: []e1.LedgerOrder{}}
			for _, o := range orders {
				s.Orders = append(s.Orders, e1.LedgerOrder{BrokerOrderID: o.BrokerOrderID, Fills: []e1.LedgerFill{}})
			}
			return s, nil
		}
		broker := func(_ context.Context, o exec.BrokerOrder) (brokerView, error) {
			working := e.State().Order
			return brokerView{Terminal: working == nil || o.Key != working.Key}, nil
		}
		var tr *latencyTracker
		if withTracker {
			tr = newLatencyTracker(func() time.Time { return current }, nil)
			// The executor was built on the bare gateway by liveExecutor; rebuild it on the wrapper.
			e, kill = liveExecutor(t, in, tr.Gateway(gateway), true)
			ledger, broker = tr.Ledger(ledger), tr.Broker(broker)
		}
		err := driveLive(context.Background(), e, kill, false, time.Time{}, liveInputs{
			Ticks: ticks, Now: func() time.Time { return current }, Ledger: ledger, Broker: broker,
			Book: func(context.Context) (bookfeed.Snapshot, bool, error) {
				current = start.Add(time.Duration(seconds[index]) * time.Second)
				index++
				if current.Equal(start.Add(30 * time.Second)) {
					kill.Engage()
				}
				s := bookfeed.Snapshot{Symbol: in.Symbol, Exchange: current, Received: current, Source: "ws"}
				s.Bids[0].Price = 70000
				s.Asks[0].Price = 70200
				return s, true, nil
			},
		})
		if err != nil || e.State().Done != exec.DoneKilled {
			t.Fatalf("done=%q err=%v", e.State().Done, err)
		}
		var out bytes.Buffer
		if tr != nil {
			if err := tr.Flush(&out); err != nil {
				t.Fatal(err)
			}
		}
		return gateway.Sends, out.String()
	}
	bare, _ := run(false)
	wrapped, report := run(true)
	if len(bare) == 0 || len(bare) != len(wrapped) {
		t.Fatalf("sends differ: bare %d wrapped %d", len(bare), len(wrapped))
	}
	for i := range bare {
		if bare[i] != wrapped[i] {
			t.Fatalf("send %d differs: %+v vs %+v", i, bare[i], wrapped[i])
		}
	}
	lines := parseLines(t, report)
	if len(lines) != len(wrapped) {
		t.Fatalf("want one line per child (%d), got %d", len(wrapped), len(lines))
	}
	for i, l := range lines {
		if l.ChildKey != wrapped[i].Key || l.Action != wrapped[i].Action {
			t.Fatalf("line %d is %s/%s, child was %s/%s", i, l.ChildKey, l.Action, wrapped[i].Key, wrapped[i].Action)
		}
	}
}
