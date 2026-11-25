package kisfutures

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/ratebudget"
	"github.com/mgh3326/fillwire/internal/testutil"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

// laneMaster is the golden master plus a second F200 quarter, so a roll swaps
// one contract of each product.
func laneMaster(t testing.TB) []MasterRow {
	rows := append(slices.Clone(loadGolden(t).Master), []string{"1", "A01706", "F 202706", "KOSPI200"})
	return parseRows(t, rows)
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

type fakeMaster struct {
	mu    sync.Mutex
	rows  []MasterRow
	err   error
	calls int
}

func (m *fakeMaster) Master(context.Context) ([]MasterRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	return m.rows, m.err
}

// fakeSession is the server side of one connection: the set of registrations.
type fakeSession struct {
	mu          sync.Mutex
	active      map[string]bool
	maxActive   int
	calls       []string // "+tr|code" and "-tr|code"
	subscribe   map[string]error
	unsubscribe map[string]error
}

func newSession() *fakeSession {
	return &fakeSession{active: map[string]bool{}, subscribe: map[string]error{}, unsubscribe: map[string]error{}}
}

func (s *fakeSession) Subscribe(_ context.Context, tr, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := tr + "|" + key
	s.calls = append(s.calls, "+"+id)
	if err := s.subscribe[id]; err != nil {
		return err
	}
	s.active[id] = true
	s.maxActive = max(s.maxActive, len(s.active))
	return nil
}

func (s *fakeSession) Unsubscribe(_ context.Context, tr, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := tr + "|" + key
	s.calls = append(s.calls, "-"+id)
	if err := s.unsubscribe[id]; err != nil {
		return err
	}
	delete(s.active, id)
	return nil
}

func (s *fakeSession) take() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	calls := s.calls
	s.calls = nil
	return calls
}

func (s *fakeSession) held() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	held := make([]string, 0, len(s.active))
	for id := range s.active {
		held = append(held, id)
	}
	slices.Sort(held)
	return held
}

type memSink struct {
	mu     sync.Mutex
	bars   []Bar
	err    error
	closed bool
}

func (s *memSink) Write(_ context.Context, bar Bar) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.bars = append(s.bars, bar)
	return nil
}

func (s *memSink) Close() error { s.closed = true; return nil }

// sessionSlots is a budget of capacity with the fixed fills slot and the
// safety margin already held, as the runtime builds it.
func sessionSlots(t testing.TB, capacity int) *ratebudget.Slots {
	t.Helper()
	slots, err := ratebudget.NewSlots(capacity)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"fills", "safety-1", "safety-2"} {
		if _, err := slots.Reserve(id, 0); err != nil {
			t.Fatal(err)
		}
	}
	return slots
}

type fixture struct {
	lane    *Lane
	clock   *fakeClock
	master  *fakeMaster
	session *fakeSession
	sink    *memSink
	slots   *ratebudget.Slots
}

// newFixture builds a lane bound to a fake session without starting its loops:
// tests drive reconcile and flush themselves.
func newFixture(t *testing.T, now string) *fixture {
	t.Helper()
	f := &fixture{
		clock:   &fakeClock{now: parseInstant(t, now)},
		master:  &fakeMaster{rows: laneMaster(t)},
		session: newSession(),
		sink:    &memSink{},
		slots:   sessionSlots(t, 41),
	}
	lane, err := New(Config{Slots: f.slots, Calendar: goldenCalendar(t), Master: f.master, Sink: f.sink, Now: f.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	lane.session = f.session
	f.lane = lane
	return f
}

func (f *fixture) reconcile() { f.lane.reconcile(context.Background()) }

func pairs(codes ...string) []string {
	var ids []string
	for _, tr := range []string{TRDayTrade, TRNightTrade} {
		for _, code := range codes {
			ids = append(ids, tr+"|"+code)
		}
	}
	slices.Sort(ids)
	return ids
}

// Subscription slots come from the ratebudget session budget: the lane holds
// eight of them, and a budget without room refuses it before it subscribes.
func TestRegistrationsComeFromTheSessionBudget(t *testing.T) {
	f := newFixture(t, "2026-11-24T10:00:00+09:00")
	futures := 0
	for _, held := range f.slots.Held() {
		if strings.HasPrefix(held.ID, "futures-") {
			if !held.Reserved {
				t.Errorf("%s is not reserved", held.ID)
			}
			futures++
		}
	}
	if futures != Registrations || len(f.slots.Held()) != 3+Registrations {
		t.Fatalf("held = %+v", f.slots.Held())
	}
	f.reconcile()
	if got := len(f.session.held()); got != Registrations {
		t.Fatalf("registrations = %d, want %d", got, Registrations)
	}
	f.lane.Close()
	if len(f.session.held()) != 0 || len(f.slots.Held()) != 3 || !f.sink.closed {
		t.Fatalf("after close: session %v, budget %+v", f.session.held(), f.slots.Held())
	}

	for name, slots := range map[string]*ratebudget.Slots{
		"seven free":                 sessionSlots(t, 3+Registrations-1),
		"full of promoted":           sessionSlots(t, 3+Registrations),
		"nothing but the fixed ones": sessionSlots(t, 3),
	} {
		if name == "full of promoted" {
			for _, id := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
				if _, err := slots.Admit(id, 1); err != nil {
					t.Fatal(err)
				}
			}
		}
		before := len(slots.Held())
		_, err := New(Config{Slots: slots, Calendar: goldenCalendar(t), Master: &fakeMaster{}, Sink: &memSink{}})
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		var refusal *ratebudget.Refusal
		if name != "full of promoted" && (!errors.As(err, &refusal) || refusal.Code != ratebudget.CodeBudgetCut) {
			t.Errorf("%s: err = %v, want budget_cut", name, err)
		}
		for _, held := range slots.Held() {
			if strings.HasPrefix(held.ID, "futures-") {
				t.Errorf("%s: a refused lane kept %s", name, held.ID)
			}
		}
		if name != "full of promoted" && len(slots.Held()) != before {
			t.Errorf("%s: held %d, want %d", name, len(slots.Held()), before)
		}
	}
}

func TestSubscribesFrontAndNextOnTheGivenSession(t *testing.T) {
	f := newFixture(t, "2026-11-24T10:00:00+09:00")
	f.reconcile()
	if got, want := f.session.held(), pairs("A01612", "A01703", "A05612", "A05703"); !slices.Equal(got, want) {
		t.Fatalf("registrations = %v, want %v", got, want)
	}
	f.session.take()
	f.clock.Set(parseInstant(t, "2026-11-24T10:00:30+09:00"))
	f.reconcile()
	if calls := f.session.take(); len(calls) != 0 || f.master.calls != 1 {
		t.Fatalf("an unchanged date called the session %v and read the master %d times", calls, f.master.calls)
	}
}

// After the day close of the last trade date the lane moves to the next two
// contracts, removing before adding so it never exceeds its reservation.
func TestRollRemovesBeforeAdding(t *testing.T) {
	f := newFixture(t, "2026-12-10T10:00:00+09:00")
	f.reconcile()
	if got, want := f.session.held(), pairs("A01612", "A01703", "A05612", "A05703"); !slices.Equal(got, want) {
		t.Fatalf("expiry day registrations = %v, want %v", got, want)
	}
	f.session.take()
	f.clock.Set(parseInstant(t, "2026-12-10T15:45:00+09:00"))
	f.reconcile()
	if got, want := f.session.held(), pairs("A01703", "A01706", "A05703", "A05706"); !slices.Equal(got, want) {
		t.Fatalf("after the roll = %v, want %v", got, want)
	}
	calls := f.session.take()
	if len(calls) != 8 || !strings.HasPrefix(calls[3], "-") || !strings.HasPrefix(calls[4], "+") {
		t.Fatalf("calls = %v, want four removals then four additions", calls)
	}
	if f.session.maxActive > Registrations {
		t.Fatalf("held %d registrations at once", f.session.maxActive)
	}
}

func TestFailedUnsubscribeKeepsTheRegistrationCounted(t *testing.T) {
	f := newFixture(t, "2026-12-10T10:00:00+09:00")
	f.reconcile()
	stuck := TRDayTrade + "|A01612"
	f.session.unsubscribe[stuck] = errors.New("no acknowledgement")
	f.clock.Set(parseInstant(t, "2026-12-10T15:45:00+09:00"))
	f.reconcile()
	held := f.session.held()
	if len(held) != Registrations || !slices.Contains(held, stuck) {
		t.Fatalf("held = %v; the unremoved stream must keep its registration", held)
	}
	if f.session.maxActive > Registrations {
		t.Fatalf("held %d registrations at once", f.session.maxActive)
	}
	f.session.take()
	f.clock.Set(f.clock.Now().Add(subscribeRetry - time.Second))
	f.reconcile()
	if calls := f.session.take(); len(calls) != 0 {
		t.Fatalf("retried early: %v", calls)
	}
	// The server now answers that it holds no such registration.
	f.session.unsubscribe[stuck] = &ws.SubscribeError{TR: TRDayTrade, RTCD: "1"}
	delete(f.session.active, stuck)
	f.clock.Set(f.clock.Now().Add(time.Second))
	f.reconcile()
	if got, want := f.session.held(), pairs("A01703", "A01706", "A05703", "A05706"); !slices.Equal(got, want) {
		t.Fatalf("after the retry = %v, want %v", got, want)
	}
}

func TestFailedSubscribeIsRemovedAndRetried(t *testing.T) {
	f := newFixture(t, "2026-11-24T10:00:00+09:00")
	refused := TRNightTrade + "|A01612"
	f.session.subscribe[refused] = &ws.SubscribeError{TR: TRNightTrade, RTCD: "1"}
	f.reconcile()
	if calls := f.session.take(); !slices.Contains(calls, "-"+refused) || len(f.session.held()) != Registrations-1 {
		t.Fatalf("calls = %v, held = %v", calls, f.session.held())
	}
	delete(f.session.subscribe, refused)
	f.reconcile()
	if calls := f.session.take(); len(calls) != 0 {
		t.Fatalf("retried at once: %v", calls)
	}
	f.clock.Set(f.clock.Now().Add(subscribeRetry))
	f.reconcile()
	if calls := f.session.take(); !slices.Equal(calls, []string{"+" + refused}) || len(f.session.held()) != Registrations {
		t.Fatalf("calls = %v, held = %v", calls, f.session.held())
	}
}

func TestMasterFailureRetriesAndKeepsReadRows(t *testing.T) {
	f := newFixture(t, "2026-12-10T10:00:00+09:00")
	f.master.err = errors.New("download failed")
	f.reconcile()
	f.reconcile()
	if len(f.session.held()) != 0 || f.master.calls != 1 {
		t.Fatalf("held %v after %d reads", f.session.held(), f.master.calls)
	}
	f.master.err = nil
	f.clock.Set(f.clock.Now().Add(masterRetry))
	f.reconcile()
	if len(f.session.held()) != Registrations {
		t.Fatalf("held = %v", f.session.held())
	}
	// A roll while the download fails resolves from the rows already read.
	f.master.err = errors.New("download failed")
	f.clock.Set(parseInstant(t, "2026-12-10T16:00:00+09:00"))
	f.reconcile()
	if got, want := f.session.held(), pairs("A01703", "A01706", "A05703", "A05706"); !slices.Equal(got, want) {
		t.Fatalf("after the roll = %v, want %v", got, want)
	}
}

func event(tr, code, price string, qty, acml int64, at time.Time) ws.Event {
	return ws.Event{TR: tr, ReceivedAt: at, Raw: frame(tr, record(tr, code, at.Format("150405"), price, qty, acml))}
}

func TestSinkFailureKeepsBarsInOrder(t *testing.T) {
	f := newFixture(t, "2026-11-24T09:00:00+09:00")
	f.reconcile()
	f.lane.Handle(event(TRDayTrade, "A01612", "400.10", 1, 1, kstAt(9, 0, 1)))
	f.lane.Handle(event(TRDayTrade, "A01612", "400.20", 1, 2, kstAt(9, 1, 1)))
	f.lane.Handle(event(TRDayTrade, "A09999", "400.20", 1, 2, kstAt(9, 1, 1)))
	f.lane.Handle(ws.Event{TR: TRDayTrade, ReceivedAt: kstAt(9, 1, 1), Raw: []byte("0|H0IFCNT0|001|x")})
	f.sink.err = errors.New("redis down")
	f.clock.Set(kstAt(9, 1, 10))
	f.lane.flush(context.Background())
	f.clock.Set(kstAt(9, 2, 10))
	f.lane.flush(context.Background())
	if len(f.sink.bars) != 0 {
		t.Fatalf("bars = %+v", f.sink.bars)
	}
	f.sink.err = nil
	f.lane.flush(context.Background())
	if len(f.sink.bars) != 2 || f.sink.bars[0].Open != "400.10" || f.sink.bars[1].Open != "400.20" {
		t.Fatalf("bars = %+v", f.sink.bars)
	}
	if got, want := f.lane.Stats(), (Stats{Trades: 2, Bars: 2, Invalid: 1, UnknownCode: 1}); got != want {
		t.Fatalf("stats = %+v, want %+v", got, want)
	}
}

// Attached to a session, the lane writes closed bars to the symbol's Redis
// Stream, holds the forming one back, also at shutdown, and removes its
// registrations.
func TestAttachedLaneWritesClosedBarsToRedis(t *testing.T) {
	mr, client := testutil.Redis(t)
	sink, err := NewRedisSink(redis.NewClient(&redis.Options{Addr: mr.Addr()}), 100)
	if err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{now: kstAt(9, 0, 0)}
	session := newSession()
	lane, err := New(Config{Slots: sessionSlots(t, 41), Calendar: goldenCalendar(t), Master: &fakeMaster{rows: laneMaster(t)}, Sink: sink, Now: clock.Now, Interval: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer lane.Close()
	lane.Attach(context.Background(), session)
	waitFor(t, "registrations", func() bool { return len(session.held()) == Registrations })
	lane.Handle(event(TRDayTrade, "A01612", "400.10", 3, 3, kstAt(9, 0, 1)))
	lane.Handle(event(TRDayTrade, "A01612", "400.30", 2, 5, kstAt(9, 0, 40)))
	lane.Handle(event(TRDayTrade, "A01612", "400.20", 1, 6, kstAt(9, 1, 5)))
	clock.Set(kstAt(9, 1, 9))
	time.Sleep(30 * time.Millisecond)
	if n := client.XLen(context.Background(), "minute:kis:futures:F200.2612").Val(); n != 0 {
		t.Fatalf("%d bars before the grace", n)
	}
	clock.Set(kstAt(9, 1, 10))
	waitFor(t, "the closed bar", func() bool { return lane.Stats().Bars == 1 })
	lane.Close()
	rows, err := client.XRange(context.Background(), "minute:kis:futures:F200.2612", "-", "+").Result()
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows = %v, %v; the forming 09:01 bar must not be written", rows, err)
	}
	want := map[string]any{
		"time_utc": "2026-11-24T00:00:00Z", "symbol": "F200.2612", "product": "F200", "contract_month": "2612",
		"roll_state": "front", "session_segment": "FUT_DAY", "session_date": "2026-11-24", "source": "KIS",
		"open": "400.10", "high": "400.30", "low": "400.10", "close": "400.30", "volume": "5",
		"received_at": "2026-11-24T00:01:10Z", "volume_gap": "false",
	}
	if len(rows[0].Values) != len(want) {
		t.Fatalf("fields = %v", rows[0].Values)
	}
	for key, value := range want {
		if rows[0].Values[key] != value {
			t.Errorf("%s = %v, want %v", key, rows[0].Values[key], value)
		}
	}
	if len(session.held()) != 0 {
		t.Fatalf("registrations after close = %v", session.held())
	}
}

func TestJSONLSinkAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bars.jsonl")
	bar := Bar{TimeUTC: kstAt(9, 0, 0), Symbol: "F200.2612", Volume: 3, ReceivedAt: kstAt(9, 1, 10)}
	for range 2 {
		sink, err := NewJSONLSink(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.Write(context.Background(), bar); err != nil {
			t.Fatal(err)
		}
		if err := sink.Close(); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	row, _ := MarshalRow(bar)
	if want := string(row) + "\n" + string(row) + "\n"; string(data) != want {
		t.Fatalf("file = %q, want %q", data, want)
	}
}

func masterZip(t *testing.T, name string, content []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	archive := zip.NewWriter(&buffer)
	entry, err := archive.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func TestHTTPMaster(t *testing.T) {
	good := masterZip(t, "fo_idx_code_mts.mst", masterBytes(loadGolden(t).Master))
	other := masterZip(t, "other.mst", []byte("x"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			_, _ = w.Write(good)
		case "/moved":
			http.Redirect(w, r, "/ok", http.StatusFound)
		case "/entry":
			_, _ = w.Write(other)
		case "/text":
			_, _ = w.Write([]byte("<html>"))
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()
	rows, err := HTTPMaster{URL: server.URL + "/ok"}.Master(context.Background())
	if err != nil || len(rows) != len(loadGolden(t).Master) {
		t.Fatalf("rows = %d, %v", len(rows), err)
	}
	contracts, unresolved, err := ResolveFrontNext(rows, Date(2026, time.November, 24))
	if err != nil || len(contracts) != 4 || len(unresolved) != 0 {
		t.Fatalf("contracts = %v, %v, %v", contracts, unresolved, err)
	}
	for _, path := range []string{"/moved", "/entry", "/text", "/error"} {
		if _, err := (HTTPMaster{URL: server.URL + path}).Master(context.Background()); err == nil {
			t.Errorf("%s: accepted", path)
		}
	}
}

func waitFor(t *testing.T, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
