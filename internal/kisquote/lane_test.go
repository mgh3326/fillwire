package kisquote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/fillwire/internal/book"
	"github.com/mgh3326/fillwire/internal/ratebudget"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

var kst = time.FixedZone("KST", 9*60*60)

func at(ms int) time.Time {
	return time.Date(2026, 11, 2, 9, 0, 0, 0, kst).Add(time.Duration(ms) * time.Millisecond)
}

// bookRecord builds a full-width (59 field) H0STASP0 record with a 100 KRW
// ladder around bestAsk/bestBid, mirroring internal/book's fixture.
func bookRecord(symbol, hour string, bestAsk, bestBid, qty int64) string {
	f := []string{symbol, hour, "0"}
	for i := range book.Depth {
		f = append(f, strconv.FormatInt(bestAsk+int64(i)*100, 10))
	}
	for i := range book.Depth {
		f = append(f, strconv.FormatInt(bestBid-int64(i)*100, 10))
	}
	for i := range 2 * book.Depth {
		f = append(f, strconv.FormatInt(qty+int64(i), 10))
	}
	f = append(f, strconv.FormatInt(qty*book.Depth, 10), strconv.FormatInt(qty*book.Depth+1, 10))
	for len(f) < 59 {
		f = append(f, "0")
	}
	return strings.Join(f, "^")
}

// tradeRecord builds a full-width (46 field) H0STCNT0 record.
func tradeRecord(symbol, hour string, price, qty, acml int64, side string) string {
	f := make([]string, 46)
	for i := range f {
		f[i] = "0"
	}
	f[0], f[1] = symbol, hour
	f[2] = strconv.FormatInt(price, 10)
	f[12] = strconv.FormatInt(qty, 10)
	f[13] = strconv.FormatInt(acml, 10)
	f[21] = side
	return strings.Join(f, "^")
}

func frame(tr string, records ...string) []byte {
	return []byte(fmt.Sprintf("0|%s|%03d|%s", tr, len(records), strings.Join(records, "^")))
}

type fakeSession struct {
	mu         sync.Mutex
	subs       map[string]bool
	trace      []string
	failUnsub  bool
	failSubTR  string        // when set, Subscribe of this TR records then fails
	blockSub   chan struct{} // when non-nil, Subscribe waits for close or ctx end
	subStarted chan struct{} // receives once when blockSub is set
}

func newFakeSession() *fakeSession { return &fakeSession{subs: map[string]bool{}} }

func (f *fakeSession) Subscribe(ctx context.Context, tr, key string) error {
	f.mu.Lock()
	block, started := f.blockSub, f.subStarted
	f.mu.Unlock()
	if block != nil {
		if started != nil {
			select {
			case started <- struct{}{}:
			default:
			}
		}
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	// A failed subscribe still leaves the registration on the server side in
	// go-kis (ErrApprovalRejected keeps it for the reconnect restore list).
	f.subs[tr+"|"+key] = true
	f.trace = append(f.trace, "sub "+tr+"|"+key)
	if f.failSubTR != "" && f.failSubTR == tr {
		return errors.New("fake session: subscribe rejected")
	}
	return nil
}

func (f *fakeSession) Unsubscribe(_ context.Context, tr, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failUnsub {
		return errors.New("fake session: unsubscribe failed")
	}
	delete(f.subs, tr+"|"+key)
	f.trace = append(f.trace, "unsub "+tr+"|"+key)
	return nil
}

func (f *fakeSession) held() map[string]bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]bool, len(f.subs))
	for k := range f.subs {
		out[k] = true
	}
	return out
}

// fakeREST answers the two read-only quotation paths with fixed JSON bodies.
type fakeREST struct {
	mu      sync.Mutex
	paths   []string
	bodies  map[string]string
	failFor string
}

func (f *fakeREST) Read(_ context.Context, path, tr string, _ map[string]string, out any) error {
	f.mu.Lock()
	f.paths = append(f.paths, path+"|"+tr)
	body, ok := f.bodies[path]
	fail := f.failFor == path
	f.mu.Unlock()
	if fail || !ok {
		return errors.New("fake rest: refused")
	}
	return json.Unmarshal([]byte(body), out)
}

func askingPriceBody(symbol string) string {
	fields := map[string]string{"aspr_acpt_hour": "090005"}
	for i := 1; i <= 5; i++ {
		n := strconv.Itoa(i)
		fields["askp"+n] = strconv.Itoa(71000 + i*100)
		fields["bidp"+n] = strconv.Itoa(71000 - i*100)
		fields["askp_rsqn"+n] = "10"
		fields["bidp_rsqn"+n] = "20"
	}
	body, _ := json.Marshal(map[string]any{"rt_cd": "0", "output1": fields})
	return string(body)
}

func newRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

func newLane(t *testing.T, cfg Config) *Lane {
	t.Helper()
	lane, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(lane.Close)
	return lane
}

func baseConfig(t *testing.T) Config {
	return Config{
		RegistrationLimit: 8,
		Fixed:             []string{"fills"},
		MinHold:           15 * time.Minute,
		FeedGap:           time.Hour,
		MaxLen:            1000,
		Buffer:            64,
		Redis:             newRedis(t),
		Now:               func() time.Time { return at(0) },
	}
}

func TestPromotedSymbolsFillTheBudgetThenCut(t *testing.T) {
	cfg := baseConfig(t)
	// limit 8 - fills 1 - safety 2 = 5 free registrations -> two symbols.
	lane := newLane(t, cfg)
	session := newFakeSession()
	lane.Attach(context.Background(), session)

	for _, symbol := range []string{"005930", "000660"} {
		if err := lane.Promote(context.Background(), symbol, 10); err != nil {
			t.Fatalf("Promote %s: %v", symbol, err)
		}
	}
	err := lane.Promote(context.Background(), "035420", 10)
	if code := ratebudget.CodeOf(err); code != ratebudget.CodeBudgetCut {
		t.Fatalf("third Promote error = %v, want budget_cut", err)
	}
	held := session.held()
	if len(held) != 4 || !held["H0STCNT0|005930"] || !held["H0STASP0|005930"] || !held["H0STCNT0|000660"] || !held["H0STASP0|000660"] {
		t.Fatalf("held subscriptions = %v", held)
	}
	if _, ok := lane.states["035420"]; ok {
		t.Fatal("refused symbol must not be held")
	}
}

func TestDemotionWaitsForMinHoldThenTakesTheLowestPriority(t *testing.T) {
	cfg := baseConfig(t)
	now := at(0)
	var mu sync.Mutex
	cfg.Now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	lane := newLane(t, cfg)
	session := newFakeSession()
	lane.Attach(context.Background(), session)

	if err := lane.Promote(context.Background(), "005930", 10); err != nil {
		t.Fatal(err)
	}
	if err := lane.Promote(context.Background(), "000660", 20); err != nil {
		t.Fatal(err)
	}
	// Budget is full and 005930 is inside its hold, so a higher-priority
	// promotion is refused.
	if err := lane.Promote(context.Background(), "035420", 30); ratebudget.CodeOf(err) != ratebudget.CodeBudgetCut {
		t.Fatalf("promotion inside min_hold = %v, want budget_cut", err)
	}
	mu.Lock()
	now = at(int(16 * time.Minute / time.Millisecond))
	mu.Unlock()
	if err := lane.Promote(context.Background(), "035420", 30); err != nil {
		t.Fatalf("promotion after min_hold: %v", err)
	}
	held := session.held()
	if held["H0STCNT0|005930"] || held["H0STASP0|005930"] {
		t.Fatalf("lowest priority symbol not demoted: %v", held)
	}
	if !held["H0STCNT0|035420"] || !held["H0STASP0|035420"] || !held["H0STCNT0|000660"] {
		t.Fatalf("held after demotion = %v", held)
	}
}

// A symbol whose book stream the fixed [books] lane already holds gains only
// its trade registration, and still publishes both trade and book ticks.
func TestReservedSymbolAddsOnlyItsTradeStream(t *testing.T) {
	cfg := baseConfig(t)
	cfg.Fixed = []string{"fills", "books"}
	cfg.FixedSymbols = []string{"005930"}
	now := at(0)
	var mu sync.Mutex
	cfg.Now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	lane := newLane(t, cfg)
	session := newFakeSession()
	lane.Attach(context.Background(), session)
	if err := lane.Promote(context.Background(), "005930", 10); err != nil {
		t.Fatalf("Promote reserved symbol: %v", err)
	}
	held := session.held()
	if len(held) != 1 || !held["H0STCNT0|005930"] {
		t.Fatalf("held = %v, want only the trade stream", held)
	}
	received := at(5000)
	lane.Handle(ws.Event{TR: book.TRTrade, ReceivedAt: received,
		Raw: frame(book.TRTrade, tradeRecord("005930", "090005", 71000, 10, 1000, "1"))})
	// The book frame comes from the fixed [books] subscription and is still
	// published for the promoted symbol.
	lane.Handle(ws.Event{TR: book.TRBook, ReceivedAt: received,
		Raw: frame(book.TRBook, bookRecord("005930", "090005", 71000, 70900, 12))})
	rows := waitRows(t, cfg.Redis.(*redis.Client), StreamPrefix+"005930", 2)
	if rows[0].Values["kind"] != "trade" || rows[1].Values["kind"] != "book" {
		t.Fatalf("published rows = %v / %v", rows[0].Values, rows[1].Values)
	}
	mu.Lock()
	now = at(int(16 * time.Minute / time.Millisecond))
	mu.Unlock()
	removed, err := lane.Demote(context.Background(), "005930")
	if err != nil || !removed {
		t.Fatalf("Demote = %v, %v", removed, err)
	}
	if held := session.held(); len(held) != 0 {
		t.Fatalf("demote left a subscription behind: %v", held)
	}
}

func TestDemoteRespectsMinHold(t *testing.T) {
	cfg := baseConfig(t)
	now := at(0)
	var mu sync.Mutex
	cfg.Now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	lane := newLane(t, cfg)
	session := newFakeSession()
	lane.Attach(context.Background(), session)
	if err := lane.Promote(context.Background(), "005930", 10); err != nil {
		t.Fatal(err)
	}
	if removed, err := lane.Demote(context.Background(), "005930"); err != nil || removed {
		t.Fatalf("Demote inside min_hold = %v, %v; want false, nil", removed, err)
	}
	mu.Lock()
	now = at(int(16 * time.Minute / time.Millisecond))
	mu.Unlock()
	if removed, err := lane.Demote(context.Background(), "005930"); err != nil || !removed {
		t.Fatalf("Demote after min_hold = %v, %v; want true, nil", removed, err)
	}
	if held := session.held(); len(held) != 0 {
		t.Fatalf("demoted symbol still subscribed: %v", held)
	}
}

func TestHandlePublishesTradeAndBookTicksPerSymbol(t *testing.T) {
	cfg := baseConfig(t)
	lane := newLane(t, cfg)
	session := newFakeSession()
	lane.Attach(context.Background(), session)
	if err := lane.Promote(context.Background(), "005930", 10); err != nil {
		t.Fatal(err)
	}

	received := at(5000)
	lane.Handle(ws.Event{
		TR:         book.TRTrade,
		ReceivedAt: received,
		Raw:        frame(book.TRTrade, tradeRecord("005930", "090005", 71000, 10, 1000, "1")),
	})
	lane.Handle(ws.Event{
		TR:         book.TRBook,
		ReceivedAt: received,
		Raw:        frame(book.TRBook, bookRecord("005930", "090005", 71000, 70900, 12)),
	})
	// An unrelated symbol is not published.
	lane.Handle(ws.Event{
		TR:         book.TRTrade,
		ReceivedAt: received,
		Raw:        frame(book.TRTrade, tradeRecord("000660", "090005", 100000, 1, 1, "5")),
	})

	rows := waitRows(t, cfg.Redis.(*redis.Client), StreamPrefix+"005930", 2)
	trade, bookRow := rows[0].Values, rows[1].Values
	if trade["kind"] != "trade" || trade["source"] != "ws" || trade["feed"] != book.TRTrade || trade["price"] != "71000" || trade["qty"] != "10" || trade["side"] != "buy" {
		t.Fatalf("trade tick = %v", trade)
	}
	if trade["ts_exchange"] == "" || trade["ts_received"] == "" || trade["bid_price_1"] != "" {
		t.Fatalf("trade tick timestamps/empty book fields = %v", trade)
	}
	if bookRow["kind"] != "book" || bookRow["feed"] != book.TRBook || bookRow["bid_price_1"] != "70900" || bookRow["ask_price_1"] != "71000" || bookRow["price"] != "" {
		t.Fatalf("book tick = %v", bookRow)
	}
}

func TestMalformedAndForeignFramesDoNotPanicOrPublish(t *testing.T) {
	cfg := baseConfig(t)
	lane := newLane(t, cfg)
	lane.Attach(context.Background(), newFakeSession())
	if err := lane.Promote(context.Background(), "005930", 10); err != nil {
		t.Fatal(err)
	}
	lane.Handle(ws.Event{TR: book.TRTrade, Raw: []byte("0|H0STCNT0|001|short")})
	lane.Handle(ws.Event{TR: ws.TRExecutionLive, Raw: []byte("0|H0STCNI0|001|x")})
	if rows := allRows(t, cfg.Redis.(*redis.Client), StreamPrefix+"005930"); len(rows) != 0 {
		t.Fatalf("malformed frames published %d rows", len(rows))
	}
}

func TestFeedGapRecoversFromREST(t *testing.T) {
	cfg := baseConfig(t)
	now := at(0)
	var mu sync.Mutex
	cfg.Now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	cfg.FeedGap = 30 * time.Second
	rest := &fakeREST{bodies: map[string]string{
		tradeRESTPath: `{"rt_cd":"0","output":[{"stck_cntg_hour":"090005","stck_prpr":"71000","cntg_vol":"10"},{"stck_cntg_hour":"090001","stck_prpr":"70900","cntg_vol":"4"}]}`,
		"/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn": askingPriceBody("005930"),
	}}
	cfg.REST = rest
	lane := newLane(t, cfg)
	lane.Attach(context.Background(), newFakeSession())
	if err := lane.Promote(context.Background(), "005930", 10); err != nil {
		t.Fatal(err)
	}

	lane.recoverGaps(context.Background())
	if rows := allRows(t, cfg.Redis.(*redis.Client), StreamPrefix+"005930"); len(rows) != 0 {
		t.Fatalf("gap recovery ran inside feed_gap: %d rows", len(rows))
	}
	mu.Lock()
	now = at(int(31 * time.Second / time.Millisecond))
	mu.Unlock()
	lane.recoverGaps(context.Background())
	rows := waitRows(t, cfg.Redis.(*redis.Client), StreamPrefix+"005930", 2)
	if rows[0].Values["source"] != "rest" || rows[0].Values["feed"] != tradeRESTTR || rows[0].Values["price"] != "71000" {
		t.Fatalf("REST trade tick = %v", rows[0].Values)
	}
	if rows[0].Values["acml_vol"] != "" {
		t.Fatalf("REST trade tick claims a cumulative volume: %v", rows[0].Values)
	}
	if rows[1].Values["source"] != "rest" || rows[1].Values["kind"] != "book" || rows[1].Values["bid_price_1"] != "70900" {
		t.Fatalf("REST book tick = %v", rows[1].Values)
	}
	// One recovery per feed_gap window: an immediate second pass reads nothing.
	lane.recoverGaps(context.Background())
	rest.mu.Lock()
	paths := len(rest.paths)
	rest.mu.Unlock()
	if paths != 2 {
		t.Fatalf("REST reads = %d, want 2", paths)
	}
	if got := lane.Stats().Gaps; got != 1 {
		t.Fatalf("gaps counter = %d, want 1", got)
	}
}

// A slow Subscribe must not hold the state mutex: Handle (the fills reader
// goroutine) keeps working while a promotion is in flight.
func TestHandleRunsWhileSubscriptionIsBlocked(t *testing.T) {
	cfg := baseConfig(t)
	lane := newLane(t, cfg)
	session := newFakeSession()
	lane.Attach(context.Background(), session)
	if err := lane.Promote(context.Background(), "005930", 10); err != nil {
		t.Fatal(err)
	}

	block := make(chan struct{})
	started := make(chan struct{}, 1)
	session.mu.Lock()
	session.blockSub, session.subStarted = block, started
	session.mu.Unlock()
	promoted := make(chan error, 1)
	go func() { promoted <- lane.Promote(context.Background(), "000660", 10) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("second promotion never reached Subscribe")
	}

	handled := make(chan struct{})
	go func() {
		lane.Handle(ws.Event{TR: book.TRTrade, ReceivedAt: at(5000),
			Raw: frame(book.TRTrade, tradeRecord("005930", "090005", 71000, 10, 1000, "1"))})
		close(handled)
	}()
	select {
	case <-handled:
	case <-time.After(2 * time.Second):
		t.Fatal("Handle blocked behind an in-flight Subscribe")
	}
	waitRows(t, cfg.Redis.(*redis.Client), StreamPrefix+"005930", 1)

	close(block)
	if err := <-promoted; err != nil {
		t.Fatalf("blocked promotion: %v", err)
	}
}

// A lost subscribe ACK is bounded by the lane's subscribe timeout, so Attach
// cannot wait on the session for the life of the process.
func TestAttachBoundsSubscribeWait(t *testing.T) {
	cfg := baseConfig(t)
	cfg.Symbols = []string{"005930"}
	lane := newLane(t, cfg)
	session := newFakeSession()
	session.blockSub = make(chan struct{})
	start := time.Now()
	lane.Attach(context.Background(), session)
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("Attach took %s; Subscribe is not bounded", elapsed)
	}
	if held := session.held(); len(held) != 0 {
		t.Fatalf("timed-out subscription was recorded: %v", held)
	}
	lane.mu.Lock()
	states := len(lane.states)
	lane.mu.Unlock()
	if states != 0 {
		t.Fatalf("timed-out symbol left %d states", states)
	}
}

// A failed unsubscribe keeps the state and the slot: the server may still hold
// the registration, so the lane must not admit a replacement over the limit.
func TestFailedUnsubscribeKeepsTheSlot(t *testing.T) {
	cfg := baseConfig(t)
	cfg.RegistrationLimit = 4 // safety 2 + one symbol
	cfg.Fixed = nil
	now := at(0)
	var mu sync.Mutex
	cfg.Now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	lane := newLane(t, cfg)
	session := newFakeSession()
	lane.Attach(context.Background(), session)
	if err := lane.Promote(context.Background(), "005930", 10); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	now = at(int(16 * time.Minute / time.Millisecond))
	mu.Unlock()

	session.mu.Lock()
	session.failUnsub = true
	session.mu.Unlock()
	if removed, err := lane.Demote(context.Background(), "005930"); err == nil || removed {
		t.Fatalf("Demote with failing unsubscribe = %v, %v; want false, error", removed, err)
	}
	lane.mu.Lock()
	_, stillHeld := lane.states["005930"]
	lane.mu.Unlock()
	if !stillHeld {
		t.Fatal("failed unsubscribe dropped the state")
	}
	// The slot is still counted, so a higher-priority promotion cannot exceed
	// the limit while the server registration exists.
	if err := lane.Promote(context.Background(), "000660", 20); err == nil {
		t.Fatal("promotion succeeded over a still-registered symbol")
	}
	if held := session.held(); !held["H0STCNT0|005930"] {
		t.Fatalf("session registration lost: %v", held)
	}

	session.mu.Lock()
	session.failUnsub = false
	session.mu.Unlock()
	if removed, err := lane.Demote(context.Background(), "005930"); err != nil || !removed {
		t.Fatalf("Demote after recovery = %v, %v", removed, err)
	}
}

// hangingRedis blocks every XADD until its context ends. Only XAdd is used by
// the lane, so the embedded nil interface is never called.
type hangingRedis struct {
	redis.Cmdable
}

func (h *hangingRedis) XAdd(ctx context.Context, _ *redis.XAddArgs) *redis.StringCmd {
	<-ctx.Done()
	cmd := redis.NewStringCmd(ctx)
	cmd.SetErr(ctx.Err())
	return cmd
}

// Close is bounded by the drain timeout even when Redis never answers.
func TestCloseIsBoundedWhenRedisHangs(t *testing.T) {
	cfg := baseConfig(t)
	cfg.Redis = &hangingRedis{}
	cfg.Buffer = 4
	lane := newLane(t, cfg)
	session := newFakeSession()
	lane.Attach(context.Background(), session)
	if err := lane.Promote(context.Background(), "005930", 10); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		lane.Handle(ws.Event{TR: book.TRTrade, ReceivedAt: at(5000),
			Raw: frame(book.TRTrade, tradeRecord("005930", "090005", 71000, 10, 1000, "1"))})
	}
	closed := make(chan struct{})
	go func() { lane.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(4 * time.Second):
		t.Fatal("Close did not return; the drain is not bounded")
	}
	if got := lane.Stats().Dropped; got == 0 {
		t.Fatal("unpublished ticks were not counted as dropped")
	}
}

// The trade read returns a list; the newest row by exchange clock is used, and
// an empty list is an error rather than a zero-value tick.
func TestPollTradeSelectsNewestRecord(t *testing.T) {
	rest := &fakeREST{bodies: map[string]string{
		tradeRESTPath: `{"rt_cd":"0","output":[{"stck_cntg_hour":"090010","stck_prpr":"71100","cntg_vol":"5"},{"stck_cntg_hour":"090012","stck_prpr":"71200","cntg_vol":"7"}]}`,
	}}
	got, err := pollTrade(context.Background(), rest, "005930", func() time.Time { return at(15000) })
	if err != nil {
		t.Fatalf("pollTrade: %v", err)
	}
	if got.price != 71200 || got.qty != 7 || got.exchange.In(kst).Format("150405") != "090012" {
		t.Fatalf("selected record = %+v", got)
	}
	rest.bodies[tradeRESTPath] = `{"rt_cd":"0","output":[]}`
	if _, err := pollTrade(context.Background(), rest, "005930", func() time.Time { return at(15000) }); err == nil {
		t.Fatal("empty trade list accepted")
	}
}

// A registration a failed subscribe left behind (and a failed rollback could
// not remove) keeps consuming budget: no replacement may be admitted over it.
func TestRetainedRegistrationCountsAgainstTheBudget(t *testing.T) {
	cfg := baseConfig(t)
	cfg.RegistrationLimit = 4 // safety 2 + one symbol
	cfg.Fixed = nil
	lane := newLane(t, cfg)
	session := newFakeSession()
	lane.Attach(context.Background(), session)
	session.mu.Lock()
	session.failSubTR = book.TRTrade
	session.failUnsub = true
	session.mu.Unlock()

	if err := lane.Promote(context.Background(), "005930", 10); err == nil {
		t.Fatal("promotion with a rejected subscribe succeeded")
	}
	err := lane.Promote(context.Background(), "000660", 10)
	if code := ratebudget.CodeOf(err); code != ratebudget.CodeBudgetCut {
		t.Fatalf("replacement promotion = %v, want budget_cut while the registration may be live", err)
	}

	// When the rollback succeeds, the slot is free again.
	cfg2 := baseConfig(t)
	cfg2.RegistrationLimit = 4
	cfg2.Fixed = nil
	lane2 := newLane(t, cfg2)
	session2 := newFakeSession()
	lane2.Attach(context.Background(), session2)
	session2.mu.Lock()
	session2.failSubTR = book.TRTrade
	session2.mu.Unlock()
	if err := lane2.Promote(context.Background(), "005930", 10); err == nil {
		t.Fatal("promotion with a rejected subscribe succeeded")
	}
	session2.mu.Lock()
	session2.failSubTR = ""
	session2.mu.Unlock()
	if err := lane2.Promote(context.Background(), "000660", 10); err != nil {
		t.Fatalf("replacement after a clean rollback: %v", err)
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	cases := map[string]func(*Config){
		"limit":   func(c *Config) { c.RegistrationLimit = 0 },
		"minhold": func(c *Config) { c.MinHold = 0 },
		"feedgap": func(c *Config) { c.FeedGap = 0 },
		"maxlen":  func(c *Config) { c.MaxLen = 0 },
		"buffer":  func(c *Config) { c.Buffer = maxBuffer + 1 },
		"redis":   func(c *Config) { c.Redis = nil },
		"symbol":  func(c *Config) { c.Symbols = []string{"bad"} },
		"dupe":    func(c *Config) { c.Symbols = []string{"005930", "005930"} },
		"fixed":   func(c *Config) { c.Fixed = make([]string, 9) },
	}
	for name, mutate := range cases {
		cfg := baseConfig(t)
		mutate(&cfg)
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: New accepted an invalid config", name)
		}
	}
}

func waitRows(t *testing.T, r *redis.Client, key string, want int) []redis.XMessage {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		rows, err := r.XRange(context.Background(), key, "-", "+").Result()
		if err != nil {
			t.Fatalf("XRange: %v", err)
		}
		if len(rows) >= want {
			return rows
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s has %d rows, want %d", key, len(rows), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func allRows(t *testing.T, r *redis.Client, key string) []redis.XMessage {
	t.Helper()
	rows, err := r.XRange(context.Background(), key, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	return rows
}

var _ = ws.TRQuotePrice
