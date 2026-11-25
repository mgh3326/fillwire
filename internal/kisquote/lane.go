// Package kisquote manages promoted-symbol trade and order-book subscriptions
// on the KIS websocket session fillwire already owns, and publishes their ticks
// to one Redis Stream per symbol, quotes:kis:<symbol>.
//
// The lane never dials a websocket. It adds and removes subscriptions on the
// existing session, so a promoted symbol costs at most two registrations
// (H0STCNT0 and H0STASP0) on that session and never a second session. A symbol
// whose book stream the fixed [books] lane already holds costs only the trade
// registration. Registration capacity is the shared subscription budget in
// internal/ratebudget: the fixed lanes reserve their slots first, a safety
// margin stays reserved, and promoted symbols take what is left.
//
// A symbol keeps its slots for at least MinHold. When a higher-priority
// promotion needs the last slots, the lowest-priority symbol whose hold has
// elapsed is removed first; with no eligible symbol the promotion is refused
// (budget_cut) and the held set is unchanged. Reservations are never removed.
//
// A symbol that stops producing websocket frames for FeedGap falls back to the
// read-only REST quotation endpoints and publishes the recovered tick with
// source=rest, keeping the exchange's own clock.
//
// No network call runs under the state mutex: promotion and demotion are
// serialized by a separate operation mutex, so Handle (called on the fills
// reader goroutine) keeps running while a subscription is in flight.
package kisquote

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mgh3326/fillwire/internal/book"
	"github.com/mgh3326/fillwire/internal/bookfeed"
	"github.com/mgh3326/fillwire/internal/ratebudget"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

const (
	// StreamPrefix is the fixed key prefix. Ticks for symbol 005930 go to
	// quotes:kis:005930.
	StreamPrefix = "quotes:kis:"

	// registrationsPerSymbol is the maximum number of session registrations
	// one promoted symbol costs: H0STCNT0 and H0STASP0.
	registrationsPerSymbol = 2

	// safetyReserve is the margin kept free below the session's registration
	// limit (design 6321 §3.2: cap - notices 1 - books 1 - safety 2).
	safetyReserve = 2

	// DefaultMaxLen and DefaultBuffer mirror the other quote lanes.
	DefaultMaxLen = 100000
	DefaultBuffer = 1024
	maxBuffer     = 65536

	// subscribeTimeout bounds one Subscribe or Unsubscribe round trip.
	// ws.Conn waits for the server ACK without its own deadline, so the lane
	// must supply one; otherwise a lost ACK would hold the promotion pass.
	subscribeTimeout = 2 * time.Second

	// drainTimeout bounds the shutdown flush of the publish queue.
	drainTimeout = 2 * time.Second
)

var (
	// ErrClosed is returned once the lane has been closed.
	ErrClosed = errors.New("kisquote: lane closed")
	// ErrNotAttached is returned by Promote before Attach has bound a session.
	ErrNotAttached = errors.New("kisquote: lane not attached")
)

// Session is one KIS websocket connection. The lane only subscribes and
// unsubscribes on it; the fills reader owns dialing and the event channel.
type Session interface {
	Subscribe(ctx context.Context, tr, key string) error
	Unsubscribe(ctx context.Context, tr, key string) error
}

// RESTReader is the read-only KIS quotation client (internal/kisread).
type RESTReader interface {
	Read(ctx context.Context, path, tr string, query map[string]string, out any) error
}

// Config is the whole lane configuration. Symbols are the promoted list in
// priority order: the first entry has the highest priority and is registered
// first.
type Config struct {
	Symbols           []string
	RegistrationLimit int      // registrations the session admits, fixed lanes included
	Fixed             []string // slot ids already held on the session (fills, books)
	FixedSymbols      []string // symbols whose book stream the fixed lane already owns
	MinHold           time.Duration
	FeedGap           time.Duration
	MaxLen            int64
	Buffer            int
	Redis             redis.Cmdable
	REST              RESTReader
	Now               func() time.Time
	Logger            *slog.Logger
	// Slots, if set, is the session budget shared with other lanes. Its
	// capacity must be RegistrationLimit, and other lanes must have reserved
	// their slots before this lane admits a symbol. Nil: the lane builds its
	// own.
	Slots *ratebudget.Slots
}

// Stats is the process-local counter set logged at shutdown.
type Stats struct {
	Published uint64
	Dropped   uint64
	Gaps      uint64
}

type state struct {
	symbol   string
	priority int
	admitted time.Time
	lastWS   time.Time
	lastRest time.Time
	// trs are the registrations this lane added for the symbol. A fixed
	// [books] symbol carries only TRTrade here; its book stream belongs to
	// that lane and is never removed by this one.
	trs []string
}

// Lane is a started promoted-quote lane. The zero value is not usable.
type Lane struct {
	cfg             Config
	now             func() time.Time
	slots           *ratebudget.Slots
	capacity        int
	reservedSymbols map[string]bool

	// ops serializes promotion and demotion. It is never taken by Handle, so
	// a slow subscription cannot stall the fills reader.
	ops sync.Mutex

	mu      sync.Mutex
	session Session
	states  map[string]*state
	closed  bool
	queue   chan tick

	// stale counts registrations a failed subscription may have left active on
	// the server. They are subtracted from the free budget for the rest of the
	// session so the lane never admits a replacement over a live registration.
	stale     int
	cancel    context.CancelFunc
	workers   sync.WaitGroup
	published atomic.Uint64
	dropped   atomic.Uint64
	gaps      atomic.Uint64
}

// ReserveSession takes the registrations a session holds before any other
// consumer: the fixed lanes' slots and the safety margin. A lane that shares
// the session budget (internal/kisfutures) reserves after this.
func ReserveSession(slots *ratebudget.Slots, fixed []string) error {
	for _, id := range fixed {
		if _, err := slots.Reserve(id, 0); err != nil {
			return fmt.Errorf("kisquote: reserve %s: %w", id, err)
		}
	}
	for i := range safetyReserve {
		if _, err := slots.Reserve("safety-"+strconv.Itoa(i+1), 0); err != nil {
			return fmt.Errorf("kisquote: reserve safety margin: %w", err)
		}
	}
	return nil
}

// New validates the configuration and builds the slot budget. It dials
// nothing: Attach binds the lane to the session the fills reader already has.
func New(cfg Config) (*Lane, error) {
	if cfg.RegistrationLimit <= 0 {
		return nil, errors.New("kisquote: registration_limit must be positive")
	}
	if cfg.MinHold <= 0 {
		return nil, errors.New("kisquote: min_hold must be positive")
	}
	if cfg.FeedGap <= 0 {
		return nil, errors.New("kisquote: feed_gap must be positive")
	}
	if cfg.MaxLen <= 0 {
		return nil, errors.New("kisquote: max_len must be positive")
	}
	if cfg.Buffer <= 0 || cfg.Buffer > maxBuffer {
		return nil, errors.New("kisquote: buffer out of range")
	}
	if cfg.Redis == nil {
		return nil, errors.New("kisquote: Redis client is required")
	}
	if len(cfg.Fixed) > cfg.RegistrationLimit {
		return nil, errors.New("kisquote: fixed registrations exceed registration_limit")
	}
	slots := cfg.Slots
	if slots == nil {
		var err error
		if slots, err = ratebudget.NewSlots(cfg.RegistrationLimit); err != nil {
			return nil, fmt.Errorf("kisquote: %w", err)
		}
	}
	// Reserving an id a shared budget already holds changes nothing.
	if err := ReserveSession(slots, cfg.Fixed); err != nil {
		return nil, err
	}
	reserved := make(map[string]bool, len(cfg.FixedSymbols))
	for _, symbol := range cfg.FixedSymbols {
		if !bookfeed.SymbolOK(symbol) {
			return nil, errors.New("kisquote: invalid fixed symbol")
		}
		reserved[symbol] = true
	}
	seen := make(map[string]bool, len(cfg.Symbols))
	for _, symbol := range cfg.Symbols {
		if !bookfeed.SymbolOK(symbol) {
			return nil, errors.New("kisquote: invalid promoted symbol")
		}
		if seen[symbol] {
			return nil, errors.New("kisquote: duplicate promoted symbol")
		}
		seen[symbol] = true
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Lane{
		cfg:             cfg,
		now:             now,
		slots:           slots,
		capacity:        cfg.RegistrationLimit,
		reservedSymbols: reserved,
		states:          make(map[string]*state),
		queue:           make(chan tick, cfg.Buffer),
	}, nil
}

// Attach binds the lane to the live session and registers the configured
// promoted symbols in priority order. It runs off the fills reader goroutine
// and returns after the pass; symbols that do not fit or cannot be registered
// are reported and left unregistered. The fills path is never failed.
func (l *Lane) Attach(ctx context.Context, session Session) {
	if session == nil {
		return
	}
	l.mu.Lock()
	if l.session != nil || l.closed {
		l.mu.Unlock()
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	l.session, l.cancel = session, cancel
	symbols := append([]string(nil), l.cfg.Symbols...)
	l.mu.Unlock()

	l.workers.Add(2)
	go func() { defer l.workers.Done(); l.publishLoop(runCtx) }()
	go func() { defer l.workers.Done(); l.gapLoop(runCtx) }()

	for i, symbol := range symbols {
		if err := l.Promote(runCtx, symbol, len(symbols)-i); err != nil {
			if l.cfg.Logger == nil {
				continue
			}
			if errors.Is(err, ErrClosed) || ctx.Err() != nil {
				return
			}
			l.cfg.Logger.Warn("KIS 승격 종목 등록 실패", "symbol", symbol, "reason", err.Error())
		}
	}
}

// Promote registers one symbol at priority (higher keeps its slots). A symbol
// whose book stream the fixed lane already owns adds only its trade stream.
// When the budget is full it removes the lowest-priority promoted symbol whose
// MinHold has elapsed, and only if that symbol's priority is below priority.
// Otherwise it returns a ratebudget budget_cut refusal and changes nothing.
func (l *Lane) Promote(ctx context.Context, symbol string, priority int) error {
	if !bookfeed.SymbolOK(symbol) {
		return errors.New("kisquote: invalid promoted symbol")
	}
	l.ops.Lock()
	defer l.ops.Unlock()

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return ErrClosed
	}
	session := l.session
	if session == nil {
		l.mu.Unlock()
		return ErrNotAttached
	}
	if st, held := l.states[symbol]; held {
		st.priority = priority
		for _, tr := range st.trs {
			l.slots.Admit(slotID(tr, symbol), priority)
		}
		l.mu.Unlock()
		return nil
	}
	trs := []string{book.TRTrade}
	if !l.reservedSymbols[symbol] {
		trs = append(trs, book.TRBook)
	}
	victim := ""
	if l.freeLocked() < len(trs) {
		victim = l.victimLocked(priority)
		if victim == "" {
			l.mu.Unlock()
			return &ratebudget.Refusal{Code: ratebudget.CodeBudgetCut}
		}
	}
	l.mu.Unlock()

	if victim != "" {
		// The budget is only freed after the server confirms the removal, so
		// a failed unsubscribe can never push the session over its limit.
		if err := l.detach(ctx, session, victim); err != nil {
			return err
		}
	}
	if err := l.add(ctx, session, symbol, trs); err != nil {
		return err
	}

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		_ = l.remove(context.WithoutCancel(ctx), session, symbol, trs)
		return ErrClosed
	}
	now := l.now()
	l.states[symbol] = &state{symbol: symbol, priority: priority, admitted: now, lastWS: now, lastRest: now, trs: trs}
	for _, tr := range trs {
		l.slots.Admit(slotID(tr, symbol), priority)
	}
	l.mu.Unlock()
	return nil
}

// Demote removes one promoted symbol if its MinHold has elapsed. A reserved
// symbol's fixed stream is never removed; only what this lane added is. It
// reports whether the symbol was removed. A failed unsubscribe leaves the
// symbol held and returns the error.
func (l *Lane) Demote(ctx context.Context, symbol string) (bool, error) {
	l.ops.Lock()
	defer l.ops.Unlock()
	l.mu.Lock()
	st, held := l.states[symbol]
	if !held || l.closed {
		l.mu.Unlock()
		return false, nil
	}
	if l.now().Sub(st.admitted) < l.cfg.MinHold {
		l.mu.Unlock()
		return false, nil
	}
	session := l.session
	l.mu.Unlock()
	if err := l.detach(ctx, session, symbol); err != nil {
		return false, err
	}
	return true, nil
}

// Handle routes one realtime frame. It is called on the fills reader's
// goroutine, so it never blocks: parsed ticks go to a bounded queue and are
// dropped (counted) under backpressure. No network call runs under the mutex.
func (l *Lane) Handle(ev ws.Event) {
	owned := func(symbol string) bool {
		l.mu.Lock()
		_, held := l.states[symbol]
		l.mu.Unlock()
		return held
	}
	ticks, err := ticksFromEvent(ev, owned)
	if err != nil {
		return
	}
	for _, t := range ticks {
		l.mu.Lock()
		st, held := l.states[t.symbol]
		if held {
			st.lastWS = t.received
		}
		closed := l.closed
		l.mu.Unlock()
		if !held {
			continue
		}
		if closed {
			return
		}
		select {
		case l.queue <- t:
		default:
			if l.dropped.Add(1) == 1 && l.cfg.Logger != nil {
				l.cfg.Logger.Warn("KIS 승격 시세 큐 초과, 틱을 버립니다")
			}
		}
	}
}

// Stats returns the process-local counters.
func (l *Lane) Stats() Stats {
	return Stats{Published: l.published.Load(), Dropped: l.dropped.Load(), Gaps: l.gaps.Load()}
}

// Close stops the loops, flushes the queue for at most drainTimeout, and
// removes every subscription this lane added. The fills subscription and the
// fixed book stream are not touched.
func (l *Lane) Close() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	cancel := l.cancel
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}

	l.ops.Lock()
	defer l.ops.Unlock()
	l.workers.Wait()
	if l.cfg.Logger != nil {
		stats := l.Stats()
		l.cfg.Logger.Info("KIS 승격 시세 종료", "published", stats.Published, "dropped", stats.Dropped, "gaps", stats.Gaps)
	}

	l.mu.Lock()
	type removal struct {
		symbol string
		trs    []string
	}
	removals := make([]removal, 0, len(l.states))
	for symbol, st := range l.states {
		removals = append(removals, removal{symbol, append([]string(nil), st.trs...)})
		for _, tr := range st.trs {
			l.slots.Release(slotID(tr, symbol))
		}
	}
	l.states = make(map[string]*state)
	session := l.session
	l.mu.Unlock()

	if session == nil {
		return
	}
	ctx, stop := context.WithTimeout(context.Background(), subscribeTimeout)
	defer stop()
	for _, r := range removals {
		if err := l.remove(ctx, session, r.symbol, r.trs); err != nil && l.cfg.Logger != nil {
			l.cfg.Logger.Warn("KIS 승격 종목 해제 실패", "symbol", r.symbol, "reason", err.Error())
		}
	}
}

func (l *Lane) freeLocked() int {
	free := l.capacity - len(l.slots.Held()) - l.stale
	if free < 0 {
		return 0
	}
	return free
}

// victimLocked picks the lowest-priority promoted symbol below belowPriority
// whose MinHold has elapsed. Ties go to the lexicographically smallest symbol,
// so the choice is deterministic.
func (l *Lane) victimLocked(belowPriority int) string {
	now := l.now()
	victim := ""
	for symbol, st := range l.states {
		if st.priority >= belowPriority || now.Sub(st.admitted) < l.cfg.MinHold {
			continue
		}
		if victim == "" {
			victim = symbol
			continue
		}
		other := l.states[victim]
		if st.priority < other.priority || (st.priority == other.priority && symbol < victim) {
			victim = symbol
		}
	}
	return victim
}

// detach removes the symbol from the live session first and from the budget
// only after the server confirms it, so the real registration count never
// exceeds the configured limit.
func (l *Lane) detach(ctx context.Context, session Session, symbol string) error {
	l.mu.Lock()
	st, held := l.states[symbol]
	if !held {
		l.mu.Unlock()
		return nil
	}
	trs := append([]string(nil), st.trs...)
	l.mu.Unlock()

	if err := l.remove(ctx, session, symbol, trs); err != nil {
		return fmt.Errorf("kisquote: unsubscribe %s: %w", symbol, err)
	}

	l.mu.Lock()
	if _, held := l.states[symbol]; held {
		delete(l.states, symbol)
		for _, tr := range trs {
			l.slots.Release(slotID(tr, symbol))
		}
	}
	l.mu.Unlock()
	return nil
}

// add subscribes trs in order, removing the ones already added when a later
// one fails. Each call carries its own deadline. A failed Subscribe may have
// left its registration in go-kis's restore list (ErrApprovalRejected keeps
// it), so the rollback covers the failed transaction too.
func (l *Lane) add(ctx context.Context, session Session, symbol string, trs []string) error {
	for i, tr := range trs {
		callCtx, cancel := context.WithTimeout(ctx, subscribeTimeout)
		err := session.Subscribe(callCtx, tr, symbol)
		cancel()
		if err != nil {
			l.rollback(context.WithoutCancel(ctx), session, symbol, trs[:i+1])
			return fmt.Errorf("kisquote: subscribe %s: %w", tr, err)
		}
	}
	return nil
}

// rollback removes registrations a failed subscribe pass may have retained.
// A removal that also fails leaves a registration the server may still hold:
// it is counted as a stale slot for the rest of the session, so the budget
// never treats it as free even though this lane handles no ticks for it.
func (l *Lane) rollback(ctx context.Context, session Session, symbol string, trs []string) {
	failed := 0
	for _, tr := range trs {
		callCtx, cancel := context.WithTimeout(ctx, subscribeTimeout)
		err := session.Unsubscribe(callCtx, tr, symbol)
		cancel()
		if err != nil {
			failed++
		}
	}
	if failed == 0 {
		return
	}
	l.mu.Lock()
	l.stale += failed
	l.mu.Unlock()
	if l.cfg.Logger != nil {
		l.cfg.Logger.Warn("KIS 승격 등록 롤백 실패, 슬롯을 계속 점유합니다", "symbol", symbol, "retained", failed)
	}
}

// remove unsubscribes trs, reporting the first failure.
func (l *Lane) remove(ctx context.Context, session Session, symbol string, trs []string) error {
	var first error
	for _, tr := range trs {
		callCtx, cancel := context.WithTimeout(ctx, subscribeTimeout)
		err := session.Unsubscribe(callCtx, tr, symbol)
		cancel()
		if err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (l *Lane) publishLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			l.drain()
			return
		case t := <-l.queue:
			l.publishOne(ctx, t)
		}
	}
}

// drain flushes the queue for at most drainTimeout, then drops and counts the
// remainder. It is bounded by time, not by queue length times a Redis timeout.
func (l *Lane) drain() {
	deadline := time.Now().Add(drainTimeout)
	for {
		select {
		case t := <-l.queue:
			if !time.Now().Before(deadline) {
				l.dropRemaining()
				return
			}
			drainCtx, cancel := context.WithDeadline(context.Background(), deadline)
			err := l.publish(drainCtx, t)
			cancel()
			if err != nil {
				l.dropped.Add(1)
				continue
			}
			l.published.Add(1)
		default:
			return
		}
	}
}

// dropRemaining counts the ticks still queued after the drain deadline.
func (l *Lane) dropRemaining() {
	remaining := uint64(0)
drain:
	for {
		select {
		case <-l.queue:
			remaining++
		default:
			break drain
		}
	}
	if remaining == 0 {
		return
	}
	l.dropped.Add(remaining)
	if l.cfg.Logger != nil {
		l.cfg.Logger.Warn("KIS 승격 시세 종료 드레인 초과, 남은 틱을 버립니다", "dropped", remaining)
	}
}

func (l *Lane) publishOne(ctx context.Context, t tick) {
	if err := l.publish(ctx, t); err != nil {
		l.dropped.Add(1)
		if l.cfg.Logger != nil {
			l.cfg.Logger.Warn("KIS 승격 시세 발행 실패", "symbol", t.symbol, "reason", err.Error())
		}
		return
	}
	l.published.Add(1)
}

func (l *Lane) publish(ctx context.Context, t tick) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return l.cfg.Redis.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamPrefix + t.symbol,
		MaxLen: l.cfg.MaxLen,
		Approx: true,
		Values: t.values(),
	}).Err()
}

// gapLoop checks silent symbols on a fixed cadence; recoverGaps is separate so
// tests can drive it without waiting.
func (l *Lane) gapLoop(ctx context.Context) {
	interval := l.cfg.FeedGap / 2
	if interval < 250*time.Millisecond {
		interval = 250 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.recoverGaps(ctx)
		}
	}
}

// recoverGaps reads REST quotations for every held symbol whose websocket feed
// has been silent for FeedGap, at most once per FeedGap per symbol. A failed
// read publishes nothing.
func (l *Lane) recoverGaps(ctx context.Context) {
	if l.cfg.REST == nil {
		return
	}
	l.mu.Lock()
	symbols := make([]string, 0, len(l.states))
	for symbol := range l.states {
		symbols = append(symbols, symbol)
	}
	l.mu.Unlock()
	sort.Strings(symbols)
	for _, symbol := range symbols {
		if ctx.Err() != nil {
			return
		}
		l.mu.Lock()
		st, held := l.states[symbol]
		if !held {
			l.mu.Unlock()
			continue
		}
		now := l.now()
		silent, due := now.Sub(st.lastWS) >= l.cfg.FeedGap, now.Sub(st.lastRest) >= l.cfg.FeedGap
		if silent && due {
			st.lastRest = now
		}
		l.mu.Unlock()
		if !silent || !due {
			continue
		}
		l.gaps.Add(1)
		if l.cfg.Logger != nil {
			l.cfg.Logger.Warn("KIS 승격 시세 수신 공백, REST 로 복구합니다", "symbol", symbol)
		}
		if t, err := pollTrade(ctx, l.cfg.REST, symbol, l.now); err == nil {
			l.send(ctx, t)
		}
		if snapshot, err := bookfeed.Poll(ctx, l.cfg.REST, symbol, l.now); err == nil {
			l.send(ctx, tickFromSnapshot(snapshot))
		}
	}
}

func (l *Lane) send(ctx context.Context, t tick) {
	select {
	case l.queue <- t:
	default:
		l.dropped.Add(1)
	}
}

func slotID(tr, symbol string) string { return tr + "|" + symbol }
