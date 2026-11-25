// Package kisquote manages promoted-symbol trade and order-book subscriptions
// on the KIS websocket session fillwire already owns, and publishes their ticks
// to one Redis Stream per symbol, quotes:kis:<symbol>.
//
// The lane never dials a websocket. It adds and removes subscriptions on the
// existing session, so a promoted symbol costs two registrations (H0STCNT0 and
// H0STASP0) on that session and never a second session. Registration capacity
// is the shared subscription budget in internal/ratebudget: the fixed lanes
// (execution notices and the optional book producer) reserve their slots first,
// a safety margin stays reserved, and promoted symbols take what is left.
//
// A symbol keeps its slots for at least MinHold. When a higher-priority
// promotion needs the last slots, the lowest-priority symbol whose hold has
// elapsed is removed first; with no eligible symbol the promotion is refused
// (budget_cut) and the held set is unchanged. Reservations are never removed.
//
// A symbol that stops producing websocket frames for FeedGap falls back to the
// read-only REST quotation endpoints and publishes the recovered tick with
// source=rest, keeping the exchange's own clock.
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

	// registrationsPerSymbol is the number of session registrations one
	// promoted symbol costs: H0STCNT0 and H0STASP0.
	registrationsPerSymbol = 2

	// safetyReserve is the margin kept free below the session's registration
	// limit (design 6321 §3.2: cap - notices 1 - books 1 - safety 2).
	safetyReserve = 2

	// DefaultMaxLen and DefaultBuffer mirror the other quote lanes.
	DefaultMaxLen = 100000
	DefaultBuffer = 1024
	maxBuffer     = 65536
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
}

// Lane is a started promoted-quote lane. The zero value is not usable.
type Lane struct {
	cfg             Config
	now             func() time.Time
	slots           *ratebudget.Slots
	capacity        int
	reservedSymbols map[string]bool

	mu      sync.Mutex
	session Session
	states  map[string]*state
	closed  bool
	queue   chan tick

	cancel    context.CancelFunc
	workers   sync.WaitGroup
	published atomic.Uint64
	dropped   atomic.Uint64
	gaps      atomic.Uint64
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
	slots, err := ratebudget.NewSlots(cfg.RegistrationLimit)
	if err != nil {
		return nil, fmt.Errorf("kisquote: %w", err)
	}
	for _, id := range cfg.Fixed {
		if _, err := slots.Reserve(id, 0); err != nil {
			return nil, fmt.Errorf("kisquote: reserve %s: %w", id, err)
		}
	}
	for i := range safetyReserve {
		if _, err := slots.Reserve("safety-"+strconv.Itoa(i+1), 0); err != nil {
			return nil, fmt.Errorf("kisquote: reserve safety margin: %w", err)
		}
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
// promoted symbols in priority order. Symbols that do not fit are reported and
// left unregistered; the fills path is never failed. It is called once, from
// the reader after the fills subscription is active.
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
		if err := l.Promote(ctx, symbol, len(symbols)-i); err != nil && l.cfg.Logger != nil {
			l.cfg.Logger.Warn("KIS 승격 종목 등록 실패", "symbol", symbol, "reason", err.Error())
		}
	}
}

// Promote registers one symbol at priority (higher keeps its slots). When the
// budget is full it removes the lowest-priority promoted symbol whose MinHold
// has elapsed, and only if that symbol's priority is below priority. Otherwise
// it returns a ratebudget budget_cut refusal and changes nothing.
func (l *Lane) Promote(ctx context.Context, symbol string, priority int) error {
	if !bookfeed.SymbolOK(symbol) {
		return errors.New("kisquote: invalid promoted symbol")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.promoteLocked(ctx, symbol, priority)
}

func (l *Lane) promoteLocked(ctx context.Context, symbol string, priority int) error {
	if l.closed {
		return errors.New("kisquote: lane closed")
	}
	if l.session == nil {
		return errors.New("kisquote: lane not attached")
	}
	if l.reservedSymbols[symbol] {
		return nil
	}
	if st, held := l.states[symbol]; held {
		st.priority = priority
		l.slots.Admit(tradeID(symbol), priority)
		l.slots.Admit(bookID(symbol), priority)
		return nil
	}
	if l.freeLocked() < registrationsPerSymbol {
		victim := l.victimLocked(priority)
		if victim == "" {
			return &ratebudget.Refusal{Code: ratebudget.CodeBudgetCut}
		}
		if err := l.dropLocked(ctx, victim); err != nil {
			return err
		}
	}
	if err := subscribeSymbol(ctx, l.session, symbol); err != nil {
		return err
	}
	if _, err := l.slots.Admit(tradeID(symbol), priority); err != nil {
		_ = unsubscribeSymbol(ctx, l.session, symbol)
		return err
	}
	if _, err := l.slots.Admit(bookID(symbol), priority); err != nil {
		l.slots.Release(tradeID(symbol))
		_ = unsubscribeSymbol(ctx, l.session, symbol)
		return err
	}
	l.states[symbol] = &state{symbol: symbol, priority: priority, admitted: l.now(), lastWS: l.now()}
	return nil
}

// Demote removes one promoted symbol if its MinHold has elapsed. A reserved
// symbol is never removed. It reports whether the symbol was removed.
func (l *Lane) Demote(ctx context.Context, symbol string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	st, held := l.states[symbol]
	if !held || l.closed {
		return false, nil
	}
	if l.now().Sub(st.admitted) < l.cfg.MinHold {
		return false, nil
	}
	if err := l.dropLocked(ctx, symbol); err != nil {
		return false, err
	}
	return true, nil
}

// Handle routes one realtime frame. It is called on the fills reader's
// goroutine, so it never blocks: parsed ticks go to a bounded queue and are
// dropped (counted) under backpressure.
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
		if st, held := l.states[t.symbol]; held {
			st.lastWS = t.received
		} else {
			l.mu.Unlock()
			continue
		}
		closed := l.closed
		l.mu.Unlock()
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

// Close stops the loops, drains the queue, and removes every promoted
// subscription from the session. The fills subscription is not touched.
func (l *Lane) Close() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	cancel, session := l.cancel, l.session
	symbols := make([]string, 0, len(l.states))
	for symbol := range l.states {
		symbols = append(symbols, symbol)
	}
	l.states = make(map[string]*state)
	l.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	l.workers.Wait()
	if l.cfg.Logger != nil {
		stats := l.Stats()
		l.cfg.Logger.Info("KIS 승격 시세 종료", "published", stats.Published, "dropped", stats.Dropped, "gaps", stats.Gaps)
	}
	if session == nil {
		return
	}
	ctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	for _, symbol := range symbols {
		_ = unsubscribeSymbol(ctx, session, symbol)
	}
	for _, symbol := range symbols {
		l.slots.Release(tradeID(symbol))
		l.slots.Release(bookID(symbol))
	}
}

func (l *Lane) freeLocked() int { return l.capacity - len(l.slots.Held()) }

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

func (l *Lane) dropLocked(ctx context.Context, symbol string) error {
	if _, held := l.states[symbol]; !held {
		return nil
	}
	delete(l.states, symbol)
	l.slots.Release(tradeID(symbol))
	l.slots.Release(bookID(symbol))
	if err := unsubscribeSymbol(ctx, l.session, symbol); err != nil && l.cfg.Logger != nil {
		l.cfg.Logger.Warn("KIS 승격 종목 해제 실패", "symbol", symbol, "reason", err.Error())
	}
	return nil
}

func (l *Lane) publishLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// Drain what is already queued, then stop. The queue is never
			// closed, so a late Handle send cannot panic.
			for {
				select {
				case t := <-l.queue:
					l.publishOne(ctx, t)
				default:
					return
				}
			}
		case t := <-l.queue:
			l.publishOne(ctx, t)
		}
	}
}

func (l *Lane) publishOne(ctx context.Context, t tick) {
	// The shutdown drain still writes: the queue is bounded, so this is a
	// short, finite flush rather than an unbounded wait.
	if err := l.publish(context.WithoutCancel(ctx), t); err != nil {
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
	stream := StreamPrefix + t.symbol
	return l.cfg.Redis.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
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
	sort.Strings(symbols)
	l.mu.Unlock()
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

func tradeID(symbol string) string { return book.TRTrade + "|" + symbol }
func bookID(symbol string) string  { return book.TRBook + "|" + symbol }

func subscribeSymbol(ctx context.Context, session Session, symbol string) error {
	if err := session.Subscribe(ctx, book.TRTrade, symbol); err != nil {
		return fmt.Errorf("kisquote: subscribe %s: %w", book.TRTrade, err)
	}
	if err := session.Subscribe(ctx, book.TRBook, symbol); err != nil {
		_ = session.Unsubscribe(ctx, book.TRTrade, symbol)
		return fmt.Errorf("kisquote: subscribe %s: %w", book.TRBook, err)
	}
	return nil
}

func unsubscribeSymbol(ctx context.Context, session Session, symbol string) error {
	tradeErr := session.Unsubscribe(ctx, book.TRTrade, symbol)
	bookErr := session.Unsubscribe(ctx, book.TRBook, symbol)
	if tradeErr != nil {
		return tradeErr
	}
	return bookErr
}
