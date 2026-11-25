// Package kisfutures collects 1-minute bars of KRX index futures from the KIS
// realtime trade streams, day and night, for the front and next contract of
// KOSPI200 futures (F200) and mini KOSPI200 futures (MF200).
//
// The lane never dials a websocket. It adds subscriptions on the session
// fillwire already owns, and the registrations it may use are reserved from
// that session's subscription budget (internal/ratebudget) when it is built:
// two transactions (day H0IFCNT0, night H0MFCNT0) for each of four contracts.
// A roll removes the old contract before it adds the new one, so the lane
// never holds more registrations than it reserved.
//
// Contracts come from the public KIS futures master, never from guessed codes.
// Contract month, roll_state and session_date follow auto_trader's Python
// rules (contracts.go, session.go).
//
// The lane is read-only: it subscribes trade streams and writes closed bars to
// Redis Streams or a JSONL file. It places no orders and posts to no ledger or
// research ingest.
package kisfutures

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/mgh3326/fillwire/internal/ratebudget"
	"github.com/mgh3326/go-kis/kis/ws"
)

// Registrations is the number of session registrations the lane reserves:
// day and night trade streams for the front and next contract of two products.
const Registrations = 8

const (
	// subscribeTimeout bounds one Subscribe or Unsubscribe round trip, and the
	// whole removal pass at shutdown. ws.Conn has no ACK deadline of its own.
	subscribeTimeout = 2 * time.Second
	subscribeRetry   = 30 * time.Second
	masterRetry      = time.Minute
	loopInterval     = time.Second
	writeTimeout     = 2 * time.Second
	drainTimeout     = 2 * time.Second
	// maxPending bounds the closed bars held while the sink fails.
	maxPending = 2000
)

// Session is one KIS websocket connection. The lane only subscribes and
// unsubscribes on it; the fills reader owns dialing and the event channel.
type Session interface {
	Subscribe(ctx context.Context, tr, key string) error
	Unsubscribe(ctx context.Context, tr, key string) error
}

// Config is the whole lane configuration.
type Config struct {
	// Slots is the session's subscription budget, with the fixed consumers
	// already reserved and no promoted symbol admitted yet.
	Slots    *ratebudget.Slots
	Calendar *Calendar
	Master   MasterSource
	Sink     Sink // owned by the lane from a successful New on
	Now      func() time.Time
	Interval time.Duration // pacing of both loops, default 1 s
	Logger   *slog.Logger
}

// Stats is the process-local counter set logged at shutdown.
type Stats struct {
	Trades      uint64 // trades put into a bar
	Bars        uint64 // bars written to the sink
	Invalid     uint64 // unreadable frames and records
	UnknownCode uint64 // trades of a code no resolved contract has
	Duplicate   uint64
	Late        uint64 // trades of a minute already closed
	NoRollState uint64 // bars of a contract beyond the next
	Dropped     uint64 // closed bars the sink never took
}

type subscription struct{ tr, code string }

// Lane is a started futures lane. The zero value is not usable.
type Lane struct {
	cfg   Config
	now   func() time.Time
	slots []string

	// mu guards the builder, the contract table, the counters and closed. It
	// is never held across network or sink I/O, so Handle cannot stall the
	// fills reader.
	mu      sync.Mutex
	builder *builder
	known   map[string]Contract
	stats   Stats
	closed  bool

	session Session
	cancel  context.CancelFunc
	workers sync.WaitGroup

	// Owned by the subscription loop (and by Close after it stopped).
	date       time.Time
	rows       []MasterRow
	fetchDue   bool
	nextFetch  time.Time
	desired    []subscription
	registered map[subscription]bool // value: the server confirmed it
	retryAt    map[subscription]time.Time

	// Owned by the flush loop (and by Close after it stopped).
	pending     []Bar
	sinkFailing bool
}

// New reserves the lane's registrations. It dials nothing and reads nothing:
// Attach binds the lane to the session the fills reader already has. A budget
// without room refuses with a ratebudget budget_cut error and reserves
// nothing.
func New(cfg Config) (*Lane, error) {
	if cfg.Slots == nil || cfg.Calendar == nil || cfg.Master == nil || cfg.Sink == nil {
		return nil, errors.New("kisfutures: slots, calendar, master source and sink are required")
	}
	ids := make([]string, 0, Registrations)
	for i := range Registrations {
		id := "futures-" + strconv.Itoa(i+1)
		demoted, err := cfg.Slots.Reserve(id, 0)
		if err == nil {
			ids = append(ids, id)
			if demoted != "" {
				err = fmt.Errorf("the budget had already admitted %q", demoted)
			}
		}
		if err != nil {
			for _, held := range ids {
				cfg.Slots.Release(held)
			}
			return nil, fmt.Errorf("kisfutures: reserve %d registrations: %w", Registrations, err)
		}
	}
	if cfg.Interval <= 0 {
		cfg.Interval = loopInterval
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Lane{
		cfg: cfg, now: now, slots: ids,
		builder:    newBuilder(cfg.Calendar),
		known:      map[string]Contract{},
		registered: map[subscription]bool{},
		retryAt:    map[subscription]time.Time{},
	}, nil
}

// Attach binds the lane to the live session and starts its loops. It returns
// at once; the session stays valid across reconnects.
func (l *Lane) Attach(ctx context.Context, session Session) {
	if session == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.session != nil || l.closed {
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	l.session, l.cancel = session, cancel
	l.workers.Add(2)
	go l.loop(runCtx, l.reconcile)
	go l.loop(runCtx, func(ctx context.Context) { l.flush(ctx) })
}

func (l *Lane) loop(ctx context.Context, step func(context.Context)) {
	defer l.workers.Done()
	ticker := time.NewTicker(l.cfg.Interval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		step(ctx)
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
}

// Handle takes one realtime frame. It is called on the fills reader's
// goroutine and does no I/O.
func (l *Lane) Handle(ev ws.Event) {
	if !IsTradeTR(ev.TR) {
		return
	}
	received := ev.ReceivedAt
	if received.IsZero() {
		received = l.now()
	}
	trades, invalid, err := ParseFrame(ev.Raw, received)
	if err != nil {
		invalid = 1
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	if invalid > 0 && l.stats.Invalid == 0 {
		l.warn("KIS 선물 체결 프레임을 읽지 못해 버립니다", "tr", ev.TR)
	}
	l.stats.Invalid += uint64(invalid)
	for _, trade := range trades {
		contract, ok := l.known[trade.Code]
		if !ok {
			if l.stats.UnknownCode == 0 {
				l.warn("KIS 선물 체결의 종목코드가 마스터의 계약과 다릅니다", "tr", ev.TR, "code", trade.Code)
			}
			l.stats.UnknownCode++
			continue
		}
		switch l.builder.add(contract, trade) {
		case added:
			l.stats.Trades++
		case duplicate:
			l.stats.Duplicate++
		case late:
			if l.stats.Late == 0 {
				l.warn("닫힌 분의 KIS 선물 체결이 늦게 도착해 버립니다", "symbol", contract.Symbol)
			}
			l.stats.Late++
		}
	}
}

// Stats returns the process-local counters.
func (l *Lane) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stats
}

// reconcile resolves the contracts of the current contract date and brings the
// session's registrations to them. One pass runs at a time.
func (l *Lane) reconcile(ctx context.Context) {
	l.resolve(ctx)
	desired := make(map[subscription]bool, len(l.desired))
	for _, sub := range l.desired {
		desired[sub] = true
	}
	// Removals first: an old contract frees its registration before a new one
	// takes it.
	for sub := range l.registered {
		if desired[sub] || ctx.Err() != nil || l.now().Before(l.retryAt[sub]) {
			continue
		}
		if err := l.unsubscribe(ctx, sub); err != nil {
			l.retryAt[sub] = l.now().Add(subscribeRetry)
			l.warn("KIS 선물 구독 해제 실패", "tr", sub.tr, "code", sub.code, "reason", err.Error())
			continue
		}
		delete(l.registered, sub)
		delete(l.retryAt, sub)
	}
	for _, sub := range l.desired {
		confirmed, held := l.registered[sub]
		if confirmed || ctx.Err() != nil || l.now().Before(l.retryAt[sub]) {
			continue
		}
		if !held && len(l.registered) >= len(l.slots) {
			continue // a removal still occupies the reserved registration
		}
		l.subscribe(ctx, sub)
	}
}

func (l *Lane) resolve(ctx context.Context) {
	now := l.now()
	date := l.cfg.Calendar.ContractDate(now)
	changed := !date.Equal(l.date)
	if changed {
		l.date, l.fetchDue, l.nextFetch = date, true, now
		if !l.cfg.Calendar.Covers(date) {
			l.warn("선물 달력이 오늘을 포함하지 않아 세션이 UNKNOWN 으로 기록됩니다", "date", date.Format(time.DateOnly))
		}
	}
	fetched := false
	if l.fetchDue && !now.Before(l.nextFetch) {
		fetchCtx, cancel := context.WithTimeout(ctx, masterTimeout)
		rows, err := l.cfg.Master.Master(fetchCtx)
		cancel()
		if err != nil {
			l.nextFetch = l.now().Add(masterRetry)
			if ctx.Err() == nil {
				l.warn("KIS 선물 마스터를 읽지 못했습니다", "reason", err.Error())
			}
		} else {
			l.rows, l.fetchDue, fetched = rows, false, true
		}
	}
	// A failed fetch keeps resolving from the rows already read: they are
	// still master rows, and a roll must not wait for the download.
	if l.rows == nil || !(changed || fetched) {
		return
	}
	contracts, unresolved, err := ResolveFrontNext(l.rows, date)
	if err != nil {
		l.warn("KIS 선물 계약을 정하지 못했습니다", "reason", err.Error())
		return
	}
	for _, product := range unresolved {
		l.warn("마스터에 살아 있는 선물 계약이 없습니다", "product", product)
	}
	desired := make([]subscription, 0, len(contracts)*len(tradeTRs))
	symbols := make([]string, 0, len(contracts))
	l.mu.Lock()
	for _, contract := range contracts {
		l.known[contract.ShortCode] = contract
		symbols = append(symbols, contract.Symbol+"="+contract.ShortCode)
		for _, tr := range tradeTRs {
			desired = append(desired, subscription{tr, contract.ShortCode})
		}
	}
	l.mu.Unlock()
	if !slices.Equal(desired, l.desired) && l.cfg.Logger != nil {
		l.cfg.Logger.Info("KIS 선물 구독 계약", "contract_date", date.Format(time.DateOnly), "contracts", symbols)
	}
	l.desired = desired
}

// subscribe registers sub. A failed Subscribe may have left the registration
// active or in the client's restore list, so it is removed again; when that
// also fails the registration stays counted against the budget.
func (l *Lane) subscribe(ctx context.Context, sub subscription) {
	callCtx, cancel := context.WithTimeout(ctx, subscribeTimeout)
	err := l.session.Subscribe(callCtx, sub.tr, sub.code)
	cancel()
	if err == nil {
		l.registered[sub] = true
		delete(l.retryAt, sub)
		return
	}
	l.retryAt[sub] = l.now().Add(subscribeRetry)
	l.warn("KIS 선물 구독 실패", "tr", sub.tr, "code", sub.code, "reason", err.Error())
	if rollbackErr := l.unsubscribe(context.WithoutCancel(ctx), sub); rollbackErr != nil {
		l.registered[sub] = false
		return
	}
	delete(l.registered, sub)
}

// unsubscribe removes one registration. A server that refuses the removal
// answers that it holds no such registration (for example after a reconnect),
// so only a request without an answer is an error.
func (l *Lane) unsubscribe(ctx context.Context, sub subscription) error {
	callCtx, cancel := context.WithTimeout(ctx, subscribeTimeout)
	defer cancel()
	err := l.session.Unsubscribe(callCtx, sub.tr, sub.code)
	var refused *ws.SubscribeError
	if errors.As(err, &refused) {
		return nil
	}
	return err
}

// flush closes the bars that are due and writes them, oldest first. A failed
// write keeps the bar for the next pass.
func (l *Lane) flush(ctx context.Context) {
	l.mu.Lock()
	bars, skipped := l.builder.closed(l.now())
	l.stats.NoRollState += uint64(skipped)
	l.mu.Unlock()
	l.pending = append(l.pending, bars...)
	if over := len(l.pending) - maxPending; over > 0 {
		l.pending = l.pending[over:]
		l.count(func(s *Stats) { s.Dropped += uint64(over) })
	}
	for len(l.pending) > 0 && ctx.Err() == nil {
		writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
		err := l.cfg.Sink.Write(writeCtx, l.pending[0])
		cancel()
		if err != nil {
			if !l.sinkFailing {
				l.warn("KIS 선물 분봉 기록 실패, 다음에 다시 씁니다", "reason", err.Error())
			}
			l.sinkFailing = true
			return
		}
		l.sinkFailing = false
		l.pending = l.pending[1:]
		l.count(func(s *Stats) { s.Bars++ })
	}
}

// Close stops the loops, writes the bars that are already closed for at most
// drainTimeout, removes the lane's registrations and frees its slots. A bar
// still forming is discarded: it is never written early.
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
	l.workers.Wait()

	drainCtx, stopDrain := context.WithTimeout(context.Background(), drainTimeout)
	l.flush(drainCtx)
	stopDrain()
	l.count(func(s *Stats) { s.Dropped += uint64(len(l.pending)) })
	l.pending = nil
	if err := l.cfg.Sink.Close(); err != nil {
		l.warn("KIS 선물 분봉 출력 닫기 실패", "reason", err.Error())
	}
	if l.cfg.Logger != nil {
		s := l.Stats()
		l.cfg.Logger.Info("KIS 선물 분봉 종료", "trades", s.Trades, "bars", s.Bars, "invalid", s.Invalid,
			"unknown_code", s.UnknownCode, "duplicate", s.Duplicate, "late", s.Late,
			"no_roll_state", s.NoRollState, "dropped", s.Dropped)
	}

	removeCtx, stopRemove := context.WithTimeout(context.Background(), subscribeTimeout)
	defer stopRemove()
	for sub := range l.registered {
		if err := l.unsubscribe(removeCtx, sub); err != nil {
			l.warn("KIS 선물 구독 해제 실패", "tr", sub.tr, "code", sub.code, "reason", err.Error())
		}
	}
	clear(l.registered)
	for _, id := range l.slots {
		l.cfg.Slots.Release(id)
	}
}

func (l *Lane) count(update func(*Stats)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	update(&l.stats)
}

func (l *Lane) warn(message string, args ...any) {
	if l.cfg.Logger != nil {
		l.cfg.Logger.Warn(message, args...)
	}
}
