package exec

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// Config holds the executor's knobs. The zero value sends nothing.
type Config struct {
	// SendEnabled must be set explicitly; when false the executor sends no
	// child request of any kind, cancels included. It still reconciles and
	// records fills, and finishes only as filled with no order working.
	SendEnabled bool
	// StepEvery is how long an order rests at one price before it moves one
	// tick toward the cap.
	StepEvery time.Duration
	// StaleAfter is the oldest book a new submit or amend may be priced from.
	StaleAfter time.Duration
}

// DefaultConfig steps every 5 s, refuses a book older than 2 s (the book
// recorder's stale bound), and leaves sending disabled.
func DefaultConfig() Config {
	return Config{StepEvery: 5 * time.Second, StaleAfter: 2 * time.Second}
}

// Gateway is the E1 intent gateway as the executor sees it.
type Gateway interface {
	// Send delivers one child request. An error means the outcome is unknown.
	Send(Child) (Outcome, error)
	// Query reports the intent's working order and broker-side filled
	// quantity, for reconciliation.
	Query(intentID string) (View, error)
}

// KillSwitch is the global kill: while engaged the executor sends no submit
// or amend, and cancels its working order (E1 keeps cancels allowed while
// submissions are blocked). With SendEnabled false the executor sends no
// cancel either: a working order is left to E1's own kill.
type KillSwitch struct{ engaged atomic.Bool }

// Engage turns the kill on; Release turns it off.
func (k *KillSwitch) Engage()  { k.engaged.Store(true) }
func (k *KillSwitch) Release() { k.engaged.Store(false) }

// Engaged reports the kill state. A nil switch counts as engaged.
func (k *KillSwitch) Engaged() bool { return k == nil || k.engaged.Load() }

// Executor works one intent. It is driven from one goroutine: OnBook, OnFill
// and Tick must not run concurrently.
type Executor struct {
	cfg   Config
	j     *Journal
	gw    Gateway
	kill  *KillSwitch
	state State
	top   *Top
	err   error
}

// ErrOverfill is returned by OnFill when ledger fills exceed the target.
// The fill is recorded; the executor sends nothing more but a cancel of its
// working order. The caller should alert an operator.
var ErrOverfill = errors.New("exec: ledger fills exceed the target")

// ErrHalted is returned once a journal write failed; the executor then does
// nothing more, because its disk state may lag what it did.
var ErrHalted = errors.New("exec: halted after a journal failure")

// New returns an executor resuming from state (as OpenJournal returned it).
func New(cfg Config, j *Journal, state State, gw Gateway, kill *KillSwitch) *Executor {
	return &Executor{cfg: cfg, j: j, gw: gw, kill: kill, state: state}
}

// State returns a copy of the current state.
func (e *Executor) State() State { return e.state.clone() }

// Start journals the intent. It fails if the journal already holds one.
func (e *Executor) Start(in Intent, nowNS int64) error {
	return e.commit(Record{Kind: RecStart, AtNS: nowNS, Intent: &in})
}

// OnBook records the latest touch. It is not journaled: after a restart the
// executor sends nothing until a fresh book arrives.
func (e *Executor) OnBook(t Top) { e.top = &t }

// OnFill records one ledger fill. A repeated fill id, or a fill for a key
// this intent never sent, changes nothing.
func (e *Executor) OnFill(f Fill) error {
	if !e.state.Started() {
		return nil
	}
	if _, dup := e.state.Fills[f.FillID]; dup {
		return nil
	}
	if _, ours := e.state.sentKey(f.Key); !ours {
		return nil
	}
	if err := e.commit(Record{Kind: RecFill, AtNS: f.AtNS, Fill: &f}); err != nil {
		return err
	}
	if over := e.state.Overfill(); over > 0 {
		return fmt.Errorf("%w: filled %d, target %d (fill %s recorded)", ErrOverfill, e.state.Filled, e.state.Target, f.FillID)
	}
	return nil
}

// maxStepsPerTick bounds one Tick; each step is a journaled record or a query.
const maxStepsPerTick = 8

// Tick lets the executor act at time nowNS: reconcile an unknown outcome,
// send at most what the state machine decides, or finish.
func (e *Executor) Tick(nowNS int64) error {
	for range maxStepsPerTick {
		if e.err != nil {
			return e.err
		}
		d := decide(e.state, inputs{now: nowNS, top: e.top, cfg: e.cfg, killed: e.kill.Engaged()})
		switch {
		case d.query:
			v, err := e.gw.Query(e.state.Intent.IntentID)
			if err != nil {
				return fmt.Errorf("exec: reconcile: %w", err) // still unresolved; nothing is sent
			}
			if err := e.commit(Record{Kind: RecReconcile, AtNS: nowNS, View: &v}); err != nil {
				return err
			}
		case d.rec != nil:
			if err := e.commit(*d.rec); err != nil {
				return err
			}
			if d.rec.Kind == RecSend {
				if err := e.send(*d.rec.Child, nowNS); err != nil {
					return err
				}
			}
		default:
			return nil
		}
	}
	return nil
}

// send calls the gateway for a child already journaled as in flight and
// journals the answer; a journal failure is returned (and halts). A transport
// error is journaled as uncertain.
func (e *Executor) send(c Child, nowNS int64) error {
	out, err := e.gw.Send(c)
	if err != nil {
		out = Outcome{Key: c.Key, Status: StatusUncertain, Reason: "transport: " + err.Error()}
	}
	if out.Key != c.Key {
		out = Outcome{Key: c.Key, Status: StatusUncertain, Reason: fmt.Sprintf("answer for key %q", out.Key)}
	}
	return e.commit(Record{Kind: RecOutcome, AtNS: nowNS, Outcome: &out})
}

// commit applies r, journals it, and only then makes it the current state.
// It refuses a submit or amend that E1 would refuse for caps: decide sizes
// every send to E1's room, so this is the backstop. The check lives here, not
// in Apply, because commit sees only new records, and a journal written
// before the executor mirrored E1 must still replay.
func (e *Executor) commit(r Record) error {
	if e.err != nil {
		return e.err
	}
	if c := r.Child; r.Kind == RecSend && c != nil && (c.Action == ActSubmit || c.Action == ActAmend) {
		if why := e.state.E1CapsRefusal(*c); why != "" {
			return fmt.Errorf("exec: %s qty %d price %d not sent: E1 would refuse: %s", c.Action, c.Qty, c.Price, why)
		}
	}
	next, err := Apply(e.state, r)
	if err != nil {
		return err
	}
	if err := e.j.append(r, next); err != nil {
		e.err = fmt.Errorf("%w: %v", ErrHalted, err)
		return e.err
	}
	e.state = next
	return nil
}
