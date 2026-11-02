package e1_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/mgh3326/fillwire/internal/exec"
	"github.com/mgh3326/fillwire/internal/exec/e1"
)

// The contract tests serve E1 answers that record_e1.py recorded from
// auto_trader's own IntentGateway and test fakes, for the child requests in
// e1_scenarios.json.

type recordedStep struct {
	Request              json.RawMessage `json:"request"`
	Response             json.RawMessage `json:"response"`
	ReconcileEmptyBefore bool            `json:"reconcile_empty_before"`
}

type recordedScenario struct {
	Name   string          `json:"name"`
	Kind   string          `json:"kind"`
	Intent json.RawMessage `json:"intent"`
	Steps  []recordedStep  `json:"steps"`
}

func recorded(t *testing.T) map[string]recordedScenario {
	t.Helper()
	data, err := os.ReadFile("testdata/e1_recorded.json")
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Commit    string             `json:"auto_trader_commit"`
		Scenarios []recordedScenario `json:"scenarios"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	if file.Commit == "" {
		t.Fatal("recording names no auto_trader commit")
	}
	out := map[string]recordedScenario{}
	for _, s := range file.Scenarios {
		out[s.Name] = s
	}
	return out
}

func (s recordedScenario) intent(t *testing.T) exec.Intent {
	t.Helper()
	in, err := exec.ParseIntent(s.Intent)
	if err != nil {
		t.Fatalf("%s: E1 accepted this intent; the executor refuses it: %v", s.Name, err)
	}
	return in
}

func canonical(t *testing.T, raw []byte) string {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("%s: %v", raw, err)
	}
	b, _ := json.Marshal(v) // map keys sort
	return string(b)
}

func child(t *testing.T, raw []byte) exec.Child {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var c exec.Child
	if err := dec.Decode(&c); err != nil {
		t.Fatal(err)
	}
	return c
}

// e1Replay serves a scenario's recorded answers in order and fails the test
// on any request that is not the next recorded one.
type e1Replay struct {
	*httptest.Server
	t     *testing.T
	steps []recordedStep
	mu    sync.Mutex
	next  int
}

func newReplay(t *testing.T, s recordedScenario) *e1Replay {
	r := &e1Replay{t: t, steps: s.Steps}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.next >= len(r.steps) {
			t.Errorf("%s: unrecorded request %d: %s", s.Name, r.next+1, body)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		st := r.steps[r.next]
		r.next++
		if got, want := canonical(t, body), canonical(t, st.Request); got != want {
			t.Errorf("%s: request %d\n got %s\nwant %s", s.Name, r.next, got, want)
		}
		if got, want := req.Header.Get(e1.ConfirmHeader), child(t, st.Request).Key; got != want {
			t.Errorf("%s: confirm header %q, want %q", s.Name, got, want)
		}
		w.Write(st.Response)
	}))
	t.Cleanup(r.Close)
	return r
}

func (r *e1Replay) consumed() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.next
}

func book(r *runner, sec float64) {
	r.e.OnBook(exec.Top{Bid: 70_000, Ask: 70_100, AtNS: at(sec), Synced: true})
}

func tick(t *testing.T, r *runner, sec float64) {
	t.Helper()
	book(r, sec)
	if err := r.e.Tick(at(sec)); err != nil {
		t.Fatalf("tick %v: %v", sec, err)
	}
}

func TestContractExecutor(t *testing.T) {
	all := recorded(t)
	cases := []struct {
		name   string
		drive  func(t *testing.T, r *runner, in exec.Intent)
		done   string
		filled int64
	}{
		{"accept_then_local_kill_cancel", func(t *testing.T, r *runner, in exec.Intent) {
			tick(t, r, 0)
			if st := r.e.State(); st.Order == nil || st.Filled != 0 {
				t.Fatalf("after accept: order %+v filled %d", st.Order, st.Filled) // accepted is not a fill
			}
			r.kill.Engage()
			tick(t, r, 1)
		}, exec.DoneKilled, 0},
		{"partial_fill_amend_to_cap_cancel", func(t *testing.T, r *runner, in exec.Intent) {
			tick(t, r, 0)
			if err := r.e.OnFill(exec.Fill{FillID: "ledger-1", Key: in.IntentID + ":1", Qty: 4, Price: 70_000, AtNS: at(1)}); err != nil {
				t.Fatal(err)
			}
			for _, s := range []float64{5, 10, 15, 20} {
				tick(t, r, s)
			}
		}, exec.DoneCap, 4},
		{"refused_notional_cap", func(t *testing.T, r *runner, in exec.Intent) { tick(t, r, 0) }, exec.DoneRefused, 0},
		{"refused_authorization_expired", func(t *testing.T, r *runner, in exec.Intent) { tick(t, r, 0) }, exec.DoneRefused, 0},
		{"refused_submissions_killed_then_cancel", func(t *testing.T, r *runner, in exec.Intent) {
			tick(t, r, 0)
			tick(t, r, 5) // amend refused by E1's kill, then the cancel E1 still allows
		}, exec.DoneRefused, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, ok := all[c.name]
			if !ok || s.Kind != "executor" {
				t.Fatalf("no executor recording %q", c.name)
			}
			in := s.intent(t)
			srv := newReplay(t, s)
			r := newRunner(t, sendConfig(srv.URL), in, e1.Options{Token: func() (string, error) { return "test-token", nil }})
			c.drive(t, r, in)
			st := r.e.State()
			if srv.consumed() != len(s.Steps) {
				t.Fatalf("sent %d of %d recorded requests", srv.consumed(), len(s.Steps))
			}
			if st.Done != c.done || st.Filled != c.filled || st.Order != nil || st.InFlight != nil {
				t.Fatalf("done %q filled %d order %+v in flight %+v; want %q %d", st.Done, st.Filled, st.Order, st.InFlight, c.done, c.filled)
			}
		})
	}
	for name, s := range all {
		if s.Kind == "executor" && !hasCase(name, len(cases), func(i int) string { return cases[i].name }) {
			t.Errorf("recording %q has no executor case", name)
		}
	}
}

func hasCase(name string, n int, at func(int) string) bool {
	for i := range n {
		if at(i) == name {
			return true
		}
	}
	return false
}

// TestContractCapsMatchE1 applies the children E1 decided through exec.Apply
// and checks that the executor allows exactly the children E1 allowed, and
// refuses the rest for the same cap.
func TestContractCapsMatchE1(t *testing.T) {
	n := 0
	for _, s := range recorded(t) {
		if s.Kind != "caps" {
			continue
		}
		n++
		t.Run(s.Name, func(t *testing.T) {
			in := s.intent(t)
			st, err := exec.Apply(exec.State{}, exec.Record{Kind: exec.RecStart, Intent: &in})
			if err != nil {
				t.Fatal(err)
			}
			for i, step := range s.Steps {
				if step.ReconcileEmptyBefore {
					if st, err = exec.Apply(st, exec.Record{Kind: exec.RecReconcile, View: &exec.View{}}); err != nil {
						t.Fatalf("step %d reconcile: %v", i+1, err)
					}
				}
				c := child(t, step.Request)
				o, err := e1.ParseOutcome(step.Response)
				if err != nil {
					t.Fatal(err)
				}
				next, err := exec.Apply(st, exec.Record{Kind: exec.RecSend, Child: &c})
				if o.Status == exec.StatusRefused {
					if err == nil || !strings.Contains(err.Error(), *o.Reason) {
						t.Fatalf("step %d: E1 refused %s; Apply: %v", i+1, *o.Reason, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("step %d: E1 %s; Apply refused: %v", i+1, o.Status, err)
				}
				out := o.ToExec(c)
				if st, err = exec.Apply(next, exec.Record{Kind: exec.RecOutcome, Outcome: &out}); err != nil {
					t.Fatalf("step %d outcome: %v", i+1, err)
				}
			}
		})
	}
	if n < 4 {
		t.Fatalf("%d caps recordings", n)
	}
}

// TestContractOutcomeMapping maps each recorded E1 outcome kind onto the
// executor: the status the client reports, and the state Apply reaches when
// that answer arrives for a child of the same action.
func TestContractOutcomeMapping(t *testing.T) {
	cases := map[string]struct {
		status string
		effect string // halt: submit/amend refused; hold: unresolved; track: order tracked
	}{
		"gateway_disabled":              {exec.StatusRefused, "halt"},
		"confirm_required":              {exec.StatusRefused, "halt"},
		"intent_not_found":              {exec.StatusRefused, "halt"},
		"account_not_mock":              {exec.StatusRefused, "halt"},
		"forwarder_unavailable":         {exec.StatusRefused, "halt"},
		"price_outside_bound":           {exec.StatusRefused, "halt"},
		"locked_envelope_cap_exceeded":  {exec.StatusRefused, "halt"},
		"quantity_cap_exceeded":         {exec.StatusRefused, "halt"},
		"notional_cap_exceeded":         {exec.StatusRefused, "halt"},
		"inventory_constraint_exceeded": {exec.StatusRefused, "halt"},
		"target_not_found_amend":        {exec.StatusRefused, "halt"},
		"target_not_found_cancel":       {exec.StatusRefused, "hold"},
		"target_not_working":            {exec.StatusRefused, "halt"},
		"target_superseded":             {exec.StatusRefused, "hold"},
		"uncertain_broker_failure":      {exec.StatusUncertain, "hold"},
		"uncertain_broker_raised":       {exec.StatusUncertain, "hold"},
		"sending_outcome_not_recorded":  {exec.StatusSending, "hold"},
		"conflict_key_reused":           {exec.StatusConflict, "hold"},
		"replayed_accepted":             {exec.StatusAccepted, "track"},
	}
	all := recorded(t)
	for name, s := range all {
		if s.Kind == "mapping" {
			if _, ok := cases[name]; !ok {
				t.Errorf("recording %q has no mapping case", name)
			}
		}
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			s, ok := all[name]
			if !ok {
				t.Fatalf("no recording %q", name)
			}
			last := s.Steps[len(s.Steps)-1]
			o, err := e1.ParseOutcome(last.Response)
			if err != nil {
				t.Fatal(err)
			}
			c := child(t, last.Request)
			out := o.ToExec(c)
			if out.Status != want.status || out.Key != c.Key {
				t.Fatalf("status %q key %q, want %q", out.Status, out.Key, want.status)
			}
			st := inFlight(t, fixtureIntent(t), c.Action)
			out.Key = st.InFlight.Key
			next, err := exec.Apply(st, exec.Record{Kind: exec.RecOutcome, Outcome: &out})
			if err != nil {
				t.Fatal(err)
			}
			var got string
			switch {
			case next.Halt == exec.DoneRefused && next.InFlight == nil:
				got = "halt"
			case next.InFlight != nil && next.InFlightStatus == exec.StatusUncertain:
				got = "hold"
			case next.InFlight == nil && next.Order != nil && next.Filled == 0:
				got = "track"
			}
			if got != want.effect {
				t.Fatalf("effect %q, want %q (state %+v)", got, want.effect, next)
			}
		})
	}
}

// inFlight is a state whose in-flight child has the given action: a submit,
// or an amend or cancel of an accepted submit.
func inFlight(t *testing.T, in exec.Intent, action string) exec.State {
	t.Helper()
	apply := func(st exec.State, r exec.Record) exec.State {
		next, err := exec.Apply(st, r)
		if err != nil {
			t.Fatal(err)
		}
		return next
	}
	st := apply(exec.State{}, exec.Record{Kind: exec.RecStart, Intent: &in})
	k1 := in.IntentID + ":1"
	sub := exec.Child{Action: exec.ActSubmit, IntentID: in.IntentID, Key: k1, Qty: 10, Price: 70_000}
	st = apply(st, exec.Record{Kind: exec.RecSend, Child: &sub})
	if action == exec.ActSubmit {
		return st
	}
	st = apply(st, exec.Record{Kind: exec.RecOutcome, Outcome: &exec.Outcome{Key: k1, Status: exec.StatusAccepted, BrokerOrderID: "1001"}})
	c := exec.Child{Action: action, IntentID: in.IntentID, Key: in.IntentID + ":2", TargetKey: k1}
	if action == exec.ActAmend {
		c.Qty, c.Price = 10, 70_100
	}
	return apply(st, exec.Record{Kind: exec.RecSend, Child: &c})
}
