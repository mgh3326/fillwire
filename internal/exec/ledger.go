package exec

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// ErrAttribution stops execution when ledger evidence cannot be bound uniquely
// to a submit or amend recorded in this executor's journal.
var ErrAttribution = errors.New("exec: ledger fill attribution failed")

type orderDay struct{ number, day string }

func orderIdentity(number string, atNS int64) orderDay {
	number = strings.TrimLeft(strings.TrimSpace(number), "0")
	if number == "" {
		number = "0"
	}
	return orderDay{number, time.Unix(0, atNS).In(time.FixedZone("KST", 9*60*60)).Format("2006-01-02")}
}

// brokerKeys derives identities from the durable send/outcome pairs, including
// superseded orders. It deliberately does not change State or its digest, so
// journals written before ledger attribution retain their replay contract.
func (j *Journal) brokerKeys() (map[orderDay]string, error) {
	info, err := j.f.Stat()
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(io.NewSectionReader(j.f, 0, info.Size()))
	sends := map[string]Record{}
	keys := map[orderDay]string{}
	for {
		var line journalLine
		if err := dec.Decode(&line); err != nil {
			if err == io.EOF {
				return keys, nil
			}
			return nil, err
		}
		r := line.Rec
		if r.Kind == RecSend && r.Child != nil {
			sends[r.Child.Key] = r
		}
		if r.Kind != RecOutcome || r.Outcome == nil || r.Outcome.Status != StatusAccepted || strings.TrimSpace(r.Outcome.BrokerOrderID) == "" {
			continue
		}
		send, ok := sends[r.Outcome.Key]
		if !ok {
			return nil, ErrAttribution
		}
		if send.Child.Action == ActCancel {
			continue
		}
		id := orderIdentity(r.Outcome.BrokerOrderID, send.AtNS)
		if previous, exists := keys[id]; exists && previous != r.Outcome.Key {
			return nil, fmt.Errorf("%w: broker order has multiple journal keys", ErrAttribution)
		}
		keys[id] = r.Outcome.Key
	}
}

// OnLedgerFill binds a scoped ledger row to its journaled broker order. The
// caller must supply only this mock account, symbol and side's effective ledger
// rows, never websocket notices or aggregate reconcile estimates. AtNS is the
// fill time, not the poll time: KIS numbers are reused across trading days.
// An optional Key must agree with the journal; it cannot substitute for a
// missing broker order identity. Any attribution error latches the executor
// stopped, including on later Tick calls and after a fresh book arrives.
func (e *Executor) OnLedgerFill(brokerOrderID string, f Fill) error {
	if e.err != nil {
		return e.err
	}
	fail := func(why string) error {
		e.err = fmt.Errorf("%w: %s", ErrAttribution, why)
		return e.err
	}
	if !e.state.Started() || strings.TrimSpace(brokerOrderID) == "" || f.AtNS <= 0 || f.FillID == "" || f.Qty <= 0 || f.Price <= 0 {
		return fail("incomplete ledger evidence")
	}
	keys, err := e.j.brokerKeys()
	if err != nil {
		return fail(err.Error())
	}
	key, ok := keys[orderIdentity(brokerOrderID, f.AtNS)]
	if !ok {
		return fail("broker order is absent from the journal")
	}
	if f.Key != "" && f.Key != key {
		return fail("ledger key disagrees with journal broker order")
	}
	f.Key = key
	if previous, exists := e.state.Fills[f.FillID]; exists && previous != f {
		return fail("ledger fill id changed evidence")
	}
	return e.OnFill(f)
}
