package exec

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/kst"
)

// ErrAttribution stops execution when ledger evidence cannot be bound uniquely
// to a submit or amend recorded in this executor's journal.
var ErrAttribution = errors.New("exec: ledger fill attribution failed")

type orderDay struct{ number, day string }

// NormalizeOrderNumber is the one comparison form of a KIS broker order
// number (ODNO): surrounding space and leading zeros removed, so "0000123",
// " 123" and "123" are the same order. A blank or all-zero number normalizes
// to "", which no caller accepts as an order identity.
func NormalizeOrderNumber(number string) string {
	return strings.TrimLeft(strings.TrimSpace(number), "0")
}

func orderIdentity(number string, atNS int64) orderDay {
	return orderDay{NormalizeOrderNumber(number), time.Unix(0, atNS).In(kst.Zone).Format("2006-01-02")}
}

// brokerKeys derives identities from the durable send/outcome pairs, including
// superseded orders. It deliberately does not change State or its digest, so
// journals written before ledger attribution retain their replay contract.
func (j *Journal) brokerKeys() (map[orderDay]string, error) {
	info, err := j.f.Stat()
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.NewSectionReader(j.f, 0, info.Size()))
	if err != nil {
		return nil, err
	}
	sends := map[string]Record{}
	keys := map[orderDay]string{}
	_, _, err = eachLine(data, func(_ uint64, line journalLine) error {
		r := line.Rec
		if r.Kind == RecSend && r.Child != nil {
			sends[r.Child.Key] = r
		}
		if r.Kind != RecOutcome || r.Outcome == nil || r.Outcome.Status != StatusAccepted || strings.TrimSpace(r.Outcome.BrokerOrderID) == "" {
			return nil
		}
		send, ok := sends[r.Outcome.Key]
		if !ok {
			return ErrAttribution
		}
		if send.Child.Action == ActCancel {
			return nil
		}
		// An accepted order whose number normalizes to nothing (all zeros) can
		// never be matched to the ledger or read back from the broker. Skipping
		// it would drop the order from BrokerOrders and let completion pass
		// without checking its fills, so refuse instead.
		if NormalizeOrderNumber(r.Outcome.BrokerOrderID) == "" {
			return fmt.Errorf("%w: accepted order has an all-zero broker order number", ErrAttribution)
		}
		id := orderIdentity(r.Outcome.BrokerOrderID, send.AtNS)
		if previous, exists := keys[id]; exists && previous != r.Outcome.Key {
			return fmt.Errorf("%w: broker order has multiple journal keys", ErrAttribution)
		}
		keys[id] = r.Outcome.Key
		return nil
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
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
	if !e.state.Started() || NormalizeOrderNumber(brokerOrderID) == "" || f.AtNS <= 0 || f.FillID == "" || f.Qty <= 0 || f.Price <= 0 {
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

// BrokerOrder is one accepted submit/amend identity projected from this journal.
type BrokerOrder struct {
	BrokerOrderID, Key, TradingDay string
	Qty                            int64
}

func (e *Executor) BrokerOrders() ([]BrokerOrder, error) {
	keys, err := e.j.brokerKeys()
	if err != nil {
		return nil, err
	}
	out := make([]BrokerOrder, 0, len(keys))
	for id, key := range keys {
		child, ok := e.state.sentKey(key)
		if !ok {
			return nil, ErrAttribution
		}
		out = append(out, BrokerOrder{BrokerOrderID: id.number, Key: key, TradingDay: id.day, Qty: child.Qty})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}
