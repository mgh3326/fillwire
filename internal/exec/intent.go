// Package exec is the E2 executor: it works one approved intent near the
// touch, inside the intent's bounds, and journals every transition before it
// acts. It reaches no broker itself; a Gateway (the E1 intent gateway, a fake
// in this package's tests) receives its child requests.
package exec

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"
)

// Intent mirrors auto_trader's ExecutionIntentV1 (app/schemas/execution_intent.py,
// ROB-1440) field for field, with the same JSON names.
type Intent struct {
	SchemaVersion    int       `json:"schema_version"`
	IntentID         string    `json:"intent_id"`
	Strategy         string    `json:"strategy"`
	Account          string    `json:"account"`
	Venue            string    `json:"venue"`
	Symbol           string    `json:"symbol"`
	Side             Side      `json:"side"`
	MaxQuantity      int64     `json:"max_quantity"`
	MaxNotionalKRW   int64     `json:"max_notional_krw"`
	LimitPriceKRW    int64     `json:"limit_price_krw"`
	MaxSlippageBps   int64     `json:"max_slippage_bps"`
	ExpiresAt        time.Time `json:"expires_at"`
	Inventory        Inventory `json:"inventory"`
	PolicyVersion    string    `json:"policy_version"`
	ModelVersion     *string   `json:"model_version"`
	AuthorizationRef string    `json:"authorization_ref"`
	// InstrumentType is not an E1 v1 field; v1 is KRX cash equity only, so an
	// absent value means InstrumentKRXEquity. It is here so a later options
	// leg can be told apart without changing the type.
	InstrumentType string `json:"instrument_type,omitempty"`
}

// Inventory mirrors E1's InventoryConstraint.
type Inventory struct {
	PositionAtAuthorization int64 `json:"position_at_authorization"`
	MinPositionAfter        int64 `json:"min_position_after"`
	MaxPositionAfter        int64 `json:"max_position_after"`
}

// Side is buy or sell.
type Side string

const (
	Buy  Side = "buy"
	Sell Side = "sell"
)

// InstrumentKRXEquity is the only instrument type v1 accepts.
const InstrumentKRXEquity = "krx_equity"

// E1 v1 bounds (app/schemas/execution_intent.py).
const (
	maxIntentNotionalKRW = 5_000_000
	maxIntentQuantity    = 1_000_000
	maxLimitPriceKRW     = 10_000_000
	maxSlippageBps       = 500
	maxPosition          = 1_000_000_000
)

var (
	uuidRe      = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	krxSymbolRe = regexp.MustCompile(`^[0-9][0-9A-Z]{5}$`)
	referenceRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)
)

// ErrIntent marks an intent this executor refuses to work.
var ErrIntent = errors.New("exec: invalid intent")

// ParseIntent decodes one E1 intent and refuses unknown fields, as E1 does.
func ParseIntent(data []byte) (Intent, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var in Intent
	if err := dec.Decode(&in); err != nil {
		return Intent{}, fmt.Errorf("%w: %v", ErrIntent, err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return Intent{}, fmt.Errorf("%w: trailing data", ErrIntent)
	}
	return in, in.Validate()
}

// Validate repeats E1's bounds and adds the executor's own: a working
// quantity must remain after the notional and inventory limits.
func (in Intent) Validate() error {
	bad := func(format string, a ...any) error {
		return fmt.Errorf("%w: "+format, append([]any{ErrIntent}, a...)...)
	}
	switch {
	case in.SchemaVersion != 1:
		return bad("schema_version %d", in.SchemaVersion)
	case !uuidRe.MatchString(in.IntentID):
		return bad("intent_id %q is not a lowercase UUID", in.IntentID)
	case in.Strategy == "" || len(in.Strategy) > 128:
		return bad("strategy length")
	case in.Account != "kis_mock":
		return bad("account %q", in.Account)
	case in.Venue != "krx":
		return bad("venue %q", in.Venue)
	case !krxSymbolRe.MatchString(in.Symbol):
		return bad("symbol %q", in.Symbol)
	case in.Side != Buy && in.Side != Sell:
		return bad("side %q", in.Side)
	case in.MaxQuantity < 1 || in.MaxQuantity > maxIntentQuantity:
		return bad("max_quantity %d", in.MaxQuantity)
	case in.MaxNotionalKRW < 1 || in.MaxNotionalKRW > maxIntentNotionalKRW:
		return bad("max_notional_krw %d", in.MaxNotionalKRW)
	case in.LimitPriceKRW < 1 || in.LimitPriceKRW > maxLimitPriceKRW:
		return bad("limit_price_krw %d", in.LimitPriceKRW)
	case in.MaxSlippageBps < 0 || in.MaxSlippageBps > maxSlippageBps:
		return bad("max_slippage_bps %d", in.MaxSlippageBps)
	case in.ExpiresAt.IsZero():
		return bad("expires_at missing")
	case !referenceRe.MatchString(in.PolicyVersion):
		return bad("policy_version %q", in.PolicyVersion)
	case in.ModelVersion != nil && !referenceRe.MatchString(*in.ModelVersion):
		return bad("model_version %q", *in.ModelVersion)
	case !referenceRe.MatchString(in.AuthorizationRef):
		return bad("authorization_ref %q", in.AuthorizationRef)
	case in.InstrumentType != "" && in.InstrumentType != InstrumentKRXEquity:
		return bad("instrument_type %q", in.InstrumentType)
	}
	inv := in.Inventory
	for _, v := range []int64{inv.PositionAtAuthorization, inv.MinPositionAfter, inv.MaxPositionAfter} {
		if v < 0 || v > maxPosition {
			return bad("inventory value %d", v)
		}
	}
	if inv.MinPositionAfter > inv.MaxPositionAfter {
		return bad("min_position_after exceeds max_position_after")
	}
	if in.Cap() < 1 {
		return bad("limit_price_krw %d has no valid tick price", in.LimitPriceKRW)
	}
	if in.TargetQty() < 1 {
		return bad("no quantity fits max_quantity, max_notional_krw and inventory")
	}
	return nil
}

// clone copies the one pointer field so no State aliases a caller's value.
func (in Intent) clone() Intent {
	if in.ModelVersion != nil {
		v := *in.ModelVersion
		in.ModelVersion = &v
	}
	return in
}

// Cap is the worst price the executor may ever send: limit_price_krw moved
// onto the tick grid in the side's favour. E1 would accept up to
// max_slippage_bps beyond the limit; the executor never uses that room.
func (in Intent) Cap() int64 {
	if in.Side == Buy {
		return FloorTick(in.LimitPriceKRW)
	}
	return CeilTick(in.LimitPriceKRW)
}

// TargetQty is the quantity the executor works: max_quantity, cut so the
// whole quantity at Cap stays within max_notional_krw and the inventory
// range. No buy is priced above Cap, so no buy can exceed either; a sell may
// be priced above Cap, so its price is also held to the notional room E1
// leaves (State.fitNewOrder).
func (in Intent) TargetQty() int64 {
	q := in.MaxQuantity
	if c := in.Cap(); c > 0 {
		q = min(q, in.MaxNotionalKRW/c)
	}
	inv := in.Inventory
	if in.Side == Buy {
		q = min(q, inv.MaxPositionAfter-inv.PositionAtAuthorization)
	} else {
		q = min(q, inv.PositionAtAuthorization-inv.MinPositionAfter)
	}
	return max(q, 0)
}

// worse reports whether price a is worse for the intent's side than b: higher
// for a buy, lower for a sell.
func (in Intent) worse(a, b int64) bool {
	if in.Side == Buy {
		return a > b
	}
	return a < b
}
