package upbit

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/mgh3326/fillwire/internal/jsonx"
)

// Local identity values. The Upbit wire contract (account name, child field
// names) is being designed in auto_trader hk 5438; until it lands these two
// strings are the shadow model's own, and the child requests carrying them are
// printed by cmd/execupbit and never sent.
const (
	VenueUpbit    = "upbit"
	AccountShadow = "upbit_shadow"
)

// Side is buy or sell.
type Side string

const (
	Buy  Side = "buy"
	Sell Side = "sell"
)

// Limits this model adds to the intent. max_notional_krw mirrors E1's own
// bound; the quantity bound keeps every scaled value inside int64.
const (
	maxIntentNotionalKRW = 5_000_000
	maxIntentUnits       = 100_000_000_000_000 // 1,000,000 base asset at 8 decimals
	maxLimitPriceKRW     = 10_000_000_000
	maxSlippageBps       = 500
	maxRequotes          = 100
)

var (
	uuidRe   = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	marketRe = regexp.MustCompile(`^KRW-[A-Z0-9]{2,20}$`)
)

// ErrIntent marks an intent this shadow model refuses to work.
var ErrIntent = errors.New("upbit: invalid intent")

// Intent is the shadow model's intent. It is deliberately not auto_trader's
// ExecutionIntentV1: this venue sizes orders in a decimal base-asset quantity
// and prices them on Upbit's KRW tick grid.
type Intent struct {
	SchemaVersion  int       `json:"schema_version"`
	IntentID       string    `json:"intent_id"`
	Venue          string    `json:"venue"`
	Account        string    `json:"account"`
	Market         string    `json:"market"`
	Side           Side      `json:"side"`
	MaxQuantity    Qty       `json:"max_quantity"`
	MaxNotionalKRW int64     `json:"max_notional_krw"`
	LimitPriceKRW  int64     `json:"limit_price_krw"`
	MaxSlippageBps int64     `json:"max_slippage_bps"`
	MaxRequotes    int       `json:"max_requotes"`
	ExpiresAt      time.Time `json:"expires_at"`
}

// ParseIntent decodes one intent and refuses unknown fields.
func ParseIntent(data []byte) (Intent, error) {
	var in Intent
	if err := jsonx.DecodeStrict(data, &in); err != nil {
		return Intent{}, fmt.Errorf("%w: %v", ErrIntent, err)
	}
	if err := in.Validate(); err != nil {
		return Intent{}, err
	}
	return in, nil
}

// Validate checks the intent and the policy bounds derived from it.
func (in Intent) Validate() error {
	bad := func(format string, a ...any) error {
		return fmt.Errorf("%w: "+format, append([]any{ErrIntent}, a...)...)
	}
	switch {
	case in.SchemaVersion != 1:
		return bad("schema_version %d", in.SchemaVersion)
	case !uuidRe.MatchString(in.IntentID):
		return bad("intent_id %q is not a lowercase UUID", in.IntentID)
	case in.Venue != VenueUpbit:
		return bad("venue %q", in.Venue)
	case in.Account != AccountShadow:
		return bad("account %q", in.Account)
	case !marketRe.MatchString(in.Market) || !ValidMarket(in.Market):
		return bad("market %q", in.Market)
	case in.Side != Buy && in.Side != Sell:
		return bad("side %q", in.Side)
	case in.MaxQuantity.IsNegative() || in.MaxQuantity.IsZero() || in.MaxQuantity.units > maxIntentUnits:
		return bad("max_quantity %q", in.MaxQuantity)
	case in.MaxNotionalKRW < 1 || in.MaxNotionalKRW > maxIntentNotionalKRW:
		return bad("max_notional_krw %d", in.MaxNotionalKRW)
	case in.LimitPriceKRW < minPriceKRW || in.LimitPriceKRW > maxLimitPriceKRW:
		return bad("limit_price_krw %d", in.LimitPriceKRW)
	case TickAt(in.LimitPriceKRW) == 0 || in.LimitPriceKRW%TickAt(in.LimitPriceKRW) != 0:
		return bad("limit_price_krw %d is not on the KRW tick grid", in.LimitPriceKRW)
	case in.MaxSlippageBps < 0 || in.MaxSlippageBps > maxSlippageBps:
		return bad("max_slippage_bps %d", in.MaxSlippageBps)
	case in.MaxRequotes < 0 || in.MaxRequotes > maxRequotes:
		return bad("max_requotes %d", in.MaxRequotes)
	case in.ExpiresAt.IsZero():
		return bad("expires_at missing")
	}
	low, high := in.Band()
	if low < minPriceKRW || low > high {
		return bad("price band [%d, %d] is empty or below the tick grid", low, high)
	}
	if in.Target().IsZero() {
		return bad("no quantity fits max_quantity and max_notional_krw")
	}
	var worst int64
	if in.Side == Buy {
		worst = high
	} else {
		worst = low
	}
	if !notionalAtLeast(in.Target(), worst, MinOrderKRW) {
		return bad("target %q at %d KRW is below the %d KRW minimum order", in.Target(), worst, MinOrderKRW)
	}
	return nil
}

// Band is the intent's price band on the tick grid, rounded inward so no
// priced child ever leaves it: low is the sell side's minimum acceptable price
// and high the buy side's maximum.
func (in Intent) Band() (low, high int64) {
	slip := in.LimitPriceKRW * in.MaxSlippageBps / 10_000
	return Ceil(in.LimitPriceKRW - slip), Floor(in.LimitPriceKRW + slip)
}

// Target is the quantity the intent may trade at most: max_quantity, cut so
// its value at the top of the band stays within max_notional_krw.
func (in Intent) Target() Qty {
	_, high := in.Band()
	q := in.MaxQuantity
	if room := qtyForBudget(in.MaxNotionalKRW, high); q.Cmp(room) > 0 {
		q = room
	}
	if q.IsNegative() {
		return Qty{}
	}
	return q
}

// worse reports whether price a is worse for the intent's side than b: higher
// for a buy, lower for a sell.
func (in Intent) worse(a, b int64) bool {
	if in.Side == Buy {
		return a > b
	}
	return a < b
}
