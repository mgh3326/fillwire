package upbit

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"strconv"
	"strings"
)

// QtyScale is the number of decimal places an Upbit base-asset quantity may
// carry. Upbit prices a pair in the market's own unit and sizes it in the base
// asset, whose precision is at most 8 decimals; there is no float anywhere on
// this path.
const QtyScale = 8

// qtyPerUnit is 10^QtyScale: one whole unit of the base asset in Qty.units.
const qtyPerUnit = 100_000_000

// ErrQty marks a quantity string this package cannot represent exactly.
var ErrQty = errors.New("upbit: invalid quantity")

// Qty is a base-asset quantity as an exact decimal with QtyScale decimals. It
// is kept as a scaled integer so that parsing, comparison and subtraction
// never round: "0.00000001" is one unit.
type Qty struct {
	units int64
}

// ParseQty reads a plain decimal quantity with at most QtyScale fraction
// digits. Sign, exponent notation, and any other form are refused, not
// rounded.
func ParseQty(s string) (Qty, error) {
	bad := func(why string) (Qty, error) {
		return Qty{}, fmt.Errorf("%w: %q %s", ErrQty, s, why)
	}
	if s == "" || len(s) > 32 {
		return bad("length")
	}
	whole, frac := s, ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		whole, frac = s[:i], s[i+1:]
		if frac == "" {
			return bad("empty fraction")
		}
	}
	if !allDigits(whole) {
		return bad("whole part is not digits")
	}
	if len(frac) > QtyScale || (frac != "" && !allDigits(frac)) {
		return bad("fraction is not at most 8 digits")
	}
	n, err := strconv.ParseInt(whole, 10, 64)
	if err != nil || n > (1<<63-1)/qtyPerUnit {
		return bad("out of range")
	}
	units := n * qtyPerUnit
	if frac != "" {
		pad := frac + strings.Repeat("0", QtyScale-len(frac))
		f, err := strconv.ParseInt(pad, 10, 64)
		if err != nil || f > (1<<63-1)-units {
			return bad("fraction out of range")
		}
		units += f
	}
	return Qty{units: units}, nil
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// String is the shortest plain decimal with the same value.
func (q Qty) String() string {
	u := uint64(q.units)
	neg := q.units < 0
	if neg {
		u = -u
	}
	whole, frac := u/qtyPerUnit, u%qtyPerUnit
	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	b.WriteString(strconv.FormatUint(whole, 10))
	if frac != 0 {
		f := strconv.FormatUint(frac+qtyPerUnit, 10)[1:]
		b.WriteByte('.')
		b.WriteString(strings.TrimRight(f, "0"))
	}
	return b.String()
}

// IsZero reports whether the quantity is exactly zero.
func (q Qty) IsZero() bool { return q.units == 0 }

// Cmp returns -1, 0 or 1 as q is less than, equal to or greater than o.
func (q Qty) Cmp(o Qty) int {
	switch {
	case q.units < o.units:
		return -1
	case q.units > o.units:
		return 1
	}
	return 0
}

// Sub returns q-o. Callers keep quantities non-negative by construction; a
// difference below zero is reported through IsNegative, never silently
// clamped.
func (q Qty) Sub(o Qty) Qty { return Qty{units: q.units - o.units} }

// Add returns q+o.
func (q Qty) Add(o Qty) Qty { return Qty{units: q.units + o.units} }

// IsNegative reports whether the quantity is below zero.
func (q Qty) IsNegative() bool { return q.units < 0 }

// MarshalJSON writes the quantity as a JSON string, as the venue expects.
func (q Qty) MarshalJSON() ([]byte, error) { return []byte(strconv.Quote(q.String())), nil }

// UnmarshalJSON reads a JSON string; a JSON number would need a float and is
// refused.
func (q *Qty) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("%w: quantity must be a JSON string", ErrQty)
	}
	v, err := ParseQty(s)
	if err != nil {
		return err
	}
	*q = v
	return nil
}

// notionalKRW is q*price in whole KRW, rounded down, without overflow for the
// validated bounds (units <= 1e14, price <= 1e10): the whole part is
// multiplied first and the fraction added separately.
func notionalKRW(q Qty, price int64) int64 {
	u := q.units
	if u < 0 {
		u = -u
	}
	return (u/qtyPerUnit)*price + (u%qtyPerUnit)*price/qtyPerUnit
}

// notionalFits reports q*price <= budgetKRW exactly, with no overflow.
func notionalFits(q Qty, price, budgetKRW int64) bool {
	if q.units < 0 || price < 0 || budgetKRW < 0 || budgetKRW > (1<<63-1)/qtyPerUnit {
		return false
	}
	hi, lo := bits.Mul64(uint64(q.units), uint64(price))
	return hi == 0 && lo <= uint64(budgetKRW)*qtyPerUnit
}

// notionalAtLeast reports q*price >= minKRW exactly, with no overflow.
func notionalAtLeast(q Qty, price, minKRW int64) bool {
	hi, lo := bits.Mul64(uint64(q.units), uint64(price))
	if hi != 0 {
		return true
	}
	return lo >= uint64(minKRW)*qtyPerUnit
}

// qtyForBudget is the largest whole-unit quantity whose value at price stays
// within budgetKRW.
func qtyForBudget(budgetKRW, price int64) Qty {
	if budgetKRW <= 0 || price <= 0 {
		return Qty{}
	}
	u := uint64(budgetKRW) * qtyPerUnit / uint64(price)
	if u > 1<<63-1 {
		u = 1<<63 - 1
	}
	return Qty{units: int64(u)}
}
