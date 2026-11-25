package marketminute

import (
	"errors"
	"fmt"
	"math/big"
	"strings"
)

// errDecimal reports a value that is not a plain decimal number.
var errDecimal = errors.New("marketminute: not a decimal number")

// decimal is an exact base-10 number that keeps the scale (number of
// fractional digits) the source wrote, because the shadow rows are compared
// byte for byte with Python's Decimal formatting: Decimal("100.50") prints
// "100.50" and Decimal("250") - Decimal("100") prints "150", never "150.0"
// or "1.5E+2".
//
// The value is negative ? magnitude / 10^scale. Only parsing, comparison and
// subtraction are needed: prices, volume and the accumulated trade value are
// passed through or differenced.
type decimal struct {
	neg   bool
	mag   *big.Int
	scale int
}

// parseDecimal accepts an optional sign, ASCII digits and at most one decimal
// point. It refuses empty input, exponents and any other character.
func parseDecimal(raw string) (*decimal, error) {
	if raw == "" {
		return nil, errDecimal
	}
	negative := false
	switch raw[0] {
	case '+':
		raw = raw[1:]
	case '-':
		negative = true
		raw = raw[1:]
	}
	if raw == "" {
		return nil, errDecimal
	}
	whole, fraction, hasPoint := strings.Cut(raw, ".")
	if hasPoint && strings.Contains(fraction, ".") {
		return nil, errDecimal
	}
	if whole == "" && fraction == "" {
		return nil, errDecimal
	}
	for _, part := range []string{whole, fraction} {
		for index := range part {
			if part[index] < '0' || part[index] > '9' {
				return nil, errDecimal
			}
		}
	}
	digits := whole + fraction
	magnitude, ok := new(big.Int).SetString(digits, 10)
	if !ok { // unreachable after the digit check; keeps the parse total
		return nil, errDecimal
	}
	return &decimal{neg: negative && magnitude.Sign() != 0, mag: magnitude, scale: len(fraction)}, nil
}

// mustDecimal is for fixture and constant input already checked by a caller.
func mustDecimal(raw string) *decimal {
	value, err := parseDecimal(raw)
	if err != nil {
		panic(fmt.Sprintf("marketminute: invalid decimal %q", raw))
	}
	return value
}

// String renders the value the way Python's str(Decimal) does for the finite
// plain values this collector sees: sign, no leading zeros, fractional digits
// padded to the scale.
func (d *decimal) String() string {
	digits := d.mag.String()
	if d.scale == 0 {
		if d.neg {
			return "-" + digits
		}
		return digits
	}
	if len(digits) <= d.scale {
		digits = strings.Repeat("0", d.scale-len(digits)+1) + digits
	}
	whole := digits[:len(digits)-d.scale]
	fraction := digits[len(digits)-d.scale:]
	if d.neg {
		return "-" + whole + "." + fraction
	}
	return whole + "." + fraction
}

// cmp compares d with other: -1, 0 or 1.
func (d *decimal) cmp(other *decimal) int {
	if d.neg != other.neg {
		if d.neg {
			return -1
		}
		return 1
	}
	scale := max(d.scale, other.scale)
	left, right := d.scaledMagnitude(scale), other.scaledMagnitude(scale)
	result := left.Cmp(right)
	if d.neg {
		return -result
	}
	return result
}

func (d *decimal) scaledMagnitude(scale int) *big.Int {
	if d.scale == scale {
		return new(big.Int).Set(d.mag)
	}
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale-d.scale)), nil)
	return new(big.Int).Mul(d.mag, factor)
}

// sub returns d - other for d >= other, with the wider scale of the two, which
// is Python's Decimal subtraction exponent rule (result exponent is the
// smaller one, so the scale is the larger).
func (d *decimal) sub(other *decimal) (*decimal, error) {
	if d.cmp(other) < 0 {
		return nil, errors.New("marketminute: negative decimal difference")
	}
	scale := max(d.scale, other.scale)
	left, right := d.scaledMagnitude(scale), other.scaledMagnitude(scale)
	return &decimal{mag: new(big.Int).Sub(left, right), scale: scale}, nil
}
