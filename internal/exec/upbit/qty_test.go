package upbit

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestParseQtyIsExactDecimal(t *testing.T) {
	ok := []struct {
		in    string
		units int64
		out   string
	}{
		{"0.00000001", 1, "0.00000001"},
		{"0.1", 10_000_000, "0.1"},
		{"0.10000000", 10_000_000, "0.1"},
		{"33", 33 * qtyPerUnit, "33"},
		{"00033.500", 33*qtyPerUnit + 50_000_000, "33.5"},
		{"1.00000000", qtyPerUnit, "1"},
	}
	for _, c := range ok {
		q, err := ParseQty(c.in)
		if err != nil {
			t.Errorf("ParseQty(%q): %v", c.in, err)
			continue
		}
		if q.units != c.units {
			t.Errorf("ParseQty(%q).units = %d, want %d", c.in, q.units, c.units)
		}
		if got := q.String(); got != c.out {
			t.Errorf("ParseQty(%q).String() = %q, want %q", c.in, got, c.out)
		}
	}
}

func TestParseQtyRefusesWhatIsNotADecimalQuantity(t *testing.T) {
	for _, in := range []string{
		"", " ", "1 ", " 1", "+1", "-1", "1e5", "1E5", ".5", "5.", "1.123456789", "1.0.0", "0x10",
		"92233720369", "99999999999999999999999",
	} {
		if q, err := ParseQty(in); err == nil {
			t.Errorf("ParseQty(%q) = %v, want an error", in, q)
		} else if !errors.Is(err, ErrQty) {
			t.Errorf("ParseQty(%q): error %v is not ErrQty", in, err)
		}
	}
}

func TestQtyJSONIsAlwaysAString(t *testing.T) {
	var q Qty
	if err := json.Unmarshal([]byte(`"0.25"`), &q); err != nil {
		t.Fatalf("JSON string: %v", err)
	}
	if got := q.String(); got != "0.25" {
		t.Fatalf("decoded %q", got)
	}
	// A JSON number would need a float on the decoding side; refuse it.
	if err := json.Unmarshal([]byte(`0.25`), &q); err == nil {
		t.Fatal("JSON number accepted for a quantity")
	}
	b, err := json.Marshal(Qty{units: 250_000_000})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `"2.5"` {
		t.Fatalf("marshal = %s", b)
	}
}

func TestNotionalHelpersDoNotOverflow(t *testing.T) {
	// 1e6 base units at 1e10 KRW would overflow a naive int64 product.
	huge := Qty{units: maxIntentUnits}
	if notionalFits(huge, maxLimitPriceKRW, maxIntentNotionalKRW) {
		t.Fatal("an enormous notional fits the budget")
	}
	if !notionalAtLeast(huge, maxLimitPriceKRW, MinOrderKRW) {
		t.Fatal("an enormous notional is below the minimum order")
	}
	if q := qtyForBudget(100_000, 3_000); q.String() != "33.33333333" {
		t.Fatalf("qtyForBudget = %q", q)
	}
	if q := qtyForBudget(0, 3_000); !q.IsZero() {
		t.Fatalf("qtyForBudget(0) = %q", q)
	}
	if notionalKRW(Qty{units: 33 * qtyPerUnit}, 3_000) != 99_000 {
		t.Fatal("notionalKRW is not exact")
	}
}
