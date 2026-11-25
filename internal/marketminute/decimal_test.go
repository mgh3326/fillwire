package marketminute

import (
	"strings"
	"testing"
)

func TestDecimalPreservesTheSourceScale(t *testing.T) {
	for _, input := range []string{"100", "100.50", "0.05", "-12.30", "000123.400"} {
		value, err := parseDecimal(input)
		if err != nil {
			t.Fatalf("parse %q: %v", input, err)
		}
		want := strings.TrimPrefix(input, "+")
		want = strings.TrimLeft(want, "0")
		if strings.HasPrefix(want, ".") {
			want = "0" + want
		}
		if input == "000123.400" {
			want = "123.400"
		}
		if got := value.String(); got != want {
			t.Errorf("%q renders %q, want %q", input, got, want)
		}
	}
}

func TestDecimalRefusesNonPlainNumbers(t *testing.T) {
	for _, input := range []string{"", "-", "1e5", "1.2.3", " 1", "+", "NaN"} {
		if _, err := parseDecimal(input); err == nil {
			t.Errorf("parse %q succeeded", input)
		}
	}
}

func TestDecimalSubtractionUsesTheWiderScale(t *testing.T) {
	for _, testCase := range []struct{ left, right, want string }{
		{"250", "100", "150"},
		{"100.50", "100.00", "0.50"},
		{"400", "250.5", "149.5"},
		{"1000", "1000", "0"},
	} {
		left, right := mustDecimal(testCase.left), mustDecimal(testCase.right)
		difference, err := left.sub(right)
		if err != nil {
			t.Fatalf("%s - %s: %v", testCase.left, testCase.right, err)
		}
		if got := difference.String(); got != testCase.want {
			t.Errorf("%s - %s = %s, want %s", testCase.left, testCase.right, got, testCase.want)
		}
	}
	if _, err := mustDecimal("100").sub(mustDecimal("250")); err == nil {
		t.Fatal("a negative difference was accepted")
	}
}

func TestCalendarRefusesMisspelledAndMalformedEntries(t *testing.T) {
	for name, raw := range map[string]string{
		"unknown field": `{"kr_closed":[],"kr_close":[]}`,
		"bad day":       `{"kr_closed":["2026-13-01"]}`,
		"bad close":     `{"us_early_close":{"2026-11-27":"25:00"}}`,
		"not json":      `nope`,
	} {
		if _, err := LoadCalendar([]byte(raw)); err == nil {
			t.Errorf("%s: configuration accepted", name)
		}
	}
}
