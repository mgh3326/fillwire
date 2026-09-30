package quote

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLoadSymbolsFixture(t *testing.T) {
	symbols, err := LoadSymbols("testdata/symbols.txt")
	if err != nil {
		t.Fatal(err)
	}
	want := []Symbol{{MarketKR, "005930"}, {MarketKR, "000660"}, {MarketKR, "0001A0"}, {MarketUS, "AAPL"}, {MarketUS, "BRK.B"}}
	if !reflect.DeepEqual(symbols, want) {
		t.Fatalf("symbols = %v, want %v", symbols, want)
	}
}

func symbolLines(n int) string {
	var builder strings.Builder
	for index := 0; index < n; index++ {
		fmt.Fprintf(&builder, "kr %06d\n", index+1)
	}
	return builder.String()
}

func TestParseSymbolsBound(t *testing.T) {
	if MaxSymbols != 40 || 2*MaxSymbols > MaxSubscriptions {
		t.Fatalf("MaxSymbols = %d with %d subscriptions per connection", MaxSymbols, MaxSubscriptions)
	}
	symbols, err := ParseSymbols([]byte(symbolLines(MaxSymbols)))
	if err != nil || len(symbols) != MaxSymbols {
		t.Fatalf("40 symbols = %d, %v; want accepted", len(symbols), err)
	}
	if _, err := ParseSymbols([]byte(symbolLines(MaxSymbols + 1))); !errors.Is(err, ErrSymbols) {
		t.Fatalf("41 symbols error = %v, want ErrSymbols", err)
	}
	if _, err := ParseSymbols([]byte(symbolLines(MaxSymbols) + "# trailing comment\n\n")); err != nil {
		t.Fatalf("comments beyond 40 symbols rejected: %v", err)
	}
	mixed := symbolLines(20) + strings.ReplaceAll(symbolLines(20), "kr ", "us A")
	if symbols, err := ParseSymbols([]byte(mixed)); err != nil || len(symbols) != 40 {
		t.Fatalf("20 kr + 20 us = %d, %v", len(symbols), err)
	}
}

func TestParseSymbolsRejectsMalformedLists(t *testing.T) {
	for _, test := range []struct{ name, contents string }{
		{"empty file", ""},
		{"comments only", "# nothing\n\n"},
		{"no market", "005930\n"},
		{"unknown market", "jp 7203\n"},
		{"upper-case market", "KR 005930\n"},
		{"kr five digits", "kr 05930\n"},
		{"kr seven digits", "kr 0059300\n"},
		{"kr lower case", "kr 0001a0\n"},
		{"us lower case", "us aapl\n"},
		{"us leading digit", "us 1AAPL\n"},
		{"us too long", "us ABCDEFGHIJK\n"},
		{"three fields", "kr 005930 extra\n"},
		{"comma separated", "kr 005930,000660\n"},
		{"duplicate", "kr 005930\nus AAPL\nkr 005930\n"},
		{"toml instead of list", "symbols = [\"005930\"]\n"},
		{"non-ascii", "kr ００５９３０\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			symbols, err := ParseSymbols([]byte(test.contents))
			if !errors.Is(err, ErrSymbols) || symbols != nil {
				t.Fatalf("ParseSymbols(%q) = %v, %v; want ErrSymbols", test.contents, symbols, err)
			}
		})
	}
	// The same code on both markets is two distinct entries.
	if symbols, err := ParseSymbols([]byte("kr ABCDEF\nus ABCDEF\n")); err != nil || len(symbols) != 2 {
		t.Fatalf("same code on kr and us = %v, %v", symbols, err)
	}
}

func TestLoadSymbolsRejectsUnreadableFiles(t *testing.T) {
	dir := t.TempDir()
	big := filepath.Join(dir, "big.txt")
	if err := os.WriteFile(big, []byte(strings.Repeat("# padding\n", 8000)), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{"missing": filepath.Join(dir, "absent.txt"), "directory": dir, "oversize": big} {
		if _, err := LoadSymbols(path); !errors.Is(err, ErrSymbols) {
			t.Fatalf("%s: error = %v, want ErrSymbols", name, err)
		}
	}
}
