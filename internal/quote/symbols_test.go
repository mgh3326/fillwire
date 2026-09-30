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
	want := []string{"005930", "000660", "0001A0", "035420"}
	if !reflect.DeepEqual(symbols, want) {
		t.Fatalf("symbols = %v, want %v", symbols, want)
	}
}

func symbolLines(n int) string {
	var builder strings.Builder
	for index := 0; index < n; index++ {
		fmt.Fprintf(&builder, "%06d\n", index+1)
	}
	return builder.String()
}

func TestParseSymbolsBound(t *testing.T) {
	symbols, err := ParseSymbols([]byte(symbolLines(MaxSymbols)))
	if err != nil || len(symbols) != MaxSymbols {
		t.Fatalf("40 symbols = %d, %v; want accepted", len(symbols), err)
	}
	if MaxSymbols != 40 {
		t.Fatalf("MaxSymbols = %d, want 40", MaxSymbols)
	}
	if _, err := ParseSymbols([]byte(symbolLines(MaxSymbols + 1))); !errors.Is(err, ErrSymbols) {
		t.Fatalf("41 symbols error = %v, want ErrSymbols", err)
	}
	commented := symbolLines(MaxSymbols) + "# trailing comment\n\n"
	if _, err := ParseSymbols([]byte(commented)); err != nil {
		t.Fatalf("comments beyond 40 symbols rejected: %v", err)
	}
}

func TestParseSymbolsRejectsMalformedLists(t *testing.T) {
	for _, test := range []struct{ name, contents string }{
		{"empty file", ""},
		{"comments only", "# nothing\n\n"},
		{"five digits", "05930\n"},
		{"seven digits", "0059300\n"},
		{"lower case", "0001a0\n"},
		{"inner space", "005 930\n"},
		{"two per line", "005930 000660\n"},
		{"comma separated", "005930,000660\n"},
		{"prefixed", "A005930\n"},
		{"duplicate", "005930\n000660\n005930\n"},
		{"toml instead of list", "symbols = [\"005930\"]\n"},
		{"non-ascii", "００５９３０\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			symbols, err := ParseSymbols([]byte(test.contents))
			if !errors.Is(err, ErrSymbols) || symbols != nil {
				t.Fatalf("ParseSymbols(%q) = %v, %v; want ErrSymbols", test.contents, symbols, err)
			}
		})
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
