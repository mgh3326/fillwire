package upbit

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestUpbitReaderHasNoOrderCapability fences this public market-data package from broker/order code.
func TestUpbitReaderHasNoOrderCapability(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	bad := regexp.MustCompile(`(?i)(place|submit|cancel|amend|modify|revise)_?orders?|/trading/order|[TV]TT[CTS][0-9]{4}U|Authorization`)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if hit := bad.Find(b); hit != nil {
			t.Errorf("order/auth capability token %q in %s", hit, file)
		}
	}
}
