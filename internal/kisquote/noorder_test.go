package kisquote_test

import (
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

// forbiddenDeps is the order and account-mutation side of the fleet. The
// promoted quote lane must stay on the read-only side of KIS.
var forbiddenDeps = []string{
	"github.com/mgh3326/fillwire/internal/kislive",
	"github.com/mgh3326/fillwire/internal/kismockedge",
	"github.com/mgh3326/fillwire/internal/exec/",
	"github.com/mgh3326/go-kis/kis/domestic",
	"github.com/mgh3326/go-kis/kis/overseas",
}

// orderEndpoint matches KIS order REST paths and order transaction IDs (order
// TR IDs end in U; read-only inquiries end in R).
var orderEndpoint = regexp.MustCompile(`/trading/order|[TV]TT[CTS][0-9]{4}U`)

func TestImportClosureStaysReadOnly(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", ".")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	deps := strings.Fields(string(out))
	if len(deps) == 0 {
		t.Fatal("go list returned nothing; the check would pass vacuously")
	}
	seenSelf := false
	for _, dep := range deps {
		if dep == "github.com/mgh3326/fillwire/internal/kisquote" {
			seenSelf = true
		}
		for _, forbidden := range forbiddenDeps {
			if dep == forbidden || strings.HasPrefix(dep, forbidden) {
				t.Errorf("kisquote links %s", dep)
			}
		}
	}
	if !seenSelf {
		t.Fatal("kisquote missing from its own dependency list; the scan would pass vacuously")
	}
}

func TestSourceHasNoOrderEndpoint(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	scanned := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		scanned++
		if hits := orderEndpoint.FindAll(raw, 5); len(hits) > 0 {
			t.Errorf("%s contains an order endpoint or transaction ID: %v", name, hits)
		}
	}
	if scanned == 0 {
		t.Fatal("no source files scanned; the check would pass vacuously")
	}
}
