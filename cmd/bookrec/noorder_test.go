package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// allowedDeps is every non-standard package bookrec may link. Adding one is a
// deliberate review decision: it is how order code or a broker client could
// enter this binary.
var allowedDeps = map[string]bool{
	"github.com/mgh3326/fillwire/cmd/bookrec":   true,
	"github.com/mgh3326/fillwire/internal/book": true,
	// kst is a single time.FixedZone constant; it imports only "time".
	"github.com/mgh3326/fillwire/internal/kst": true,
}

// forbiddenStdDeps keeps bookrec free of any network client.
var forbiddenStdDeps = []string{"net", "net/http", "crypto/tls"}

var (
	// orderSymbol matches function and type names of order clients.
	orderSymbol = regexp.MustCompile(`(?i)(place|submit|send|cancel|amend|modify|revise)_?orders?|order_?(cash|credit|rvsecncl|resv|client)`)
	// orderEndpoint matches KIS order REST paths and order transaction IDs
	// (domestic TTTC/VTTC, overseas TTTT/VTTT/TTTS; order TR IDs end in U,
	// read-only inquiries end in R).
	orderEndpoint = regexp.MustCompile(`/trading/order|[TV]TT[CTS][0-9]{4}U`)
)

func goTool(t *testing.T, args ...string) []byte {
	t.Helper()
	cmd := exec.Command("go", args...)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	out, err := cmd.Output()
	if err != nil {
		stderr := ""
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = string(exitErr.Stderr)
		}
		t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, stderr)
	}
	return out
}

func TestBinaryLinksOnlyAllowedPackages(t *testing.T) {
	std := map[string]bool{}
	for _, dep := range strings.Fields(string(goTool(t, "list", "-deps", "-f", "{{if .Standard}}{{.ImportPath}}{{end}}", "."))) {
		std[dep] = true
	}
	for _, dep := range forbiddenStdDeps {
		if std[dep] {
			t.Errorf("bookrec links %s; it must have no network client", dep)
		}
	}
	nonStd := strings.Fields(string(goTool(t, "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", ".")))
	if len(nonStd) == 0 || !std["os"] {
		t.Fatal("go list returned nothing; the check would pass vacuously")
	}
	for _, dep := range nonStd {
		if !allowedDeps[dep] {
			t.Errorf("bookrec links %s, which is not in allowedDeps", dep)
		}
	}
}

func TestBinaryHasNoOrderCode(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "bookrec")
	goTool(t, "build", "-o", binary, ".")

	symbols := string(goTool(t, "tool", "nm", binary))
	// Positive controls: the scan sees this binary's own code and literals.
	for _, want := range []string{"main.run", "internal/book.Replay"} {
		if !strings.Contains(symbols, want) {
			t.Fatalf("symbol table lacks %s; the scan would pass vacuously", want)
		}
	}
	if hits := orderSymbols(symbols); len(hits) > 0 {
		t.Errorf("order symbols linked into bookrec: %v", hits)
	}

	raw, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "book: malformed frame") {
		t.Fatal("binary lacks a known string literal; the string scan would pass vacuously")
	}
	if hits := orderEndpoints(raw); len(hits) > 0 {
		t.Errorf("order endpoints or transaction IDs in bookrec: %v", hits)
	}
}

// The scanners must catch the shapes they claim to catch.
func TestOrderScannersCatchOrderCode(t *testing.T) {
	nm := "  4a1f20 T github.com/mgh3326/go-kis/kis/domestic.PlaceOrder\n  4a2000 T main.run\n  4a3000 T example.com/broker.(*OrderClient).Do\n"
	if got := orderSymbols(nm); len(got) != 2 {
		t.Fatalf("orderSymbols = %v, want PlaceOrder and OrderClient", got)
	}
	binary := []byte("\x00/uapi/domestic-stock/v1/trading/order-cash\x00TTTC0802U\x00TTTC8434R\x00")
	if got := orderEndpoints(binary); len(got) != 2 {
		t.Fatalf("orderEndpoints = %v, want the order path and TTTC0802U only", got)
	}
}

func orderSymbols(nm string) []string {
	var hits []string
	for _, line := range strings.Split(nm, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if name := fields[len(fields)-1]; orderSymbol.MatchString(name) {
			hits = append(hits, name)
		}
	}
	return hits
}

func orderEndpoints(binary []byte) []string {
	var hits []string
	for _, m := range orderEndpoint.FindAll(binary, 20) {
		hits = append(hits, string(m))
	}
	return hits
}
