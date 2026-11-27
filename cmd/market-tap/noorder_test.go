package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// forbiddenImports are the packages that could carry an order, ledger or token
// issuance path into this binary. The linked go-kis root client is the shared
// HTTP client kisread already uses; its order packages and everything under
// fillwire that places orders or issues tokens are refused.
var forbiddenImports = []string{
	"github.com/mgh3326/go-kis/kis/domestic",
	"github.com/mgh3326/go-kis/kis/overseas",
	"github.com/mgh3326/go-kis/kis/ws",
	"github.com/mgh3326/fillwire/internal/exec",
	"github.com/mgh3326/fillwire/internal/kislive",
	"github.com/mgh3326/fillwire/internal/kismockedge",
	"github.com/mgh3326/fillwire/internal/gatewayd",
	"github.com/mgh3326/fillwire/internal/reader",
}

var (
	// orderSymbol matches function and type names of order clients.
	orderSymbol = regexp.MustCompile(`(?i)(place|submit|send|cancel|amend|modify|revise|daytime)_?orders?|order_?(cash|credit|rvsecncl|resv|client)`)
	// orderEndpoint matches KIS order REST paths, the US day-market order
	// paths included, and order transaction IDs (domestic TTTC/VTTC, overseas
	// TTTT/VTTT/TTTS; order TR IDs end in U, read-only inquiries end in R).
	orderEndpoint = regexp.MustCompile(`/trading/(daytime-)?order|[TV]TT[CTS][0-9]{4}U`)
)

func goTool(t *testing.T, args ...string) []byte {
	t.Helper()
	command := exec.Command("go", args...)
	command.Env = append(os.Environ(), "CGO_ENABLED=0")
	output, err := command.Output()
	if err != nil {
		stderr := ""
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = string(exitErr.Stderr)
		}
		t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, stderr)
	}
	return output
}

func TestBinaryLinksNoOrderOrIssuancePackage(t *testing.T) {
	dependencies := strings.Fields(string(goTool(t, "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", ".")))
	if len(dependencies) == 0 {
		t.Fatal("go list returned nothing; the check would pass vacuously")
	}
	linked := map[string]bool{}
	for _, dependency := range dependencies {
		linked[dependency] = true
	}
	for _, forbidden := range forbiddenImports {
		for dependency := range linked {
			if dependency == forbidden || strings.HasPrefix(dependency, forbidden+"/") {
				t.Errorf("market-tap links %s", dependency)
			}
		}
	}
	if !linked["github.com/mgh3326/fillwire/internal/marketminute"] {
		t.Fatal("the scan lacks the poller package; the check would pass vacuously")
	}
}

func TestBinaryHasNoOrderCode(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "market-tap")
	goTool(t, "build", "-o", binary, ".")

	symbols := string(goTool(t, "tool", "nm", binary))
	for _, want := range []string{"internal/markettapcmd.Run", "marketminute.(*Client).Fetch"} {
		if !strings.Contains(symbols, want) {
			t.Fatalf("symbol table lacks %s; the scan would pass vacuously", want)
		}
	}
	if hits := orderSymbols(symbols); len(hits) > 0 {
		t.Errorf("order symbols linked into market-tap: %v", hits)
	}

	raw, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "market-tap: disabled") {
		t.Fatal("binary lacks a known string literal; the string scan would pass vacuously")
	}
	if hits := orderEndpoints(raw); len(hits) > 0 {
		t.Errorf("order endpoints or transaction IDs in market-tap: %v", hits)
	}
}

func TestOrderScannersCatchOrderCode(t *testing.T) {
	nm := "  4a1f20 T github.com/mgh3326/go-kis/kis/domestic.PlaceOrder\n  4a2000 T main.run\n  4a3000 T example.com/broker.(*OrderClient).Do\n"
	if got := orderSymbols(nm); len(got) != 2 {
		t.Fatalf("orderSymbols = %v, want PlaceOrder and OrderClient", got)
	}
	binary := []byte("\x00/uapi/domestic-stock/v1/trading/order-cash\x00TTTC0802U\x00FHKST03010200\x00")
	if got := orderEndpoints(binary); len(got) != 2 {
		t.Fatalf("orderEndpoints = %v, want the order path and TTTC0802U only", got)
	}
	// The US day-market order surface of the same official sample set.
	dayNM := "  4a1f20 T example.com/overseas.DaytimeOrder\n  4a2000 T example.com/overseas.daytime_order_rvsecncl\n"
	if got := orderSymbols(dayNM); len(got) != 2 {
		t.Fatalf("orderSymbols = %v, want both day-market order symbols", got)
	}
	dayBinary := []byte("\x00/uapi/overseas-stock/v1/trading/daytime-order\x00TTTS6036U\x00HHDFS76950200\x00")
	if got := orderEndpoints(dayBinary); len(got) != 2 {
		t.Fatalf("orderEndpoints = %v, want the day-market order path and TTTS6036U only", got)
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
	for _, match := range orderEndpoint.FindAll(binary, 20) {
		hits = append(hits, string(match))
	}
	return hits
}
