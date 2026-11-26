package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// allowedDeps is every non-standard package execupbit may link. Adding one is
// a deliberate review decision: it is how order code or a broker client could
// enter this binary. The E1 gateway client and the KIS executor are absent by
// design; the Upbit shadow path has no send mode.
var allowedDeps = map[string]bool{
	"github.com/mgh3326/fillwire/cmd/execupbit":         true,
	"github.com/mgh3326/fillwire/internal/execupbitcmd": true,
	"github.com/mgh3326/fillwire/internal/exec/upbit":   true,
	"github.com/mgh3326/fillwire/internal/exitcode":     true,
	"github.com/mgh3326/fillwire/internal/jsonx":        true,
	"github.com/mgh3326/fillwire/internal/recording":    true,
	"github.com/mgh3326/fillwire/internal/upbit":        true,
	// The public market websocket client; it has no order endpoint.
	"github.com/coder/websocket":                true,
	"github.com/coder/websocket/internal/bpool": true,
	"github.com/coder/websocket/internal/errd":  true,
	"github.com/coder/websocket/internal/util":  true,
}

var (
	orderEndpoint = regexp.MustCompile(`/trading/order|/v1/orders|[TV]TT[CTS][0-9]{4}U`)
	orderSymbol   = regexp.MustCompile(`(?i)place_?orders?|order_?client|exec/e1`)
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
	nonStd := strings.Fields(string(goTool(t, "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", ".")))
	if len(nonStd) == 0 {
		t.Fatal("go list returned nothing; the check would pass vacuously")
	}
	seen := false
	for _, dep := range nonStd {
		if dep == "github.com/mgh3326/fillwire/cmd/execupbit" {
			seen = true
		}
		if !allowedDeps[dep] {
			t.Errorf("execupbit links %s, which is not in allowedDeps", dep)
		}
	}
	if !seen {
		t.Fatal("the dependency list does not name this command; the check would pass vacuously")
	}
	for _, bad := range []string{
		"github.com/mgh3326/fillwire/internal/exec",
		"github.com/mgh3326/fillwire/internal/exec/e1",
		"github.com/mgh3326/fillwire/internal/kisread",
		"github.com/mgh3326/fillwire/internal/quote",
	} {
		for _, dep := range nonStd {
			if dep == bad {
				t.Errorf("execupbit links %s; it has no send path", bad)
			}
		}
	}
}

func TestBinaryHasNoOrderEndpoint(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "execupbit")
	goTool(t, "build", "-o", binary, ".")

	symbols := string(goTool(t, "tool", "nm", binary))
	// Positive controls: the scan sees this binary's own code.
	for _, want := range []string{"internal/execupbitcmd.Run", "internal/exec/upbit.NewPlan"} {
		if !strings.Contains(symbols, want) {
			t.Fatalf("symbol table lacks %s; the scan would pass vacuously", want)
		}
	}
	for _, line := range strings.Split(symbols, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if name := fields[len(fields)-1]; orderSymbol.MatchString(name) {
			t.Errorf("order symbol linked into execupbit: %s", name)
		}
	}

	raw, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "shadow only") {
		t.Fatal("binary lacks a known string literal; the token scan would pass vacuously")
	}
	if hits := orderEndpoint.FindAll(raw, 20); len(hits) > 0 {
		t.Errorf("order endpoints or transaction IDs in execupbit: %q", hits)
	}
}

// The scanners must catch the shapes they claim to catch.
func TestOrderScannersCatchOrderCode(t *testing.T) {
	if !orderSymbol.MatchString("github.com/mgh3326/fillwire/internal/exec/e1.(*Client).Send") {
		t.Fatal("orderSymbol misses the E1 client")
	}
	if hits := orderEndpoint.FindAll([]byte("/uapi/domestic-stock/v1/trading/order-cash\x00/v1/orders\x00TTTC0802U"), 20); len(hits) != 3 {
		t.Fatalf("orderEndpoint hits = %q, want 3", hits)
	}
}
