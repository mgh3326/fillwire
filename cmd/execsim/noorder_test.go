package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// execsim never sends: its KIS mode feeds an in-process fake gateway and its
// Upbit mode has no gateway at all. These checks pin that to the binary, so a
// future import of an order client or a websocket dial fails here.
var (
	// forbiddenDeps would give execsim a way to reach a broker or the E1
	// gateway. internal/exec itself is allowed: it holds the executor and the
	// Gateway interface the fake implements, not a client.
	forbiddenDeps = []string{
		"github.com/mgh3326/fillwire/internal/exec/e1",
		"github.com/mgh3326/fillwire/internal/kisread",
		"github.com/mgh3326/fillwire/internal/quote",
	}
	// networkSymbol matches the websocket dial, the live Upbit source and the
	// recorder's reader. internal/exec/upbit and internal/upbit are linked
	// (the plan and its types live there), but nothing in execsim reaches
	// these, and the linker drops them.
	networkSymbol = regexp.MustCompile(`websocket\.Dial|upbit\.NewSource|\(\*Source\)\.Run|upbit\.\(\*Reader\)|net/http\.\(\*Client\)`)
	orderEndpoint = regexp.MustCompile(`/trading/order|/v1/orders|[TV]TT[CTS][0-9]{4}U`)
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

func TestBinaryHasNoOrderPathOrNetworkDial(t *testing.T) {
	deps := strings.Fields(string(goTool(t, "list", "-deps", "-f", "{{.ImportPath}}", ".")))
	seen := map[string]bool{}
	for _, d := range deps {
		seen[d] = true
	}
	if !seen["github.com/mgh3326/fillwire/internal/exec/upbit"] {
		t.Fatal("execsim does not link the Upbit plan; the check would pass vacuously")
	}
	for _, bad := range forbiddenDeps {
		if seen[bad] {
			t.Errorf("execsim links %s", bad)
		}
	}

	binary := filepath.Join(t.TempDir(), "execsim")
	goTool(t, "build", "-o", binary, ".")
	symbols := string(goTool(t, "tool", "nm", binary))
	for _, want := range []string{"internal/execsimcmd.Run", "internal/exec/upbit.NewReplay"} {
		if !strings.Contains(symbols, want) {
			t.Fatalf("symbol table lacks %s; the scan would pass vacuously", want)
		}
	}
	if hits := networkSymbol.FindAllString(symbols, 10); len(hits) > 0 {
		t.Errorf("network symbols linked into execsim: %q", hits)
	}
	raw, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	if hits := orderEndpoint.FindAll(raw, 20); len(hits) > 0 {
		t.Errorf("order endpoints or transaction IDs in execsim: %q", hits)
	}
}

func TestNetworkSymbolScanCatchesTheDial(t *testing.T) {
	for _, s := range []string{"github.com/coder/websocket.Dial", "internal/exec/upbit.NewSource", "internal/exec/upbit.(*Source).Run"} {
		if !networkSymbol.MatchString(s) {
			t.Errorf("networkSymbol misses %s", s)
		}
	}
}
