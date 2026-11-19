package upbit

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// forbiddenImports are the packages that would give this model a way to reach
// a broker or the E1 gateway. The Upbit shadow path has no send capability at
// all; the E1 client's allowlist stays exactly kis_mock in internal/exec/e1.
var forbiddenImports = []string{
	"github.com/mgh3326/fillwire/internal/exec",
	"github.com/mgh3326/fillwire/internal/exec/e1",
	"github.com/mgh3326/fillwire/internal/kisread",
	"github.com/mgh3326/fillwire/internal/quote",
}

// forbiddenTokens match order and credential surfaces in source text.
var forbiddenTokens = regexp.MustCompile(`/trading/order|/v1/orders|Authorization|Bearer|access_key|secret_key`)

func TestModelImportsNoSendPath(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", ".")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	deps := strings.Fields(string(out))
	if len(deps) == 0 {
		t.Fatal("go list returned nothing; the check would pass vacuously")
	}
	seen := map[string]bool{}
	for _, dep := range deps {
		seen[dep] = true
	}
	if !seen["github.com/mgh3326/fillwire/internal/exec/upbit"] {
		t.Fatal("the dependency list does not name this package; the check would pass vacuously")
	}
	for _, bad := range forbiddenImports {
		if seen[bad] {
			t.Errorf("package links %s", bad)
		}
	}
}

func TestModelSourceHasNoOrderCredentials(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		if hit := forbiddenTokens.Find(b); hit != nil {
			t.Errorf("order/credential token %q in %s", hit, file)
		}
	}
	if checked == 0 {
		t.Fatal("no source files scanned; the check would pass vacuously")
	}
}
