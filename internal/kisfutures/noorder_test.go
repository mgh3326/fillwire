package kisfutures

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// forbiddenDeps is the order, account-mutation, ledger and token issuance side
// of the fleet. The futures lane must stay on the read-only side of KIS.
var forbiddenDeps = []string{
	"github.com/mgh3326/fillwire/internal/kislive",
	"github.com/mgh3326/fillwire/internal/kismockedge",
	"github.com/mgh3326/fillwire/internal/kismockread",
	"github.com/mgh3326/fillwire/internal/exec",
	"github.com/mgh3326/fillwire/internal/gatewayd",
	"github.com/mgh3326/fillwire/internal/reader",
	"github.com/mgh3326/fillwire/internal/sink",
	"github.com/mgh3326/go-kis/kis/domestic",
	"github.com/mgh3326/go-kis/kis/overseas",
}

// orderText matches KIS order REST paths and order transaction IDs of every
// market (they end in U: TTTC0802U, TTTO1101U, STTN1101U; inquiries end in R).
var orderText = regexp.MustCompile(`/trading/order|\b[A-Z]{4}[0-9]{4}U\b`)

type dependency struct{ path, dir string }

func dependencies(t *testing.T) []dependency {
	t.Helper()
	out, err := exec.Command("go", "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}\t{{.Dir}}{{end}}", ".").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	var deps []dependency
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if path, dir, ok := strings.Cut(line, "\t"); ok {
			deps = append(deps, dependency{path, dir})
		}
	}
	if !slices.ContainsFunc(deps, func(d dependency) bool { return d.path == "github.com/mgh3326/fillwire/internal/kisfutures" }) {
		t.Fatal("kisfutures missing from its own dependency list; the scan would pass vacuously")
	}
	return deps
}

func sources(t *testing.T, dir string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	return slices.DeleteFunc(files, func(name string) bool { return strings.HasSuffix(name, "_test.go") })
}

func TestImportClosureStaysReadOnly(t *testing.T) {
	for _, dep := range dependencies(t) {
		for _, forbidden := range forbiddenDeps {
			if dep.path == forbidden || strings.HasPrefix(dep.path, forbidden+"/") {
				t.Errorf("kisfutures links %s", dep.path)
			}
		}
	}
}

// No source file the lane links, in this module or a third party one, names an
// order path or an order transaction.
func TestNoOrderTransactionReachable(t *testing.T) {
	scanned := 0
	for _, dep := range dependencies(t) {
		for _, name := range sources(t, dep.dir) {
			raw, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			scanned++
			if hits := orderText.FindAll(raw, 5); len(hits) > 0 {
				t.Errorf("%s (%s) contains an order path or transaction ID: %q", filepath.Base(name), dep.path, hits)
			}
		}
	}
	if scanned == 0 {
		t.Fatal("no source files scanned; the check would pass vacuously")
	}
}

func TestOrderScannerCatchesOrderText(t *testing.T) {
	for _, bad := range []string{
		`"/uapi/domestic-futureoption/v1/trading/order"`, `"TTTO1101U"`, `tr := "STTN1101U"`, `"VTTO1103U"`, `"TTTC0802U"`,
	} {
		if !orderText.MatchString(bad) {
			t.Errorf("scanner misses %s", bad)
		}
	}
	for _, fine := range []string{`"H0IFCNT0"`, `"H0MFCNT0"`, `"FHKST01010300"`, `"/uapi/domestic-stock/v1/trading/inquire-balance"`} {
		if orderText.MatchString(fine) {
			t.Errorf("scanner flags %s", fine)
		}
	}
}

// The only transaction IDs written in the package are the two trade streams:
// no execution notice and nothing else can be subscribed.
func TestOnlyTradeTransactionsAreNamed(t *testing.T) {
	transaction := regexp.MustCompile(`^[HFCTVJS][0-9A-Z]{6,11}$`)
	var found []string
	fileSet := token.NewFileSet()
	for _, name := range sources(t, ".") {
		file, err := parser.ParseFile(fileSet, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			if value, err := strconv.Unquote(literal.Value); err == nil && transaction.MatchString(value) {
				found = append(found, value)
			}
			return true
		})
	}
	slices.Sort(found)
	if want := []string{"H0IFCNT0", "H0MFCNT0"}; !slices.Equal(found, want) {
		t.Fatalf("transaction IDs in the package = %v, want %v", found, want)
	}
}

// The lane shares the fills session: it uses nothing of the websocket package
// that could dial or own a connection.
func TestLaneUsesNoDialingAPI(t *testing.T) {
	allowed := map[string]bool{"Event": true, "SubscribeError": true}
	used := 0
	fileSet := token.NewFileSet()
	for _, name := range sources(t, ".") {
		file, err := parser.ParseFile(fileSet, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, imported := range file.Imports {
			path := strings.Trim(imported.Path.Value, `"`)
			if strings.Contains(path, "websocket") {
				t.Errorf("%s imports %s", name, path)
			}
			if path == "github.com/mgh3326/go-kis/kis/ws" && imported.Name != nil {
				t.Errorf("%s renames the ws import; the check reads the name ws", name)
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := selector.X.(*ast.Ident); ok && pkg.Name == "ws" {
				used++
				if !allowed[selector.Sel.Name] {
					t.Errorf("%s uses ws.%s", name, selector.Sel.Name)
				}
			}
			return true
		})
	}
	if used == 0 {
		t.Fatal("no use of the ws package seen; the check would pass vacuously")
	}
}
