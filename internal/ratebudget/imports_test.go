package ratebudget_test

import (
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// allowedImports is the complete import set of the package. Anything outside
// this list (net, net/http, database/sql, a broker client, a Redis or SQLite
// driver) means the budget has grown an I/O dependency it must not have.
var allowedImports = map[string]bool{
	"errors":       true,
	"fmt":          true,
	"math":         true,
	"math/rand/v2": true,
	"sort":         true,
	"strings":      true,
	"sync":         true,
	"time":         true,
}

func TestPackageImportsNoNetworkBrokerOrDatabase(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	fset := token.NewFileSet()
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, imp := range f.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if !allowedImports[path] {
				t.Errorf("%s imports %q; the package may import only %v", name, path, allowedImportList())
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("no imports checked: glob found no package files")
	}
}

func allowedImportList() []string {
	names := make([]string, 0, len(allowedImports))
	for name := range allowedImports {
		names = append(names, name)
	}
	return names
}
