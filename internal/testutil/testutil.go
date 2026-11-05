// Package testutil holds the test helpers several packages share, so that
// fixtures are set up, compared and regenerated the same way everywhere. It is
// imported only from _test.go files.
package testutil

import (
	"bytes"
	"flag"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Update is the one -update flag. With it, Golden rewrites the golden file it
// is given instead of comparing against it. Regenerate in dependency order,
// since some packages read fixtures that others write:
//
//	go test ./internal/exec/sim -update   # sim.evidence.jsonl (read by bookfeed, execsim, execmock)
//	go test ./cmd/execsim -update         # sim.golden.jsonl (read by execmock)
//	go test ./cmd/execmock ./internal/book -update
//
// Then review the diff before committing.
var Update = flag.Bool("update", false, "rewrite golden files instead of comparing against them")

// Golden compares got with the file at path, or rewrites the file when
// -update is set.
func Golden(t testing.TB, path string, got []byte) {
	t.Helper()
	if *Update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s differs from output (rerun with -update after checking the diff)\ngot:\n%s", path, got)
	}
}

// Redis starts an in-memory Redis for the test and a client connected to it.
// Both are closed when the test ends.
func Redis(t testing.TB) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	m := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: m.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return m, c
}
