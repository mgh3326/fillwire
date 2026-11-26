package quote

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoFillwireCodeCanReachTheTossTokenEndpoint scans every non-test Go
// file in the module. fillwire is a read-only consumer of the cached Toss
// token: Toss allows one valid token per client, so a fillwire issue or
// refresh would invalidate the token auto_trader orders with. The only Toss
// address in the code is the websocket Endpoint, which Dial pins; the REST
// host, the OAuth path, and the credential grant never appear.
func TestNoFillwireCodeCanReachTheTossTokenEndpoint(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("module root not found at %s", root)
	}
	// gatewayd owns the Toss/KIS OAuth issuance. NHPLUG has one separately
	// pinned token issuer file; keep that exception file-scoped so unrelated Go
	// code remains covered by the generic credential-marker scan.
	issuerDirs := map[string]bool{
		filepath.Join(root, "internal", "gatewayd"): true,
		filepath.Join(root, "cmd", "gatewayd"):      true,
	}
	for dir := range issuerDirs {
		if info, err := os.Stat(dir); err != nil || !info.IsDir() {
			t.Fatalf("token issuer directory %s not found", dir)
		}
	}
	nhplugIssuer := filepath.Join(root, "internal", "nhplugws", "token.go")
	if info, err := os.Stat(nhplugIssuer); err != nil || info.IsDir() {
		t.Fatalf("NHPLUG token issuer file %s not found", nhplugIssuer)
	}
	forbidden := []string{"oauth2", "/oauth", "client_secret", "client_credentials", "grant_type", "openapi.tossinvest.com", "toss_api_client"}
	tossHosts := 0
	scanned := 0
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && (entry.Name() == ".git" || entry.Name() == "testdata") {
			return filepath.SkipDir
		}
		if entry.IsDir() && issuerDirs[path] {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		scanned++
		text := strings.ToLower(string(raw))
		if path == nhplugIssuer {
			for _, needle := range []string{"openapi.tossinvest.com", "toss_api_client"} {
				if strings.Contains(text, needle) {
					t.Errorf("%s contains Toss-specific marker %q", path, needle)
				}
			}
		} else {
			for _, needle := range forbidden {
				if strings.Contains(text, needle) {
					t.Errorf("%s contains %q", path, needle)
				}
			}
		}
		tossHosts += strings.Count(text, "tossinvest.com")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 10 {
		t.Fatalf("scanned only %d files", scanned)
	}
	if tossHosts != 1 || !strings.Contains(Endpoint, "openapi-ws.tossinvest.com") {
		t.Fatalf("tossinvest.com appears %d times in production code; want exactly the websocket Endpoint", tossHosts)
	}
}
