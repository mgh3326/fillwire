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
	kiwoomIssuer := filepath.Join(root, "internal", "kiwoomws", "oauth.go")
	if info, err := os.Stat(kiwoomIssuer); err != nil || info.IsDir() {
		t.Fatalf("Kiwoom mock token issuer file %s not found", kiwoomIssuer)
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
		if hasTossTokenEndpoint(text) {
			t.Errorf("%s can reach the Toss token endpoint", path)
		}
		if path == nhplugIssuer {
			for _, needle := range []string{"openapi.tossinvest.com", "toss_api_client"} {
				if strings.Contains(text, needle) {
					t.Errorf("%s contains Toss-specific marker %q", path, needle)
				}
			}
		} else if path == kiwoomIssuer {
			if !strings.Contains(text, `mockrestendpoint+"/oauth2/token"`) {
				t.Errorf("%s no longer pins the Kiwoom mock token path to MockRESTEndpoint", path)
			}
			for _, needle := range []string{"openapi.tossinvest.com", "toss_api_client", "tossbaseurl"} {
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

func hasTossTokenEndpoint(text string) bool {
	tossBase := strings.Contains(text, "openapi.tossinvest.com") || strings.Contains(text, "tossbaseurl")
	tokenPath := strings.Contains(text, "/oauth2/token") || strings.Contains(text, "toss_oauth_token_path")
	return tossBase && tokenPath
}

func TestTossTokenEndpointGuardScopesOAuthPathByTossHost(t *testing.T) {
	tests := []struct {
		name string
		code string
		want bool
	}{
		{
			name: "Toss token endpoint is caught",
			code: `http.Post("https://openapi.tossinvest.com/oauth2/token", body)`,
			want: true,
		},
		{
			name: "Kiwoom mock token endpoint is allowed",
			code: `http.Post(MockRESTEndpoint+"/oauth2/token", body)`,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasTossTokenEndpoint(strings.ToLower(tt.code)); got != tt.want {
				t.Fatalf("hasTossTokenEndpoint(%q) = %v, want %v", tt.code, got, tt.want)
			}
		})
	}
}
