package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/fillwire/internal/kiwoomws"
)

func TestKiwoomCommandDisabledByDefaultAndUsesAccountEnvNames(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fillwire.toml")
	if err := os.WriteFile(path, []byte(""), 0600); err != nil {
		t.Fatal(err)
	}
	if err := runKiwoomCommand(kiwoomws.AccountKR, []string{"-config", path}); err != nil {
		t.Fatalf("missing table should remain disabled: %v", err)
	}
	content := `[kiwoom_mock_kr]
enabled = true
symbols = ["005930"]
directory = "` + dir + `"
app_key_env = "KIWOOM_MOCK_APP_KEY"
app_secret_env = "KIWOOM_MOCK_APP_SECRET"
`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KIWOOM_MOCK_APP_KEY", "")
	t.Setenv("KIWOOM_MOCK_APP_SECRET", "")
	err := runKiwoomCommand(kiwoomws.AccountKR, []string{"-config", path})
	if err == nil || !strings.Contains(err.Error(), "KIWOOM_MOCK_APP_KEY") {
		t.Fatalf("got %v, want missing documented env name", err)
	}
}

func TestKiwoomCommandRefusesMoreThan200AcrossAccounts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fillwire.toml")
	content := `[kiwoom_mock_kr]
enabled = true
symbols = [` + symbolList("K", 101) + `]
directory = "` + dir + `"
app_key_env = "KIWOOM_MOCK_APP_KEY"
app_secret_env = "KIWOOM_MOCK_APP_SECRET"

[kiwoom_mock_us]
enabled = true
symbols = [` + symbolList("U", 100) + `]
directory = "` + dir + `"
app_key_env = "KIWOOM_MOCK_US_APP_KEY"
app_secret_env = "KIWOOM_MOCK_US_APP_SECRET"
`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	err := runKiwoomCommand(kiwoomws.AccountKR, []string{"-config", path})
	if err == nil || !strings.Contains(err.Error(), "200") {
		t.Fatalf("got %v, want combined symbol cap error", err)
	}
}

func symbolList(prefix string, n int) string {
	values := make([]string, n)
	for i := range values {
		values[i] = `"` + prefix + string(rune('A'+i/26)) + string(rune('A'+i%26)) + `"`
	}
	return strings.Join(values, ",")
}
