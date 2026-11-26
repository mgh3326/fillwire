package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestKISMockWSCommandRefusesLiveConfigBeforeCredentialsOrDial(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fillwire.toml")
	config := `[kis]
endpoint = "live"
account_mode = "live"
hts_id = "fixture-hts"
app_key_env = "KIS_MOCK_APP_KEY"
app_secret_env = "KIS_MOCK_APP_SECRET"

[redis]
url = "redis://127.0.0.1:6379/0"

[kis_mock_ws]
enabled = true
symbols = ["005930"]
recording_directory = "recordings"
`
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	err := runKISMockWSCommand([]string{"-config", path})
	if err == nil || !strings.Contains(err.Error(), "refusing non-mock") {
		t.Fatalf("live config error = %v", err)
	}
}

func TestKISMockWSCommandDisabledByDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fillwire.toml")
	config := `[kis_mock_ws]
enabled = false
`
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	err := runKISMockWSCommand([]string{"-config", path})
	if err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("disabled config error = %v", err)
	}
}
