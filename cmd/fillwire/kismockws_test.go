package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/fillwire/internal/reader"
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

func TestKISMockWSCommandRefusesRemotePlaintextRedisBeforeCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fillwire.toml")
	config := `[kis]
endpoint = "mock"
account_mode = "mock"
hts_id = "fixture-hts"
app_key_env = "UNSET_KIS_MOCK_KEY"
app_secret_env = "UNSET_KIS_MOCK_SECRET"

[redis]
url = "redis://example.com:6379/0"

[kis_mock_ws]
enabled = true
symbols = ["005930"]
recording_directory = "recordings"
`
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	err := runKISMockWSCommand([]string{"-config", path})
	if !errors.Is(err, reader.ErrApprovalConfig) || !strings.Contains(err.Error(), "Redis URL refused") {
		t.Fatalf("remote plaintext Redis error = %v", err)
	}
	if got := kisMockWSCommandExitCode(err); got != 78 {
		t.Fatalf("config exit code = %d, want 78", got)
	}
}

func TestKISMockWSCommandExitCodes(t *testing.T) {
	if got := dispatch([]string{"kis-mock-ws", "-h"}); got != 0 {
		t.Fatalf("help exit code = %d, want 0", got)
	}
	if got := dispatch([]string{"kis-mock-ws", "-unknown"}); got != 2 {
		t.Fatalf("unknown flag exit code = %d, want 2", got)
	}
	if got := kisMockWSCommandExitCode(errKISMockWSUsage); got != 2 {
		t.Fatalf("usage exit code = %d, want 2", got)
	}
	if got := kisMockWSCommandExitCode(reader.ErrApprovalConfig); got != 78 {
		t.Fatalf("config exit code = %d, want 78", got)
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

func TestDispatchKISMockWSCommand(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fillwire.toml")
	if err := os.WriteFile(path, []byte("[kis_mock_ws]\nenabled = false\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if got := dispatch([]string{"kis-mock-ws", "-config", path}); got != 78 {
		t.Fatalf("dispatch exit code = %d, want 78", got)
	}
}
