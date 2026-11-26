package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUpbitFillsCommandConfig(t *testing.T) {
	t.Setenv("UPBIT_ACCESS_KEY", "")
	t.Setenv("UPBIT_SECRET_KEY", "")
	dir := t.TempDir()
	for _, tc := range []struct {
		name    string
		config  string
		wantErr string
	}{
		{name: "absent"},
		{name: "disabled", config: "[upbit_fills]\nenabled = false\ndirectory = 'unused'"},
		{name: "missing directory", config: "[upbit_fills]\nenabled = true", wantErr: "directory"},
		{name: "missing credentials", config: "[upbit_fills]\nenabled = true\ndirectory = 'unused'", wantErr: "UPBIT_ACCESS_KEY"},
		{name: "reject inline credentials", config: "[upbit_fills]\naccess_key = 'fixture'", wantErr: "upbit_fills"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(dir, "fillwire.toml")
			if err := os.WriteFile(path, []byte(tc.config), 0600); err != nil {
				t.Fatal(err)
			}
			err := runUpbitFillsCommand([]string{"-config", path})
			if tc.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got %v, want error containing %q", err, tc.wantErr)
			}
		})
	}
}
