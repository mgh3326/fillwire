package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDisabledByDefault(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(nil, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d, want 0", code)
	}
	if !strings.Contains(stdout.String(), "disabled") {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestEnabledWithoutTargetsIsAUsageError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-enabled"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
}

func TestEnabledRefusesMissingConfiguration(t *testing.T) {
	dir := t.TempDir()
	targets := filepath.Join(dir, "targets.json")
	calendar := filepath.Join(dir, "calendar.json")
	output := filepath.Join(dir, "rows.jsonl")
	if err := os.WriteFile(targets, []byte(`{"targets":[{"market":"kr","symbol":"005930","venue":"KRX"}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(calendar, []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FILLWIRE_TEST_APP_KEY", "")
	t.Setenv("FILLWIRE_TEST_APP_SECRET", "")
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-enabled", "-targets", targets, "-calendar", calendar, "-out", output,
		"-once", "-app-key-env", "FILLWIRE_TEST_APP_KEY", "-app-secret-env", "FILLWIRE_TEST_APP_SECRET",
	}, &stdout, &stderr)
	if code != ExitConfiguration {
		t.Fatalf("exit = %d, want %d (stderr %q)", code, ExitConfiguration, stderr.String())
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatal("a refused configuration created the output file")
	}
}

func TestTargetListIsStrict(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "targets.json")
	if err := os.WriteFile(path, []byte(`{"targets":[],"extra":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadTargets(path); err == nil {
		t.Fatal("unknown field accepted")
	}
	if err := os.WriteFile(path, []byte(`{"targets":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadTargets(path); err == nil {
		t.Fatal("empty target list accepted")
	}
}
