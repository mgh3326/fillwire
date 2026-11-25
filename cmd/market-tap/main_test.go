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
		"-once", "-token-redis", "redis://127.0.0.1:1",
		"-app-key-env", "FILLWIRE_TEST_APP_KEY", "-app-secret-env", "FILLWIRE_TEST_APP_SECRET",
	}, &stdout, &stderr)
	if code != ExitConfiguration {
		t.Fatalf("exit = %d, want %d (stderr %q)", code, ExitConfiguration, stderr.String())
	}
	if !strings.Contains(stderr.String(), "app key and secret") {
		t.Fatalf("stderr = %q, want the credential refusal (the token source must accept -token-redis)", stderr.String())
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatal("a refused configuration created the output file")
	}
}

func TestRefusesRemotePlaintextRedis(t *testing.T) {
	dir := t.TempDir()
	targets := filepath.Join(dir, "targets.json")
	calendar := filepath.Join(dir, "calendar.json")
	if err := os.WriteFile(targets, []byte(`{"targets":[{"market":"kr","symbol":"005930","venue":"KRX"}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(calendar, []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-enabled", "-targets", targets, "-calendar", calendar, "-out", filepath.Join(dir, "rows.jsonl"),
		"-redis", "redis://redis.example.invalid:6379", "-once",
	}, &stdout, &stderr)
	if code != ExitConfiguration {
		t.Fatalf("exit = %d, want %d (stderr %q)", code, ExitConfiguration, stderr.String())
	}
	if !strings.Contains(stderr.String(), "not permitted") {
		t.Fatalf("stderr = %q, want the Redis scheme refusal", stderr.String())
	}
}

func TestRefusesACredentialHostThatIsNotTLSOrLoopback(t *testing.T) {
	dir := t.TempDir()
	targets := filepath.Join(dir, "targets.json")
	calendar := filepath.Join(dir, "calendar.json")
	if err := os.WriteFile(targets, []byte(`{"targets":[{"market":"kr","symbol":"005930","venue":"KRX"}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(calendar, []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-enabled", "-targets", targets, "-calendar", calendar, "-out", filepath.Join(dir, "rows.jsonl"),
		"-host", "http://kis.example.invalid:9443", "-once",
	}, &stdout, &stderr)
	if code != ExitConfiguration {
		t.Fatalf("exit = %d, want %d (stderr %q)", code, ExitConfiguration, stderr.String())
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
