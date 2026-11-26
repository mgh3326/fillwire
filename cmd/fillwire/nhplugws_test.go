package main

import (
	"strings"
	"testing"
)

func TestNHPlugWSConfigDefaultsOffAndUsesCredentialNames(t *testing.T) {
	for _, contents := range []string{"", "[nhplug_ws]\nenabled = false"} {
		settings, err := resolveNHPlugWSSettings([]byte(contents))
		if err != nil || settings.file.Enabled {
			t.Fatalf("default-off config = %#v, %v", settings, err)
		}
	}
	const enabled = "[nhplug_ws]\nenabled = true\nuser_id = 'mock-user'\ndirectory = 'recordings'\napp_key_env = 'FILLWIRE_TEST_NH_KEY'\napp_secret_env = 'FILLWIRE_TEST_NH_SECRET'"
	t.Setenv("FILLWIRE_TEST_NH_KEY", "test-key")
	t.Setenv("FILLWIRE_TEST_NH_SECRET", "test-secret")
	settings, err := resolveNHPlugWSSettings([]byte(enabled))
	if err != nil || settings.appKey != "test-key" || settings.appSecret != "test-secret" || settings.file.UserID != "mock-user" {
		t.Fatalf("enabled config = %#v, %v", settings, err)
	}
	for _, contents := range []string{
		"[nhplug_ws]\nenabled = true\nuser_id='mock-user'",
		enabled + "\nendpoint = 'wss://api.nhplug.com:7070/websocket'",
		enabled + "\napp_key = 'inline-secret'",
	} {
		if _, err := resolveNHPlugWSSettings([]byte(contents)); err == nil {
			t.Fatalf("accepted invalid configuration %q", contents)
		}
	}
	if strings.Contains(settings.file.AppKeyEnv, "test-key") || strings.Contains(settings.file.AppSecretEnv, "test-secret") {
		t.Fatal("credential value leaked into config")
	}
}

func TestNHPlugWSConfigRegistrationCap(t *testing.T) {
	var b strings.Builder
	b.WriteString("[nhplug_ws]\nenabled=true\nuser_id='user'\ndirectory='records'\n")
	for i := 0; i < 27; i++ {
		b.WriteString("[[nhplug_ws.quotes]]\ntr_cd='mc'\ntr_key='005930'\n")
	}
	if _, err := resolveNHPlugWSSettings([]byte(b.String())); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("over-cap config error = %v", err)
	}
}
