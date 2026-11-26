package main

import (
	"strings"
	"testing"
)

func TestTossWSConfigExplicitOptIn(t *testing.T) {
	for _, config := range []string{"", "[toss_ws]\nenabled = false"} {
		settings, err := resolveTossWSSettings([]byte(config), gatewaydSettings{})
		if err != nil || settings.Enabled {
			t.Fatalf("reader must default off, enabled = %v, error = %v", settings.Enabled, err)
		}
	}
	const on = "[toss_ws]\nenabled = true\naccount_seq = '3'\ndirectory = 'test-output'"
	const fakeCredential = "fake-gatewayd-test-credential"
	t.Setenv("FILLWIRE_TEST_TOSS_GATEWAYD", fakeCredential)
	gateway := gatewaydSettings{URL: "http://127.0.0.1:8080", TokenEnv: "FILLWIRE_TEST_TOSS_GATEWAYD", TokenHeader: "Authorization"}
	settings, err := resolveTossWSSettings([]byte(on), gateway)
	if err != nil || !settings.Enabled || settings.AccountSeq != "3" || settings.Directory != "test-output" || settings.credential != fakeCredential {
		t.Fatal("opt-in did not resolve the expected account, directory and gatewayd settings")
	}
	for _, config := range []string{strings.Replace(on, "'3'", "'3/4'", 1), on + "\nendpoint = 'wss://elsewhere.invalid'"} {
		if _, err := resolveTossWSSettings([]byte(config), gateway); err == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
	gateway.URL = "http://gateway.invalid"
	if _, err := resolveTossWSSettings([]byte(on), gateway); err == nil {
		t.Fatal("untrusted credential transport accepted")
	}
}
