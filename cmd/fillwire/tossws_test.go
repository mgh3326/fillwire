package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/fillwire/internal/tossws"
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
	symbolsFile := filepath.Join(t.TempDir(), "symbols.txt")
	if err := os.WriteFile(symbolsFile, []byte("kr 005930\nus AAPL\n"), 0600); err != nil {
		t.Fatal(err)
	}
	gateway := gatewaydSettings{URL: "http://127.0.0.1:8080", TokenEnv: "FILLWIRE_TEST_TOSS_GATEWAYD", TokenHeader: "Authorization"}
	withSymbols := on + "\nsymbols_file = '" + symbolsFile + "'"
	settings, err := resolveTossWSSettings([]byte(withSymbols), gateway)
	if err != nil || !settings.Enabled || settings.AccountSeq != "3" || settings.Directory != "test-output" || settings.credential != fakeCredential || len(settings.symbols) != 2 {
		t.Fatal("opt-in did not resolve the expected account, directory and gatewayd settings")
	}
	for _, config := range []string{strings.Replace(on, "'3'", "'3/4'", 1), on + "\nendpoint = 'wss://elsewhere.invalid'", on + "\nsymbols_file = '/missing/symbols'"} {
		if _, err := resolveTossWSSettings([]byte(config), gateway); err == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
	tooManySymbols := filepath.Join(t.TempDir(), "too-many-symbols.txt")
	var contents strings.Builder
	for index := 0; index < 50; index++ {
		fmt.Fprintf(&contents, "kr %06d\n", index+1)
	}
	if err := os.WriteFile(tooManySymbols, []byte(contents.String()), 0600); err != nil {
		t.Fatal(err)
	}
	tooMany := on + "\nsymbols_file = '" + tooManySymbols + "'"
	if _, err := resolveTossWSSettings([]byte(tooMany), gateway); err == nil {
		t.Fatal("configuration with 101 trade/orderbook subscriptions was accepted")
	}
	gateway.URL = "http://gateway.invalid"
	if _, err := resolveTossWSSettings([]byte(on), gateway); err == nil {
		t.Fatal("untrusted credential transport accepted")
	}
}

func TestTossWSModuleStop(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		stop bool
	}{
		{"record-only", tossws.ErrRecord, false},
		{"forbidden", tossws.ErrForbidden, true},
		{"account-not-found", tossws.ErrAccountNotFound, true},
		{"rejected", tossws.ErrRejected, true},
		{"ack", tossws.ErrAck, true},
		{"auth-revoked-stop", tossws.ErrAuthRevokedStop, true},
		{"forbidden-with-record", errors.Join(tossws.ErrForbidden, tossws.ErrRecord), true},
		{"account-not-found-with-record", errors.Join(tossws.ErrAccountNotFound, tossws.ErrRecord), true},
		{"rejected-with-record", errors.Join(tossws.ErrRejected, tossws.ErrRecord), true},
		{"ack-with-record", errors.Join(tossws.ErrAck, tossws.ErrRecord), true},
		{"auth-stop-with-record", errors.Join(tossws.ErrAuthRevokedStop, tossws.ErrRecord), true},
		{"generic", errors.New("dial failed"), true},
		{"generic-with-record", errors.Join(errors.New("dial failed"), tossws.ErrRecord), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tossWSModuleStop(tc.err); got != tc.stop {
				t.Fatalf("tossWSModuleStop(%v) = %v, want %v", tc.err, got, tc.stop)
			}
		})
	}
}
