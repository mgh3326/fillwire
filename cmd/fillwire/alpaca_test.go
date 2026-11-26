package main

import "testing"

func TestAlpacaConfigDefaultsOffAndRequiresSymbolsWhenEnabled(t *testing.T) {
	for _, input := range []string{"", "[alpaca]\nenabled = false"} {
		settings, err := resolveAlpacaSettings([]byte(input))
		if err != nil || settings.Enabled {
			t.Fatalf("default-off settings = %#v, error = %v", settings, err)
		}
	}
	if _, err := resolveAlpacaSettings([]byte("[alpaca]\nenabled = true\ndirectory = '/tmp/records'")); err == nil {
		t.Fatal("enabled configuration without symbols accepted")
	}
	settings, err := resolveAlpacaSettings([]byte("[alpaca]\nenabled = true\ndirectory = '/tmp/records'\nsymbols = ['AAPL']"))
	if err != nil || !settings.Enabled || settings.Directory != "/tmp/records" || len(settings.Symbols) != 1 || settings.Symbols[0] != "AAPL" {
		t.Fatalf("enabled settings = %#v, error = %v", settings, err)
	}
	if _, err := resolveAlpacaSettings([]byte("[alpaca]\nenabled = true\ndirectory = '/tmp/records'\nsymbols = ['AAPL']\nendpoint = 'wss://api.alpaca.markets/stream'")); err == nil {
		t.Fatal("unknown endpoint setting accepted")
	}
}
