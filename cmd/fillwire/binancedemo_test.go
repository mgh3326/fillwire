package main

import (
	"log/slog"
	"strings"
	"testing"
)

func TestBinanceDemoCommandIsDefaultOff(t *testing.T) {
	t.Setenv("BINANCE_SPOT_DEMO_STREAMS_ENABLED", "false")
	err := runBinanceDemoCommand(nil)
	if err == nil || !strings.Contains(err.Error(), "must be exactly true") {
		t.Fatalf("default-off command error = %v", err)
	}
}

func TestBinanceDemoServeSettingsAreOptInAndValidateConfiguration(t *testing.T) {
	for _, name := range []string{"BINANCE_SPOT_DEMO_API_KEY", "BINANCE_SPOT_DEMO_API_SECRET", "BINANCE_DEMO_API_KEY", "BINANCE_DEMO_API_SECRET"} {
		t.Setenv(name, "")
	}
	settings, err := resolveBinanceDemoSettings(nil)
	if err != nil || settings.Enabled {
		t.Fatalf("default settings = %+v, %v; want disabled", settings, err)
	}
	settings, err = resolveBinanceDemoSettings([]byte("[binance_demo]\nenabled = true\nsymbols = ['BTCUSDT']\ndirectory = '/tmp/records'"))
	if err != nil || !settings.Enabled || settings.UserStream != nil {
		t.Fatalf("enabled settings = %+v, %v; want user stream default", settings, err)
	}
	if _, err := newBinanceDemo(settings, slog.Default()); err == nil || !strings.Contains(err.Error(), "credentials are not set") {
		t.Fatalf("enabled user stream without credentials error = %v", err)
	}
	if _, err := resolveBinanceDemoSettings([]byte("[binance_demo]\nenabled = true\nsymbols = ['BTCUSDT']")); err == nil {
		t.Fatal("enabled configuration without directory was accepted")
	}
	if _, err := resolveBinanceDemoSettings([]byte("[binance_demo]\nenabled = false\nunknown = true")); err == nil {
		t.Fatal("unknown configuration key was accepted")
	}
}
