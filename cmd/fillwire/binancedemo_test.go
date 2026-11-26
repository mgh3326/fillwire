package main

import (
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
