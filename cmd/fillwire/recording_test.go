package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRecordingTablesDefaultOffAndRejectUnknownKeys(t *testing.T) {
	r, err := resolveRecordingSettings([]byte("[recording]\nenabled=false\nunknown=1\n"))
	if err == nil || r.enabled {
		t.Fatalf("unknown recording key accepted: %+v %v", r, err)
	}
	r, err = resolveRecordingSettings([]byte("[recording]\nenabled=false\n"))
	if err != nil || r.enabled {
		t.Fatalf("recording default=%+v err=%v", r, err)
	}
	u, err := resolveUpbitSettings([]byte("[upbit]\nenabled=false\nunknown=1\n"))
	if err == nil || u.enabled {
		t.Fatalf("unknown Upbit key accepted: %+v %v", u, err)
	}
	u, err = resolveUpbitSettings([]byte("[upbit]\nenabled=false\n"))
	if err != nil || u.enabled {
		t.Fatalf("Upbit default=%+v err=%v", u, err)
	}
}
func TestUpbitSettingsLoadsAtMostTwentyKRWMarkets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "markets.txt")
	if err := os.WriteFile(path, []byte("KRW-BTC\nKRW-ETH\n"), 0600); err != nil {
		t.Fatal(err)
	}
	text := "[upbit]\nenabled=true\nmarkets_file=\"" + path + "\"\ndirectory=\"/tmp/evidence\"\n"
	got, err := resolveUpbitSettings([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.markets) != 2 || got.markets[1] != "KRW-ETH" {
		t.Fatalf("settings=%+v", got)
	}
}
