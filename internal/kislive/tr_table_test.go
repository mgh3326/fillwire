package kislive

import "testing"

func TestTRTableIsDeclarationOnlyAndMatchesKISLiveIDs(t *testing.T) {
	want := map[string]map[string]string{
		"krx": {"buy": "TTTC0012U", "sell": "TTTC0011U", "cancel": "TTTC0013U"},
		"us":  {"buy": "TTTT1002U", "sell": "TTTT1001U", "cancel": "TTTT1004U"},
	}
	for market, entries := range want {
		for action, id := range entries {
			if TRTable[market][action] != id {
				t.Fatalf("TRTable[%q][%q] = %q, want %q", market, action, TRTable[market][action], id)
			}
		}
	}
	if len(TRTable) != 2 || len(TRTable["krx"]) != 3 || len(TRTable["us"]) != 3 {
		t.Fatalf("TRTable contains undeclared entries: %#v", TRTable)
	}
}
