package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/fillwire/internal/quote"
	"github.com/mgh3326/fillwire/internal/recording"
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

func TestExistingBooksTableRemainsLenient(t *testing.T) {
	contents := shippedConfig(t)
	marker := "\n[recording]\n"
	index := strings.Index(contents, marker)
	if index < 0 {
		t.Fatal("recording table is missing from shipped config")
	}
	contents = contents[:index] + "\nlegacy_retired_key = true\n" + contents[index:]
	path := writeTemp(t, "legacy-books.toml", contents)
	setFillsEnv(t)
	if _, err := loadConfig(path); err != nil {
		t.Fatalf("legacy unknown [books] key rejected: %v", err)
	}
}

func TestQuoteArchiveUsesMarketQualifiedSafeNames(t *testing.T) {
	if got := quoteArchiveName(quote.Symbol{Market: quote.MarketKR, Code: "005930"}); got != "KR-005930" {
		t.Fatalf("KR archive name=%q", got)
	}
	us := quote.Symbol{Market: quote.MarketUS, Code: "BRK.B"}
	if got := quoteArchiveName(us); got != "US-BRK_B" || !recording.SafeName(got) {
		t.Fatalf("US archive name=%q is not a safe market-qualified name", got)
	}
	line, err := json.Marshal(tossArchiveRecord{Kind: "quote", Market: us.Market, Tick: quote.Tick{Symbol: us.Code}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded tossArchiveRecord
	if err := json.Unmarshal(line, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Market != quote.MarketUS || decoded.Tick.Symbol != us.Code {
		t.Fatalf("market identity missing from archived quote: %+v", decoded)
	}
}
