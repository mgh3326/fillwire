package recording

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDailyWriterRotationAppendAndRestart(t *testing.T) {
	root := t.TempDir()
	day1 := time.Date(2026, 1, 1, 23, 59, 59, 0, time.UTC)
	w, err := NewDailyWriter(root, "toss", "KR-005930")
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Append(day1, map[string]string{"n": "one"}); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = NewDailyWriter(root, "toss", "KR-005930")
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Append(day1.Add(time.Second), map[string]string{"n": "two"}); err != nil {
		t.Fatal(err)
	}
	if err = w.Append(day1.Add(2*time.Second), map[string]string{"n": "three"}); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "toss", "2026-01-02", "KR-005930.jsonl")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(b), "\n") != 2 {
		t.Fatalf("append count = %d, data=%s", strings.Count(string(b), "\n"), b)
	}
	// A date transition closes and compresses the previous active day.
	w, err = NewDailyWriter(root, "toss", "KR-005930")
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Append(day1, map[string]string{"n": "old"})
	if err = w.Append(day1.Add(24*time.Hour), map[string]string{"n": "new"}); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	gz, err := os.Open(filepath.Join(root, "toss", "2026-01-01", "KR-005930.jsonl.gz"))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := gzip.NewReader(gz)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(reader)
	_ = reader.Close()
	_ = gz.Close()
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("gzip lines=%d data=%s", len(lines), raw)
	}
	var first, last map[string]string
	_ = json.Unmarshal([]byte(lines[0]), &first)
	_ = json.Unmarshal([]byte(lines[1]), &last)
	if first["n"] != "one" || last["n"] != "old" {
		t.Fatalf("gzip data=%s", raw)
	}
}
func TestCoverageUnionsIntervalsAndClipsDay(t *testing.T) {
	day := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows, err := SummarizeCoverage("KRW-BTC", "2026-01-01", []Interval{{day.Add(-time.Hour), day.Add(time.Hour)}, {day.Add(30 * time.Minute), day.Add(2 * time.Hour)}, {day.Add(23 * time.Hour), day.Add(25 * time.Hour)}})
	if err != nil {
		t.Fatal(err)
	}
	if rows.CoveredSeconds != 3*3600 || rows.DaySeconds != 86400 || rows.Ratio != 3.0/24 {
		t.Fatalf("coverage=%+v", rows)
	}
}

func TestDailyWriterKeepsBackwardTimestampsInOpenPartition(t *testing.T) {
	root := t.TempDir()
	start := time.Date(2026, 1, 1, 23, 59, 59, 0, time.UTC)
	w, err := NewDailyWriter(root, "toss", "KR-005930")
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Time{start.Add(2 * time.Second), start.Add(500 * time.Millisecond), start.Add(3 * time.Second)} {
		if err := w.Append(at, map[string]time.Time{"at": at}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(root, "toss", "2026-01-02", "KR-005930.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	contents, err := io.ReadAll(f)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(contents), "\n") != 3 {
		t.Fatalf("backward append lines=%d data=%s", strings.Count(string(contents), "\n"), contents)
	}
}

func TestDailyWriterRestoresAndRecompressesExistingDay(t *testing.T) {
	root := t.TempDir()
	first := time.Date(2026, 1, 1, 23, 59, 59, 0, time.UTC)
	w, _ := NewDailyWriter(root, "toss", "KR-005930")
	if err := w.Append(first, map[string]string{"value": "before"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(first.Add(time.Second), map[string]string{"value": "next-day"}); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	w, _ = NewDailyWriter(root, "toss", "KR-005930")
	if err := w.Append(first, map[string]string{"value": "late"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(first.Add(24*time.Hour), map[string]string{"value": "next-day-again"}); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	f, err := os.Open(filepath.Join(root, "toss", "2026-01-01", "KR-005930.jsonl.gz"))
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(gz)
	_ = gz.Close()
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(data), "\n") != 2 || !strings.Contains(string(data), "before") || !strings.Contains(string(data), "late") {
		t.Fatalf("restored gzip=%s", data)
	}
}

func TestStreamLockRejectsSecondWriter(t *testing.T) {
	root := t.TempDir()
	first, err := AcquireStreamLock(root, "upbit")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireStreamLock(root, "upbit"); err == nil {
		t.Fatal("second Upbit writer acquired the same stream lock")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := AcquireStreamLock(root, "upbit")
	if err != nil {
		t.Fatal(err)
	}
	_ = second.Close()
}

func TestQuoteAndBookFilesAppendAcrossRestart(t *testing.T) {
	root := t.TempDir()
	at := time.Date(2026, 2, 3, 10, 0, 0, 0, time.UTC)
	for _, item := range []struct{ stream, name string }{{"toss", "KR-005930"}, {"kis", "005930"}} {
		for _, value := range []string{"first", "second"} {
			s, err := NewSink(root)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Append(item.stream, item.name, at, map[string]string{"value": value}); err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
		}
		path := filepath.Join(root, item.stream, "2026-02-03", item.name+".jsonl")
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(string(b), "\n") != 2 {
			t.Fatalf("%s records=%s", path, b)
		}
	}
}
func TestSummarizeDayCarriesSessionAcrossUTCPartition(t *testing.T) {
	root := t.TempDir()
	w, err := NewDailyWriter(root, "upbit", "KRW-BTC")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 3, 1, 23, 59, 58, 0, time.UTC)
	if err = w.Append(start, map[string]any{"kind": "connected", "market": "KRW-BTC", "received_at": start}); err != nil {
		t.Fatal(err)
	}
	if err = w.Append(start.Add(3*time.Second), map[string]any{"kind": "orderbook", "market": "KRW-BTC", "received_at": start.Add(3 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if err = w.Append(start.Add(4*time.Second), map[string]any{"kind": "gap", "market": "KRW-BTC", "received_at": start.Add(4 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	rows, err := SummarizeDay(root, "2026-03-02")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].CoveredSeconds != 1 {
		t.Fatalf("day coverage=%+v", rows)
	}
}

func TestCoverageDoesNotExtendUnclosedSessionsAndKeepsPriorRestartSpan(t *testing.T) {
	root := t.TempDir()
	w, err := NewDailyWriter(root, "upbit", "KRW-BTC")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 5, 1, 23, 59, 0, 0, time.UTC)
	for _, event := range []struct {
		kind string
		at   time.Time
	}{
		{"connected", start}, {"trade", start.Add(30 * time.Second)},
		{"connected", start.Add(2 * time.Minute)}, {"orderbook", start.Add(2*time.Minute + 20*time.Second)},
	} {
		if err := w.Append(event.at, map[string]any{"kind": event.kind, "market": "KRW-BTC", "received_at": event.at}); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.Close()
	day1, err := SummarizeDay(root, "2026-05-01")
	if err != nil {
		t.Fatal(err)
	}
	day2, err := SummarizeDay(root, "2026-05-02")
	if err != nil {
		t.Fatal(err)
	}
	if len(day1) != 1 || day1[0].CoveredSeconds != 30 {
		t.Fatalf("first unclosed span=%+v", day1)
	}
	if len(day2) != 1 || day2[0].CoveredSeconds != 20 {
		t.Fatalf("restart span=%+v", day2)
	}
}
func TestSummarizeWindowCalculatesExpectedRatio(t *testing.T) {
	root := t.TempDir()
	w, err := NewDailyWriter(root, "upbit", "KRW-BTC")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	for _, e := range []struct {
		k string
		t time.Time
	}{{"connected", from.Add(2 * time.Second)}, {"heartbeat", from.Add(7 * time.Second)}, {"gap", from.Add(8 * time.Second)}} {
		if err := w.Append(e.t, map[string]any{"kind": e.k, "market": "KRW-BTC", "received_at": e.t}); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.Close()
	rows, err := SummarizeWindow(root, from, from.Add(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].CoveredSeconds != 5 || rows[0].WindowSeconds != 10 || rows[0].Ratio != 0.5 {
		t.Fatalf("window=%+v", rows)
	}
}

func TestDailyWriterRepairsTornTrailingRecord(t *testing.T) {
	root := t.TempDir()
	at := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	dir := filepath.Join(root, "toss", "2026-06-01")
	if err := os.MkdirAll(dir, 0750); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "KR-005930.jsonl")
	if err := os.WriteFile(path, []byte("{\"value\":\"good\"}\n{\"value\":"), 0640); err != nil {
		t.Fatal(err)
	}
	w, err := NewDailyWriter(root, "toss", "KR-005930")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(at, map[string]string{"value": "after"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(data), "\n") != 2 || strings.Contains(string(data), "good}{") {
		t.Fatalf("torn tail was not repaired: %s", data)
	}
}

func TestCoverageSkipsAndCountsCorruptRecords(t *testing.T) {
	root := t.TempDir()
	w, err := NewDailyWriter(root, "upbit", "KRW-BTC")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	for _, record := range []map[string]any{
		{"kind": "connected", "market": "KRW-BTC", "received_at": start},
		{"kind": "trade", "market": "KRW-BTC", "received_at": start.Add(5 * time.Second)},
		{"kind": "gap", "market": "KRW-BTC", "received_at": start.Add(10 * time.Second)},
	} {
		if err := w.Append(record["received_at"].(time.Time), record); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "upbit", "2026-07-01", "KRW-BTC.jsonl")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := f.WriteString("{torn tail\n")
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatal(errors.Join(writeErr, closeErr))
	}
	rows, err := SummarizeDay(root, "2026-07-01")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].CoveredSeconds != 5 || rows[0].CorruptRecords != 1 {
		t.Fatalf("coverage=%+v", rows)
	}
}

func TestRestoreCompressedDoesNotDuplicateAlreadyRestoredPrefix(t *testing.T) {
	root := t.TempDir()
	day := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	dir := filepath.Join(root, "toss", "2026-08-01")
	if err := os.MkdirAll(dir, 0750); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "KR-005930.jsonl")
	archive := []byte("{\"value\":\"old\"}\n")
	compressed, err := os.Create(path + ".gz")
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(compressed)
	if _, err := gz.Write(archive); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := compressed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{\"value\":\"old\"}\n{\"value\":\"between\"}\n"), 0640); err != nil {
		t.Fatal(err)
	}
	w, err := NewDailyWriter(root, "toss", "KR-005930")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(day, map[string]string{"value": "after"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(day.Add(24*time.Hour), map[string]string{"value": "next-day"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path + ".gz")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(data), "\n") != 3 || strings.Count(string(data), `"old"`) != 1 {
		t.Fatalf("restored records duplicated: %s", data)
	}
}
