package recording

import (
	"compress/gzip"
	"encoding/json"
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
	if err = w.Append(start.Add(4*time.Second), map[string]any{"kind": "gap", "market": "KRW-BTC", "received_at": start.Add(4 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	rows, err := SummarizeDay(root, "2026-03-02")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].CoveredSeconds != 2 {
		t.Fatalf("day coverage=%+v", rows)
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
	}{{"connected", from.Add(2 * time.Second)}, {"gap", from.Add(7 * time.Second)}} {
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
