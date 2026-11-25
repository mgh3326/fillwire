package marketminute_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/jsonx"
	"github.com/mgh3326/fillwire/internal/marketminute"
)

type goldenInput struct {
	Market         string                  `json:"market"`
	Symbol         string                  `json:"symbol"`
	Venue          string                  `json:"venue"`
	ObservedBefore string                  `json:"observed_before"`
	Frame          []marketminute.FrameRow `json:"frame"`
	Expect         struct {
		SkippedForming int `json:"skipped_forming"`
		SkippedInvalid int `json:"skipped_invalid"`
		NullValue      int `json:"null_value"`
	} `json:"expect"`
}

type segmentGolden struct {
	GeneratedBy string          `json:"generated_by"`
	Calendar    json.RawMessage `json:"calendar"`
	Cases       []struct {
		Market   string `json:"market"`
		Venue    string `json:"venue"`
		At       string `json:"at"`
		Naive    bool   `json:"naive"`
		Expected string `json:"expected"`
	} `json:"cases"`
}

func loadSegmentGolden(t *testing.T) segmentGolden {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "segments.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var golden segmentGolden
	if err := jsonx.DecodeStrict(data, &golden); err != nil {
		t.Fatalf("segments golden: %v", err)
	}
	return golden
}

func testCalendar(t *testing.T) *marketminute.Calendar {
	t.Helper()
	golden := loadSegmentGolden(t)
	calendar, err := marketminute.LoadCalendar(golden.Calendar)
	if err != nil {
		t.Fatalf("calendar: %v", err)
	}
	return calendar
}

// TestBarsMatchPythonGolden compares the serialised rows byte for byte with
// the JSONL that auto_trader's bars_from_frame produced (testdata/generate_goldens.py).
func TestBarsMatchPythonGolden(t *testing.T) {
	calendar := testCalendar(t)
	inputs, err := filepath.Glob(filepath.Join("testdata", "bars", "*.input.json"))
	if err != nil || len(inputs) == 0 {
		t.Fatalf("golden inputs: %v (%d)", err, len(inputs))
	}
	for _, path := range inputs {
		name := strings.TrimSuffix(filepath.Base(path), ".input.json")
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var input goldenInput
			if err := jsonx.DecodeStrict(data, &input); err != nil {
				t.Fatalf("input: %v", err)
			}
			observedBefore, err := time.Parse(time.RFC3339, input.ObservedBefore)
			if err != nil {
				t.Fatalf("observed_before: %v", err)
			}
			result := marketminute.BarsFromFrame(input.Frame, input.Market, input.Symbol, input.Venue, observedBefore, calendar)
			if result.SkippedForming != input.Expect.SkippedForming ||
				result.SkippedInvalid != input.Expect.SkippedInvalid ||
				result.NullValue != input.Expect.NullValue {
				t.Errorf("counters = forming %d invalid %d null %d, want %d/%d/%d",
					result.SkippedForming, result.SkippedInvalid, result.NullValue,
					input.Expect.SkippedForming, input.Expect.SkippedInvalid, input.Expect.NullValue)
			}
			var got bytes.Buffer
			for _, bar := range result.Bars {
				row, err := marketminute.MarshalRow(bar)
				if err != nil {
					t.Fatal(err)
				}
				got.Write(row)
				got.WriteByte('\n')
			}
			want, err := os.ReadFile(filepath.Join("testdata", "bars", name+".golden.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got.Bytes(), want) {
				t.Fatalf("rows differ from the Python golden\n got: %q\nwant: %q", got.String(), string(want))
			}
		})
	}
}

// TestSegmentsMatchPythonGolden pins the session labels (holiday, NXT windows,
// US early close and DST) to auto_trader's session_segment_of.
func TestSegmentsMatchPythonGolden(t *testing.T) {
	golden := loadSegmentGolden(t)
	calendar := testCalendar(t)
	if len(golden.Cases) == 0 {
		t.Fatal("no segment golden cases")
	}
	for _, item := range golden.Cases {
		at, err := time.Parse(time.RFC3339, item.At)
		if err != nil {
			t.Fatalf("%s: %v", item.At, err)
		}
		got := calendar.Segment(item.Market, item.Venue, at)
		if got != item.Expected {
			t.Errorf("Segment(%s, %s, %s) = %s, want %s", item.Market, item.Venue, item.At, got, item.Expected)
		}
	}
}
