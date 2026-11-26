package kismockws

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/recording"
	"github.com/mgh3326/go-kis/kis/ws"
)

func TestQuoteFramesDecodeAndRecord(t *testing.T) {
	at := time.Date(2026, 11, 26, 1, 2, 3, 4, time.UTC)
	for _, tr := range []string{"H0STCNT0", "H0STASP0"} {
		t.Run(tr, func(t *testing.T) {
			fields := make([]string, 46)
			if tr == "H0STASP0" {
				fields = make([]string, 45)
			}
			fields[0], fields[1] = "005930", "100203"
			if tr == "H0STCNT0" {
				fields[2] = "100"
				fields[12], fields[13] = "1", "1"
				fields[21] = "1"
			}
			raw := []byte("0|" + tr + "|001|" + strings.Join(fields, "^"))
			value, err := DecodeQuote(ws.Event{TR: tr, Raw: raw, ReceivedAt: at})
			if err != nil {
				t.Fatalf("DecodeQuote: %v", err)
			}
			root := t.TempDir()
			sink, err := recording.NewSink(root, Name)
			if err != nil {
				t.Fatal(err)
			}
			defer sink.Close()
			if err := record(sink, quoteRecordingName, at, value); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, Name, at.Format("2006-01-02"), quoteRecordingName+".jsonl")
			line, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]any
			if err := json.Unmarshal(line[:len(line)-1], &got); err != nil {
				t.Fatalf("record is not JSON: %v", err)
			}
			if got["tr"] != tr || got["received_at"] != at.Format(time.RFC3339Nano) {
				t.Fatalf("record = %#v", got)
			}
		})
	}
}

func TestMockNoticesDecodeAndRecord(t *testing.T) {
	cases := []struct {
		tr, name string
		count    int
	}{
		{TROverseasNotice, overseasRecordingName, 25},
		{TRFuturesNotice, futuresRecordingName, 22},
	}
	for _, tc := range cases {
		t.Run(tc.tr, func(t *testing.T) {
			at := time.Date(2026, 11, 26, 1, 2, 3, 4, time.UTC)
			fields := make([]string, tc.count)
			fields[5], fields[11], fields[12], fields[13] = "0", "0", "2", "2"
			event := ws.Event{TR: tc.tr, Fields: fields, Raw: []byte("0|" + tc.tr + "|001|ciphertext"), ReceivedAt: at}
			notices, err := DecodeNotices(event)
			if err != nil {
				t.Fatalf("DecodeNotices: %v", err)
			}
			if len(notices) != 1 {
				t.Fatalf("decoded notices = %d, want 1", len(notices))
			}
			notice := notices[0]
			if notice.Kind != "fill" || notice.Fields["cntg_yn"] != "2" && notice.Fields["CNTG_YN"] != "2" {
				t.Fatalf("decoded notice = %+v", notice)
			}
			root := t.TempDir()
			sink, err := recording.NewSink(root, Name)
			if err != nil {
				t.Fatal(err)
			}
			defer sink.Close()
			if err := record(sink, tc.name, at, notice); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, Name, at.Format("2006-01-02"), tc.name+".jsonl")
			line, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var got Notice
			if err := json.Unmarshal(line[:len(line)-1], &got); err != nil {
				t.Fatal(err)
			}
			if got.TR != tc.tr || got.Kind != "fill" || !got.ReceivedAt.Equal(at) {
				t.Fatalf("recorded notice = %+v", got)
			}
		})
	}
}

func TestDecodeNoticeDropsMalformedFrames(t *testing.T) {
	if _, err := DecodeNotices(ws.Event{TR: TROverseasNotice, Fields: []string{"short"}}); err == nil {
		t.Fatal("short notice accepted")
	}
	if _, err := DecodeQuote(ws.Event{TR: "H0STCNT0", Raw: []byte("0|H0STCNT0|001|005930^09")}); err == nil {
		t.Fatal("malformed quote accepted")
	}
}

func TestDecodeNoticesSplitsFlattenedRows(t *testing.T) {
	fields := make([]string, 44)
	fields[12], fields[34] = "2", "2"
	notices, err := DecodeNotices(ws.Event{TR: TRFuturesNotice, Fields: fields, Raw: []byte("0|" + TRFuturesNotice + "|002|ciphertext")})
	if err != nil {
		t.Fatal(err)
	}
	if len(notices) != 2 || notices[0].Kind != "fill" || notices[1].Kind != "fill" {
		t.Fatalf("decoded notices = %+v, want two fills", notices)
	}
}

func TestMalformedNoticeIsRecordedAndWriteFailureCancels(t *testing.T) {
	root := t.TempDir()
	sink, err := recording.NewSink(root, Name)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	module, err := New(Config{Endpoint: "mock", HTSID: "fixture", Symbols: []string{"005930"}, RecordingDirectory: root, Approval: unusedApproval{}})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 26, 1, 2, 3, 0, time.UTC)
	module.recordNotice(sink, ws.Event{TR: TROverseasNotice, Fields: []string{"bad"}, Raw: []byte("0|" + TROverseasNotice + "|001|ciphertext"), ReceivedAt: at}, func() {})
	path := filepath.Join(root, Name, at.Format("2006-01-02"), malformedRecording+".jsonl")
	line, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(line[:len(line)-1], &got); err != nil {
		t.Fatal(err)
	}
	if got["tr"] != TROverseasNotice || got["kind"] != "malformed_notice" {
		t.Fatalf("malformed recording = %#v", got)
	}
	canceled := false
	module.appendRecord = func(string, time.Time, any) error { return errors.New("disk full") }
	if module.append(sink, overseasRecordingName, at, got, func() { canceled = true }) {
		t.Fatal("failed append reported success")
	}
	if !canceled || module.Health().LastError != "recording append failed" {
		t.Fatalf("append failure state: canceled=%v health=%+v", canceled, module.Health())
	}
}
