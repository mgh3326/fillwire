package upbit

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// Event kinds a recording yields, besides source.go's EventBook and EventGap.
// Book and trade carry a decoded frame; a
// frame this model refuses to decode is a Refused event (replay counts it and
// prices nothing from it); gap and connected are the recorder's own
// disconnect and reconnect marks; everything else (heartbeat, an unknown
// frame) is Other and only advances the clock.
const (
	EventTrade     = "trade"
	EventConnected = "connected"
	EventRefused   = "refused"
	EventOther     = "other"
)

// Event is one record of a recording, decoded. Market is the recorder's
// market for the record, At its receive time.
type Event struct {
	Kind   string
	Market string
	At     time.Time
	Book   Depth
	Trade  Trade
	Reason string
}

// recordLine is one line of the recorder's file: an internal/upbit.Record.
type recordLine struct {
	Kind       string          `json:"kind"`
	Market     string          `json:"market"`
	ReceivedAt time.Time       `json:"received_at"`
	Raw        json.RawMessage `json:"raw"`
	Reason     string          `json:"reason"`
}

// DecodeRecord turns one recorder line into an Event. A line that is not a
// record at all is an error; a frame inside it that this model cannot use is
// a Refused event, because a recording is evidence to be counted, not trusted
// to be clean.
func DecodeRecord(line []byte) (Event, error) {
	var rec recordLine
	if err := json.Unmarshal(line, &rec); err != nil {
		return Event{}, err
	}
	if rec.ReceivedAt.IsZero() || rec.Market == "" {
		return Event{}, errors.New("record without received_at or market")
	}
	ev := Event{Kind: rec.Kind, Market: rec.Market, At: rec.ReceivedAt.UTC(), Reason: rec.Reason}
	switch rec.Kind {
	case "orderbook":
		d, err := ParseDepth(rec.Raw, ev.At)
		if err != nil || d.Market != rec.Market {
			ev.Kind, ev.Reason = EventRefused, "orderbook"
			return ev, nil
		}
		ev.Kind, ev.Book = EventBook, d
	case EventTrade:
		t, err := ParseTrade(rec.Raw, ev.At)
		if err != nil || t.Market != rec.Market {
			ev.Kind, ev.Reason = EventRefused, "trade"
			return ev, nil
		}
		ev.Trade = t
	case EventGap, EventConnected:
	default:
		ev.Kind = EventOther
	}
	return ev, nil
}

// ReadRecords streams the records of one recorder file to fn in file order; a
// path ending in .gz is a closed day. It stops at the first error.
func ReadRecords(path string, fn func(Event) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var r io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		defer gz.Close()
		r = gz
	}
	br := bufio.NewReaderSize(r, 1<<20)
	for n := 1; ; n++ {
		line, err := br.ReadBytes('\n')
		if len(strings.TrimSpace(string(line))) > 0 {
			ev, derr := DecodeRecord(line)
			if derr != nil {
				return fmt.Errorf("%s line %d: %w", path, n, derr)
			}
			if ferr := fn(ev); ferr != nil {
				return fmt.Errorf("%s line %d: %w", path, n, ferr)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("%s: %w", path, err)
		}
	}
}
