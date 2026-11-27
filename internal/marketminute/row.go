package marketminute

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
)

// RowJSON is the one-line shadow row. Its field order is the serialisation
// contract the Python golden fixtures pin: time_utc, session_date, symbol,
// venue, session_segment, source, open, high, low, close, volume, value.
type RowJSON struct {
	TimeUTC        string  `json:"time_utc"`
	SessionDate    string  `json:"session_date"`
	Symbol         string  `json:"symbol"`
	Venue          string  `json:"venue"`
	SessionSegment string  `json:"session_segment"`
	Source         string  `json:"source"`
	Open           string  `json:"open"`
	High           string  `json:"high"`
	Low            string  `json:"low"`
	Close          string  `json:"close"`
	Volume         string  `json:"volume"`
	Value          *string `json:"value"`
}

// Row converts a bar to the serialised shape. The time is RFC3339 UTC with a
// Z suffix and the session date is the bare date.
func (b Bar) Row() RowJSON {
	return RowJSON{
		TimeUTC:        b.TimeUTC.UTC().Format("2006-01-02T15:04:05Z"),
		SessionDate:    b.SessionDate.Format("2006-01-02"),
		Symbol:         b.Symbol,
		Venue:          b.Venue,
		SessionSegment: b.SessionSegment,
		Source:         b.Source,
		Open:           b.Open,
		High:           b.High,
		Low:            b.Low,
		Close:          b.Close,
		Volume:         b.Volume,
		Value:          b.Value,
	}
}

// MarshalRow encodes one row as compact JSON bytes without a trailing newline.
func MarshalRow(bar Bar) ([]byte, error) {
	return json.Marshal(bar.Row())
}

// Sink receives closed bars. Write must be safe for concurrent use.
type Sink interface {
	Write(context.Context, Bar) error
}

// MultiSink writes each bar to every sink in order and returns the first error.
type MultiSink []Sink

// Write implements Sink.
func (s MultiSink) Write(ctx context.Context, bar Bar) error {
	for _, sink := range s {
		if err := sink.Write(ctx, bar); err != nil {
			return err
		}
	}
	return nil
}

// Flush flushes every member that buffers rows, in order. Without it the
// poller's flush would see no buffer behind the production sink.
func (s MultiSink) Flush() error {
	var errs []error
	for _, sink := range s {
		if f, ok := sink.(flusher); ok {
			if err := f.Flush(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// ErrNoSink reports a poller with nowhere to write.
var ErrNoSink = errors.New("marketminute: no sink configured")

// lockedSink serialises writes so a JSON encoder is never shared unsafely.
type lockedSink struct {
	mu   sync.Mutex
	sink Sink
}

func (s *lockedSink) Write(ctx context.Context, bar Bar) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sink.Write(ctx, bar)
}
