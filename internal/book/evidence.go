package book

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"unicode/utf8"
)

// FormatVersion is the evidence line format. Replay rejects any other.
const FormatVersion = 1

// Kind names an evidence line.
type Kind string

const (
	// KindSession opens one recording session. A file may hold several.
	KindSession Kind = "session"
	// KindFrame is one received book or trade frame, verbatim, with the
	// recorder's verdict on each of its records.
	KindFrame Kind = "frame"
	// KindReconnect marks a socket reconnect; books resync after it.
	KindReconnect Kind = "reconnect"
	// KindStale marks a stream silent for longer than the stale threshold.
	KindStale Kind = "stale"
)

// Line is one evidence record. Every line is one JSON object terminated by
// '\n' and written with a single write, so a crash leaves at most one torn
// line at the end of the file.
type Line struct {
	V    int    `json:"v"`
	Seq  uint64 `json:"seq"` // 1-based, restarts with each session
	Kind Kind   `json:"kind"`
	AtNS int64  `json:"at_ns"` // receive (or observation) time, Unix ns

	// KindSession
	StaleAfterMS int64 `json:"stale_after_ms,omitempty"`
	// KindReconnect
	Attempt int `json:"attempt,omitempty"`
	// KindStale: the stream, and when it last delivered a frame.
	Symbol  string `json:"symbol,omitempty"`
	SinceNS int64  `json:"since_ns,omitempty"`

	// KindFrame (TR is also set on KindStale)
	TR string `json:"tr,omitempty"`
	// Raw is the frame as received. A frame that is not valid UTF-8 is kept
	// byte-exact in RawBytes (base64) instead, never lossily re-encoded.
	Raw      string    `json:"raw,omitempty"`
	RawBytes []byte    `json:"raw_bytes,omitempty"`
	Error    string    `json:"error,omitempty"` // set when the frame did not parse
	Verdicts []Verdict `json:"verdicts,omitempty"`
}

func (l Line) raw() []byte {
	if l.RawBytes != nil {
		return l.RawBytes
	}
	return []byte(l.Raw)
}

func setRaw(l *Line, raw []byte) {
	if utf8.Valid(raw) {
		l.Raw = string(raw)
		return
	}
	l.RawBytes = append([]byte(nil), raw...)
}

func writeLine(w io.Writer, l Line) error {
	encoded, err := json.Marshal(l)
	if err != nil {
		return err
	}
	_, err = w.Write(append(encoded, '\n'))
	return err
}

// OpenEvidence opens path for appending, creating it if needed. Existing
// bytes are never rewritten or truncated. If a previous run left a torn last
// line, a newline is appended first so the next session starts on its own
// line; replay counts the torn line and skips it.
func OpenEvidence(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o640)
	if err != nil {
		return nil, err
	}
	torn, err := endsTorn(path)
	if err == nil && torn {
		_, err = f.Write([]byte{'\n'})
	}
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("book: open evidence: %w", err)
	}
	return f, nil
}

func endsTorn(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() == 0 {
		return false, err
	}
	last := make([]byte, 1)
	if _, err := f.ReadAt(last, info.Size()-1); err != nil {
		return false, err
	}
	return last[0] != '\n', nil
}
