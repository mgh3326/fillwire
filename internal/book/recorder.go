package book

import (
	"errors"
	"io"
	"sort"
	"sync"
	"time"
)

// DefaultStaleAfter is how long a stream may stay silent before the recorder
// marks it stale.
const DefaultStaleAfter = 2 * time.Second

type streamKey struct{ tr, symbol string }

type streamState struct {
	lastNS int64
	stale  bool
}

// Recorder turns received frames into evidence lines. It is safe for
// concurrent use: a socket reader may call Frame and Reconnect while a ticker
// calls Tick.
//
// The caller supplies every timestamp, so a recorder driven by fixture frames
// and a fake clock writes the same bytes on every run.
type Recorder struct {
	mu         sync.Mutex
	w          io.Writer
	staleAfter time.Duration
	seq        uint64
	tracker    *tracker
	streams    map[streamKey]*streamState
	err        error
}

// NewRecorder starts a session on w (normally a file from OpenEvidence) and
// writes its session line. staleAfter <= 0 means DefaultStaleAfter.
func NewRecorder(w io.Writer, staleAfter time.Duration, now time.Time) (*Recorder, error) {
	if staleAfter <= 0 {
		staleAfter = DefaultStaleAfter
	}
	r := &Recorder{w: w, staleAfter: staleAfter, tracker: newTracker(), streams: map[streamKey]*streamState{}}
	r.write(Line{Kind: KindSession, AtNS: now.UnixNano(), StaleAfterMS: staleAfter.Milliseconds()})
	return r, r.err
}

// Frame records one raw websocket frame received at at. System frames and
// transactions other than H0STASP0/H0STCNT0 are ignored. A book or trade
// frame that does not parse is still recorded, with its error, and changes no
// state. The returned error is a write failure; once one occurs every later
// call returns it.
func (r *Recorder) Frame(raw []byte, at time.Time) error {
	frame, err := ParseFrame(raw)
	if errors.Is(err, ErrNotRealtime) || errors.Is(err, ErrUnsupportedTR) {
		return r.Err()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	line := Line{Kind: KindFrame, AtNS: at.UnixNano(), TR: frame.TR}
	setRaw(&line, raw)
	if err != nil {
		line.Error = err.Error()
		r.write(line)
		return r.err
	}
	line.Verdicts = r.tracker.frame(frame)
	for _, v := range line.Verdicts {
		r.arrive(streamKey{frame.TR, v.Symbol}, line.AtNS)
	}
	r.write(line)
	return r.err
}

// arrive notes a frame on a stream. A stream that was silent for longer than
// the threshold and not already flagged by Tick gets its stale line now,
// before the frame that ended the silence.
func (r *Recorder) arrive(key streamKey, atNS int64) {
	s, seen := r.streams[key]
	if !seen {
		r.streams[key] = &streamState{lastNS: atNS}
		return
	}
	if !s.stale && time.Duration(atNS-s.lastNS) > r.staleAfter {
		r.write(Line{Kind: KindStale, AtNS: atNS, TR: key.tr, Symbol: key.symbol, SinceNS: s.lastNS})
	}
	s.lastNS, s.stale = atNS, false
}

// Reconnect records that the socket reconnected at at. Every book is unsynced
// until its next snapshot.
func (r *Recorder) Reconnect(at time.Time, attempt int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.tracker.reconnect()
	r.write(Line{Kind: KindReconnect, AtNS: at.UnixNano(), Attempt: attempt})
	return r.err
}

// Tick flags every stream that has been silent for longer than the stale
// threshold at now. A stream is flagged once per silence.
func (r *Recorder) Tick(now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	keys := make([]streamKey, 0, len(r.streams))
	for key, s := range r.streams {
		if !s.stale && time.Duration(now.UnixNano()-s.lastNS) > r.staleAfter {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].tr != keys[j].tr {
			return keys[i].tr < keys[j].tr
		}
		return keys[i].symbol < keys[j].symbol
	})
	for _, key := range keys {
		s := r.streams[key]
		s.stale = true
		r.write(Line{Kind: KindStale, AtNS: now.UnixNano(), TR: key.tr, Symbol: key.symbol, SinceNS: s.lastNS})
	}
	return r.err
}

// Err returns the first write error, if any.
func (r *Recorder) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *Recorder) write(l Line) {
	if r.err != nil {
		return
	}
	r.seq++
	l.V, l.Seq = FormatVersion, r.seq
	r.err = writeLine(r.w, l)
}
