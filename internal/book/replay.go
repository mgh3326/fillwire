package book

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
)

// EventType names a replay event.
type EventType string

const (
	EventSession   EventType = "session"
	EventBook      EventType = "book"
	EventTrade     EventType = "trade"
	EventGap       EventType = "gap"
	EventStale     EventType = "stale"
	EventReconnect EventType = "reconnect"
)

// BookState is one symbol's book after the latest applied snapshot.
type BookState struct {
	Symbol   string       `json:"symbol"`
	Hour     string       `json:"hour"`
	Asks     [Depth]Level `json:"asks"`
	Bids     [Depth]Level `json:"bids"`
	TotalAsk int64        `json:"total_ask"`
	TotalBid int64        `json:"total_bid"`
	AtNS     int64        `json:"at_ns"`
	// Synced is false from a reconnect or new session until this symbol's
	// next snapshot arrives. An executor must not act on an unsynced book.
	Synced bool `json:"synced"`
}

// Trade is one applied trade on the tape.
type Trade struct {
	Symbol  string `json:"symbol"`
	Hour    string `json:"hour"`
	Price   int64  `json:"price"`
	Qty     int64  `json:"qty"`
	AcmlVol int64  `json:"acml_vol"`
	Side    string `json:"side"`
	AtNS    int64  `json:"at_ns"`
}

// Event is one replay output, in evidence order. Session and Seq locate the
// evidence line it came from.
type Event struct {
	Type       EventType  `json:"type"`
	Session    int        `json:"session"`
	Seq        uint64     `json:"seq"`
	AtNS       int64      `json:"at_ns"`
	TR         string     `json:"tr,omitempty"`
	Symbol     string     `json:"symbol,omitempty"`
	Book       *BookState `json:"book,omitempty"`
	Trade      *Trade     `json:"trade,omitempty"`
	MissingVol int64      `json:"missing_vol,omitempty"` // EventGap
	SinceNS    int64      `json:"since_ns,omitempty"`    // EventStale
}

// Summary counts what a replay saw.
type Summary struct {
	Sessions   int   `json:"sessions"`
	Frames     int   `json:"frames"`
	Records    int   `json:"records"`
	Applied    int   `json:"applied"`
	Duplicates int   `json:"duplicates"`
	Regressed  int   `json:"regressed"`
	Gaps       int   `json:"gaps"`
	MissingVol int64 `json:"missing_vol"`
	Stale      int   `json:"stale"`
	Reconnects int   `json:"reconnects"`
	BadFrames  int   `json:"bad_frames"`
	// TornLines counts lines that are not valid JSON: the torn tail of a
	// crashed recorder. They are skipped.
	TornLines int `json:"torn_lines"`
	// VerdictMismatches counts frames whose recorded verdicts differ from the
	// ones replay recomputed. Replay always applies its own verdicts; a
	// non-zero count means the file was written by different detection code.
	VerdictMismatches int `json:"verdict_mismatches"`
}

// State is the replay result: the final books, the full tape, and counts.
type State struct {
	Books   map[string]BookState
	Tape    []Trade
	Summary Summary
}

// SortedBooks returns the final books ordered by symbol.
func (s State) SortedBooks() []BookState {
	books := make([]BookState, 0, len(s.Books))
	for _, b := range s.Books {
		books = append(books, b)
	}
	sort.Slice(books, func(i, j int) bool { return books[i].Symbol < books[j].Symbol })
	return books
}

// ErrFormat marks an evidence file this replay cannot read.
var ErrFormat = errors.New("book: unsupported evidence")

// Replay reads an evidence file and rebuilds book state and the trade tape.
// It recomputes every verdict from the raw frames with the same detection
// code the recorder uses, so the result depends only on the file's bytes.
// emit, if not nil, receives each event in order; an emit error stops the
// replay and is returned.
func Replay(r io.Reader, emit func(Event) error) (State, error) {
	rp := replayer{
		tracker: newTracker(),
		state:   State{Books: map[string]BookState{}},
		emit:    emit,
	}
	reader := bufio.NewReaderSize(r, 64*1024)
	for {
		chunk, readErr := reader.ReadBytes('\n')
		if len(bytes.TrimSpace(chunk)) > 0 {
			if err := rp.line(chunk); err != nil {
				return rp.state, err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return rp.state, nil
		}
		if readErr != nil {
			return rp.state, readErr
		}
	}
}

type replayer struct {
	tracker *tracker
	state   State
	session int
	emit    func(Event) error
}

func (rp *replayer) line(raw []byte) error {
	var l Line
	if err := json.Unmarshal(raw, &l); err != nil {
		rp.state.Summary.TornLines++
		return nil
	}
	if l.V != FormatVersion {
		return fmt.Errorf("%w: version %d (session %d seq %d)", ErrFormat, l.V, rp.session, l.Seq)
	}
	if l.Kind != KindSession && rp.session == 0 {
		return fmt.Errorf("%w: %s line before any session line", ErrFormat, l.Kind)
	}
	base := Event{Session: rp.session, Seq: l.Seq, AtNS: l.AtNS}
	switch l.Kind {
	case KindSession:
		rp.session++
		rp.state.Summary.Sessions++
		// A new session starts from a fresh tracker, as the recorder did, so
		// trade continuity is not checked across sessions.
		rp.tracker = newTracker()
		rp.unsyncBooks()
		base.Session, base.Type = rp.session, EventSession
		return rp.send(base)
	case KindReconnect:
		rp.state.Summary.Reconnects++
		rp.tracker.reconnect()
		rp.unsyncBooks()
		base.Type = EventReconnect
		return rp.send(base)
	case KindStale:
		rp.state.Summary.Stale++
		base.Type, base.TR, base.Symbol, base.SinceNS = EventStale, l.TR, l.Symbol, l.SinceNS
		return rp.send(base)
	case KindFrame:
		return rp.frame(l, base)
	}
	return fmt.Errorf("%w: kind %q (session %d seq %d)", ErrFormat, l.Kind, rp.session, l.Seq)
}

// unsyncBooks marks every book unsynced and emits nothing: the next snapshot
// of each symbol carries Synced=true again.
func (rp *replayer) unsyncBooks() {
	for symbol, b := range rp.state.Books {
		b.Synced = false
		rp.state.Books[symbol] = b
	}
}

func (rp *replayer) frame(l Line, base Event) error {
	sum := &rp.state.Summary
	sum.Frames++
	frame, err := ParseFrame(l.raw())
	if err != nil {
		sum.BadFrames++
		return nil
	}
	verdicts := rp.tracker.frame(frame)
	if !reflect.DeepEqual(verdicts, l.Verdicts) {
		sum.VerdictMismatches++
	}
	base.TR = frame.TR
	for i, v := range verdicts {
		sum.Records++
		switch v.Class {
		case ClassDuplicate:
			sum.Duplicates++
			continue
		case ClassRegress:
			sum.Regressed++
			continue
		case ClassGap:
			sum.Gaps++
			sum.MissingVol += v.MissingVol
			gap := base
			gap.Type, gap.Symbol, gap.MissingVol = EventGap, v.Symbol, v.MissingVol
			if err := rp.send(gap); err != nil {
				return err
			}
		}
		sum.Applied++
		event := base
		event.Symbol = v.Symbol
		if frame.TR == TRBook {
			r := frame.Books[i]
			b := BookState{Symbol: r.Symbol, Hour: r.Hour, Asks: r.Asks, Bids: r.Bids, TotalAsk: r.TotalAsk, TotalBid: r.TotalBid, AtNS: l.AtNS, Synced: true}
			rp.state.Books[r.Symbol] = b
			event.Type, event.Book = EventBook, &b
		} else {
			r := frame.Trades[i]
			t := Trade{Symbol: r.Symbol, Hour: r.Hour, Price: r.Price, Qty: r.Qty, AcmlVol: r.AcmlVol, Side: r.Side, AtNS: l.AtNS}
			rp.state.Tape = append(rp.state.Tape, t)
			event.Type, event.Trade = EventTrade, &t
		}
		if err := rp.send(event); err != nil {
			return err
		}
	}
	return nil
}

func (rp *replayer) send(e Event) error {
	if rp.emit == nil {
		return nil
	}
	return rp.emit(e)
}
