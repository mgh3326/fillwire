package book

import "time"

// Class is the verdict on one book or trade record.
type Class string

const (
	// ClassOK records are applied.
	ClassOK Class = "ok"
	// ClassGap records are applied; MissingVol trades were never received.
	ClassGap Class = "gap"
	// ClassDuplicate records repeat what was already applied and are skipped.
	ClassDuplicate Class = "duplicate"
	// ClassRegress records are older than what was already applied and are
	// skipped.
	ClassRegress Class = "regress"
)

// Applied reports whether a record with this class changes book or tape.
func (c Class) Applied() bool { return c == ClassOK || c == ClassGap }

// Verdict classifies one record. KIS book and trade frames carry no sequence
// number, so continuity comes from the data itself:
//
//   - trade: ACML_VOL is the day's cumulative volume. A trade continues the
//     tape when ACML_VOL - CNTG_VOL equals the previous ACML_VOL. A larger
//     value is a gap of that many shares; an ACML_VOL already applied is a
//     duplicate; a smaller one is a regression.
//   - book: every H0STASP0 record is a full ten-level snapshot, so a missed
//     book frame loses no state. A record identical to the last applied one is
//     a duplicate; an earlier exchange time is a regression.
//
// After a reconnect every book is unsynced until its next snapshot, which is
// applied as a resync even when it is identical to the last one. The first trade per symbol after a reconnect is checked
// against the pre-reconnect cumulative volume, so trades lost during the
// outage show up as a gap; if ACML_VOL went backwards (a new trading day) the
// tape restarts from it.
//
// A frame received on a later KST calendar date than the previous one starts
// a new exchange day even without a reconnect: every book is unsynced and the
// first trade per symbol becomes that day's baseline (Resync, never a gap or
// regression).
type Verdict struct {
	Symbol     string `json:"symbol"`
	Class      Class  `json:"class"`
	Resync     bool   `json:"resync,omitempty"`
	MissingVol int64  `json:"missing_vol,omitempty"`
}

type bookCursor struct {
	text   string
	hour   string
	synced bool
}

type tradeCursor struct {
	acml    int64
	text    string
	resync  bool
	restart bool // new exchange day: the next trade is the baseline
}

// tracker holds the continuity state shared by the recorder (which writes
// verdicts as evidence) and replay (which recomputes them from raw frames).
type tracker struct {
	books  map[string]*bookCursor
	trades map[string]*tradeCursor
	day    int64 // KST calendar day of the latest frame, days since epoch
}

// kstOffset is KRX's fixed UTC offset; Korea has no daylight saving.
const kstOffset = 9 * time.Hour

func exchangeDay(atNS int64) int64 {
	return (atNS + int64(kstOffset)) / int64(24*time.Hour)
}

func newTracker() *tracker {
	return &tracker{books: map[string]*bookCursor{}, trades: map[string]*tradeCursor{}}
}

func (t *tracker) reconnect() {
	for _, c := range t.books {
		c.synced = false
	}
	for _, c := range t.trades {
		c.resync = true
	}
}

// frame classifies every record of f, received at atNS. newDay reports that
// f is the first frame of a later exchange day; the caller unsyncs its books.
func (t *tracker) frame(f Frame, atNS int64) (verdicts []Verdict, newDay bool) {
	if day := exchangeDay(atNS); day > t.day {
		newDay = t.day != 0
		t.day = day
		if newDay {
			t.startDay()
		}
	}
	verdicts = make([]Verdict, 0, len(f.Books)+len(f.Trades))
	for _, r := range f.Books {
		verdicts = append(verdicts, t.book(r))
	}
	for _, r := range f.Trades {
		verdicts = append(verdicts, t.trade(r))
	}
	return verdicts, newDay
}

func (t *tracker) startDay() {
	for _, c := range t.books {
		c.synced = false
	}
	for _, c := range t.trades {
		c.restart = true
	}
}

func (t *tracker) book(r BookRecord) Verdict {
	v := Verdict{Symbol: r.Symbol, Class: ClassOK}
	c, seen := t.books[r.Symbol]
	switch {
	case !seen:
		c = &bookCursor{}
		t.books[r.Symbol] = c
	case !c.synced:
		v.Resync = true
	case r.text == c.text:
		v.Class = ClassDuplicate
		return v
	case r.Hour < c.hour:
		v.Class = ClassRegress
		return v
	}
	c.text, c.hour, c.synced = r.text, r.Hour, true
	return v
}

func (t *tracker) trade(r TradeRecord) Verdict {
	v := Verdict{Symbol: r.Symbol, Class: ClassOK}
	c, seen := t.trades[r.Symbol]
	if !seen {
		t.trades[r.Symbol] = &tradeCursor{acml: r.AcmlVol, text: r.text}
		return v
	}
	if c.restart {
		*c = tradeCursor{acml: r.AcmlVol, text: r.text}
		v.Resync = true
		return v
	}
	// The first trade after a reconnect consumes the resync, even when it is a
	// byte-identical resend of the last one.
	if c.resync {
		v.Resync = true
		c.resync = false
		if r.AcmlVol < c.acml {
			c.acml, c.text = r.AcmlVol, r.text
			return v
		}
	}
	if r.text == c.text {
		v.Class = ClassDuplicate
		return v
	}
	switch previous := r.AcmlVol - r.Qty; {
	case r.AcmlVol == c.acml:
		v.Class = ClassDuplicate
		return v
	case r.AcmlVol < c.acml:
		v.Class = ClassRegress
		return v
	case previous > c.acml:
		v.Class = ClassGap
		v.MissingVol = previous - c.acml
	}
	c.acml, c.text = r.AcmlVol, r.text
	return v
}
