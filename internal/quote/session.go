package quote

import (
	"sort"
	"time"
	// The distroless image has no zoneinfo; embed it for America/New_York.
	_ "time/tzdata"
)

// Session labels written to the stream's session field.
const (
	SessionNXTPre     = "nxt_pre"
	SessionKRXRegular = "krx_regular"
	SessionNXTAfter   = "nxt_after"
	SessionUSPre      = "us_pre"
	SessionUSRegular  = "us_regular"
	SessionUSAfter    = "us_after"
)

// KST is Korea Standard Time. Korea observes no daylight saving time.
var KST = time.FixedZone("KST", 9*60*60)

// Eastern is US Eastern time, for the US session windows.
var Eastern = mustLoad("America/New_York")

func mustLoad(name string) *time.Location {
	location, err := time.LoadLocation(name)
	if err != nil {
		panic("quote: embedded tzdata lacks " + name)
	}
	return location
}

// window is one daily session in seconds after local midnight, end exclusive.
type window struct {
	name       string
	start, end int
}

const minute = 60

// Session boundaries follow auto_trader: KR from classify_kr_accept_session
// (app/services/brokers/kis/live_order_expiry.py: premarket 08:00-08:50,
// regular 09:00-15:30, nxt_after 16:00-20:00 KST) and US from
// us_market_session (app/mcp_server/tooling/market_session.py: premarket
// from 04:00 ET, regular to 16:00 ET, afterhours to 20:00 ET). A window that
// is followed by a gap also includes its closing minute, so a print stamped
// with the close (for example the 15:30 closing auction) is kept. Exchange
// holidays, early closes, the NXT 15:40-16:00 after-market start, and the US
// day market are not modelled; ticks there are dropped as out of session.
var sessionWindows = map[string][]window{
	MarketKR: {
		{name: SessionNXTPre, start: 8 * 60 * minute, end: 8*60*minute + 51*minute},
		{name: SessionKRXRegular, start: 9 * 60 * minute, end: 15*60*minute + 31*minute},
		{name: SessionNXTAfter, start: 16 * 60 * minute, end: 20*60*minute + 1*minute},
	},
	MarketUS: {
		{name: SessionUSPre, start: 4 * 60 * minute, end: 9*60*minute + 30*minute},
		{name: SessionUSRegular, start: 9*60*minute + 30*minute, end: 16 * 60 * minute},
		{name: SessionUSAfter, start: 16 * 60 * minute, end: 20*60*minute + 1*minute},
	},
}

func marketLocation(market string) *time.Location {
	if market == MarketUS {
		return Eastern
	}
	return KST
}

func weekday(t time.Time) bool {
	switch t.Weekday() {
	case time.Saturday, time.Sunday:
		return false
	default:
		return true
	}
}

// SessionLabel labels a tick's own timestamp for its market. It reports
// false outside every session window, including weekends in the market's
// own time zone.
func SessionLabel(market string, t time.Time) (string, bool) {
	windows, ok := sessionWindows[market]
	if !ok {
		return "", false
	}
	local := t.In(marketLocation(market))
	if !weekday(local) {
		return "", false
	}
	second := local.Hour()*3600 + local.Minute()*60 + local.Second()
	for _, w := range windows {
		if second >= w.start && second < w.end {
			return w.name, true
		}
	}
	return "", false
}

// mergeGap joins socket windows separated by a shorter pause (for example
// KRX 15:31-16:00), so the socket is not closed and reopened for a gap in
// which no labelled tick can arrive anyway.
const mergeGap = 30 * time.Minute

type interval struct{ start, end time.Time }

// Schedule is the union of the session windows of the configured markets:
// the times the quote socket is open.
type Schedule struct{ markets []string }

// NewSchedule covers the given markets. Unknown markets are ignored.
func NewSchedule(markets []string) Schedule {
	var known []string
	for _, market := range markets {
		if _, ok := sessionWindows[market]; ok {
			known = append(known, market)
		}
	}
	return Schedule{markets: known}
}

// intervals returns the merged socket windows touching [t-2d, t+9d].
func (s Schedule) intervals(t time.Time) []interval {
	var all []interval
	for _, market := range s.markets {
		location := marketLocation(market)
		local := t.In(location)
		for offset := -2; offset <= 9; offset++ {
			day := time.Date(local.Year(), local.Month(), local.Day()+offset, 0, 0, 0, 0, location)
			if !weekday(day) {
				continue
			}
			for _, w := range sessionWindows[market] {
				start := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, w.start, 0, location)
				end := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, w.end, 0, location)
				all = append(all, interval{start: start, end: end})
			}
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].start.Before(all[j].start) })
	var merged []interval
	for _, next := range all {
		if last := len(merged) - 1; last >= 0 && !next.start.After(merged[last].end.Add(mergeGap)) {
			if next.end.After(merged[last].end) {
				merged[last].end = next.end
			}
			continue
		}
		merged = append(merged, next)
	}
	return merged
}

// At reports whether the socket window is open at t and when it closes.
func (s Schedule) At(t time.Time) (closes time.Time, open bool) {
	for _, window := range s.intervals(t) {
		if !t.Before(window.start) && t.Before(window.end) {
			return window.end, true
		}
	}
	return time.Time{}, false
}

// NextOpen returns the start of the first socket window that begins after t.
func (s Schedule) NextOpen(t time.Time) time.Time {
	for _, window := range s.intervals(t) {
		if window.start.After(t) {
			return window.start
		}
	}
	// Unreachable with a configured market: every eleven-day span contains a
	// weekday window.
	return t.Add(24 * time.Hour)
}
