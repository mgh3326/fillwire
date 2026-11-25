package kisfutures

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/mgh3326/fillwire/internal/jsonx"
	"github.com/mgh3326/fillwire/internal/kst"
)

// The session rules follow the futures branch of auto_trader
// app/services/research_candles/session_segment.py (hk 6148) at origin/main
// 68b7ca593:
//
//	08:45-15:45 FUT_DAY on a confirmed XKRX trading day; session_date = that day
//	18:00-06:00 FUT_NIGHT: the night that STARTED on KST day D (bars before
//	06:00 belong to the previous day's night); D must be a trading day and
//	session_date is the next trading day after D
//	anything else, a non-trading D, or a calendar that cannot confirm the next
//	trading day: UNKNOWN with no session_date
//
// Windows are start-inclusive and end-exclusive on the bar START.

// Session segments.
const (
	SegmentDay     = "FUT_DAY"
	SegmentNight   = "FUT_NIGHT"
	SegmentUnknown = "UNKNOWN"
)

const (
	dayOpen    = 8*60 + 45
	dayClose   = 15*60 + 45
	nightOpen  = 18 * 60
	nightClose = 6 * 60
	// nextDaySearch is how many calendar days are searched for the trading day
	// after a night session start.
	nextDaySearch = 10
)

// ErrCalendar is wrapped by every calendar the loader refuses.
var ErrCalendar = errors.New("kisfutures: invalid calendar")

// Calendar is the XKRX trading-day calendar. auto_trader reads it from
// exchange_calendars; here the operator supplies the closed weekdays of a
// covered range from that same calendar. A day outside the range cannot be
// confirmed and is not a trading day, so its bars are UNKNOWN.
type Calendar struct {
	from, to time.Time
	closed   map[time.Time]bool
}

type calendarFile struct {
	From     string    `json:"from"`
	To       string    `json:"to"`
	KRClosed *[]string `json:"kr_closed"`
}

// ParseCalendar parses {"from":"2026-01-01","to":"2026-12-31","kr_closed":[…]}.
// Unknown or missing keys, malformed dates and a closed day outside the range
// are refused.
func ParseCalendar(data []byte) (*Calendar, error) {
	var file calendarFile
	if err := jsonx.DecodeStrict(data, &file); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCalendar, err)
	}
	if file.KRClosed == nil {
		return nil, fmt.Errorf("%w: kr_closed is required", ErrCalendar)
	}
	parse := func(raw string) (time.Time, error) {
		day, err := time.Parse(time.DateOnly, raw)
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: %q is not YYYY-MM-DD", ErrCalendar, raw)
		}
		return day, nil
	}
	from, err := parse(file.From)
	if err != nil {
		return nil, err
	}
	to, err := parse(file.To)
	if err != nil {
		return nil, err
	}
	if to.Before(from) {
		return nil, fmt.Errorf("%w: to is before from", ErrCalendar)
	}
	calendar := &Calendar{from: from, to: to, closed: make(map[time.Time]bool, len(*file.KRClosed))}
	for _, raw := range *file.KRClosed {
		day, err := parse(raw)
		if err != nil {
			return nil, err
		}
		if day.Before(from) || day.After(to) {
			return nil, fmt.Errorf("%w: closed day %s is outside the covered range", ErrCalendar, raw)
		}
		calendar.closed[day] = true
	}
	return calendar, nil
}

// LoadCalendar reads and parses the calendar file.
func LoadCalendar(path string) (*Calendar, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCalendar, err)
	}
	return ParseCalendar(data)
}

// Covers reports whether day lies in the calendar's range.
func (c *Calendar) Covers(day time.Time) bool {
	return !day.Before(c.from) && !day.After(c.to)
}

// tradingDay reports whether day (midnight UTC) is a confirmed session.
func (c *Calendar) tradingDay(day time.Time) bool {
	if !c.Covers(day) || day.Weekday() == time.Saturday || day.Weekday() == time.Sunday {
		return false
	}
	return !c.closed[day]
}

func (c *Calendar) nextTradingDay(day time.Time) (time.Time, bool) {
	for offset := 1; offset <= nextDaySearch; offset++ {
		if candidate := day.AddDate(0, 0, offset); c.tradingDay(candidate) {
			return candidate, true
		}
	}
	return time.Time{}, false
}

// kstParts splits an instant into its KST date and minute of the day.
func kstParts(at time.Time) (date time.Time, minute int) {
	local := at.In(kst.Zone)
	return Date(local.Year(), local.Month(), local.Day()), local.Hour()*60 + local.Minute()
}

// Classify labels the bar that starts at start. sessionDate is the zero time
// exactly when the segment is UNKNOWN.
func (c *Calendar) Classify(start time.Time) (segment string, sessionDate time.Time) {
	date, minute := kstParts(start)
	switch {
	case minute >= dayOpen && minute < dayClose:
		if c.tradingDay(date) {
			return SegmentDay, date
		}
	case minute >= nightOpen || minute < nightClose:
		startDay := date
		if minute < nightClose {
			startDay = date.AddDate(0, 0, -1)
		}
		if c.tradingDay(startDay) {
			if next, ok := c.nextTradingDay(startDay); ok {
				return SegmentNight, next
			}
		}
	}
	return SegmentUnknown, time.Time{}
}

// ContractDate is the date whose front and next contracts are subscribed at
// now: the trading date of the running or upcoming session, so that after the
// day close the night session already holds the contracts of its session_date.
// When the calendar cannot decide, it is the KST date, which is what
// auto_trader's collector resolves on.
func (c *Calendar) ContractDate(now time.Time) time.Time {
	date, minute := kstParts(now)
	after := date
	switch {
	case minute < nightClose:
		after = date.AddDate(0, 0, -1)
	case minute < dayClose:
		return date
	}
	if next, ok := c.nextTradingDay(after); ok {
		return next
	}
	return date
}
