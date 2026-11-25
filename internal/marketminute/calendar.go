package marketminute

import (
	"errors"
	"fmt"
	"time"

	// The distroless image has no zoneinfo; embed it for America/New_York.
	_ "time/tzdata"

	"github.com/mgh3326/fillwire/internal/jsonx"
	"github.com/mgh3326/fillwire/internal/kst"
)

// Session labels written to the shadow rows. They are the vocabulary of
// auto_trader's session_segment_of; UNKNOWN is a value, not an error.
const (
	SegmentUnknown    = "UNKNOWN"
	SegmentKRXRegular = "KRX_REGULAR"
	SegmentKRXAfter   = "KRX_AFTER"
	SegmentNXTPre     = "NXT_PRE"
	SegmentNXTOverlap = "NXT_OVERLAP"
	SegmentNXTPost    = "NXT_POST"
	SegmentUSPre      = "US_PRE"
	SegmentUSRegular  = "US_REGULAR"
	SegmentUSPost     = "US_POST"
)

// ErrCalendar is wrapped by every calendar configuration the loader refuses.
var ErrCalendar = errors.New("marketminute: invalid calendar")

// Eastern is US Eastern time, for the US session windows.
var Eastern = mustLoadLocation("America/New_York")

func mustLoadLocation(name string) *time.Location {
	location, err := time.LoadLocation(name)
	if err != nil {
		panic("marketminute: embedded tzdata lacks " + name)
	}
	return location
}

// dayKey is the local calendar date of an instant in location.
func dayKey(at time.Time, location *time.Location) string {
	return at.In(location).Format("2006-01-02")
}

type calendarFile struct {
	KRClosed     []string          `json:"kr_closed"`
	USClosed     []string          `json:"us_closed"`
	USEarlyClose map[string]string `json:"us_early_close"`
}

type clockTime struct {
	hour, minute int
}

// Calendar is the exchange calendar the shadow poller labels bars with.
//
// Go cannot reproduce auto_trader's exchange_calendars data, so the operator
// supplies the closed days and US early closes from that same calendar. Every
// date the file does not positively open is closed except weekends, and a
// missing date is never guessed into a session (auto_trader's fail-closed
// contract). LoadCalendar refuses any malformed entry.
type Calendar struct {
	krClosed     map[string]bool
	usClosed     map[string]bool
	usEarlyClose map[string]clockTime
}

// LoadCalendar parses the calendar JSON. Unknown fields are refused so a
// misspelled key cannot silently open a closed day.
func LoadCalendar(data []byte) (*Calendar, error) {
	var file calendarFile
	if err := jsonx.DecodeStrict(data, &file); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCalendar, err)
	}
	calendar := &Calendar{
		krClosed:     make(map[string]bool, len(file.KRClosed)),
		usClosed:     make(map[string]bool, len(file.USClosed)),
		usEarlyClose: make(map[string]clockTime, len(file.USEarlyClose)),
	}
	for _, day := range file.KRClosed {
		key, err := parseCalendarDay(day)
		if err != nil {
			return nil, err
		}
		calendar.krClosed[key] = true
	}
	for _, day := range file.USClosed {
		key, err := parseCalendarDay(day)
		if err != nil {
			return nil, err
		}
		calendar.usClosed[key] = true
	}
	for day, raw := range file.USEarlyClose {
		key, err := parseCalendarDay(day)
		if err != nil {
			return nil, err
		}
		clock, err := parseClockTime(raw)
		if err != nil {
			return nil, err
		}
		calendar.usEarlyClose[key] = clock
	}
	return calendar, nil
}

func parseCalendarDay(raw string) (string, error) {
	day, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return "", fmt.Errorf("%w: closed day %q is not YYYY-MM-DD", ErrCalendar, raw)
	}
	return day.Format("2006-01-02"), nil
}

func parseClockTime(raw string) (clockTime, error) {
	parsed, err := time.Parse("15:04", raw)
	if err != nil {
		return clockTime{}, fmt.Errorf("%w: early close %q is not HH:MM", ErrCalendar, raw)
	}
	return clockTime{hour: parsed.Hour(), minute: parsed.Minute()}, nil
}

func weekday(day time.Time) bool {
	switch day.Weekday() {
	case time.Saturday, time.Sunday:
		return false
	default:
		return true
	}
}

func (c *Calendar) krTradingDay(day time.Time) bool {
	return weekday(day) && !c.krClosed[day.Format("2006-01-02")]
}

// usRegularBounds returns the XNYS regular-session bounds for the ET date of
// day. The close is 16:00 ET except on an early-close day.
func (c *Calendar) usRegularBounds(day time.Time) (time.Time, time.Time, bool) {
	local := day.In(Eastern)
	if !weekday(local) || c.usClosed[local.Format("2006-01-02")] {
		return time.Time{}, time.Time{}, false
	}
	open := time.Date(local.Year(), local.Month(), local.Day(), 9, 30, 0, 0, Eastern)
	closeHour, closeMinute := 16, 0
	if early, ok := c.usEarlyClose[local.Format("2006-01-02")]; ok {
		closeHour, closeMinute = early.hour, early.minute
	}
	closeAt := time.Date(local.Year(), local.Month(), local.Day(), closeHour, closeMinute, 0, 0, Eastern)
	return open, closeAt, true
}

func within(clock, start, end int) bool { return start <= clock && clock < end }

// Segment is session_segment_of(market, venue, at): the session label of the
// one-minute bar that STARTS at at. Windows are start-inclusive and
// end-exclusive. Every unmodelled window, a non-trading day, an unknown venue
// and an unknown market are UNKNOWN.
func (c *Calendar) Segment(market, venue string, at time.Time) string {
	switch market {
	case "kr":
		return c.krSegment(venue, at)
	case "us":
		return c.usSegment(venue, at)
	default:
		return SegmentUnknown
	}
}

func (c *Calendar) krSegment(venue string, at time.Time) string {
	local := at.In(kst.Zone)
	if !c.krTradingDay(local) {
		return SegmentUnknown
	}
	clock := local.Hour()*60 + local.Minute()
	const (
		krxRegularOpen = 9 * 60
		krxRegularEnd  = 15*60 + 30
		krxAfterOpen   = 16 * 60
		krxAfterEnd    = 20 * 60
		nxtPreOpen     = 8 * 60
		nxtPreEnd      = 8*60 + 50
		nxtMainOpen    = 9 * 60
		nxtMainEnd     = 15*60 + 20
		nxtPostOpen    = 16 * 60
		nxtPostEnd     = 20 * 60
	)
	switch venue {
	case "KRX":
		switch {
		case within(clock, krxRegularOpen, krxRegularEnd):
			return SegmentKRXRegular
		case within(clock, krxAfterOpen, krxAfterEnd):
			return SegmentKRXAfter
		}
	case "NTX":
		switch {
		case within(clock, nxtPreOpen, nxtPreEnd):
			return SegmentNXTPre
		case within(clock, nxtMainOpen, nxtMainEnd):
			return SegmentNXTOverlap
		case within(clock, nxtPostOpen, nxtPostEnd):
			return SegmentNXTPost
		}
	}
	return SegmentUnknown
}

func (c *Calendar) usSegment(venue string, at time.Time) string {
	switch venue {
	case "NASD", "NYSE", "AMEX":
	default:
		return SegmentUnknown
	}
	local := at.In(Eastern)
	open, closeAt, ok := c.usRegularBounds(local)
	if !ok {
		return SegmentUnknown
	}
	utc := at.UTC()
	switch {
	case !utc.Before(open.UTC()) && utc.Before(closeAt.UTC()):
		return SegmentUSRegular
	}
	openLocal, closeLocal := open.In(Eastern), closeAt.In(Eastern)
	preOpen := time.Date(local.Year(), local.Month(), local.Day(), 4, 0, 0, 0, Eastern)
	afterClose := time.Date(local.Year(), local.Month(), local.Day(), 20, 0, 0, 0, Eastern)
	switch {
	case !local.Before(preOpen) && local.Before(openLocal):
		return SegmentUSPre
	case !local.Before(closeLocal) && local.Before(afterClose):
		return SegmentUSPost
	default:
		return SegmentUnknown
	}
}

// usSessionDate is the ET trading day of an instant: it rolls over at 20:00 ET,
// as auto_trader's session_segment.us_session_date does.
func usSessionDate(at time.Time) time.Time {
	local := at.In(Eastern)
	day := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, Eastern)
	if local.Hour() >= 20 {
		day = day.AddDate(0, 0, 1)
	}
	return day
}
