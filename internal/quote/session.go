package quote

import "time"

// Session labels written to the stream's session field.
const (
	SessionRegular    = "regular"
	SessionAfterHours = "after_hours"
)

// KST is Korea Standard Time. Korea observes no daylight saving time, so a
// fixed zone avoids depending on tzdata inside a distroless image.
var KST = time.FixedZone("KST", 9*60*60)

// window is one daily KRX trading window in seconds after KST midnight. end is
// exclusive and sits one minute after the published close, so the minute
// labelled with the close (for example the 15:30 closing auction print) is
// inside the window.
type window struct {
	name       string
	start, end int
}

// windows are the only times the quote socket is open: KRX regular trading
// 09:00-15:30 and after-hours 16:00-20:00, Monday to Friday, both inclusive of
// their closing minute. Exchange holidays are not modelled; on a holiday the
// socket opens and receives nothing.
var windows = []window{
	{name: SessionRegular, start: 9 * 3600, end: 15*3600 + 31*60},
	{name: SessionAfterHours, start: 16 * 3600, end: 20*3600 + 1*60},
}

func tradingDay(t time.Time) bool {
	switch t.In(KST).Weekday() {
	case time.Saturday, time.Sunday:
		return false
	default:
		return true
	}
}

func secondsOfDay(t time.Time) int {
	t = t.In(KST)
	return t.Hour()*3600 + t.Minute()*60 + t.Second()
}

func kstMidnight(t time.Time) time.Time {
	t = t.In(KST)
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, KST)
}

// sessionForSecond labels a KST time of day, ignoring the weekday.
func sessionForSecond(second int) (string, bool) {
	for _, w := range windows {
		if second >= w.start && second < w.end {
			return w.name, true
		}
	}
	return "", false
}

// SessionAt reports the window containing t and when it closes.
func SessionAt(t time.Time) (name string, closes time.Time, ok bool) {
	if !tradingDay(t) {
		return "", time.Time{}, false
	}
	second := secondsOfDay(t)
	for _, w := range windows {
		if second >= w.start && second < w.end {
			return w.name, kstMidnight(t).Add(time.Duration(w.end) * time.Second), true
		}
	}
	return "", time.Time{}, false
}

// NextOpen returns the start of the first window that begins after t. When t
// is inside a window, that window is skipped: the caller is already open.
func NextOpen(t time.Time) time.Time {
	day := kstMidnight(t)
	for offset := 0; offset < 8; offset++ {
		// AddDate keeps the wall clock at midnight; KST has no DST gaps.
		candidate := day.AddDate(0, 0, offset)
		if !tradingDay(candidate) {
			continue
		}
		for _, w := range windows {
			start := candidate.Add(time.Duration(w.start) * time.Second)
			if start.After(t) {
				return start
			}
		}
	}
	// Unreachable: every eight-day span contains a weekday window.
	return t.Add(24 * time.Hour)
}
