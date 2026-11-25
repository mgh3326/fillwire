package marketminute

import (
	"sort"
	"time"

	"github.com/mgh3326/fillwire/internal/kst"
)

// Source is the row's source label. The shadow poller only writes KIS rows.
const Source = "KIS"

// FrameRow is one row of a KIS minute-chart page: the bar START label in the
// exchange's local clock (KST for KR, ET for US) plus the raw numeric fields.
// Value is the KR ACCUMULATED trade value of the day; for US it is the per-bar
// value. A nil field is a value the page did not carry (or did not carry a
// number for).
type FrameRow struct {
	DateTime string  `json:"datetime"`
	Open     *string `json:"open"`
	High     *string `json:"high"`
	Low      *string `json:"low"`
	Close    *string `json:"close"`
	Volume   *string `json:"volume"`
	Value    *string `json:"value"`
}

// Bar is one closed minute row in the shadow shape. Decimal fields are exact
// decimal strings; Value is nil when the per-minute value cannot be proven.
type Bar struct {
	TimeUTC        time.Time `json:"-"`
	SessionDate    time.Time `json:"-"`
	Symbol         string    `json:"symbol"`
	Venue          string    `json:"venue"`
	SessionSegment string    `json:"session_segment"`
	Source         string    `json:"source"`
	Open           string    `json:"open"`
	High           string    `json:"high"`
	Low            string    `json:"low"`
	Close          string    `json:"close"`
	Volume         string    `json:"volume"`
	Value          *string   `json:"value"`
}

// FrameResult is what one page produced.
type FrameResult struct {
	Bars           []Bar
	SkippedForming int
	SkippedInvalid int
	// NullValue counts closed KR bars kept with a NULL value.
	NullValue int
}

const closeGrace = time.Minute + 10*time.Second

// isClosed reports whether a bar starting at startUTC is over by the 10 s
// grace at observedBefore. observedBefore is read BEFORE the request goes out.
func isClosed(startUTC, observedBefore time.Time) bool {
	return !startUTC.Add(closeGrace).After(observedBefore)
}

type parsedRow struct {
	start       time.Time
	open        *decimal
	high        *decimal
	low         *decimal
	close       *decimal
	volume      *decimal
	accumulated *decimal
}

func firstMinute(venue string) (int, bool) {
	switch venue {
	case "KRX":
		return 9 * 60, true
	case "NTX":
		return 8 * 60, true
	default:
		return 0, false
	}
}

// BarsFromFrame applies auto_trader session_minutes_collector.bars_from_frame
// to one page: KIS labels a bar by its START in the exchange's local clock and
// includes the still-forming minute, so a bar is kept only when
// start + 1 min + 10 s <= observedBefore.
//
// KR value (the page's acml_tr_pbmn) is the ACCUMULATED trade value of the day,
// not the per-minute amount (volume already is per minute). The per-minute
// value is proven only from the difference to the previous row of the same KST
// day, or, for the venue's first minute of the day (KRX 09:00, NTX 08:00),
// from the accumulated value itself. Everything else is NULL: the bar is kept,
// the value is not guessed. A row whose accumulated value went down, the next
// row, a row read without a usable accumulated value, and the row after a
// dropped invalid row are NULL too, because the difference would otherwise
// fold in a value that was not that minute's trade.
//
// US value is per bar and is stored as is.
func BarsFromFrame(rows []FrameRow, market, symbol, venue string, observedBefore time.Time, calendar *Calendar) FrameResult {
	result := FrameResult{}
	if len(rows) == 0 {
		return result
	}
	local := kst.Zone
	if market != "kr" {
		local = Eastern
	}
	parsedRows := make(map[int64]parsedRow, len(rows))
	starts := make([]time.Time, 0, len(rows))
	invalidStarts := make([]time.Time, 0, 4)
	for _, row := range rows {
		start, ok := parseBarLabel(row.DateTime, local)
		if !ok {
			result.SkippedInvalid++
			continue
		}
		key := start.UnixNano()
		if _, duplicate := parsedRows[key]; duplicate {
			continue
		}
		open, high, low, closeAt, volume := optionalDecimal(row.Open), optionalDecimal(row.High), optionalDecimal(row.Low), optionalDecimal(row.Close), optionalDecimal(row.Volume)
		accumulated := optionalDecimal(row.Value)
		if open == nil || high == nil || low == nil || closeAt == nil || volume == nil || (accumulated == nil && market != "kr") {
			result.SkippedInvalid++
			invalidStarts = append(invalidStarts, start)
			continue
		}
		if accumulated == nil {
			// The KR bar is usable, its accumulated value is not: this row acts
			// as a chain break for the next difference.
			invalidStarts = append(invalidStarts, start)
		}
		parsedRows[key] = parsedRow{start: start, open: open, high: high, low: low, close: closeAt, volume: volume, accumulated: accumulated}
		starts = append(starts, start)
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })

	var previous parsedRow
	havePrevious := false
	suspectNext := false
	for _, start := range starts {
		row := parsedRows[start.UnixNano()]
		var value *decimal
		if market == "kr" {
			value = krMinuteValue(row, previous, havePrevious, suspectNext, invalidStarts, venue, &suspectNext)
			if row.accumulated != nil {
				previous = row
				havePrevious = true
			}
			if value == nil && isClosed(start, observedBefore) {
				result.NullValue++
			}
		} else {
			value = row.accumulated
		}
		if !isClosed(start, observedBefore) {
			result.SkippedForming++
			continue
		}
		sessionDate := start.In(local)
		if market != "kr" {
			sessionDate = usSessionDate(start)
		}
		result.Bars = append(result.Bars, Bar{
			TimeUTC:        start,
			SessionDate:    time.Date(sessionDate.Year(), sessionDate.Month(), sessionDate.Day(), 0, 0, 0, 0, time.UTC),
			Symbol:         symbol,
			Venue:          venue,
			SessionSegment: calendar.Segment(market, venue, start),
			Source:         Source,
			Open:           row.open.String(),
			High:           row.high.String(),
			Low:            row.low.String(),
			Close:          row.close.String(),
			Volume:         row.volume.String(),
			Value:          decimalPointer(value),
		})
	}
	return result
}

// krMinuteValue reproduces the KR branch of bars_from_frame for one row.
func krMinuteValue(row parsedRow, previous parsedRow, havePrevious, suspectNext bool, invalidStarts []time.Time, venue string, nextSuspect *bool) *decimal {
	local := row.start.In(kst.Zone)
	sameDayPrevious := havePrevious && dayKey(previous.start, kst.Zone) == dayKey(row.start, kst.Zone)
	droppedBetween := false
	for _, dropped := range invalidStarts {
		if !dropped.Before(row.start) || dayKey(dropped, kst.Zone) != dayKey(row.start, kst.Zone) {
			continue
		}
		if !sameDayPrevious || dropped.After(previous.start) {
			droppedBetween = true
			break
		}
	}
	if !sameDayPrevious {
		suspectNext = false
	}
	var value *decimal
	decreased := false
	if !droppedBetween && !suspectNext && row.accumulated != nil {
		if sameDayPrevious {
			if row.accumulated.cmp(previous.accumulated) >= 0 {
				if difference, err := row.accumulated.sub(previous.accumulated); err == nil {
					value = difference
				}
			} else {
				decreased = true
			}
		} else if minute, ok := firstMinute(venue); ok && local.Hour()*60+local.Minute() == minute {
			value = row.accumulated
		}
	}
	*nextSuspect = decreased
	return value
}

func decimalPointer(value *decimal) *string {
	if value == nil {
		return nil
	}
	text := value.String()
	return &text
}

func optionalDecimal(raw *string) *decimal {
	if raw == nil {
		return nil
	}
	value, err := parseDecimal(*raw)
	if err != nil {
		return nil
	}
	return value
}

// parseBarLabel parses a KIS minute label in the exchange's local clock. The
// seconds are dropped because KIS labels are minute starts.
func parseBarLabel(raw string, location *time.Location) (time.Time, bool) {
	for _, layout := range []string{"2006-01-02 15:04", "2006-01-02 15:04:05"} {
		parsed, err := time.ParseInLocation(layout, raw, location)
		if err == nil {
			return parsed.UTC().Truncate(time.Minute), true
		}
	}
	return time.Time{}, false
}
