package marketminute

import (
	"fmt"
	"sort"
	"time"
)

// The KIS US day-market (주간거래) minute source (hk 6347). It is off unless
// Config.USDayMarket is set.
//
// Confirmed from KIS's official samples, github.com/koreainvestment/
// open-trading-api at commit b0a0d33b7e5f863a8f3c6790f50adb840829c806:
//
//   - The overseas minute chart (해외주식분봉조회, v1_해외주식-030) is
//     HHDFS76950200, the transaction the regular US read already uses. Its
//     exchange codes include BAY (NYSE day), BAQ (NASDAQ day) and BAA (AMEX
//     day), and a day-market read returns at most one day of minute bars:
//     https://github.com/koreainvestment/open-trading-api/blob/b0a0d33b7e5f863a8f3c6790f50adb840829c806/examples_llm/overseas_stock/inquire_time_itemchartprice/inquire_time_itemchartprice.py#L51
//   - The sample's column names call kymd and khms the Korean date and time:
//     https://github.com/koreainvestment/open-trading-api/blob/b0a0d33b7e5f863a8f3c6790f50adb840829c806/examples_llm/overseas_stock/inquire_time_itemchartprice/chk_inquire_time_itemchartprice.py#L35-L38
//     Day rows are timed from that pair: the samples do not say which clock
//     xymd and xhms follow for a day-market code.
//
// Not confirmed, so not hard-coded: the session hours. The official samples
// disagree (10:00-16:00 KST in overseas_stock_functions_ws.py, 10:00-18:00 KST
// in legacy/Sample01/kis_ovrseastk.py), so the calendar file lists the windows.
//
// Not measured against the live API: that a day-market label is the bar's
// START, which is assumed from the regular read of the same transaction.

// SegmentUSDay labels a day-market bar that starts inside a calendar
// day-market window, as auto_trader's session_segment.US_DAY does.
const SegmentUSDay = "US_DAY"

// KIS day-market exchange codes. They are also the venue of the day rows, so a
// day-market bar never shares a key or a stream with a regular-venue bar.
const (
	VenueDayNASD = "BAQ"
	VenueDayNYSE = "BAY"
	VenueDayAMEX = "BAA"
)

var dayVenueOf = map[string]string{"NASD": VenueDayNASD, "NYSE": VenueDayNYSE, "AMEX": VenueDayAMEX}

func isDayVenue(venue string) bool {
	switch venue {
	case VenueDayNASD, VenueDayNYSE, VenueDayAMEX:
		return true
	default:
		return false
	}
}

// DayMarketEndpoint is the ratebudget endpoint of day-market reads: the same
// transaction and path as OverseasTR, with its own bucket and circuit. Configure
// it as Isolated so a failing day source cannot block the regular reads.
const DayMarketEndpoint = OverseasTR + ".US_DAY|" + OverseasPath

// DefaultDayMarketTrail is how long after a window ends the source still reads,
// so the window's last bars are collected. It matches auto_trader's
// KIS_DAY_MARKET_GRACE length.
const DefaultDayMarketTrail = 15 * time.Minute

// DefaultDayMarketBudget caps one day-market pass, waits and retries included,
// so a rate-limited day source delays the next regular poll by at most this
// much.
const DefaultDayMarketBudget = 30 * time.Second

// DayMarketState is the day market's own open or closed state. It comes only
// from the calendar's day-market windows: the regular session being closed (or
// open) says nothing about it.
type DayMarketState string

const (
	// DayMarketOpen means a listed window contains the instant.
	DayMarketOpen DayMarketState = "open"
	// DayMarketClosed means the instant falls between listed windows.
	DayMarketClosed DayMarketState = "closed"
	// DayMarketUncovered means the calendar lists nothing before or after the
	// instant, so it cannot say the market is closed.
	DayMarketUncovered DayMarketState = "uncovered"
)

// maxDayWindow bounds one window so a mistyped date cannot hold the market open
// for weeks.
const maxDayWindow = 24 * time.Hour

type dayWindowFile struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// dayWindow is start-inclusive and end-exclusive.
type dayWindow struct {
	start, end time.Time
}

// parseDayWindows returns the windows in start order. Overlapping windows are
// refused.
func parseDayWindows(raw []dayWindowFile) ([]dayWindow, error) {
	windows := make([]dayWindow, 0, len(raw))
	for _, item := range raw {
		start, startErr := time.Parse(time.RFC3339, item.Start)
		end, endErr := time.Parse(time.RFC3339, item.End)
		if startErr != nil || endErr != nil {
			return nil, fmt.Errorf("%w: day-market window %q to %q is not RFC3339", ErrCalendar, item.Start, item.End)
		}
		if !end.After(start) || end.Sub(start) > maxDayWindow {
			return nil, fmt.Errorf("%w: day-market window %q to %q must end after it starts and last at most %s", ErrCalendar, item.Start, item.End, maxDayWindow)
		}
		windows = append(windows, dayWindow{start: start, end: end})
	}
	sort.Slice(windows, func(i, j int) bool { return windows[i].start.Before(windows[j].start) })
	for index := 1; index < len(windows); index++ {
		if windows[index].start.Before(windows[index-1].end) {
			return nil, fmt.Errorf("%w: day-market windows overlap at %s", ErrCalendar, windows[index].start.Format(time.RFC3339))
		}
	}
	return windows, nil
}

// HasUSDayMarket reports whether the calendar lists any day-market window.
func (c *Calendar) HasUSDayMarket() bool { return len(c.usDayWindows) > 0 }

// USDayMarket is the day market's state at an instant.
func (c *Calendar) USDayMarket(at time.Time) DayMarketState {
	if c.inUSDayWindow(at, 0) {
		return DayMarketOpen
	}
	count := len(c.usDayWindows)
	if count == 0 || at.Before(c.usDayWindows[0].start) || !at.Before(c.usDayWindows[count-1].end) {
		return DayMarketUncovered
	}
	return DayMarketClosed
}

// inUSDayWindow reports whether at is inside a window whose end is extended by
// trail.
func (c *Calendar) inUSDayWindow(at time.Time, trail time.Duration) bool {
	for _, window := range c.usDayWindows {
		if !at.Before(window.start) && at.Before(window.end.Add(trail)) {
			return true
		}
	}
	return false
}
