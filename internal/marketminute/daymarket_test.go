package marketminute

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/ratebudget"
)

// Two day-market windows around US trading days 2026-11-24 and 2026-11-26. The
// second one belongs to a day the regular session is closed (Thanksgiving). The
// hours are fixture values, not a claim about KIS's hours.
const dayCalendarJSON = `{
  "us_closed": ["2026-11-26"],
  "us_day_market": [
    {"start": "2026-11-24T10:00:00+09:00", "end": "2026-11-24T18:00:00+09:00"},
    {"start": "2026-11-26T10:00:00+09:00", "end": "2026-11-26T18:00:00+09:00"}
  ]
}`

func dayCalendar(t *testing.T) *Calendar {
	t.Helper()
	calendar, err := LoadCalendar([]byte(dayCalendarJSON))
	if err != nil {
		t.Fatal(err)
	}
	return calendar
}

func kstAt(t *testing.T, value string) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339, value+"+09:00")
	if err != nil {
		t.Fatal(err)
	}
	return at
}

func TestRegularSessionClosedIsNotDayMarketClosed(t *testing.T) {
	calendar := dayCalendar(t)
	cases := []struct {
		name        string
		at          string
		regularOpen bool
		day         DayMarketState
		daySegment  string
	}{
		{"regular closed overnight, day market open", "2026-11-24T11:00:00", false, DayMarketOpen, SegmentUSDay},
		// 15:00 KST is 01:00 ET on the holiday itself.
		{"regular holiday, day market open", "2026-11-26T15:00:00", false, DayMarketOpen, SegmentUSDay},
		{"regular open, day market closed", "2026-11-25T00:00:00", true, DayMarketClosed, SegmentUnknown},
		{"both closed", "2026-11-25T11:00:00", false, DayMarketClosed, SegmentUnknown},
		{"window start is inside", "2026-11-24T10:00:00", false, DayMarketOpen, SegmentUSDay},
		{"window end is outside", "2026-11-24T18:00:00", false, DayMarketClosed, SegmentUnknown},
		{"before the listed windows", "2026-11-24T09:59:00", false, DayMarketUncovered, SegmentUnknown},
		{"after the listed windows", "2026-11-26T18:00:00", false, DayMarketUncovered, SegmentUnknown},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			at := kstAt(t, item.at)
			regular := calendar.Segment("us", "NASD", at)
			if (regular == SegmentUSRegular) != item.regularOpen {
				t.Fatalf("regular segment = %s, want open %v", regular, item.regularOpen)
			}
			if state := calendar.USDayMarket(at); state != item.day {
				t.Errorf("day market = %s, want %s", state, item.day)
			}
			for _, venue := range []string{VenueDayNASD, VenueDayNYSE, VenueDayAMEX} {
				if segment := calendar.Segment("us", venue, at); segment != item.daySegment {
					t.Errorf("Segment(us, %s) = %s, want %s", venue, segment, item.daySegment)
				}
			}
			// A day window never relabels a regular-venue bar.
			if regular == SegmentUSDay {
				t.Error("a regular venue was labelled US_DAY")
			}
		})
	}
}

func TestCalendarWithoutDayWindowsCannotSayClosed(t *testing.T) {
	calendar, err := LoadCalendar([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if calendar.HasUSDayMarket() || calendar.USDayMarket(kstAt(t, "2026-11-24T11:00:00")) != DayMarketUncovered {
		t.Fatal("a calendar without day windows must be uncovered")
	}
}

func TestCalendarRefusesBadDayWindows(t *testing.T) {
	window := func(start, end string) string { return `{"start":"` + start + `","end":"` + end + `"}` }
	good := window("2026-11-24T10:00:00+09:00", "2026-11-24T18:00:00+09:00")
	for name, windows := range map[string]string{
		"no offset":     window("2026-11-24T10:00:00", "2026-11-24T18:00:00"),
		"missing end":   `{"start":"2026-11-24T10:00:00+09:00"}`,
		"end first":     window("2026-11-24T18:00:00+09:00", "2026-11-24T10:00:00+09:00"),
		"empty":         window("2026-11-24T10:00:00+09:00", "2026-11-24T10:00:00+09:00"),
		"mistyped date": window("2026-11-24T10:00:00+09:00", "2026-12-24T18:00:00+09:00"),
		"overlap":       good + "," + window("2026-11-24T17:00:00+09:00", "2026-11-24T19:00:00+09:00"),
		"unknown field": `{"start":"2026-11-24T10:00:00+09:00","end":"2026-11-24T18:00:00+09:00","open":true}`,
	} {
		if _, err := LoadCalendar([]byte(`{"us_day_market":[` + windows + `]}`)); !errors.Is(err, ErrCalendar) {
			t.Errorf("%s: err = %v, want ErrCalendar", name, err)
		}
	}
}

// venueReader answers by venue and records the venues it was asked for.
type venueReader struct {
	scriptedReader
	fetch func(Target) FetchResult
}

func (r *venueReader) Fetch(_ context.Context, target Target) FetchResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, target)
	return r.fetch(target)
}

func (r *venueReader) venues() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var venues []string
	for _, call := range r.calls {
		venues = append(venues, call.Venue)
	}
	slices.Sort(venues)
	return venues
}

func labelled(label string) []FrameRow {
	return []FrameRow{{
		DateTime: label,
		Open:     new("100"), High: new("101"), Low: new("99"), Close: new("100"),
		Volume: new("10"), Value: new("1000"),
	}}
}

type dayHarness struct {
	poller *Poller
	reader *venueReader
	sink   *recordSink
	clock  *testClock
	waits  []time.Duration
}

func newDayHarness(t *testing.T, enabled bool, targets []Target, fetch func(Target) FetchResult) *dayHarness {
	t.Helper()
	harness := &dayHarness{reader: &venueReader{fetch: fetch}, sink: &recordSink{}, clock: &testClock{}}
	fast := ratebudget.Limit{Rate: 1000, Burst: 100}
	domestic, _ := Endpoint("KRX")
	overseas, _ := Endpoint("NASD")
	limiter, err := ratebudget.New(ratebudget.Config{
		Providers: []ratebudget.Provider{{
			Name: "kis", AccountRef: "live", Limit: fast,
			Endpoints: []ratebudget.Endpoint{
				{Name: domestic, Limit: fast},
				{Name: overseas, Limit: fast},
				{Name: DayMarketEndpoint, Limit: fast, Isolated: true},
			},
		}},
		ResearchShare: 1,
		Clock:         harness.clock,
		Jitter:        func(d time.Duration) time.Duration { return d },
	})
	if err != nil {
		t.Fatal(err)
	}
	harness.poller, err = NewPoller(Config{
		Targets: targets, Reader: harness.reader, Limiter: limiter, Sink: harness.sink,
		Calendar: dayCalendar(t), Provider: "kis", AccountRef: "live",
		Clock: harness.clock.Now, Workers: 1, USDayMarket: enabled,
		Sleep: func(_ context.Context, d time.Duration) error {
			harness.waits = append(harness.waits, d)
			harness.clock.advance(d)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return harness
}

func (h *dayHarness) runAt(t *testing.T, at time.Time) Report {
	t.Helper()
	h.clock.mu.Lock()
	h.clock.now = at
	h.clock.mu.Unlock()
	h.reader.mu.Lock()
	h.reader.calls = nil
	h.reader.mu.Unlock()
	report, err := h.poller.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return report
}

var usTargets = []Target{
	{Market: "us", Symbol: "NVDA", Venue: "NASD"},
	{Market: "us", Symbol: "IBM", Venue: "NYSE"},
	{Market: "us", Symbol: "IMO", Venue: "AMEX"},
	{Market: "kr", Symbol: "005930", Venue: "KRX"},
}

func noRows(Target) FetchResult { return FetchResult{} }

func TestDayMarketSourceIsOffByDefault(t *testing.T) {
	harness := newDayHarness(t, false, usTargets, noRows)
	report := harness.runAt(t, kstAt(t, "2026-11-24T11:00:00"))
	if got := harness.reader.venues(); !slices.Equal(got, []string{"AMEX", "KRX", "NASD", "NYSE"}) {
		t.Fatalf("venues read with the source off = %v", got)
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if report.USDayMarket != nil || strings.Contains(string(encoded), "us_day_market") {
		t.Fatalf("report mentions a source that is off: %s", encoded)
	}
}

func TestDayMarketPollsFollowTheDayMarketWindows(t *testing.T) {
	harness := newDayHarness(t, true, usTargets, func(target Target) FetchResult {
		if target.Venue == VenueDayNASD {
			return FetchResult{Rows: labelled("2026-11-24 10:58")}
		}
		return FetchResult{}
	})

	// Regular session closed (21:00 ET), day market open.
	report := harness.runAt(t, kstAt(t, "2026-11-24T11:00:00"))
	if got := harness.reader.venues(); !slices.Equal(got, []string{"AMEX", "BAA", "BAQ", "BAY", "KRX", "NASD", "NYSE"}) {
		t.Fatalf("venues in the day window = %v", got)
	}
	day := report.USDayMarket
	if day == nil || *day != (DayMarketReport{State: DayMarketOpen, Targets: 3, Read: true, Rows: 1}) {
		t.Fatalf("day report = %+v", day)
	}
	if report.Targets != 4 || report.Reads != 7 || report.Rows != 1 {
		t.Fatalf("report = %+v", report)
	}
	if len(harness.sink.bars) != 1 {
		t.Fatalf("bars = %+v", harness.sink.bars)
	}
	bar := harness.sink.bars[0]
	if bar.Venue != "BAQ" || bar.Symbol != "NVDA" || bar.SessionSegment != "US_DAY" || bar.Source != "KIS" ||
		!bar.TimeUTC.Equal(kstAt(t, "2026-11-24T10:58:00")) || bar.SessionDate.Format("2006-01-02") != "2026-11-24" {
		t.Fatalf("day bar = %+v", bar)
	}

	// Regular session open (10:00 ET), day market closed.
	report = harness.runAt(t, kstAt(t, "2026-11-25T00:00:00"))
	if got := harness.reader.venues(); !slices.Equal(got, []string{"AMEX", "KRX", "NASD", "NYSE"}) {
		t.Fatalf("venues outside the day window = %v", got)
	}
	if day = report.USDayMarket; day == nil || *day != (DayMarketReport{State: DayMarketClosed, Targets: 3}) {
		t.Fatalf("day report = %+v, want closed and unread", day)
	}

	// Regular holiday, day window listed: still read.
	report = harness.runAt(t, kstAt(t, "2026-11-26T15:00:00"))
	if day = report.USDayMarket; day == nil || day.State != DayMarketOpen || !day.Read {
		t.Fatalf("day report on the regular holiday = %+v", day)
	}
}

func TestDayMarketReadsTheLastBarsAfterTheWindowEnds(t *testing.T) {
	harness := newDayHarness(t, true, usTargets[:1], func(target Target) FetchResult {
		if target.Venue == VenueDayNASD {
			return FetchResult{Rows: labelled("2026-11-24 17:59")}
		}
		return FetchResult{}
	})
	report := harness.runAt(t, kstAt(t, "2026-11-24T18:05:00"))
	if day := report.USDayMarket; day == nil || *day != (DayMarketReport{State: DayMarketClosed, Targets: 1, Read: true, Rows: 1}) {
		t.Fatalf("day report in the trail = %+v", day)
	}
	if len(harness.sink.bars) != 1 || harness.sink.bars[0].SessionSegment != SegmentUSDay {
		t.Fatalf("bars = %+v", harness.sink.bars)
	}
	report = harness.runAt(t, kstAt(t, "2026-11-24T18:15:00"))
	if day := report.USDayMarket; day == nil || day.Read {
		t.Fatalf("day report past the trail = %+v", day)
	}
}

func TestFailingDayReadsDoNotHoldOrBlockTheRegularReads(t *testing.T) {
	var targets []Target
	for _, symbol := range []string{"A", "B", "C", "D", "E", "F", "G", "H"} {
		targets = append(targets, Target{Market: "us", Symbol: symbol, Venue: "NASD"})
	}
	targets = append(targets, Target{Market: "kr", Symbol: "005930", Venue: "KRX"})
	harness := newDayHarness(t, true, targets, func(target Target) FetchResult {
		if target.Venue == VenueDayNASD {
			return FetchResult{StatusCode: 200, Err: ErrResponseRefused}
		}
		return FetchResult{StatusCode: 200}
	})
	at := kstAt(t, "2026-11-24T11:00:00")
	report := harness.runAt(t, at)
	classes := map[string]int{}
	for _, failure := range report.Errors {
		if failure.Venue != VenueDayNASD {
			t.Fatalf("a regular target failed: %+v", failure)
		}
		classes[failure.Class]++
	}
	// Five refusals open the day circuit; the other three are skipped.
	if classes[classFetchError] != 5 || classes[classCircuitOpen] != 3 {
		t.Fatalf("day failures = %v", classes)
	}
	for _, wait := range harness.waits {
		if wait >= 30*time.Second {
			t.Fatalf("the run waited out a day circuit window: %v", harness.waits)
		}
	}
	report = harness.runAt(t, at.Add(time.Second))
	if got := harness.reader.venues(); len(got) != len(targets) || slices.Contains(got, VenueDayNASD) {
		t.Fatalf("venues while the day circuit is open = %v", got)
	}
	if report.Refusals[string(ratebudget.CodeCircuitOpen)] != 8 {
		t.Fatalf("refusals = %v, want the eight day targets only", report.Refusals)
	}
}

func TestNewPollerRefusesAListedDayVenueAndAMissingDayCalendar(t *testing.T) {
	empty, err := LoadCalendar([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	build := func(enabled bool, calendar *Calendar, venue string) error {
		clock := &testClock{}
		_, err := NewPoller(Config{
			Targets: []Target{{Market: "us", Symbol: "NVDA", Venue: venue}},
			Reader:  &scriptedReader{}, Limiter: testLimiter(t, clock, ratebudget.Limit{Rate: 1, Burst: 1}),
			Sink: &recordSink{}, Calendar: calendar, Provider: "kis", AccountRef: "live", USDayMarket: enabled,
		})
		return err
	}
	for _, enabled := range []bool{false, true} {
		for _, venue := range []string{VenueDayNASD, VenueDayNYSE, VenueDayAMEX} {
			if err := build(enabled, dayCalendar(t), venue); !errors.Is(err, ErrConfig) {
				t.Errorf("listed %s with the source %v: err = %v, want ErrConfig", venue, enabled, err)
			}
		}
	}
	if err := build(true, empty, "NASD"); !errors.Is(err, ErrConfig) {
		t.Errorf("source on without day windows: err = %v, want ErrConfig", err)
	}
	if err := build(false, empty, "NASD"); err != nil {
		t.Errorf("source off without day windows: %v", err)
	}
}
