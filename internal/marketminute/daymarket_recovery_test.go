package marketminute

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/ratebudget"
)

// newDayLimiter is the limiter the day-market tests share: the regular
// endpoints and the isolated day endpoint, as production configures them.
func newDayLimiter(t *testing.T, clock ratebudget.Clock) *ratebudget.Limiter {
	t.Helper()
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
		Clock:         clock,
		Jitter:        func(d time.Duration) time.Duration { return d },
	})
	if err != nil {
		t.Fatal(err)
	}
	return limiter
}

// newDayPoller builds a poller with the day-market source on. Its sleeps
// advance the test clock, so a wait is visible without real time passing.
func newDayPoller(t *testing.T, clock *testClock, sink Sink, reader Reader, targets []Target, workers int) *Poller {
	t.Helper()
	poller, err := NewPoller(Config{
		Targets: targets, Reader: reader, Limiter: newDayLimiter(t, clock), Sink: sink,
		Calendar: dayCalendar(t), Provider: "kis", AccountRef: "live",
		Clock: clock.Now, Workers: workers, USDayMarket: true,
		Sleep: func(_ context.Context, d time.Duration) error {
			clock.advance(d)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return poller
}

// dayErrorClasses maps each day-venue symbol that failed to its class.
func dayErrorClasses(report Report) map[string]string {
	classes := map[string]string{}
	for _, failure := range report.Errors {
		if failure.Venue == VenueDayNASD {
			classes[failure.Symbol] = failure.Class
		}
	}
	return classes
}

// logSink records writes and flushes in the order they happen. Writing
// failSymbol's bar fails.
type logSink struct {
	mu         sync.Mutex
	events     []string
	failSymbol string
}

func (s *logSink) Write(_ context.Context, bar Bar) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if bar.Symbol == s.failSymbol {
		return errors.New("disk full")
	}
	s.events = append(s.events, "write:"+bar.Symbol)
	return nil
}

func (s *logSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, "flush")
	return nil
}

// The first day target fails every time, with four workers. Its failed probe
// keeps the day circuit shut for the next window, so that pass skips the rest.
// Once the window passes again the probe rotates to a different target, and
// every other day target is polled.
func TestDayPassPollsTheOtherTargetsAfterAFailingFirstTarget(t *testing.T) {
	clock := &testClock{now: kstAt(t, "2026-11-24T11:00:00")}
	var dayTargets []Target
	for _, symbol := range []string{"A", "B", "C", "D", "E", "F", "G", "H"} {
		dayTargets = append(dayTargets, Target{Market: "us", Symbol: symbol, Venue: "NASD"})
	}
	targets := append([]Target{{Market: "kr", Symbol: "005930", Venue: "KRX"}}, dayTargets...)
	reader := &venueReader{fetch: func(target Target) FetchResult {
		switch {
		case target.Venue != VenueDayNASD:
			return FetchResult{}
		case target.Symbol == "A":
			return FetchResult{Err: ErrResponseRefused}
		default:
			return FetchResult{Rows: labelled("2026-11-24 10:58")}
		}
	}}
	sink := &recordSink{}
	poller := newDayPoller(t, clock, sink, reader, targets, 4)
	// Five failures open the day circuit (the default OpenAfter). The day pass
	// reads the derived day venue, so the key is that venue's.
	dayKey := poller.Key(Target{Market: "us", Symbol: "A", Venue: VenueDayNASD})
	for range 5 {
		poller.config.Limiter.Observe(dayKey, ratebudget.Outcome{Err: ErrResponseRefused})
	}

	// The circuit's window has passed: A takes the probe and fails.
	clock.advance(30 * time.Second)
	first, err := poller.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"A": classFetchError, "B": classCircuitOpen, "C": classCircuitOpen, "D": classCircuitOpen, "E": classCircuitOpen, "F": classCircuitOpen, "G": classCircuitOpen, "H": classCircuitOpen}
	if got := dayErrorClasses(first); !maps.Equal(got, want) {
		t.Fatalf("first day pass = %v, want %v", got, want)
	}
	if first.USDayMarket == nil || first.USDayMarket.Rows != 0 {
		t.Fatalf("first day pass report = %+v", first.USDayMarket)
	}

	// The next window: the probe moved to B, which succeeds, so C to H are read.
	clock.advance(60 * time.Second)
	second, err := poller.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := dayErrorClasses(second); !maps.Equal(got, map[string]string{"A": classFetchError}) {
		t.Fatalf("second day pass errors = %v, want only A", got)
	}
	if second.USDayMarket == nil || second.USDayMarket.Rows != 7 {
		t.Fatalf("second day pass report = %+v, want 7 rows", second.USDayMarket)
	}
	var written []string
	for _, bar := range sink.bars {
		if bar.Venue == VenueDayNASD {
			written = append(written, bar.Symbol)
		}
	}
	if !slices.Equal(written, []string{"B", "C", "D", "E", "F", "G", "H"}) {
		t.Fatalf("day rows written = %v, want B through H in list order", written)
	}
}

// Rows written by the regular pass reach the sink's writer before the day pass
// starts, and the sink is flushed again on the way out.
func TestRegularRowsAreFlushedBeforeTheDayPassAndAtExit(t *testing.T) {
	clock := &testClock{now: kstAt(t, "2026-11-24T11:00:00")}
	reader := &venueReader{fetch: func(Target) FetchResult {
		return FetchResult{Rows: labelled("2026-11-24 10:58")}
	}}
	sink := &logSink{}
	targets := []Target{{Market: "kr", Symbol: "005930", Venue: "KRX"}, {Market: "us", Symbol: "NVDA", Venue: "NASD"}}
	if _, err := newDayPoller(t, clock, sink, reader, targets, 1).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"write:005930", "flush", "write:NVDA", "flush"}
	if !slices.Equal(sink.events, want) {
		t.Fatalf("sink events = %v, want %v", sink.events, want)
	}
}

// A regular write that fails stops the poll, and the rows already written are
// still flushed before Run returns.
func TestSinkIsFlushedWhenThePollFails(t *testing.T) {
	clock := &testClock{now: kstAt(t, "2026-11-24T11:00:00")}
	reader := &venueReader{fetch: func(Target) FetchResult {
		return FetchResult{Rows: labelled("2026-11-24 10:58")}
	}}
	sink := &logSink{failSymbol: "000660"}
	targets := []Target{{Market: "kr", Symbol: "005930", Venue: "KRX"}, {Market: "kr", Symbol: "000660", Venue: "KRX"}}
	_, err := newDayPoller(t, clock, sink, reader, targets, 1).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("err = %v, want the sink failure", err)
	}
	if want := []string{"write:005930", "flush"}; !slices.Equal(sink.events, want) {
		t.Fatalf("sink events = %v, want %v", sink.events, want)
	}
}

// A day source that answers rate-limited with a ten-minute Retry-After must not
// hold the day pass: the remaining day targets are recorded as day_budget
// instead of sleeping through the wait, and the regular targets are unaffected.
func TestDayPassIsCappedAtItsBudget(t *testing.T) {
	targets := []Target{
		{Market: "kr", Symbol: "005930", Venue: "KRX"},
		{Market: "us", Symbol: "A", Venue: "NASD"},
		{Market: "us", Symbol: "B", Venue: "NASD"},
		{Market: "us", Symbol: "C", Venue: "NASD"},
	}
	harness := newDayHarness(t, true, targets, func(target Target) FetchResult {
		if target.Venue == VenueDayNASD {
			return FetchResult{RateLimited: true, RetryAfter: 10 * time.Minute}
		}
		return FetchResult{}
	})
	report := harness.runAt(t, kstAt(t, "2026-11-24T11:00:00"))
	for _, wait := range harness.waits {
		if wait >= DefaultDayMarketBudget {
			t.Fatalf("the day pass slept %v, past its budget", wait)
		}
	}
	if len(harness.waits) != 0 {
		t.Fatalf("waits = %v, want none: the first wait already exceeds the budget", harness.waits)
	}
	if got := dayErrorClasses(report); !maps.Equal(got, map[string]string{"B": classDayBudget, "C": classDayBudget}) {
		t.Fatalf("day errors = %v, want B and C over budget", got)
	}
	for _, failure := range report.Errors {
		if failure.Venue != VenueDayNASD {
			t.Fatalf("a regular target failed: %+v", failure)
		}
	}
	if report.USDayMarket == nil || !report.USDayMarket.Read {
		t.Fatalf("day report = %+v", report.USDayMarket)
	}
}

// failingVenueSink fails when a bar of its venue is written.
type failingVenueSink struct{ venue string }

func (s failingVenueSink) Write(_ context.Context, bar Bar) error {
	if bar.Venue == s.venue {
		return errors.New("disk full")
	}
	return nil
}

// The production sink is a MultiSink around a JSONLSink, which buffers its rows.
// The regular rows must reach the writer before the day pass starts, and they
// must still be written when the day pass fails.
func TestMultiSinkFlushesTheJSONLRowsBeforeTheDayPass(t *testing.T) {
	clock := &testClock{now: kstAt(t, "2026-11-24T11:00:00")}
	var out bytes.Buffer
	sink := MultiSink{NewJSONLSink(&out), failingVenueSink{venue: VenueDayNASD}}
	var beforeDay string
	reader := &venueReader{fetch: func(target Target) FetchResult {
		if target.Venue == VenueDayNASD {
			beforeDay = out.String()
		}
		return FetchResult{Rows: labelled("2026-11-24 10:58")}
	}}
	targets := []Target{{Market: "kr", Symbol: "005930", Venue: "KRX"}, {Market: "us", Symbol: "NVDA", Venue: "NASD"}}
	_, err := newDayPoller(t, clock, sink, reader, targets, 1).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("err = %v, want the day-pass sink failure", err)
	}
	if !strings.Contains(beforeDay, "005930") {
		t.Fatalf("the regular row was not flushed before the day pass: %q", beforeDay)
	}
	if !strings.Contains(out.String(), "005930") {
		t.Fatalf("the regular row was lost on the day-pass error: %q", out.String())
	}
}

// markettapcmd.newSink returns MultiSink{jsonl} for JSONL-only output, which is
// the composition the production command runs. Its rows must reach the writer
// before the day pass starts, and after the run.
func TestJSONLOnlyMultiSinkFlushesBeforeTheDayPass(t *testing.T) {
	clock := &testClock{now: kstAt(t, "2026-11-24T11:00:00")}
	var out bytes.Buffer
	sink := MultiSink{NewJSONLSink(&out)}
	var beforeDay string
	reader := &venueReader{fetch: func(target Target) FetchResult {
		if target.Venue == VenueDayNASD {
			beforeDay = out.String()
		}
		return FetchResult{Rows: labelled("2026-11-24 10:58")}
	}}
	targets := []Target{{Market: "kr", Symbol: "005930", Venue: "KRX"}, {Market: "us", Symbol: "NVDA", Venue: "NASD"}}
	if _, err := newDayPoller(t, clock, sink, reader, targets, 1).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(beforeDay, "005930") {
		t.Fatalf("the regular row was not flushed before the day pass: %q", beforeDay)
	}
	if !strings.Contains(out.String(), "NVDA") {
		t.Fatalf("the day row was not written: %q", out.String())
	}
}
