package marketminute

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/ratebudget"
)

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func testLimiter(t *testing.T, clock *testClock, global ratebudget.Limit) *ratebudget.Limiter {
	t.Helper()
	domestic, _ := Endpoint("KRX")
	overseas, _ := Endpoint("NASD")
	endpoint := ratebudget.Endpoint{Name: domestic, Limit: ratebudget.Limit{Rate: 1000, Burst: 100}}
	limiter, err := ratebudget.New(ratebudget.Config{
		Providers: []ratebudget.Provider{{
			Name:       "kis",
			AccountRef: "live",
			Limit:      global,
			Endpoints:  []ratebudget.Endpoint{endpoint, {Name: overseas, Limit: ratebudget.Limit{Rate: 1000, Burst: 100}}},
		}},
		ResearchShare: 1,
		Clock:         clock,
		Jitter:        func(d time.Duration) time.Duration { return d },
	})
	if err != nil {
		t.Fatalf("limiter: %v", err)
	}
	return limiter
}

type recordSink struct {
	mu   sync.Mutex
	bars []Bar
	err  error
}

func (s *recordSink) Write(_ context.Context, bar Bar) error {
	if s.err != nil {
		return s.err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bars = append(s.bars, bar)
	return nil
}

type scriptedReader struct {
	mu      sync.Mutex
	results map[string]FetchResult
	calls   []Target
}

func (r *scriptedReader) Fetch(_ context.Context, target Target) FetchResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, target)
	return r.results[target.Symbol]
}

func oneBar(symbol string) []FrameRow {
	return []FrameRow{{
		DateTime: "2026-11-24 09:00",
		Open:     new("100"), High: new("101"), Low: new("99"), Close: new("100"),
		Volume: new("10"), Value: new("100"),
	}}
}

func newTestPoller(t *testing.T, reader Reader, sink Sink, clock *testClock, targets []Target, workers int) *Poller {
	t.Helper()
	calendar, err := LoadCalendar([]byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	poller, err := NewPoller(Config{
		Targets:    targets,
		Reader:     reader,
		Limiter:    testLimiter(t, clock, ratebudget.Limit{Rate: 1000, Burst: 100}),
		Sink:       sink,
		Calendar:   calendar,
		Provider:   "kis",
		AccountRef: "live",
		Clock:      clock.Now,
		Workers:    workers,
	})
	if err != nil {
		t.Fatalf("poller: %v", err)
	}
	return poller
}

func TestPollerWritesTargetOrderAndCounts(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)}
	reader := &scriptedReader{results: map[string]FetchResult{
		"005930": {Rows: oneBar("005930")},
		"000660": {Rows: oneBar("000660")},
	}}
	sink := &recordSink{}
	targets := []Target{
		{Market: "kr", Symbol: "005930", Venue: "KRX"},
		{Market: "kr", Symbol: "000660", Venue: "KRX"},
	}
	report, err := newTestPoller(t, reader, sink, clock, targets, 2).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Rows != 2 || report.Targets != 2 || report.SkippedInvalid != 0 {
		t.Fatalf("report = %+v", report)
	}
	if len(sink.bars) != 2 || sink.bars[0].Symbol != "005930" || sink.bars[1].Symbol != "000660" {
		t.Fatalf("bars are not in target order: %+v", sink.bars)
	}
	if sink.bars[0].SessionSegment != SegmentKRXRegular {
		t.Fatalf("segment = %s", sink.bars[0].SessionSegment)
	}
}

func TestPollerRetriesARefusalAndCountsIt(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)}
	reader := &scriptedReader{results: map[string]FetchResult{"005930": {Rows: oneBar("005930")}}}
	sink := &recordSink{}
	calendar, _ := LoadCalendar([]byte(`{}`))
	limiter := testLimiter(t, clock, ratebudget.Limit{Rate: 1, Burst: 1})
	poller, err := NewPoller(Config{
		Targets:    []Target{{Market: "kr", Symbol: "005930", Venue: "KRX"}, {Market: "kr", Symbol: "000660", Venue: "KRX"}},
		Reader:     reader,
		Limiter:    limiter,
		Sink:       sink,
		Calendar:   calendar,
		Provider:   "kis",
		AccountRef: "live",
		Clock:      clock.Now,
		Workers:    1,
		Sleep: func(_ context.Context, d time.Duration) error {
			clock.advance(d)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	reader.results["000660"] = FetchResult{Rows: oneBar("000660")}
	report, err := poller.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Refusals[string(ratebudget.CodeRateLimited)] == 0 {
		t.Fatalf("no refusal counted: %+v", report.Refusals)
	}
	if report.Rows != 2 {
		t.Fatalf("rows = %d, want 2 (a refusal must be retried)", report.Rows)
	}
}

func TestPollerReportsOnlyAClosedClass(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)}
	reader := &scriptedReader{results: map[string]FetchResult{
		"005930": {Err: fmt.Errorf("%w: secret-token-abc", ErrTokenUnavailable)},
	}}
	report, err := newTestPoller(t, reader, &recordSink{}, clock, []Target{{Market: "kr", Symbol: "005930", Venue: "KRX"}}, 1).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Errors) != 1 || report.Errors[0].Class != classTokenUnavailable {
		t.Fatalf("errors = %+v", report.Errors)
	}
	if strings.Contains(fmt.Sprintf("%+v", report), "secret-token") {
		t.Fatal("the report leaked the reader message")
	}
}

func TestPollerPropagatesSinkFailure(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)}
	reader := &scriptedReader{results: map[string]FetchResult{"005930": {Rows: oneBar("005930")}}}
	sink := &recordSink{err: errors.New("disk full")}
	_, err := newTestPoller(t, reader, sink, clock, []Target{{Market: "kr", Symbol: "005930", Venue: "KRX"}}, 1).Run(context.Background())
	if err == nil {
		t.Fatal("sink failure was swallowed")
	}
}
