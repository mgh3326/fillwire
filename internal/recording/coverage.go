package recording

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Interval struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}
type Coverage struct {
	Market         string  `json:"market"`
	Day            string  `json:"day"`
	CoveredSeconds float64 `json:"covered_seconds"`
	DaySeconds     float64 `json:"day_seconds"`
	Ratio          float64 `json:"ratio"`
}

// SummarizeCoverage unions connected intervals and clips them to the UTC day.
func SummarizeCoverage(market, day string, intervals []Interval) (Coverage, error) {
	start, err := time.Parse("2006-01-02", day)
	if err != nil {
		return Coverage{}, fmt.Errorf("coverage: UTC 날짜 형식이 잘못되었습니다: %w", err)
	}
	end := start.Add(24 * time.Hour)
	clipped := make([]Interval, 0, len(intervals))
	for _, in := range intervals {
		a, b := in.Start.UTC(), in.End.UTC()
		if a.Before(start) {
			a = start
		}
		if b.After(end) {
			b = end
		}
		if b.After(a) {
			clipped = append(clipped, Interval{a, b})
		}
	}
	covered := unionDuration(clipped)
	seconds := covered.Seconds()
	return Coverage{Market: market, Day: day, CoveredSeconds: seconds, DaySeconds: 86400, Ratio: seconds / 86400}, nil
}

func unionDuration(intervals []Interval) time.Duration {
	sort.Slice(intervals, func(i, j int) bool { return intervals[i].Start.Before(intervals[j].Start) })
	var covered time.Duration
	if len(intervals) == 0 {
		return 0
	}
	a, b := intervals[0].Start, intervals[0].End
	for _, in := range intervals[1:] {
		if !in.Start.After(b) {
			if in.End.After(b) {
				b = in.End
			}
			continue
		}
		covered += b.Sub(a)
		a, b = in.Start, in.End
	}
	return covered + b.Sub(a)
}

// WriteCoverage writes a stable JSON summary suitable for a later health check.
func WriteCoverage(path string, rows []Coverage) error {
	b, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0640)
}

type coverageEvent struct {
	kind string
	at   time.Time
}

// readCoverageEvents retains all event timestamps: frames and heartbeat records
// bound an open connection after a crash or while a summary is being read.
func readCoverageEvents(root string, now time.Time) (map[string][]coverageEvent, error) {
	base := filepath.Join(root, "upbit")
	events := map[string][]coverageEvent{}
	found := false
	err := filepath.WalkDir(base, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".jsonl") && !strings.HasSuffix(name, ".jsonl.gz") {
			return nil
		}
		found = true
		var in io.ReadCloser
		if strings.HasSuffix(name, ".gz") {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			gz, err := gzip.NewReader(f)
			if err != nil {
				_ = f.Close()
				return err
			}
			in = struct {
				io.Reader
				io.Closer
			}{gz, closeBoth{gz, f}}
		} else {
			in, err = os.Open(path)
			if err != nil {
				return err
			}
		}
		dec := json.NewDecoder(in)
		for {
			var rec struct {
				Kind       string    `json:"kind"`
				Market     string    `json:"market"`
				ReceivedAt time.Time `json:"received_at"`
			}
			if err := dec.Decode(&rec); err == io.EOF {
				break
			} else if err != nil {
				_ = in.Close()
				return err
			}
			at := rec.ReceivedAt.UTC()
			if rec.Market == "" || at.IsZero() || at.After(now) {
				continue
			}
			events[rec.Market] = append(events[rec.Market], coverageEvent{rec.Kind, at})
		}
		return in.Close()
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, os.ErrNotExist
	}
	for market := range events {
		sort.SliceStable(events[market], func(i, j int) bool {
			return events[market][i].at.Before(events[market][j].at)
		})
	}
	return events, nil
}

// connectedIntervals closes prior open sessions at their last observed activity.
// A second connection without a gap closes the preceding session rather than
// replacing its start and losing its coverage.
func connectedIntervals(events []coverageEvent) []Interval {
	var start, lastAlive time.Time
	var intervals []Interval
	for _, event := range events {
		switch event.kind {
		case "connected":
			if !start.IsZero() && lastAlive.After(start) {
				intervals = append(intervals, Interval{start, lastAlive})
			}
			start, lastAlive = event.at, event.at
		case "gap":
			if !start.IsZero() {
				if event.at.After(start) {
					intervals = append(intervals, Interval{start, event.at})
				}
				start, lastAlive = time.Time{}, time.Time{}
			}
		default:
			if !start.IsZero() && event.at.After(lastAlive) {
				lastAlive = event.at
			}
		}
	}
	if !start.IsZero() && lastAlive.After(start) {
		intervals = append(intervals, Interval{start, lastAlive})
	}
	return intervals
}

// SummarizeDay reads Upbit partitions and reports only intervals bounded by
// recorded activity. An unterminated run never implies coverage after its last record.
func SummarizeDay(root, day string) ([]Coverage, error) {
	if _, err := time.Parse("2006-01-02", day); err != nil {
		return nil, fmt.Errorf("coverage: UTC 날짜 형식이 잘못되었습니다: %w", err)
	}
	events, err := readCoverageEvents(root, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	rows := make([]Coverage, 0, len(events))
	for market, items := range events {
		row, err := SummarizeCoverage(market, day, connectedIntervals(items))
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Market < rows[j].Market })
	return rows, nil
}

type closeBoth struct{ first, second io.Closer }

func (c closeBoth) Close() error { return errors.Join(c.first.Close(), c.second.Close()) }

type WindowCoverage struct {
	Market         string    `json:"market"`
	From           time.Time `json:"from"`
	To             time.Time `json:"to"`
	CoveredSeconds float64   `json:"covered_seconds"`
	WindowSeconds  float64   `json:"window_seconds"`
	Ratio          float64   `json:"ratio"`
}

// SummarizeWindow measures connected seconds within a requested interval,
// bounded by the last recorded activity and the current time.
func SummarizeWindow(root string, from, to time.Time) ([]WindowCoverage, error) {
	from, to = from.UTC(), to.UTC()
	if !to.After(from) {
		return nil, errors.New("coverage: 종료 시각은 시작 시각보다 뒤여야 합니다")
	}
	events, err := readCoverageEvents(root, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	window := to.Sub(from).Seconds()
	rows := make([]WindowCoverage, 0, len(events))
	for market, items := range events {
		clipped := make([]Interval, 0)
		for _, interval := range connectedIntervals(items) {
			a, b := interval.Start, interval.End
			if a.Before(from) {
				a = from
			}
			if b.After(to) {
				b = to
			}
			if b.After(a) {
				clipped = append(clipped, Interval{a, b})
			}
		}
		seconds := unionDuration(clipped).Seconds()
		rows = append(rows, WindowCoverage{market, from, to, seconds, window, seconds / window})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Market < rows[j].Market })
	return rows, nil
}
