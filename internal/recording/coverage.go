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
	sort.Slice(clipped, func(i, j int) bool { return clipped[i].Start.Before(clipped[j].Start) })
	var covered time.Duration
	if len(clipped) > 0 {
		a, b := clipped[0].Start, clipped[0].End
		for _, in := range clipped[1:] {
			if !in.Start.After(b) {
				if in.End.After(b) {
					b = in.End
				}
				continue
			}
			covered += b.Sub(a)
			a, b = in.Start, in.End
		}
		covered += b.Sub(a)
	}
	seconds := covered.Seconds()
	return Coverage{Market: market, Day: day, CoveredSeconds: seconds, DaySeconds: 86400, Ratio: seconds / 86400}, nil
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

// SummarizeDay reads an Upbit daily partition and computes coverage from connected/gap records.
func SummarizeDay(root, day string) ([]Coverage, error) {
	target, err := time.Parse("2006-01-02", day)
	if err != nil {
		return nil, err
	}
	cutoff := target.Add(24 * time.Hour)
	base := filepath.Join(root, "upbit")
	events := map[string][]struct {
		kind string
		at   time.Time
	}{}
	found := false
	err = filepath.WalkDir(base, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
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
			f, e := os.Open(path)
			if e != nil {
				return e
			}
			gz, e := gzip.NewReader(f)
			if e != nil {
				f.Close()
				return e
			}
			in = struct {
				io.Reader
				io.Closer
			}{gz, closeBoth{gz, f}}
		} else {
			in, e = os.Open(path)
			if e != nil {
				return e
			}
		}
		dec := json.NewDecoder(in)
		for {
			var rec struct {
				Kind       string    `json:"kind"`
				Market     string    `json:"market"`
				ReceivedAt time.Time `json:"received_at"`
			}
			e := dec.Decode(&rec)
			if e == io.EOF {
				break
			}
			if e != nil {
				in.Close()
				return e
			}
			at := rec.ReceivedAt.UTC()
			if at.Before(cutoff) && (rec.Kind == "connected" || rec.Kind == "gap") {
				events[rec.Market] = append(events[rec.Market], struct {
					kind string
					at   time.Time
				}{rec.Kind, at})
			}
		}
		return in.Close()
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, os.ErrNotExist
	}
	rows := make([]Coverage, 0, len(events))
	for market, items := range events {
		sort.Slice(items, func(i, j int) bool { return items[i].at.Before(items[j].at) })
		var start *time.Time
		intervals := []Interval{}
		for _, item := range items {
			if item.kind == "connected" {
				copy := item.at
				start = &copy
			} else if start != nil {
				if item.at.After(*start) {
					intervals = append(intervals, Interval{*start, item.at})
				}
				start = nil
			}
		}
		if start != nil {
			intervals = append(intervals, Interval{*start, cutoff})
		}
		row, e := SummarizeCoverage(market, day, intervals)
		if e != nil {
			return nil, e
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

// SummarizeWindow measures connected seconds within a requested interval.
func SummarizeWindow(root string, from, to time.Time) ([]WindowCoverage, error) {
	from, to = from.UTC(), to.UTC()
	if !to.After(from) {
		return nil, errors.New("coverage: 종료 시각은 시작 시각보다 뒤여야 합니다")
	}
	base := filepath.Join(root, "upbit")
	events := map[string][]struct {
		kind string
		at   time.Time
	}{}
	found := false
	err := filepath.WalkDir(base, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
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
			f, e := os.Open(path)
			if e != nil {
				return e
			}
			gz, e := gzip.NewReader(f)
			if e != nil {
				f.Close()
				return e
			}
			in = struct {
				io.Reader
				io.Closer
			}{gz, closeBoth{gz, f}}
		} else {
			in, e = os.Open(path)
			if e != nil {
				return e
			}
		}
		dec := json.NewDecoder(in)
		for {
			var rec struct {
				Kind, Market string
				ReceivedAt   time.Time `json:"received_at"`
			}
			e := dec.Decode(&rec)
			if e == io.EOF {
				break
			}
			if e != nil {
				in.Close()
				return e
			}
			if rec.Kind == "connected" || rec.Kind == "gap" {
				events[rec.Market] = append(events[rec.Market], struct {
					kind string
					at   time.Time
				}{rec.Kind, rec.ReceivedAt.UTC()})
			}
		}
		return in.Close()
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, os.ErrNotExist
	}
	window := to.Sub(from).Seconds()
	rows := []WindowCoverage{}
	for market, items := range events {
		sort.Slice(items, func(i, j int) bool { return items[i].at.Before(items[j].at) })
		var start *time.Time
		sum := time.Duration(0)
		for _, item := range items {
			if item.kind == "connected" {
				v := item.at
				start = &v
			} else if start != nil {
				a, b := *start, item.at
				if a.Before(from) {
					a = from
				}
				if b.After(to) {
					b = to
				}
				if b.After(a) {
					sum += b.Sub(a)
				}
				start = nil
			}
		}
		if start != nil {
			a := *start
			if a.Before(from) {
				a = from
			}
			if to.After(a) {
				sum += to.Sub(a)
			}
		}
		seconds := sum.Seconds()
		rows = append(rows, WindowCoverage{market, from, to, seconds, window, seconds / window})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Market < rows[j].Market })
	return rows, nil
}
