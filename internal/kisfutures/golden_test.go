package kisfutures

import (
	"errors"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/jsonx"
	"github.com/mgh3326/fillwire/internal/kst"
)

type goldenContract struct {
	Symbol    string `json:"symbol"`
	ShortCode string `json:"short_code"`
	Month     string `json:"contract_month"`
	YYMM      string `json:"yymm"`
	LastTrade string `json:"last_trade_date"`
	Rank      int    `json:"rank"`
}

type goldenFile struct {
	Source   string `json:"source"`
	Calendar struct {
		From     string   `json:"from"`
		To       string   `json:"to"`
		KRClosed []string `json:"kr_closed"`
	} `json:"calendar"`
	Sessions []struct {
		Test        string  `json:"test"`
		At          string  `json:"at"`
		Segment     string  `json:"segment"`
		SessionDate *string `json:"session_date"`
	} `json:"sessions"`
	Unconfirmed struct {
		Test string   `json:"test"`
		At   []string `json:"at"`
	} `json:"unconfirmed_calendar"`
	Master  [][]string `json:"master"`
	Resolve []struct {
		Test       string           `json:"test"`
		AsOf       string           `json:"as_of"`
		Master     *[][]string      `json:"master"` // null: the shared master
		Contracts  []goldenContract `json:"contracts"`
		Unresolved []string         `json:"unresolved"`
	} `json:"resolve"`
	LastTradeDate []struct {
		Month string `json:"contract_month"`
		Date  string `json:"date"`
	} `json:"last_trade_date"`
	RollListed []string `json:"roll_listed"`
	RollState  []struct {
		Month string `json:"contract_month"`
		On    string `json:"on"`
		State string `json:"state"`
	} `json:"roll_state"`
	BeyondNext struct {
		Month string `json:"contract_month"`
		On    string `json:"on"`
	} `json:"roll_state_beyond_next"`
	CollectorBars []struct {
		Test        string  `json:"test"`
		Symbol      string  `json:"symbol"`
		At          string  `json:"at"`
		Product     string  `json:"product"`
		Month       string  `json:"contract_month"`
		RollState   string  `json:"roll_state"`
		Segment     string  `json:"session_segment"`
		SessionDate *string `json:"session_date"`
		TimeUTC     string  `json:"time_utc"`
	} `json:"collector_bars"`
}

func loadGolden(t testing.TB) goldenFile {
	t.Helper()
	data, err := os.ReadFile("testdata/python_rules.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden goldenFile
	if err := jsonx.DecodeStrict(data, &golden); err != nil {
		t.Fatalf("golden: %v", err)
	}
	return golden
}

// goldenCalendar is the XKRX calendar of the golden cases.
func goldenCalendar(t testing.TB) *Calendar {
	t.Helper()
	golden := loadGolden(t)
	closed := `"` + strings.Join(golden.Calendar.KRClosed, `","`) + `"`
	calendar, err := ParseCalendar([]byte(`{"from":"` + golden.Calendar.From + `","to":"` + golden.Calendar.To + `","kr_closed":[` + closed + `]}`))
	if err != nil {
		t.Fatal(err)
	}
	return calendar
}

// masterBytes writes rows in the layout of fo_idx_code_mts.mst, with the
// Korean name of a mini contract in cp949.
func masterBytes(rows [][]string) []byte {
	var out strings.Builder
	for _, row := range rows {
		name := strings.ReplaceAll(row[2], "미니F ", miniName)
		out.WriteString(row[0] + "|" + row[1] + "|KR4X|" + name + "| |00000.00|1|2001|" + row[3] + "\r\n")
	}
	return []byte(out.String())
}

func parseRows(t testing.TB, rows [][]string) []MasterRow {
	t.Helper()
	if len(rows) == 0 {
		return nil
	}
	parsed, err := ParseMaster(masterBytes(rows))
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func parseDay(t testing.TB, raw string) time.Time {
	t.Helper()
	day, err := time.Parse(time.DateOnly, raw)
	if err != nil {
		t.Fatal(err)
	}
	return day
}

func parseInstant(t testing.TB, raw string) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatal(err)
	}
	return at
}

func dayText(day time.Time) *string {
	if day.IsZero() {
		return nil
	}
	text := day.Format(time.DateOnly)
	return &text
}

func show(day *string) string {
	if day == nil {
		return "null"
	}
	return *day
}

func TestGoldenSessionSegmentAndDate(t *testing.T) {
	golden := loadGolden(t)
	calendar := goldenCalendar(t)
	if len(golden.Sessions) == 0 {
		t.Fatal("no session cases")
	}
	for _, c := range golden.Sessions {
		segment, sessionDate := calendar.Classify(parseInstant(t, c.At))
		if segment != c.Segment || !reflect.DeepEqual(dayText(sessionDate), c.SessionDate) {
			t.Errorf("%s %s = %s/%s, want %s/%s", c.Test, c.At, segment, show(dayText(sessionDate)), c.Segment, show(c.SessionDate))
		}
	}
}

// A calendar that cannot confirm a day gives UNKNOWN and no date, never a
// guess: here the covered range misses the day, or ends before the next
// trading day.
func TestUnconfirmedCalendarIsUnknown(t *testing.T) {
	golden := loadGolden(t)
	elsewhere, err := ParseCalendar([]byte(`{"from":"2025-01-01","to":"2025-12-31","kr_closed":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range golden.Unconfirmed.At {
		if segment, date := elsewhere.Classify(parseInstant(t, raw)); segment != SegmentUnknown || !date.IsZero() {
			t.Errorf("%s = %s/%v, want UNKNOWN", raw, segment, date)
		}
	}
	short, err := ParseCalendar([]byte(`{"from":"2026-11-01","to":"2026-11-24","kr_closed":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	if segment, date := short.Classify(parseInstant(t, "2026-11-24T19:00:00+09:00")); segment != SegmentUnknown || !date.IsZero() {
		t.Errorf("night with an uncovered next day = %s/%v, want UNKNOWN", segment, date)
	}
	if segment, _ := short.Classify(parseInstant(t, "2026-11-24T10:00:00+09:00")); segment != SegmentDay {
		t.Errorf("day inside the range = %s", segment)
	}
}

func TestCalendarRefusesBadFiles(t *testing.T) {
	for name, body := range map[string]string{
		"not json":        `from: 2026`,
		"unknown key":     `{"from":"2026-01-01","to":"2026-12-31","kr_closed":[],"us_closed":[]}`,
		"no kr_closed":    `{"from":"2026-01-01","to":"2026-12-31"}`,
		"no range":        `{"kr_closed":[]}`,
		"reversed range":  `{"from":"2026-12-31","to":"2026-01-01","kr_closed":[]}`,
		"bad day":         `{"from":"2026-01-01","to":"2026-12-31","kr_closed":["12-25"]}`,
		"closed outside":  `{"from":"2026-01-01","to":"2026-12-31","kr_closed":["2027-01-01"]}`,
		"trailing values": `{"from":"2026-01-01","to":"2026-12-31","kr_closed":[]} {}`,
	} {
		if _, err := ParseCalendar([]byte(body)); !errors.Is(err, ErrCalendar) {
			t.Errorf("%s: err = %v, want ErrCalendar", name, err)
		}
	}
}

func TestGoldenFrontNextResolution(t *testing.T) {
	golden := loadGolden(t)
	for _, c := range golden.Resolve {
		t.Run(c.Test, func(t *testing.T) {
			rows := golden.Master
			if c.Master != nil {
				rows = *c.Master
			}
			contracts, unresolved, err := ResolveFrontNext(parseRows(t, rows), parseDay(t, c.AsOf))
			if err != nil {
				t.Fatal(err)
			}
			got := []goldenContract{}
			for _, x := range contracts {
				if x.Product+"."+x.YYMM != x.Symbol {
					t.Errorf("symbol %s does not match product %s and %s", x.Symbol, x.Product, x.YYMM)
				}
				got = append(got, goldenContract{x.Symbol, x.ShortCode, x.Month, x.YYMM, x.LastTrade.Format(time.DateOnly), x.Rank})
			}
			if !reflect.DeepEqual(got, c.Contracts) {
				t.Errorf("contracts =\n%+v, want\n%+v", got, c.Contracts)
			}
			if !slices.Equal(unresolved, c.Unresolved) {
				t.Errorf("unresolved = %v, want %v", unresolved, c.Unresolved)
			}
		})
	}
}

func TestGoldenLastTradeDateAndRollState(t *testing.T) {
	golden := loadGolden(t)
	for _, c := range golden.LastTradeDate {
		got, err := LastTradeDate(c.Month)
		if err != nil || got.Format(time.DateOnly) != c.Date {
			t.Errorf("LastTradeDate(%s) = %v, %v; want %s", c.Month, got, err, c.Date)
		}
	}
	if len(golden.RollState) == 0 {
		t.Fatal("no roll_state cases")
	}
	for _, c := range golden.RollState {
		got, err := RollState(c.Month, parseDay(t, c.On), golden.RollListed)
		if err != nil || got != c.State {
			t.Errorf("RollState(%s, %s) = %q, %v; want %q", c.Month, c.On, got, err, c.State)
		}
	}
	if got, err := RollState(golden.BeyondNext.Month, parseDay(t, golden.BeyondNext.On), golden.RollListed); !errors.Is(err, ErrBeyondNext) {
		t.Errorf("third contract = %q, %v; want ErrBeyondNext", got, err)
	}
}

// The bars of the Python collector tests: the same contract, session and roll
// labels from trades.
func TestGoldenCollectorBarLabels(t *testing.T) {
	golden := loadGolden(t)
	contracts, _, err := ResolveFrontNext(parseRows(t, golden.Master), Date(2026, time.November, 24))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range golden.CollectorBars {
		t.Run(c.Test, func(t *testing.T) {
			index := slices.IndexFunc(contracts, func(x Contract) bool { return x.Symbol == c.Symbol })
			if index < 0 {
				t.Fatalf("no contract %s", c.Symbol)
			}
			at := parseInstant(t, c.At).In(kst.Zone)
			builder := newBuilder(goldenCalendar(t))
			builder.add(contracts[index], trade(t, contracts[index].ShortCode, at, "400.10", 5, 5))
			bars, skipped := builder.closed(at.Add(2 * time.Minute))
			if len(bars) != 1 || skipped != 0 {
				t.Fatalf("bars = %d, skipped = %d", len(bars), skipped)
			}
			bar := bars[0]
			if bar.Symbol != c.Symbol || bar.Product != c.Product || bar.ContractMonth != c.Month ||
				bar.RollState != c.RollState || bar.SessionSegment != c.Segment ||
				!reflect.DeepEqual(dayText(bar.SessionDate), c.SessionDate) || bar.TimeUTC.Format(timeLayout) != c.TimeUTC {
				t.Errorf("bar = %+v, want %+v", bar, c)
			}
		})
	}
}

// roll_state of a night bar is taken on its session_date: on the night after
// the last trade date the old front has expired and the next one is front.
func TestNightBarRollStateUsesSessionDate(t *testing.T) {
	golden := loadGolden(t)
	calendar := goldenCalendar(t)
	night := parseInstant(t, "2026-12-10T19:00:00+09:00")
	date := calendar.ContractDate(night)
	if got := date.Format(time.DateOnly); got != "2026-12-11" {
		t.Fatalf("contract date = %s", got)
	}
	contracts, _, err := ResolveFrontNext(parseRows(t, golden.Master), date)
	if err != nil {
		t.Fatal(err)
	}
	builder := newBuilder(calendar)
	want := map[string]string{"MF200.2703": RollFront, "MF200.2706": RollNext}
	for _, contract := range contracts {
		if _, ok := want[contract.Symbol]; ok {
			builder.add(contract, trade(t, contract.ShortCode, night, "400.10", 1, 1))
		}
	}
	bars, skipped := builder.closed(night.Add(2 * time.Minute))
	if len(bars) != 2 || skipped != 0 {
		t.Fatalf("bars = %d, skipped = %d", len(bars), skipped)
	}
	for _, bar := range bars {
		if bar.RollState != want[bar.Symbol] || bar.SessionSegment != SegmentNight || show(dayText(bar.SessionDate)) != "2026-12-11" {
			t.Errorf("bar = %+v", bar)
		}
	}
}

func TestContractDate(t *testing.T) {
	calendar := goldenCalendar(t)
	for at, want := range map[string]string{
		"2026-11-24T05:59:00+09:00": "2026-11-24", // Monday's night
		"2026-11-24T06:00:00+09:00": "2026-11-24",
		"2026-11-24T15:44:59+09:00": "2026-11-24",
		"2026-11-24T15:45:00+09:00": "2026-11-25",
		"2026-11-27T20:00:00+09:00": "2026-11-30", // Friday night
		"2026-11-28T02:00:00+09:00": "2026-11-30",
		"2026-11-28T12:00:00+09:00": "2026-11-28",
		"2026-11-28T20:00:00+09:00": "2026-11-30",
		"2026-11-30T03:00:00+09:00": "2026-11-30",
		"2026-12-24T20:00:00+09:00": "2026-12-28",
		"2027-03-02T20:00:00+09:00": "2027-03-02", // uncovered: the KST date
	} {
		if got := calendar.ContractDate(parseInstant(t, at)).Format(time.DateOnly); got != want {
			t.Errorf("ContractDate(%s) = %s, want %s", at, got, want)
		}
	}
}

func TestParseMasterRefusesBadRows(t *testing.T) {
	good := string(masterBytes([][]string{{"B", "A05612", "미니F 202612", "KOSPI200"}}))
	for name, data := range map[string]string{
		"empty":       "",
		"short row":   good + "B|A05703\r\n",
		"blank line":  good + "\r\n" + good,
		"extra field": strings.Replace(good, "|2001|", "|2001|x|", 1),
	} {
		if _, err := ParseMaster([]byte(data)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
