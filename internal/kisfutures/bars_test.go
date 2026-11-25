package kisfutures

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/kst"
)

func kstAt(hour, minute, second int) time.Time {
	return time.Date(2026, time.November, 24, hour, minute, second, 0, kst.Zone)
}

func trade(t testing.TB, code string, at time.Time, price string, qty, acml int64) Trade {
	t.Helper()
	parsed, err := ParsePrice(price)
	if err != nil {
		t.Fatal(err)
	}
	return Trade{Code: code, At: at, Price: parsed, Qty: qty, AcmlVol: acml}
}

// record is one H0IFCNT0 (50 columns) or H0MFCNT0 (49 columns) record in the
// column order of the official samples; unread columns are zero.
func record(tr, code, hour, price string, qty, acml int64) string {
	width := 50
	if tr == TRNightTrade {
		width = 49
	}
	fields := make([]string, width)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0], fields[1] = code, hour
	fields[5] = price
	// Decoys around the read columns: the day open, high and low.
	fields[6], fields[7], fields[8] = "1.00", "999.00", "0.50"
	fields[9] = strconv.FormatInt(qty, 10)
	fields[10] = strconv.FormatInt(acml, 10)
	return strings.Join(fields, "^")
}

func frame(tr string, records ...string) []byte {
	return []byte("0|" + tr + "|" + strconv.Itoa(1000 + len(records))[1:] + "|" + strings.Join(records, "^"))
}

var testContract = Contract{
	Product: "F200", ShortCode: "A01612", Month: "2026-12", Symbol: "F200.2612", YYMM: "2612",
	LastTrade: Date(2026, time.December, 10), Listed: []string{"2026-12", "2027-03"},
}

// Fixture trades across a minute boundary give the expected OHLCV, and a
// forming bar is not emitted before the grace.
func TestBarsAcrossMinuteBoundaryAndGrace(t *testing.T) {
	builder := newBuilder(goldenCalendar(t))
	fixture := []struct {
		hour, price string
		qty, acml   int64
	}{
		{"090000", "400.10", 3, 3},
		{"090012", "400.35", 2, 5},
		{"090031", "399.95", 4, 9},
		{"090059", "400.05", 1, 10},
		{"090100", "400.20", 5, 15},
		{"090140", "400.25", 2, 17},
	}
	for _, f := range fixture {
		received := kstAt(9, 2, 0)
		trades, invalid, err := ParseFrame(frame(TRDayTrade, record(TRDayTrade, "A01612", f.hour, f.price, f.qty, f.acml)), received)
		if err != nil || invalid != 0 || len(trades) != 1 {
			t.Fatalf("frame %s: %v, %d, %d", f.hour, err, invalid, len(trades))
		}
		if got := builder.add(testContract, trades[0]); got != added {
			t.Fatalf("add %s = %v", f.hour, got)
		}
	}
	for _, now := range []time.Time{kstAt(9, 0, 59), kstAt(9, 1, 0), kstAt(9, 1, 9), kstAt(9, 1, 10).Add(-time.Nanosecond)} {
		if bars, _ := builder.closed(now); len(bars) != 0 {
			t.Fatalf("at %s the forming 09:00 bar was emitted: %+v", now.Format("15:04:05.000000000"), bars)
		}
	}
	bars, skipped := builder.closed(kstAt(9, 1, 10))
	if len(bars) != 1 || skipped != 0 {
		t.Fatalf("at 09:01:10 bars = %+v, skipped %d; want only 09:00", bars, skipped)
	}
	first := Bar{
		TimeUTC: time.Date(2026, time.November, 24, 0, 0, 0, 0, time.UTC),
		Symbol:  "F200.2612", Product: "F200", ContractMonth: "2612", RollState: RollFront,
		SessionSegment: SegmentDay, SessionDate: Date(2026, time.November, 24),
		Open: "400.10", High: "400.35", Low: "399.95", Close: "400.05", Volume: 10,
		ReceivedAt: kstAt(9, 1, 10),
	}
	if bars[0] != first {
		t.Fatalf("09:00 bar =\n%+v, want\n%+v", bars[0], first)
	}
	if bars, _ := builder.closed(kstAt(9, 2, 9)); len(bars) != 0 {
		t.Fatalf("09:01 bar emitted before its grace: %+v", bars)
	}
	bars, _ = builder.closed(kstAt(9, 2, 10))
	if len(bars) != 1 || bars[0].Open != "400.20" || bars[0].High != "400.25" || bars[0].Low != "400.20" ||
		bars[0].Close != "400.25" || bars[0].Volume != 7 || bars[0].VolumeGap {
		t.Fatalf("09:01 bar = %+v", bars)
	}
	row, err := MarshalRow(first)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"time_utc":"2026-11-24T00:00:00Z","symbol":"F200.2612","product":"F200","contract_month":"2612","roll_state":"front","session_segment":"FUT_DAY","session_date":"2026-11-24","source":"KIS","open":"400.10","high":"400.35","low":"399.95","close":"400.05","volume":"10","received_at":"2026-11-24T00:01:10Z","volume_gap":false}`
	if string(row) != want {
		t.Fatalf("row =\n%s\nwant\n%s", row, want)
	}
}

func TestUnknownBarHasNullSessionDate(t *testing.T) {
	builder := newBuilder(goldenCalendar(t))
	builder.add(testContract, trade(t, "A01612", kstAt(16, 0, 0), "400.10", 1, 1))
	bars, _ := builder.closed(kstAt(16, 2, 0))
	if len(bars) != 1 || bars[0].SessionSegment != SegmentUnknown || !bars[0].SessionDate.IsZero() {
		t.Fatalf("bars = %+v", bars)
	}
	row, err := MarshalRow(bars[0])
	if err != nil || !strings.Contains(string(row), `"session_date":null`) {
		t.Fatalf("row = %s, %v", row, err)
	}
}

func TestLateAndDuplicateTrades(t *testing.T) {
	builder := newBuilder(goldenCalendar(t))
	first := trade(t, "A01612", kstAt(9, 0, 5), "400.10", 2, 2)
	builder.add(testContract, first)
	if got := builder.add(testContract, first); got != duplicate {
		t.Fatalf("repeated trade = %v, want duplicate", got)
	}
	if bars, _ := builder.closed(kstAt(9, 1, 10)); len(bars) != 1 || bars[0].Volume != 2 {
		t.Fatalf("bars = %+v", bars)
	}
	if got := builder.add(testContract, trade(t, "A01612", kstAt(9, 0, 59), "401.00", 1, 3)); got != late {
		t.Fatalf("trade of a closed minute = %v, want late", got)
	}
	// The late trade still continues the cumulative volume.
	builder.add(testContract, trade(t, "A01612", kstAt(9, 1, 2), "400.50", 1, 4))
	bars, _ := builder.closed(kstAt(9, 3, 0))
	if len(bars) != 1 || bars[0].Volume != 1 || bars[0].VolumeGap {
		t.Fatalf("bars after a late trade = %+v", bars)
	}
}

func TestVolumeGap(t *testing.T) {
	t.Run("joined mid session", func(t *testing.T) {
		builder := newBuilder(goldenCalendar(t))
		builder.add(testContract, trade(t, "A01612", kstAt(10, 0, 5), "400.10", 2, 500))
		builder.add(testContract, trade(t, "A01612", kstAt(10, 1, 5), "400.10", 2, 502))
		bars, _ := builder.closed(kstAt(10, 3, 0))
		if len(bars) != 2 || !bars[0].VolumeGap || bars[1].VolumeGap {
			t.Fatalf("bars = %+v", bars)
		}
	})
	t.Run("missed trades mark both sides", func(t *testing.T) {
		builder := newBuilder(goldenCalendar(t))
		builder.add(testContract, trade(t, "A01612", kstAt(10, 0, 5), "400.10", 1, 1))
		builder.add(testContract, trade(t, "A01612", kstAt(10, 1, 5), "400.10", 1, 9))
		bars, _ := builder.closed(kstAt(10, 3, 0))
		if len(bars) != 2 || !bars[0].VolumeGap || !bars[1].VolumeGap {
			t.Fatalf("bars = %+v", bars)
		}
	})
	on := func(tr string, at time.Time, qty, acml int64) Trade {
		x := trade(t, "A01612", at, "400.10", qty, acml)
		x.TR = tr
		return x
	}
	t.Run("the night stream counts apart from the day", func(t *testing.T) {
		builder := newBuilder(goldenCalendar(t))
		builder.add(testContract, on(TRDayTrade, kstAt(15, 0, 0), 2, 2))
		// The same total as the day's: still the night's first trade.
		if got := builder.add(testContract, on(TRNightTrade, kstAt(18, 0, 0), 2, 2)); got != added {
			t.Fatalf("night trade = %v", got)
		}
		bars, _ := builder.closed(kstAt(18, 2, 0))
		if len(bars) != 2 || bars[1].VolumeGap || bars[1].Volume != 2 || bars[1].SessionSegment != SegmentNight {
			t.Fatalf("bars = %+v", bars)
		}
	})
	t.Run("a new day restarts the total, equal or lower", func(t *testing.T) {
		builder := newBuilder(goldenCalendar(t))
		builder.add(testContract, on(TRDayTrade, kstAt(15, 0, 0), 1, 1))
		if got := builder.add(testContract, on(TRDayTrade, kstAt(9, 0, 0).AddDate(0, 0, 1), 1, 1)); got != added {
			t.Fatalf("next day's first trade = %v", got)
		}
		bars, _ := builder.closed(kstAt(9, 2, 0).AddDate(0, 0, 1))
		if len(bars) != 2 || bars[1].VolumeGap {
			t.Fatalf("bars = %+v", bars)
		}
	})
	t.Run("midnight does not restart a night", func(t *testing.T) {
		builder := newBuilder(goldenCalendar(t))
		midnight := time.Date(2026, time.November, 25, 0, 0, 0, 0, kst.Zone)
		builder.add(testContract, on(TRNightTrade, midnight.Add(-time.Second), 9, 9))
		builder.add(testContract, on(TRNightTrade, midnight.Add(time.Second), 1, 10))
		bars, _ := builder.closed(midnight.Add(2 * time.Minute))
		if len(bars) != 2 || bars[1].VolumeGap {
			t.Fatalf("bars = %+v", bars)
		}
	})
	t.Run("an unchanged total is not a trade", func(t *testing.T) {
		builder := newBuilder(goldenCalendar(t))
		builder.add(testContract, on(TRDayTrade, kstAt(10, 0, 0), 3, 3))
		if got := builder.add(testContract, on(TRDayTrade, kstAt(10, 0, 20), 3, 3)); got != duplicate {
			t.Fatalf("repeat with a newer clock = %v", got)
		}
		if bars, _ := builder.closed(kstAt(10, 2, 0)); len(bars) != 1 || bars[0].Volume != 3 {
			t.Fatalf("bars = %+v", bars)
		}
	})
	t.Run("a total that starts over mid day is kept and marked", func(t *testing.T) {
		builder := newBuilder(goldenCalendar(t))
		builder.add(testContract, on(TRDayTrade, kstAt(10, 0, 0), 9, 9))
		if got := builder.add(testContract, on(TRDayTrade, kstAt(10, 1, 0), 2, 5)); got != added {
			t.Fatalf("restarted total = %v", got)
		}
		bars, _ := builder.closed(kstAt(10, 3, 0))
		if len(bars) != 2 || !bars[1].VolumeGap || bars[1].Volume != 2 {
			t.Fatalf("bars = %+v", bars)
		}
	})
}

func TestContractBeyondNextHasNoBar(t *testing.T) {
	builder := newBuilder(goldenCalendar(t))
	third := testContract
	third.Month, third.Listed = "2027-03", []string{"2026-11", "2026-12", "2027-03"}
	builder.add(third, trade(t, "A01703", time.Date(2026, time.November, 6, 10, 0, 0, 0, kst.Zone), "400.10", 1, 1))
	bars, skipped := builder.closed(time.Date(2026, time.November, 6, 10, 2, 0, 0, kst.Zone))
	if len(bars) != 0 || skipped != 1 {
		t.Fatalf("bars = %+v, skipped = %d", bars, skipped)
	}
}

func TestParseFrame(t *testing.T) {
	received := kstAt(9, 0, 10)
	two := frame(TRDayTrade,
		record(TRDayTrade, "A01612", "090001", "400.10", 3, 3),
		record(TRDayTrade, "A05612", "090002", "400.12", 1, 7))
	trades, invalid, err := ParseFrame(append(two, '\n'), received)
	if err != nil || invalid != 0 || len(trades) != 2 {
		t.Fatalf("two records: %v, %d, %d", err, invalid, len(trades))
	}
	got := trades[1]
	if got.Code != "A05612" || got.Price.Text != "400.12" || got.Qty != 1 || got.AcmlVol != 7 || !got.At.Equal(kstAt(9, 0, 2)) {
		t.Fatalf("second record = %+v", got)
	}
	night, invalid, err := ParseFrame(frame(TRNightTrade, record(TRNightTrade, "A01612", "090001", "400.10", 3, 3)), received)
	if err != nil || invalid != 0 || len(night) != 1 || night[0].Price.Text != "400.10" {
		t.Fatalf("night record: %v, %d, %+v", err, invalid, night)
	}
	for name, bad := range map[string]string{
		"price":        record(TRDayTrade, "A01612", "090001", "", 3, 3),
		"negative":     record(TRDayTrade, "A01612", "090001", "-1.00", 3, 3),
		"exponent":     record(TRDayTrade, "A01612", "090001", "4e2", 3, 3),
		"zero price":   record(TRDayTrade, "A01612", "090001", "0.00", 3, 3),
		"zero qty":     record(TRDayTrade, "A01612", "090001", "400.10", 0, 3),
		"total":        record(TRDayTrade, "A01612", "090001", "400.10", 4, 3),
		"hour":         record(TRDayTrade, "A01612", "09000x", "400.10", 3, 3),
		"hour range":   record(TRDayTrade, "A01612", "096000", "400.10", 3, 3),
		"stale clock":  record(TRDayTrade, "A01612", "085000", "400.10", 3, 3),
		"future clock": record(TRDayTrade, "A01612", "091000", "400.10", 3, 3),
		"no code":      record(TRDayTrade, "", "090001", "400.10", 3, 3),
	} {
		trades, invalid, err := ParseFrame(frame(TRDayTrade, bad), received)
		if err != nil || invalid != 1 || len(trades) != 0 {
			t.Errorf("%s: %v, invalid %d, trades %+v", name, err, invalid, trades)
		}
	}
	for name, bad := range map[string]string{
		"notice TR": "0|H0IFCNI0|001|a^b",
		"stock TR":  "0|H0STCNT0|001|a^b",
		"encrypted": "1|H0IFCNT0|001|abc",
		"count":     "0|H0IFCNT0|000|a",
		"narrow":    "0|H0IFCNT0|001|A01612^090001",
		"uneven":    "0|H0IFCNT0|002|" + record(TRDayTrade, "A01612", "090001", "400.10", 3, 3) + "^x",
		"json":      `{"header":{}}`,
	} {
		if _, _, err := ParseFrame([]byte(bad), received); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The exchange sends no date: it comes from the receive time, across midnight
// and for a clock written past 24 hours.
func TestExchangeClockAcrossMidnight(t *testing.T) {
	midnight := time.Date(2026, time.November, 25, 0, 0, 0, 0, kst.Zone)
	for name, c := range map[string]struct {
		hour     string
		received time.Time
		want     time.Time
	}{
		"before, received after": {"235959", midnight.Add(time.Second), midnight.Add(-time.Second)},
		"after, received after":  {"000001", midnight.Add(2 * time.Second), midnight.Add(time.Second)},
		"after, written as 24":   {"240001", midnight.Add(2 * time.Second), midnight.Add(time.Second)},
		"small hours, 29":        {"295900", midnight.Add(5*time.Hour + 59*time.Minute), midnight.Add(5*time.Hour + 59*time.Minute)},
	} {
		got, ok := exchangeTime(c.hour, c.received.UTC())
		if !ok || !got.Equal(c.want) {
			t.Errorf("%s: %v, %v; want %v", name, got, ok, c.want)
		}
	}
	if _, ok := exchangeTime("300000", midnight.Add(6*time.Hour)); ok {
		t.Error("hour 30 accepted")
	}
}
