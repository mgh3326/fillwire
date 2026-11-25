package kisfutures

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Contract identity, last trade date and roll state follow auto_trader
// app/services/kis_futures_quotes/contracts.py (hk 6148) at origin/main
// 68b7ca593. testdata/python_rules.golden.json holds the cases of its tests.

// Roll states of a contract on a trading date, auto_trader's vocabulary and the
// CHECK vocabulary of research.kr_futures_candles_1m.
const (
	RollFront    = "front"
	RollNext     = "next"
	RollExpiring = "expiring"
	RollExpired  = "expired"
)

// product is one member of the closed product set. A master row belongs to it
// only when product type, underlying name, Korean name pattern and short code
// all agree.
type product struct {
	code        string // DB product code: symbol F200.2612
	masterType  string // product type column of fo_idx_code_mts.mst
	namePrefix  string // cp949 bytes in front of the YYYYMM of the name
	shortPrefix string // short code is this prefix, the year digit and the month
}

// miniName is "미니F " in cp949, the encoding of the master file.
const miniName = "\xb9\xcc\xb4\xcfF "

// products is the closed set: KOSPI200 futures and mini KOSPI200 futures. No
// options, no stock futures.
var products = []product{
	{code: "F200", masterType: "1", namePrefix: "F ", shortPrefix: "A01"},
	{code: "MF200", masterType: "B", namePrefix: miniName, shortPrefix: "A05"},
}

// contractsPerProduct is the front and the next contract.
const contractsPerProduct = 2

// MasterRow is the part of one KIS index futures/options master row the
// resolver reads. Name keeps the file's cp949 bytes.
type MasterRow struct {
	ProductType    string
	ShortCode      string
	Name           string
	UnderlyingName string
}

// masterColumns is the column count of fo_idx_code_mts.mst: product type, short
// code, standard code, Korean name, ATM class, strike, month class, underlying
// short code, underlying name.
const masterColumns = 9

// ParseMaster splits the pipe separated master file. As in auto_trader, one
// row of another width refuses the whole file. '|' is never part of a cp949
// double byte character, so the raw bytes split safely.
func ParseMaster(data []byte) ([]MasterRow, error) {
	lines := bytes.Split(data, []byte("\n"))
	if len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	rows := make([]MasterRow, 0, len(lines))
	for number, line := range lines {
		fields := bytes.Split(bytes.TrimSuffix(line, []byte("\r")), []byte("|"))
		if len(fields) != masterColumns {
			return nil, fmt.Errorf("kisfutures: master line %d has %d columns, want %d", number+1, len(fields), masterColumns)
		}
		column := func(i int) string { return string(bytes.Trim(fields[i], " \t\r")) }
		rows = append(rows, MasterRow{ProductType: column(0), ShortCode: column(1), Name: column(3), UnderlyingName: column(8)})
	}
	if len(rows) == 0 {
		return nil, errors.New("kisfutures: master is empty")
	}
	return rows, nil
}

// Contract is one resolved contract.
type Contract struct {
	Product   string
	ShortCode string // KIS short code from the master row, the subscription key
	Month     string // YYYY-MM
	Symbol    string // DB symbol, F200.2612
	YYMM      string
	LastTrade time.Time // date, midnight UTC
	Rank      int       // 0 front, 1 next among contracts live on the as-of date
	// Listed is every master month of the product, ascending: RollState input.
	Listed []string
}

// Date returns a calendar date as midnight UTC, the date form of this package.
func Date(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// The seeded KRX derivatives calendar of auto_trader
// app/services/kis_futures_mock/session_calendar.py, which holiday-adjusts a
// last trade date only inside this range. Extend both together.
var (
	seedFrom     = Date(2026, time.November, 23)
	seedTo       = Date(2026, time.December, 30)
	seedHolidays = []time.Time{Date(2026, time.December, 25)}
)

// seedState reports whether the seeded calendar knows day, and whether it is a
// session.
func seedState(day time.Time) (open, known bool) {
	if day.Before(seedFrom) || day.After(seedTo) {
		return false, false
	}
	if day.Weekday() == time.Saturday || day.Weekday() == time.Sunday {
		return false, true
	}
	return !slices.ContainsFunc(seedHolidays, day.Equal), true
}

func secondThursday(year int, month time.Month) time.Time {
	first := Date(year, month, 1)
	offset := (int(time.Thursday) - int(first.Weekday()) + 7) % 7
	return first.AddDate(0, 0, offset+7)
}

// LastTradeDate is the last trade date of a YYYY-MM contract month: the second
// Thursday, moved back over holidays the seeded calendar knows. Outside that
// calendar the plain second Thursday applies, as in auto_trader.
func LastTradeDate(month string) (time.Time, error) {
	parsed, err := time.Parse("2006-01", month)
	if err != nil {
		return time.Time{}, fmt.Errorf("kisfutures: contract month %q is not YYYY-MM", month)
	}
	nominal := secondThursday(parsed.Year(), parsed.Month())
	candidate := nominal
	for range 7 {
		open, known := seedState(candidate)
		if !known {
			break
		}
		if open {
			return candidate, nil
		}
		candidate = candidate.AddDate(0, 0, -1)
	}
	return nominal, nil
}

// ErrBeyondNext reports a contract that is neither front nor next on the date:
// it is outside the collected set, so it has no roll state.
var ErrBeyondNext = errors.New("kisfutures: contract is beyond the next contract")

// RollState is front, next, expiring or expired for a contract month on a
// date. expired is after the last trade date; expiring is the ISO week of the
// last trade date, up to and including it; otherwise the position among the
// listed months not yet expired on that date.
func RollState(month string, on time.Time, listed []string) (string, error) {
	last, err := LastTradeDate(month)
	if err != nil {
		return "", err
	}
	if on.After(last) {
		return RollExpired, nil
	}
	onYear, onWeek := on.ISOWeek()
	if lastYear, lastWeek := last.ISOWeek(); onYear == lastYear && onWeek == lastWeek {
		return RollExpiring, nil
	}
	earlierLive := 0
	for _, other := range listed {
		if other >= month {
			continue
		}
		otherLast, err := LastTradeDate(other)
		if err != nil {
			return "", err
		}
		if !otherLast.Before(on) {
			earlierLive++
		}
	}
	switch earlierLive {
	case 0:
		return RollFront, nil
	case 1:
		return RollNext, nil
	default:
		return "", fmt.Errorf("%w: %s on %s", ErrBeyondNext, month, on.Format(time.DateOnly))
	}
}

// candidates maps YYYY-MM to the short code of the master rows that match the
// product. A month listed under two different short codes is dropped.
func candidates(rows []MasterRow, p product) map[string]string {
	found := map[string][]string{}
	for _, row := range rows {
		if row.ProductType != p.masterType || row.UnderlyingName != "KOSPI200" {
			continue
		}
		digits, ok := strings.CutPrefix(row.Name, p.namePrefix)
		if !ok || len(digits) != 6 || !allDigits(digits) {
			continue
		}
		year, month := digits[:4], digits[4:]
		short := row.ShortCode
		if len(short) != 6 || !strings.HasPrefix(short, p.shortPrefix) || !allDigits(short[3:]) {
			continue
		}
		if short[3] != year[3] || short[4:] != month || month < "01" || month > "12" {
			continue
		}
		key := year + "-" + month
		if !slices.Contains(found[key], short) {
			found[key] = append(found[key], short)
		}
	}
	result := make(map[string]string, len(found))
	for month, codes := range found {
		if len(codes) == 1 {
			result[month] = codes[0]
		}
	}
	return result
}

func allDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// ResolveFrontNext returns the front and next contract of every product on
// asOf, taken from master rows and never guessed. A product with no live
// contract in the master is returned in unresolved.
func ResolveFrontNext(rows []MasterRow, asOf time.Time) (contracts []Contract, unresolved []string, err error) {
	for _, p := range products {
		byMonth := candidates(rows, p)
		listed := make([]string, 0, len(byMonth))
		for month := range byMonth {
			listed = append(listed, month)
		}
		slices.Sort(listed)
		rank := 0
		for _, month := range listed {
			last, err := LastTradeDate(month)
			if err != nil {
				return nil, nil, err
			}
			if last.Before(asOf) {
				continue
			}
			yymm := month[2:4] + month[5:]
			contracts = append(contracts, Contract{
				Product: p.code, ShortCode: byMonth[month], Month: month,
				Symbol: p.code + "." + yymm, YYMM: yymm, LastTrade: last,
				Rank: rank, Listed: listed,
			})
			if rank++; rank == contractsPerProduct {
				break
			}
		}
		if rank == 0 {
			unresolved = append(unresolved, p.code)
		}
	}
	return contracts, unresolved, nil
}
