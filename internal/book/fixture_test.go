package book

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/testutil"
)

// t0 is the fixture session start: 2026-11-02 09:00:00 KST.
var t0 = time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)

func at(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

// bookRecord builds a full-width (59 field) H0STASP0 record with a 100 KRW
// tick ladder around bestAsk/bestBid.
func bookRecord(symbol, hour string, bestAsk, bestBid, qty int64) string {
	f := []string{symbol, hour, "0"}
	for i := int64(0); i < Depth; i++ {
		f = append(f, strconv.FormatInt(bestAsk+i*100, 10))
	}
	for i := int64(0); i < Depth; i++ {
		f = append(f, strconv.FormatInt(bestBid-i*100, 10))
	}
	for i := int64(0); i < 2*Depth; i++ {
		f = append(f, strconv.FormatInt(qty+i, 10))
	}
	f = append(f, strconv.FormatInt(qty*Depth, 10), strconv.FormatInt(qty*Depth+1, 10))
	for len(f) < 59 {
		f = append(f, "0")
	}
	return strings.Join(f, "^")
}

// tradeRecord builds a full-width (46 field) H0STCNT0 record.
func tradeRecord(symbol, hour string, price, qty, acml int64, side string) string {
	f := make([]string, 46)
	for i := range f {
		f[i] = "0"
	}
	f[tradeSymbol], f[tradeHour] = symbol, hour
	f[tradePrice] = strconv.FormatInt(price, 10)
	f[tradeQty] = strconv.FormatInt(qty, 10)
	f[tradeAcmlVol] = strconv.FormatInt(acml, 10)
	f[tradeSideCode] = side
	return strings.Join(f, "^")
}

func frame(tr string, records ...string) []byte {
	return []byte(fmt.Sprintf("0|%s|%03d|%s", tr, len(records), strings.Join(records, "^")))
}

func bookFrame(symbol, hour string, bestAsk, bestBid, qty int64) []byte {
	return frame(TRBook, bookRecord(symbol, hour, bestAsk, bestBid, qty))
}

func tradeFrame(symbol, hour string, price, qty, acml int64) []byte {
	return frame(TRTrade, tradeRecord(symbol, hour, price, qty, acml, "1"))
}

// recordScenario drives a recorder through every case the recorder handles:
// duplicate, gap, Tick-detected and arrival-detected staleness, reconnect
// resync, a multi-record frame, a malformed frame, and ignored frames.
func recordScenario(t *testing.T) []byte {
	t.Helper()
	var out bytes.Buffer
	rec, err := NewRecorder(&out, DefaultStaleAfter, at(0))
	if err != nil {
		t.Fatal(err)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	samsungBook := bookFrame("005930", "090000", 71100, 71000, 10)
	trade150 := tradeFrame("005930", "090000", 71100, 50, 150)
	must(rec.Frame(samsungBook, at(0)))
	must(rec.Frame(tradeFrame("005930", "090000", 71100, 100, 100), at(100)))
	must(rec.Frame(trade150, at(200)))
	must(rec.Frame(trade150, at(250))) // duplicate
	must(rec.Frame(bookFrame("000660", "090000", 180500, 180000, 7), at(300)))
	must(rec.Frame(tradeFrame("005930", "090001", 71000, 50, 300), at(400))) // gap of 100
	must(rec.Frame(samsungBook, at(500)))                                    // duplicate
	must(rec.Tick(at(2300)))                                                 // 000660 book silent exactly 2 s: not stale
	must(rec.Tick(at(3000)))                                                 // all three streams stale
	must(rec.Tick(at(3050)))                                                 // already flagged
	must(rec.Reconnect(at(3100), 1))
	must(rec.Frame(samsungBook, at(3200)))                                    // identical, but resync
	must(rec.Frame(tradeFrame("005930", "090005", 71200, 20, 400), at(3300))) // outage gap of 80
	must(rec.Frame(frame(TRBook,
		bookRecord("005930", "090005", 71300, 71200, 12),
		bookRecord("000660", "090005", 180600, 180100, 8)), at(3400)))
	must(rec.Frame([]byte("0|H0STASP0|001|005930^0900"), at(3500)))                                  // malformed
	must(rec.Frame([]byte(`{"header":{"tr_id":"PINGPONG","datetime":"20261102090003"}}`), at(3600))) // ignored
	must(rec.Frame([]byte("0|H0STCNI0|001|x^y^z"), at(3700)))                                        // ignored
	must(rec.Frame(tradeFrame("005930", "090008", 71300, 50, 450), at(6000)))                        // stale on arrival
	return out.Bytes()
}

func golden(t *testing.T, name string, got []byte) {
	t.Helper()
	testutil.Golden(t, "testdata/"+name, got)
}
