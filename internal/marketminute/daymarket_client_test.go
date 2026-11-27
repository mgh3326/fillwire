package marketminute_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/marketminute"
)

// The request shape is the official overseas minute-chart sample's with its
// day-market exchange codes; see daymarket.go for the URL.
func TestDayMarketReadIsTheQuotationGETWithTheDayExchangeCode(t *testing.T) {
	for _, venue := range []string{"BAQ", "BAY", "BAA"} {
		var method, path, transaction, exchange string
		client, _ := newClient(t, func(writer http.ResponseWriter, request *http.Request) {
			method, path = request.Method, request.URL.Path
			transaction, exchange = request.Header.Get("tr_id"), request.URL.Query().Get("EXCD")
			// xymd/xhms differ from kymd/khms on purpose.
			writer.Write([]byte(`{"rt_cd":"0","output2":[{"xymd":"20261123","xhms":"205800","kymd":"20261124","khms":"105800","open":"100","high":"101","low":"99","last":"100.5","evol":"10","eamt":"1005"}]}`))
		}, fixedToken("fixture-token"))
		result := client.Fetch(context.Background(), marketminute.Target{Market: "us", Symbol: "NVDA", Venue: venue})
		if result.Err != nil || len(result.Rows) != 1 {
			t.Fatalf("%s: result = %+v", venue, result)
		}
		if method != http.MethodGet || path != "/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice" ||
			transaction != "HHDFS76950200" || exchange != venue {
			t.Fatalf("%s: request = %s %s tr_id=%s EXCD=%s", venue, method, path, transaction, exchange)
		}
		calendar, err := marketminute.LoadCalendar([]byte(`{"us_day_market":[{"start":"2026-11-24T10:00:00+09:00","end":"2026-11-24T18:00:00+09:00"}]}`))
		if err != nil {
			t.Fatal(err)
		}
		frame := marketminute.BarsFromFrame(result.Rows, "us", "NVDA", venue, time.Date(2026, 11, 24, 3, 0, 0, 0, time.UTC), calendar)
		if len(frame.Bars) != 1 {
			t.Fatalf("%s: frame = %+v", venue, frame)
		}
		row := frame.Bars[0].Row()
		if row.TimeUTC != "2026-11-24T01:58:00Z" || row.SessionDate != "2026-11-24" || row.Venue != venue ||
			row.SessionSegment != "US_DAY" || row.Source != "KIS" || row.Close != "100.5" || row.Value == nil || *row.Value != "1005" {
			t.Fatalf("%s: row = %+v", venue, row)
		}
	}
}

func TestDayMarketRowWithoutAKoreanTimeIsMalformed(t *testing.T) {
	client, _ := newClient(t, func(writer http.ResponseWriter, _ *http.Request) {
		writer.Write([]byte(`{"rt_cd":"0","output2":[{"xymd":"20261123","xhms":"205800","open":"100","high":"101","low":"99","last":"100","evol":"10","eamt":"1000"}]}`))
	}, fixedToken("fixture-token"))
	result := client.Fetch(context.Background(), marketminute.Target{Market: "us", Symbol: "NVDA", Venue: "BAQ"})
	if !errors.Is(result.Err, marketminute.ErrMalformedResponse) {
		t.Fatalf("err = %v, want ErrMalformedResponse", result.Err)
	}
}
