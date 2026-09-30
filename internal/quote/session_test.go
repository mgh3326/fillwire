package quote

import (
	"testing"
	"time"
)

func kst(month time.Month, day, hour, minute, second int) time.Time {
	return time.Date(2026, month, day, hour, minute, second, 0, KST)
}

func et(month time.Month, day, hour, minute, second int) time.Time {
	return time.Date(2026, month, day, hour, minute, second, 0, Eastern)
}

func TestSessionLabelKR(t *testing.T) {
	// 30 September 2026 is a Wednesday; 3 October a Saturday.
	for _, test := range []struct {
		at   time.Time
		want string
	}{
		{kst(9, 30, 7, 59, 59), ""},
		{kst(9, 30, 8, 0, 0), SessionNXTPre},
		{kst(9, 30, 8, 50, 59), SessionNXTPre},
		{kst(9, 30, 8, 51, 0), ""},
		{kst(9, 30, 8, 59, 59), ""},
		{kst(9, 30, 9, 0, 0), SessionKRXRegular},
		{kst(9, 30, 15, 30, 59), SessionKRXRegular},
		{kst(9, 30, 15, 31, 0), ""},
		{kst(9, 30, 15, 59, 59), ""},
		{kst(9, 30, 16, 0, 0), SessionNXTAfter},
		{kst(9, 30, 20, 0, 59), SessionNXTAfter},
		{kst(9, 30, 20, 1, 0), ""},
		{kst(10, 3, 10, 0, 0), ""},
		{time.Date(2026, 9, 30, 1, 0, 0, 0, time.UTC), SessionKRXRegular}, // 10:00 KST
	} {
		got, ok := SessionLabel(MarketKR, test.at)
		if got != test.want || ok != (test.want != "") {
			t.Fatalf("SessionLabel(kr, %s) = %q, %t; want %q", test.at, got, ok, test.want)
		}
	}
}

func TestSessionLabelUSAcrossDaylightSaving(t *testing.T) {
	for _, test := range []struct {
		at   time.Time
		want string
	}{
		{et(9, 30, 3, 59, 59), ""},
		{et(9, 30, 4, 0, 0), SessionUSPre},
		{et(9, 30, 9, 29, 59), SessionUSPre},
		{et(9, 30, 9, 30, 0), SessionUSRegular},
		{et(9, 30, 15, 59, 59), SessionUSRegular},
		{et(9, 30, 16, 0, 0), SessionUSAfter},
		{et(9, 30, 20, 0, 59), SessionUSAfter},
		{et(9, 30, 20, 1, 0), ""},
		{et(10, 3, 10, 0, 0), ""}, // Saturday in New York
		// 22:30 KST is 09:30 EDT in September but 08:30 EST in December.
		{kst(9, 30, 22, 30, 0), SessionUSRegular},
		{kst(12, 2, 22, 30, 0), SessionUSPre},
		{kst(12, 2, 23, 30, 0), SessionUSRegular},
		// Saturday 01:00 KST is Friday 12:00 EDT: a US trading day.
		{kst(10, 3, 1, 0, 0), SessionUSRegular},
	} {
		got, ok := SessionLabel(MarketUS, test.at)
		if got != test.want || ok != (test.want != "") {
			t.Fatalf("SessionLabel(us, %s) = %q, %t; want %q", test.at, got, ok, test.want)
		}
	}
	if _, ok := SessionLabel("jp", kst(9, 30, 10, 0, 0)); ok {
		t.Fatal("unknown market labelled")
	}
}

func TestScheduleKROnly(t *testing.T) {
	schedule := NewSchedule([]string{MarketKR})
	closes, open := schedule.At(kst(9, 30, 8, 0, 0))
	if !open || !closes.Equal(kst(9, 30, 20, 1, 0)) {
		t.Fatalf("KR 08:00 = %s %t; want one merged window to 20:01", closes, open)
	}
	if _, open := schedule.At(kst(9, 30, 15, 45, 0)); !open {
		t.Fatal("the 29-minute 15:31-16:00 gap should stay open (merged)")
	}
	for _, at := range []time.Time{kst(9, 30, 7, 59, 0), kst(9, 30, 20, 1, 0), kst(10, 3, 12, 0, 0)} {
		if _, open := schedule.At(at); open {
			t.Fatalf("KR socket open at %s", at)
		}
	}
	for _, test := range []struct{ at, want time.Time }{
		{kst(9, 30, 5, 0, 0), kst(9, 30, 8, 0, 0)},
		{kst(9, 30, 21, 0, 0), kst(10, 1, 8, 0, 0)},
		{kst(10, 2, 21, 0, 0), kst(10, 5, 8, 0, 0)}, // Friday night -> Monday
	} {
		if got := schedule.NextOpen(test.at); !got.Equal(test.want) {
			t.Fatalf("NextOpen(%s) = %s, want %s", test.at, got, test.want)
		}
	}
}

func TestScheduleKRAndUSUnion(t *testing.T) {
	schedule := NewSchedule([]string{MarketKR, MarketUS})
	// September (EDT): US 04:00-20:01 ET is 17:00-09:01 KST. With KR
	// 08:00-20:01 KST the weekday union has no gap from Monday 08:00 KST.
	closes, open := schedule.At(kst(9, 30, 12, 0, 0))
	if !open {
		t.Fatal("union closed at midday KST")
	}
	if !closes.After(kst(10, 2, 0, 0, 0)) {
		t.Fatalf("union window closes %s; want it to run into the weekend", closes)
	}
	if _, open := schedule.At(kst(10, 4, 12, 0, 0)); open {
		t.Fatal("union open on Sunday midday KST")
	}
	if got, want := schedule.NextOpen(kst(10, 4, 12, 0, 0)), kst(10, 5, 8, 0, 0); !got.Equal(want) {
		t.Fatalf("NextOpen(Sunday) = %s, want %s", got, want)
	}
}
