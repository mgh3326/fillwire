package quote

import (
	"testing"
	"time"
)

func kst(day, hour, minute, second int) time.Time {
	// September 2026: the 28th is a Monday, the 3rd of October a Saturday.
	return time.Date(2026, time.September, day, hour, minute, second, 0, KST)
}

func TestSessionAtBoundaries(t *testing.T) {
	for _, test := range []struct {
		at   time.Time
		want string
		open bool
	}{
		{kst(30, 8, 59, 59), "", false},
		{kst(30, 9, 0, 0), SessionRegular, true},
		{kst(30, 15, 30, 0), SessionRegular, true},
		{kst(30, 15, 30, 59), SessionRegular, true},
		{kst(30, 15, 31, 0), "", false},
		{kst(30, 15, 59, 59), "", false},
		{kst(30, 16, 0, 0), SessionAfterHours, true},
		{kst(30, 20, 0, 59), SessionAfterHours, true},
		{kst(30, 20, 1, 0), "", false},
		{kst(30, 23, 0, 0), "", false},
		{time.Date(2026, time.October, 3, 10, 0, 0, 0, KST), "", false}, // Saturday
		{time.Date(2026, time.October, 4, 10, 0, 0, 0, KST), "", false}, // Sunday
		{time.Date(2026, time.September, 30, 1, 0, 0, 0, time.UTC), SessionRegular, true},
	} {
		name, closes, open := SessionAt(test.at)
		if name != test.want || open != test.open {
			t.Fatalf("SessionAt(%s) = %q, %t; want %q, %t", test.at, name, open, test.want, test.open)
		}
		if open && !closes.After(test.at) {
			t.Fatalf("SessionAt(%s) closes %s, want later", test.at, closes)
		}
	}
	_, closes, _ := SessionAt(kst(30, 10, 0, 0))
	if !closes.Equal(kst(30, 15, 31, 0)) {
		t.Fatalf("regular close = %s, want 15:31:00 KST", closes)
	}
	_, closes, _ = SessionAt(kst(30, 17, 0, 0))
	if !closes.Equal(kst(30, 20, 1, 0)) {
		t.Fatalf("after-hours close = %s, want 20:01:00 KST", closes)
	}
}

func TestNextOpen(t *testing.T) {
	for _, test := range []struct{ at, want time.Time }{
		{kst(30, 5, 0, 0), kst(30, 9, 0, 0)},
		{kst(30, 10, 0, 0), kst(30, 16, 0, 0)},
		{kst(30, 15, 40, 0), kst(30, 16, 0, 0)},
		{kst(30, 21, 0, 0), time.Date(2026, time.October, 1, 9, 0, 0, 0, KST)},
		{time.Date(2026, time.October, 2, 20, 30, 0, 0, KST), time.Date(2026, time.October, 5, 9, 0, 0, 0, KST)}, // Friday night -> Monday
		{time.Date(2026, time.October, 3, 12, 0, 0, 0, KST), time.Date(2026, time.October, 5, 9, 0, 0, 0, KST)},  // Saturday
	} {
		if got := NextOpen(test.at); !got.Equal(test.want) {
			t.Fatalf("NextOpen(%s) = %s, want %s", test.at, got, test.want)
		}
	}
}
