package upbit

import (
	"testing"
)

// The table under test is Upbit's published KRW market order-price unit:
//
//	https://docs.upbit.com/kr/docs/krw-market-info
//	https://docs.upbit.com/kr/changelog/krw_tick_unit_change_250731
//
// Every boundary below is copied from that page (호가 표시 단위), and the
// whole-won bands are exactly the ones this model represents.
func TestTickAtMatchesUpbitKRWTable(t *testing.T) {
	cases := []struct{ price, tick int64 }{
		{99, 0}, // sub-won ticks; outside the whole-won grid
		{100, 1},
		{499, 1},
		{500, 5},
		{999, 5},
		{1_000, 1},
		{4_999, 1},
		{5_000, 5},
		{9_999, 5},
		{10_000, 10},
		{49_999, 10},
		{50_000, 50},
		{99_999, 50},
		{100_000, 100},
		{499_999, 100},
		{500_000, 500},
		{999_999, 500},
		{1_000_000, 1_000},
		{1_999_999, 1_000},
		{2_000_000, 1_000},
		{109_950_000, 1_000},
	}
	for _, c := range cases {
		if got := TickAt(c.price); got != c.tick {
			t.Errorf("TickAt(%d) = %d, want %d", c.price, got, c.tick)
		}
	}
}

// Every band boundary must be a multiple of both neighbouring ticks, because
// Floor and Ceil snap with the tick of the price they are given.
func TestBandBoundariesShareTheirTicks(t *testing.T) {
	for _, b := range krwBands {
		if b.below%b.tick != 0 {
			t.Errorf("boundary %d is not a multiple of tick %d", b.below, b.tick)
		}
		above := TickAt(b.below)
		if b.below%above != 0 {
			t.Errorf("boundary %d is not a multiple of the next tick %d", b.below, above)
		}
	}
}

func TestFloorAndCeilStayOnTheVenueGrid(t *testing.T) {
	cases := []struct {
		in, floor, ceil int64
	}{
		{99, 0, 0},
		{499, 499, 499},
		{500, 500, 500},
		{5_002, 5_000, 5_005},
		{9_999, 9_995, 10_000},
		{49_999, 49_990, 50_000},
		{500_001, 500_000, 500_500},
		{109_950_000, 109_950_000, 109_950_000},
	}
	for _, c := range cases {
		if got := Floor(c.in); got != c.floor {
			t.Errorf("Floor(%d) = %d, want %d", c.in, got, c.floor)
		}
		if got := Ceil(c.in); got != c.ceil {
			t.Errorf("Ceil(%d) = %d, want %d", c.in, got, c.ceil)
		}
	}
}
