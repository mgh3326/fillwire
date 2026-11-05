package sim

import (
	"math/big"
	"testing"
)

func TestRound2IsExactHalfAwayFromZero(t *testing.T) {
	for _, tc := range []struct {
		num, den int64
		want     float64
	}{
		{201, 200, 1.01}, // 1.005: float64(1.005)*100 is 100.4999..., the old rounding gave 1
		{-201, 200, -1.01},
		{1, 8, 0.13},
		{-1, 8, -0.13},
		{632200, 9, 70244.44},
		{70000, 1, 70000},
		{0, 1, 0},
		{1, 300, 0},
	} {
		if got := round2(big.NewRat(tc.num, tc.den)); got != tc.want {
			t.Errorf("round2(%d/%d) = %v, want %v", tc.num, tc.den, got, tc.want)
		}
	}
}
