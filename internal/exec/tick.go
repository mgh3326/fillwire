package exec

// krxBands is the KRX equity tick table (in force since 2023-01-25): a price
// below bands[i].below trades in steps of bands[i].tick. Every band boundary
// is a multiple of every smaller tick, so stepping up from a grid price lands
// on the grid of the next band.
var krxBands = []struct{ below, tick int64 }{
	{2_000, 1},
	{5_000, 5},
	{20_000, 10},
	{50_000, 50},
	{200_000, 100},
	{500_000, 500},
}

// TickAt is the KRX tick size at price p.
func TickAt(p int64) int64 {
	for _, b := range krxBands {
		if p < b.below {
			return b.tick
		}
	}
	return 1_000
}

// FloorTick is the highest grid price at or below p (0 if p < 1).
func FloorTick(p int64) int64 {
	if p < 1 {
		return 0
	}
	return p - p%TickAt(p)
}

// CeilTick is the lowest grid price at or above p.
func CeilTick(p int64) int64 {
	if p < 1 {
		return 0
	}
	if r := p % TickAt(p); r != 0 {
		return p - r + TickAt(p)
	}
	return p
}

// stepToward moves grid price p one tick toward cap and never past it.
func stepToward(side Side, p, cap int64) int64 {
	if side == Buy {
		return min(p+TickAt(p), cap)
	}
	return max(p-TickAt(p-1), cap)
}
