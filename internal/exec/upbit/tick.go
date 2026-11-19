// Package upbit is the shadow-only Upbit order model: exact decimal
// quantities, the KRW price tick grid, the public orderbook frame, a live book
// source built on internal/upbit's reconnect loop, and the timing policy a
// shadow run applies to one intent.
//
// Nothing here can send an order. Child requests are values printed by
// cmd/execupbit; no gateway or broker client is imported (noorder_test.go
// enforces that), and the E1 client's account allowlist stays exactly
// kis_mock. The Upbit wire contract is still being designed in auto_trader
// hk 5438, so intent and child field names are local to this package and
// nothing in them leaves the process.
package upbit

// krwBands is Upbit's KRW market order-price unit (tick) table, in force since
// 2025-07-31 (docs.upbit.com/kr/changelog/krw_tick_unit_change_250731) and
// unchanged in the current guide,
// https://docs.upbit.com/kr/docs/krw-market-info ("호가 표시 단위").
//
// | price range (KRW)            | tick (KRW) |
// | 100 ~ 500                    | 1          |
// | 500 ~ 1,000                  | 5          |
// | 1,000 ~ 5,000                | 1          |
// | 5,000 ~ 10,000               | 5          |
// | 10,000 ~ 50,000              | 10         |
// | 50,000 ~ 100,000             | 50         |
// | 100,000 ~ 500,000            | 100        |
// | 500,000 ~ 1,000,000          | 500        |
// | 1,000,000 ~ 2,000,000        | 1,000      |
// | 2,000,000 and above          | 1,000      |
//
// The published table continues below 100 KRW with sub-won ticks (0.1, 0.01,
// ...). This model prices in whole KRW, so prices below 100 KRW are outside
// what it can represent: TickAt reports 0 there and every caller refuses.
// That range is unreachable for the KRW markets this model accepts, whose
// order value must also be at least 5,000 KRW (same guide, "최소 주문 가능
// 금액").
var krwBands = []struct{ below, tick int64 }{
	{500, 1},
	{1_000, 5},
	{5_000, 1},
	{10_000, 5},
	{50_000, 10},
	{100_000, 50},
	{500_000, 100},
	{1_000_000, 500},
	{2_000_000, 1_000},
}

// minPriceKRW is the lowest whole-won price with a tick on this grid.
const minPriceKRW = 100

// MinOrderKRW is Upbit's minimum order value for a KRW market.
const MinOrderKRW = 5_000

// TickAt is the KRW tick size at price p, or 0 when p is below the whole-won
// grid (p < 100).
func TickAt(p int64) int64 {
	if p < minPriceKRW {
		return 0
	}
	for _, b := range krwBands {
		if p < b.below {
			return b.tick
		}
	}
	return 1_000
}

// Floor is the highest grid price at or below p, snapping a price that is not
// on its own band's grid with the band's own tick. It returns 0 when p is
// below the grid.
func Floor(p int64) int64 {
	t := TickAt(p)
	if t == 0 {
		return 0
	}
	return p - p%t
}

// Ceil is the lowest grid price at or above p. It returns 0 when p is below
// the grid.
func Ceil(p int64) int64 {
	t := TickAt(p)
	if t == 0 {
		return 0
	}
	if r := p % t; r != 0 {
		return p - r + t
	}
	return p
}
