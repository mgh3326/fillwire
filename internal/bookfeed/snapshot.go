// Package bookfeed publishes read-only KIS snapshots and reads their Redis stream.
package bookfeed

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/mgh3326/fillwire/internal/book"
	"github.com/redis/go-redis/v9"
)

var KST = time.FixedZone("KST", 9*60*60)

type Snapshot struct {
	Symbol             string
	Exchange, Received time.Time
	Bids, Asks         [5]book.Level
	Source             string
}

func SymbolOK(s string) bool {
	if len(s) != 6 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'A' && c <= 'Z') {
			return false
		}
	}
	return true
}

func (s Snapshot) Validate() error {
	if !SymbolOK(s.Symbol) || s.Exchange.IsZero() || s.Received.IsZero() || (s.Source != "ws" && s.Source != "rest") {
		return errors.New("bookfeed: invalid snapshot identity")
	}
	for i := range 5 {
		b, a := s.Bids[i], s.Asks[i]
		if b.Price <= 0 || a.Price <= 0 || b.Qty < 0 || a.Qty < 0 || b.Price > a.Price {
			return errors.New("bookfeed: invalid levels")
		}
		if i > 0 && (b.Price > s.Bids[i-1].Price || a.Price < s.Asks[i-1].Price) {
			return errors.New("bookfeed: unordered levels")
		}
	}
	return nil
}

// ExchangeTime interprets KIS's HHMMSS clock on the nearest KST receive day.
// Keeping the exchange clock, rather than stamping it with Now, exposes a stale
// REST response to consumers. The nearest-day rule handles midnight rollover.
func ExchangeTime(hour string, received time.Time) (time.Time, error) {
	t, err := time.Parse("150405", hour)
	if err != nil {
		return time.Time{}, errors.New("bookfeed: invalid exchange clock")
	}
	r := received.In(KST)
	at := time.Date(r.Year(), r.Month(), r.Day(), t.Hour(), t.Minute(), t.Second(), 0, KST)
	if at.Sub(r) > 12*time.Hour {
		at = at.Add(-24 * time.Hour)
	}
	if r.Sub(at) > 12*time.Hour {
		at = at.Add(24 * time.Hour)
	}
	return at, nil
}

func (s Snapshot) At() time.Time {
	if s.Exchange.Before(s.Received) {
		return s.Exchange
	}
	return s.Received
}
func (s Snapshot) Values() map[string]any {
	v := map[string]any{"symbol": s.Symbol, "ts_exchange": s.Exchange.Format(time.RFC3339Nano), "ts_received": s.Received.Format(time.RFC3339Nano), "source": s.Source}
	for i := range 5 {
		n := strconv.Itoa(i + 1)
		v["bid_price_"+n] = strconv.FormatInt(s.Bids[i].Price, 10)
		v["bid_qty_"+n] = strconv.FormatInt(s.Bids[i].Qty, 10)
		v["ask_price_"+n] = strconv.FormatInt(s.Asks[i].Price, 10)
		v["ask_qty_"+n] = strconv.FormatInt(s.Asks[i].Qty, 10)
	}
	return v
}

func Decode(v map[string]any) (Snapshot, error) {
	var s Snapshot
	get := func(k string) string {
		v, ok := v[k].(string)
		if !ok {
			return ""
		}
		return v
	}
	s.Symbol, s.Source = get("symbol"), get("source")
	var err error
	if s.Exchange, err = time.Parse(time.RFC3339Nano, get("ts_exchange")); err != nil {
		return s, errors.New("bookfeed: invalid exchange timestamp")
	}
	if s.Received, err = time.Parse(time.RFC3339Nano, get("ts_received")); err != nil {
		return s, errors.New("bookfeed: invalid receive timestamp")
	}
	for i := range 5 {
		n := strconv.Itoa(i + 1)
		for k, p := range map[string]*int64{"bid_price_" + n: &s.Bids[i].Price, "bid_qty_" + n: &s.Bids[i].Qty, "ask_price_" + n: &s.Asks[i].Price, "ask_qty_" + n: &s.Asks[i].Qty} {
			*p, err = strconv.ParseInt(get(k), 10, 64)
			if err != nil {
				return s, errors.New("bookfeed: invalid level number")
			}
		}
	}
	return s, s.Validate()
}

func Publish(ctx context.Context, r redis.Cmdable, s Snapshot) error {
	if err := s.Validate(); err != nil {
		return err
	}
	return r.XAdd(ctx, &redis.XAddArgs{Stream: "book:kis:" + s.Symbol, MaxLen: 10000, Approx: true, Values: s.Values()}).Err()
}

// Latest reads the most recent full snapshot; no consumer group or cursor is
// needed. Silence remains visible through the snapshot's original timestamps.
func Latest(ctx context.Context, r redis.Cmdable, symbol string) (Snapshot, bool, error) {
	if !SymbolOK(symbol) {
		return Snapshot{}, false, errors.New("bookfeed: invalid symbol")
	}
	rows, err := r.XRevRangeN(ctx, "book:kis:"+symbol, "+", "-", 1).Result()
	if err != nil {
		return Snapshot{}, false, errors.New("bookfeed: Redis read failed")
	}
	if len(rows) == 0 {
		return Snapshot{}, false, nil
	}
	s, err := Decode(rows[0].Values)
	if err != nil {
		return s, false, err
	}
	if s.Symbol != symbol {
		return s, false, fmt.Errorf("bookfeed: stream symbol mismatch")
	}
	return s, true, nil
}
