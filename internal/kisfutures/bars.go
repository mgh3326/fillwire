package kisfutures

import (
	"cmp"
	"encoding/json"
	"slices"
	"time"
)

const (
	// closeGrace is auto_trader's CLOSE_GRACE: a bar is closed when
	// bar start + 1 minute + 10 s <= now.
	closeGrace = 10 * time.Second
	source     = "KIS"
)

// Bar is one closed 1-minute bar: the columns of
// research.kr_futures_candles_1m that a collector owns, the receive clock and
// the continuity flag.
type Bar struct {
	TimeUTC        time.Time // bar start
	Symbol         string
	Product        string
	ContractMonth  string // YYMM
	RollState      string
	SessionSegment string
	SessionDate    time.Time // zero: unknown
	Open           string
	High           string
	Low            string
	Close          string
	Volume         int64     // sum of the trade quantities received
	ReceivedAt     time.Time // when the bar was closed here
	// VolumeGap reports that the cumulative volume of the feed does not
	// continue across the trades of this bar or the ones just before it, or
	// that the feed was joined after the session's first trade: trades are
	// missing, so the bar may be partial.
	VolumeGap bool
}

type barRow struct {
	TimeUTC        string  `json:"time_utc"`
	Symbol         string  `json:"symbol"`
	Product        string  `json:"product"`
	ContractMonth  string  `json:"contract_month"`
	RollState      string  `json:"roll_state"`
	SessionSegment string  `json:"session_segment"`
	SessionDate    *string `json:"session_date"`
	Source         string  `json:"source"`
	Open           string  `json:"open"`
	High           string  `json:"high"`
	Low            string  `json:"low"`
	Close          string  `json:"close"`
	Volume         int64   `json:"volume,string"`
	ReceivedAt     string  `json:"received_at"`
	VolumeGap      bool    `json:"volume_gap"`
}

const timeLayout = "2006-01-02T15:04:05Z"

func (b Bar) row() barRow {
	row := barRow{
		TimeUTC: b.TimeUTC.UTC().Format(timeLayout), Symbol: b.Symbol, Product: b.Product,
		ContractMonth: b.ContractMonth, RollState: b.RollState, SessionSegment: b.SessionSegment,
		Source: source, Open: b.Open, High: b.High, Low: b.Low, Close: b.Close, Volume: b.Volume,
		ReceivedAt: b.ReceivedAt.UTC().Format(time.RFC3339Nano), VolumeGap: b.VolumeGap,
	}
	if !b.SessionDate.IsZero() {
		day := b.SessionDate.Format(time.DateOnly)
		row.SessionDate = &day
	}
	return row
}

// MarshalRow renders one JSONL row without the newline. Prices and volume are
// exact decimal strings and an unknown session_date is null.
func MarshalRow(b Bar) ([]byte, error) { return json.Marshal(b.row()) }

type barKey struct {
	code  string
	start int64
}

type openBar struct {
	contract               Contract
	start                  time.Time
	open, high, low, close Price
	volume                 int64
	gap                    bool
}

// feedState is what the builder remembers about one contract's trade feed.
type feedState struct {
	seen    bool
	acmlVol int64
	at      time.Time
	emitted time.Time // start of the newest bar already closed
}

// addResult says what the builder did with a trade.
type addResult int

const (
	added addResult = iota
	// duplicate: the cumulative volume and the clock did not move forward.
	duplicate
	// late: the trade belongs to a minute that was already closed.
	late
)

// builder aggregates trades into 1-minute bars. It is not safe for concurrent
// use.
type builder struct {
	calendar *Calendar
	bars     map[barKey]*openBar
	feeds    map[string]*feedState
}

func newBuilder(calendar *Calendar) *builder {
	return &builder{calendar: calendar, bars: map[barKey]*openBar{}, feeds: map[string]*feedState{}}
}

// add puts one trade into the bar of its exchange minute.
func (b *builder) add(contract Contract, t Trade) addResult {
	feed := b.feeds[t.Code]
	if feed == nil {
		feed = &feedState{}
		b.feeds[t.Code] = feed
	}
	var gap bool
	switch {
	case feed.seen && t.AcmlVol > feed.acmlVol:
		gap = t.AcmlVol-t.Qty != feed.acmlVol
	case feed.seen && !t.At.After(feed.at):
		return duplicate
	default:
		// The first trade seen, or the cumulative volume restarted with a
		// new session: only a session's first trade is its own total.
		gap = t.AcmlVol != t.Qty
	}
	start := t.At.Truncate(time.Minute)
	if gap && feed.seen {
		// The missing trades may belong to the bar before this one.
		if previous := b.bars[barKey{t.Code, feed.at.Truncate(time.Minute).Unix()}]; previous != nil {
			previous.gap = true
		}
	}
	feed.seen, feed.acmlVol, feed.at = true, t.AcmlVol, t.At
	if !feed.emitted.IsZero() && !start.After(feed.emitted) {
		return late
	}
	key := barKey{t.Code, start.Unix()}
	bar := b.bars[key]
	if bar == nil {
		b.bars[key] = &openBar{
			contract: contract, start: start,
			open: t.Price, high: t.Price, low: t.Price, close: t.Price,
			volume: t.Qty, gap: gap,
		}
		return added
	}
	if t.Price.cmp(bar.high) > 0 {
		bar.high = t.Price
	}
	if t.Price.cmp(bar.low) < 0 {
		bar.low = t.Price
	}
	bar.close = t.Price
	bar.volume += t.Qty
	bar.gap = bar.gap || gap
	return added
}

// closed removes and returns, oldest first, every bar whose minute plus the
// grace has passed at now. A forming bar stays. skipped counts bars that have
// no roll state (a contract beyond the next on the bar's date); auto_trader
// refuses those too.
func (b *builder) closed(now time.Time) (bars []Bar, skipped int) {
	for key, open := range b.bars {
		if now.Before(open.start.Add(time.Minute + closeGrace)) {
			continue
		}
		delete(b.bars, key)
		if feed := b.feeds[key.code]; open.start.After(feed.emitted) {
			feed.emitted = open.start
		}
		segment, sessionDate := b.calendar.Classify(open.start)
		on := sessionDate
		if on.IsZero() {
			on, _ = kstParts(open.start)
		}
		roll, err := RollState(open.contract.Month, on, open.contract.Listed)
		if err != nil {
			skipped++
			continue
		}
		bars = append(bars, Bar{
			TimeUTC: open.start.UTC(), Symbol: open.contract.Symbol, Product: open.contract.Product,
			ContractMonth: open.contract.YYMM, RollState: roll,
			SessionSegment: segment, SessionDate: sessionDate,
			Open: open.open.Text, High: open.high.Text, Low: open.low.Text, Close: open.close.Text,
			Volume: open.volume, ReceivedAt: now, VolumeGap: open.gap,
		})
	}
	slices.SortFunc(bars, func(x, y Bar) int {
		return cmp.Or(x.TimeUTC.Compare(y.TimeUTC), cmp.Compare(x.Symbol, y.Symbol))
	})
	return bars, skipped
}
