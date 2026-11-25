package kisfutures

import (
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/kst"
	"github.com/mgh3326/go-kis/kis/ws"
)

// Realtime trade transactions, from the official KIS sample repository
// https://github.com/koreainvestment/open-trading-api (commit b0a0d33),
// examples_llm/domestic_futureoption/:
//
//	H0IFCNT0  index_futures_realtime_conclusion  지수선물 실시간체결가 [실시간-010]
//	H0MFCNT0  krx_ngt_futures_ccnl               KRX야간선물 실시간종목체결 [실시간-064]
//
// Both samples list the same first eleven columns, which are all this package
// reads. The futures execution notices are account data and are not
// subscribed.
const (
	TRDayTrade   = "H0IFCNT0"
	TRNightTrade = "H0MFCNT0"
)

// tradeTRs is the closed set of transactions the lane subscribes.
var tradeTRs = []string{TRDayTrade, TRNightTrade}

// IsTradeTR reports whether tr is one of the futures trade transactions.
func IsTradeTR(tr string) bool { return tr == TRDayTrade || tr == TRNightTrade }

// Column positions of one trade record.
const (
	colCode     = 0  // futs_shrn_iscd
	colHour     = 1  // bsop_hour
	colPrice    = 5  // futs_prpr
	colQty      = 9  // last_cnqn
	colAcmlVol  = 10 // acml_vol
	minColumns  = colAcmlVol + 1
	maxClockGap = 5 * time.Minute
)

// Trade is one execution read from a realtime frame.
type Trade struct {
	TR      string // the stream that carried it
	Code    string
	At      time.Time // exchange time, KST
	Price   Price
	Qty     int64
	AcmlVol int64
}

// Price is an exact decimal price that keeps the text KIS sent.
type Price struct {
	Text  string
	value *big.Rat
}

// ParsePrice accepts a positive plain decimal number.
func ParsePrice(raw string) (Price, error) {
	whole, fraction, point := strings.Cut(raw, ".")
	if whole == "" || !allDigits(whole) || !allDigits(fraction) || (point && fraction == "") {
		return Price{}, fmt.Errorf("kisfutures: price %q is not a decimal number", raw)
	}
	value, ok := new(big.Rat).SetString(raw)
	if !ok || value.Sign() <= 0 {
		return Price{}, fmt.Errorf("kisfutures: price %q is not positive", raw)
	}
	return Price{Text: raw, value: value}, nil
}

func (p Price) cmp(other Price) int { return p.value.Cmp(other.value) }

var errFrame = errors.New("kisfutures: malformed frame")

// ParseEvent reads the trades of one realtime event. The frame header,
//
//	<encrypted flag>|<tr_id>|<record count>|<payload>
//
// is clear in ev.Raw and gives the record count. The records are ev.Fields,
// the payload the client has split at '^' and, for an encrypted frame,
// decrypted first: count records of equal width. received places the
// date-less exchange clock. Records that cannot be read are counted in invalid
// and the others are returned.
func ParseEvent(ev ws.Event, received time.Time) (trades []Trade, invalid int, err error) {
	parts := strings.SplitN(string(ev.Raw), "|", 4)
	if len(parts) < 4 || !IsTradeTR(parts[1]) {
		return nil, 0, errFrame
	}
	count, convErr := strconv.Atoi(strings.TrimSpace(parts[2]))
	if convErr != nil || count <= 0 {
		return nil, 0, errFrame
	}
	fields := ev.Fields
	if len(fields)%count != 0 || len(fields)/count < minColumns {
		return nil, 0, errFrame
	}
	width := len(fields) / count
	for i := range count {
		trade, ok := parseRecord(fields[i*width:(i+1)*width], received)
		if !ok {
			invalid++
			continue
		}
		trade.TR = parts[1]
		trades = append(trades, trade)
	}
	return trades, invalid, nil
}

func parseRecord(f []string, received time.Time) (Trade, bool) {
	at, ok := exchangeTime(f[colHour], received)
	if !ok || f[colCode] == "" {
		return Trade{}, false
	}
	price, err := ParsePrice(f[colPrice])
	if err != nil {
		return Trade{}, false
	}
	qty, err := strconv.ParseInt(f[colQty], 10, 64)
	if err != nil || qty <= 0 {
		return Trade{}, false
	}
	acml, err := strconv.ParseInt(f[colAcmlVol], 10, 64)
	if err != nil || acml < qty {
		return Trade{}, false
	}
	return Trade{Code: f[colCode], At: at, Price: price, Qty: qty, AcmlVol: acml}, true
}

// exchangeTime places an HHMMSS exchange clock on the KST day nearest to the
// receive time. Whether KIS writes the hours after midnight of a night session
// as 00-05 or as 24-29 is [unverified], so both are accepted. A clock further
// than maxClockGap from the receive time is refused: it cannot be dated.
func exchangeTime(hhmmss string, received time.Time) (time.Time, bool) {
	if len(hhmmss) != 6 || !allDigits(hhmmss) {
		return time.Time{}, false
	}
	hour, _ := strconv.Atoi(hhmmss[:2])
	minute, _ := strconv.Atoi(hhmmss[2:4])
	second, _ := strconv.Atoi(hhmmss[4:])
	if hour > 29 || minute > 59 || second > 59 {
		return time.Time{}, false
	}
	local := received.In(kst.Zone)
	at := time.Date(local.Year(), local.Month(), local.Day(), hour%24, minute, second, 0, kst.Zone)
	switch gap := at.Sub(local); {
	case gap > 12*time.Hour:
		at = at.AddDate(0, 0, -1)
	case gap < -12*time.Hour:
		at = at.AddDate(0, 0, 1)
	}
	if gap := at.Sub(local); gap > maxClockGap || gap < -maxClockGap {
		return time.Time{}, false
	}
	return at, true
}
