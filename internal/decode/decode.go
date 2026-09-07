// Package decode turns KIS websocket events into ledger upsert records.
package decode

import (
	"crypto/sha256"
	"encoding/binary"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/mgh3326/go-kis/kis/ws"
)

var kst = time.FixedZone("KST", 9*60*60)

// Config supplies the immutable ledger attributes for a KIS decoder.
type Config struct {
	AccountMode string
	Venue       string
	DupTrackMax int
	Counters    *Counters
	Logger      *slog.Logger
}

// Decoder validates and normalizes execution events. Invalid executions are
// intentionally dropped before they reach Redis; all other records are safe
// to deliver at least once.
type Decoder struct {
	accountMode string
	venue       string
	logger      *slog.Logger
	counters    *Counters
	duplicates  *duplicateTracker
}

// New creates a decoder for one KIS account mode and venue.
func New(cfg Config) *Decoder {
	return &Decoder{
		accountMode: cfg.AccountMode,
		venue:       cfg.Venue,
		logger:      cfg.Logger,
		counters:    cfg.Counters,
		duplicates:  newDuplicateTracker(cfg.DupTrackMax),
	}
}

// Dropped returns the count of validation failures. Non-execution events are
// ignored, not counted as drops.
func (d *Decoder) Dropped() uint64 { return d.counters.Snapshot().Dropped }

// TrackerSize exposes bounded duplicate-tracker occupancy for diagnostics and
// tests. It is not a durable ledger count.
func (d *Decoder) TrackerSize() int { return d.duplicates.size() }

// RawPayload is a reparsable, deliberately redacted representation of the
// websocket event. It never includes Event.Raw, approval keys, or tokens.
type RawPayload struct {
	TR                  string    `json:"tr"`
	Fields              []string  `json:"fields"`
	ReceivedAt          time.Time `json:"received_at"`
	DupSuspect          bool      `json:"dup_suspect,omitempty"`
	DupObservationCount uint64    `json:"dup_observation_count,omitempty"`
}

// Record is the exact allowlisted execution-ledger upsert payload. Fields
// controlled by the ingest envelope (source and source_run_id) are not here.
type Record struct {
	Broker              string     `json:"broker"`
	AccountMode         string     `json:"account_mode"`
	Venue               string     `json:"venue"`
	InstrumentType      string     `json:"instrument_type"`
	Symbol              string     `json:"symbol"`
	RawSymbol           string     `json:"raw_symbol"`
	Side                string     `json:"side"`
	BrokerOrderID       string     `json:"broker_order_id"`
	FillSeq             int64      `json:"fill_seq"`
	FilledQty           string     `json:"filled_qty"`
	FilledPrice         string     `json:"filled_price"`
	FilledNotional      *string    `json:"filled_notional"`
	FeeAmount           *string    `json:"fee_amount"`
	FeeCurrency         string     `json:"fee_currency"`
	FilledAt            time.Time  `json:"filled_at"`
	Currency            string     `json:"currency"`
	CorrelationID       *string    `json:"correlation_id"`
	RawPayloadJSON      RawPayload `json:"raw_payload_json"`
	DupSuspect          bool       `json:"-"`
	DupObservationCount uint64     `json:"-"`
}

// Decode returns a normalized ledger record when event is a valid KIS
// execution. The false result means either a non-execution event or a
// validation failure; only validation failures increment Dropped.
func (d *Decoder) Decode(event ws.Event) (Record, bool) {
	if event.Execution == nil {
		return Record{}, false
	}

	execution := event.Execution
	orderNo := strings.TrimSpace(execution.OrderNo)
	if orderNo == "" {
		d.drop("empty broker order id", execution)
		return Record{}, false
	}

	symbol := strings.ToUpper(strings.TrimSpace(execution.Symbol))
	if symbol == "" {
		d.drop("empty symbol", execution)
		return Record{}, false
	}

	qty, err := execution.QtyInt()
	if err != nil || qty <= 0 {
		d.drop("filled quantity must be a positive integer", execution)
		return Record{}, false
	}

	price, err := execution.PriceRat()
	if err != nil || price.Sign() <= 0 {
		d.drop("filled price must be positive", execution)
		return Record{}, false
	}

	if execution.Side != ws.SideBuy && execution.Side != ws.SideSell {
		d.drop("unknown execution side", execution)
		return Record{}, false
	}

	filledAt, err := combineKSTDate(event.ReceivedAt, execution.FilledAt)
	if err != nil {
		d.drop("invalid filled_at", execution)
		return Record{}, false
	}

	fillSeq := DeriveFillSeq(event.Fields)
	observationCount := d.duplicates.observe(duplicateKey{orderNo: orderNo, fillSeq: fillSeq})
	dupSuspect := observationCount >= 2
	if dupSuspect {
		d.counters.IncDupSuspect()
	}

	rawPayload := RawPayload{TR: event.TR, Fields: slices.Clone(event.Fields), ReceivedAt: event.ReceivedAt}
	if dupSuspect {
		rawPayload.DupSuspect = true
		rawPayload.DupObservationCount = observationCount
	}

	return Record{
		Broker:              "kis",
		AccountMode:         d.accountMode,
		Venue:               d.venue,
		InstrumentType:      "equity_kr",
		Symbol:              symbol,
		RawSymbol:           execution.Symbol,
		Side:                string(execution.Side),
		BrokerOrderID:       orderNo,
		FillSeq:             fillSeq,
		FilledQty:           execution.Qty,
		FilledPrice:         execution.Price,
		FeeCurrency:         "KRW",
		FilledAt:            filledAt,
		Currency:            "KRW",
		RawPayloadJSON:      rawPayload,
		DupSuspect:          dupSuspect,
		DupObservationCount: observationCount,
	}, true
}

func (d *Decoder) drop(reason string, execution *ws.Execution) {
	d.counters.IncDropped()
	if d.logger == nil {
		return
	}
	// These values are execution data, never credentials. Event.Raw and Event.Key
	// are deliberately excluded so no key material can reach logs.
	d.logger.Warn("dropping invalid KIS execution", "reason", reason, "order_no", execution.OrderNo, "symbol", execution.Symbol, "qty", execution.Qty, "price", execution.Price)
}

func combineKSTDate(receivedAt time.Time, filledAt string) (time.Time, error) {
	clock, err := time.ParseInLocation("150405", filledAt, kst)
	if err != nil {
		return time.Time{}, err
	}
	receivedKST := receivedAt.In(kst)
	candidate := time.Date(receivedKST.Year(), receivedKST.Month(), receivedKST.Day(), clock.Hour(), clock.Minute(), clock.Second(), 0, kst)
	if candidate.After(receivedKST.Add(time.Hour)) {
		candidate = candidate.AddDate(0, 0, -1)
	}
	return candidate, nil
}

// DeriveFillSeq is deterministic per complete decrypted KIS record. It must
// run exactly once at decode time; consumers use the carried stream value.
func DeriveFillSeq(fields []string) int64 {
	digest := sha256.Sum256([]byte(strings.Join(fields, "^")))
	return int64(binary.BigEndian.Uint32(digest[:4]) & 0x7fffffff)
}
