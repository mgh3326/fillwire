package kisfutures

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// Sink receives closed bars, oldest first.
type Sink interface {
	Write(ctx context.Context, bar Bar) error
	Close() error
}

// StreamPrefix is the Redis Stream key prefix: bars of F200.2612 go to
// minute:kis:futures:F200.2612.
const StreamPrefix = "minute:kis:futures:"

// DefaultMaxLen bounds each stream, as in the other lanes.
const DefaultMaxLen = 100000

type redisSink struct {
	client redis.UniversalClient
	maxLen int64
}

// NewRedisSink appends bars to per-symbol streams with MAXLEN ~. It takes
// ownership of client and closes it. An unknown session_date has no field.
func NewRedisSink(client redis.UniversalClient, maxLen int64) (Sink, error) {
	if client == nil || maxLen <= 0 {
		return nil, errors.New("kisfutures: Redis sink needs a client and a positive max length")
	}
	return &redisSink{client: client, maxLen: maxLen}, nil
}

func (s *redisSink) Write(ctx context.Context, bar Bar) error {
	row := bar.row()
	values := []any{
		"time_utc", row.TimeUTC, "symbol", row.Symbol, "product", row.Product,
		"contract_month", row.ContractMonth, "roll_state", row.RollState,
		"session_segment", row.SessionSegment, "source", row.Source,
		"open", row.Open, "high", row.High, "low", row.Low, "close", row.Close,
		"volume", strconv.FormatInt(row.Volume, 10), "received_at", row.ReceivedAt,
		"volume_gap", strconv.FormatBool(row.VolumeGap),
	}
	if row.SessionDate != nil {
		values = append(values, "session_date", *row.SessionDate)
	}
	err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamPrefix + bar.Symbol, MaxLen: s.maxLen, Approx: true, Values: values,
	}).Err()
	if err != nil {
		return errors.New("kisfutures: Redis stream write failed")
	}
	return nil
}

func (s *redisSink) Close() error { return s.client.Close() }

type jsonlSink struct {
	file *os.File
	torn bool
}

// NewJSONLSink appends one row per line to the file at path, one write per bar.
func NewJSONLSink(path string) (Sink, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("kisfutures: open JSONL output: %w", err)
	}
	return &jsonlSink{file: file}, nil
}

// Write is only called by the lane's flush path, one call at a time.
func (s *jsonlSink) Write(_ context.Context, bar Bar) error {
	data, err := MarshalRow(bar)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if s.torn {
		data = append([]byte{'\n'}, data...)
	}
	n, err := s.file.Write(data)
	if err != nil {
		s.torn = s.torn || n > 0
		return fmt.Errorf("kisfutures: JSONL write failed: %w", err)
	}
	s.torn = false
	return nil
}

func (s *jsonlSink) Close() error { return s.file.Close() }
