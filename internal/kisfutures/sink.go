package kisfutures

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

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

const (
	// writtenPrefix keys the marker of a written bar, outside StreamPrefix:
	// dedupe:minute:kis:futures:F200.2612:2026-11-24T00:00:00Z.
	writtenPrefix = "dedupe:" + StreamPrefix
	// writtenTTL outlives the longest a bar is held for a retry (maxPending
	// bars of four contracts are about eight hours).
	writtenTTL = 48 * time.Hour
)

// appendOnce appends a bar unless its marker exists, atomically, so a retry
// after a lost reply adds nothing. The marker is set after the XADD: a refused
// XADD ends the script and leaves the bar to be written again.
var appendOnce = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 1 then
  return 0
end
redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[1], '*', unpack(ARGV, 3))
redis.call('SET', KEYS[2], '1', 'EX', ARGV[2])
return 1
`)

type redisSink struct {
	client redis.UniversalClient
	maxLen int64
}

// NewRedisSink appends bars to per-symbol streams with MAXLEN ~. A symbol's
// minute is appended once: the first write wins, as in the insert-only
// research table. It takes ownership of client and closes it. An unknown
// session_date has no field.
func NewRedisSink(client redis.UniversalClient, maxLen int64) (Sink, error) {
	if client == nil || maxLen <= 0 {
		return nil, errors.New("kisfutures: Redis sink needs a client and a positive max length")
	}
	return &redisSink{client: client, maxLen: maxLen}, nil
}

func (s *redisSink) Write(ctx context.Context, bar Bar) error {
	row := bar.row()
	args := []any{
		s.maxLen, int64(writtenTTL / time.Second),
		"time_utc", row.TimeUTC, "symbol", row.Symbol, "product", row.Product,
		"contract_month", row.ContractMonth, "roll_state", row.RollState,
		"session_segment", row.SessionSegment, "source", row.Source,
		"open", row.Open, "high", row.High, "low", row.Low, "close", row.Close,
		"volume", strconv.FormatInt(row.Volume, 10), "received_at", row.ReceivedAt,
		"volume_gap", strconv.FormatBool(row.VolumeGap),
	}
	if row.SessionDate != nil {
		args = append(args, "session_date", *row.SessionDate)
	}
	keys := []string{StreamPrefix + bar.Symbol, writtenPrefix + bar.Symbol + ":" + row.TimeUTC}
	if err := appendOnce.Run(ctx, s.client, keys, args...).Err(); err != nil {
		return fmt.Errorf("kisfutures: Redis stream write failed: %w", err)
	}
	return nil
}

func (s *redisSink) Close() error { return s.client.Close() }

type jsonlSink struct {
	file *os.File
	// torn: a failed write left part of a row after the first whole bytes,
	// and it has not been cut off yet.
	torn  bool
	whole int64
}

// NewJSONLSink appends one row per line to the file at path, one write per bar.
func NewJSONLSink(path string) (Sink, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("kisfutures: open JSONL output: %w", err)
	}
	return &jsonlSink{file: file}, nil
}

// Write is only called by the lane's flush path, one call at a time, and the
// lane is the file's only writer. A row cut short is removed, at once or
// before the next write; until that succeeds nothing more is appended.
func (s *jsonlSink) Write(_ context.Context, bar Bar) error {
	data, err := MarshalRow(bar)
	if err != nil {
		return err
	}
	if err := s.cut(); err != nil {
		return err
	}
	info, err := s.file.Stat()
	if err != nil {
		return fmt.Errorf("kisfutures: JSONL stat failed: %w", err)
	}
	if n, err := s.file.Write(append(data, '\n')); err != nil {
		if n > 0 {
			s.torn, s.whole = true, info.Size()
		}
		return errors.Join(fmt.Errorf("kisfutures: JSONL write failed: %w", err), s.cut())
	}
	return nil
}

// cut truncates a partial row left by a failed write.
func (s *jsonlSink) cut() error {
	if !s.torn {
		return nil
	}
	if err := s.file.Truncate(s.whole); err != nil {
		return fmt.Errorf("kisfutures: remove the partial JSONL row: %w", err)
	}
	s.torn = false
	return nil
}

func (s *jsonlSink) Close() error { return s.file.Close() }
