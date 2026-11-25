package marketminute

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/redis/go-redis/v9"
)

// JSONLSink appends one shadow row per line. It is the byte contract checked
// against the Python goldens.
type JSONLSink struct {
	mu     sync.Mutex
	buffer *bufio.Writer
}

// NewJSONLSink wraps w. Flush must be called before the process exits.
func NewJSONLSink(w io.Writer) *JSONLSink {
	return &JSONLSink{buffer: bufio.NewWriterSize(w, 64*1024)}
}

// Write implements Sink.
func (s *JSONLSink) Write(_ context.Context, bar Bar) error {
	data, err := MarshalRow(bar)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.buffer.Write(data); err != nil {
		return err
	}
	return s.buffer.WriteByte('\n')
}

// Flush writes buffered rows to the underlying writer.
func (s *JSONLSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buffer.Flush()
}

// RedisSink appends rows to Redis Streams named minute:kis:<symbol> with
// MAXLEN ~, so the shadow output stays bounded. Redis has no null field, so a
// nil value is omitted from the entry instead of written as an empty string.
type RedisSink struct {
	mu      sync.Mutex
	client  redis.UniversalClient
	keyBase string
	maxLen  int64
}

// DefaultRedisMaxLen matches the quote lane's bounded stream.
const DefaultRedisMaxLen int64 = 100000

// NewRedisSink validates the client and stream length.
func NewRedisSink(client redis.UniversalClient, maxLen int64) (*RedisSink, error) {
	if client == nil {
		return nil, errors.New("marketminute: redis sink needs a client")
	}
	if maxLen <= 0 {
		return nil, errors.New("marketminute: redis max length must be positive")
	}
	return &RedisSink{client: client, keyBase: "minute:kis:", maxLen: maxLen}, nil
}

// Write implements Sink.
func (s *RedisSink) Write(ctx context.Context, bar Bar) error {
	values := []any{"time_utc", bar.TimeUTC.UTC().Format("2006-01-02T15:04:05Z")}
	values = append(values,
		"session_date", bar.SessionDate.Format("2006-01-02"),
		"symbol", bar.Symbol,
		"venue", bar.Venue,
		"session_segment", bar.SessionSegment,
		"source", bar.Source,
		"open", bar.Open,
		"high", bar.High,
		"low", bar.Low,
		"close", bar.Close,
		"volume", bar.Volume,
	)
	if bar.Value != nil {
		values = append(values, "value", *bar.Value)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.keyBase + bar.Symbol,
		MaxLen: s.maxLen,
		Approx: true,
		Values: values,
	}).Err()
	if err != nil {
		return fmt.Errorf("marketminute: redis stream write failed")
	}
	return nil
}
