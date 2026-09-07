// Package stream provides the Redis Streams durability boundary for fills.
package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/decode"
	"github.com/redis/go-redis/v9"
)

const payloadField = "payload"

// Config identifies one broker stream and its one initial consumer.
type Config struct {
	Key          string
	MaxLen       int64
	Group        string
	Consumer     string
	BatchSize    int64
	Block        time.Duration
	ClaimMinIdle time.Duration
	Counters     *decode.Counters
	Logger       *slog.Logger
}

// Message is a decoded stream entry awaiting an ingest response.
type Message struct {
	ID                  string
	Record              decode.Record
	DupSuspect          bool
	DupObservationCount uint64
}

// Queue writes normalized records and reads them through one consumer group.
type Queue struct {
	client redis.UniversalClient
	cfg    Config
}

// New validates the durable queue settings.
func New(client redis.UniversalClient, cfg Config) (*Queue, error) {
	if client == nil {
		return nil, errors.New("stream: redis client is required")
	}
	if strings.TrimSpace(cfg.Key) == "" || strings.TrimSpace(cfg.Group) == "" || strings.TrimSpace(cfg.Consumer) == "" {
		return nil, errors.New("stream: key, group, and consumer are required")
	}
	if cfg.MaxLen <= 0 {
		return nil, errors.New("stream: max length must be positive")
	}
	if cfg.BatchSize < 1 || cfg.BatchSize > 200 {
		return nil, errors.New("stream: batch size must be between 1 and 200")
	}
	if cfg.Block < 0 || cfg.ClaimMinIdle < 0 {
		return nil, errors.New("stream: durations cannot be negative")
	}
	return &Queue{client: client, cfg: cfg}, nil
}

// EnsureGroup creates the group before either reads or writes. Starting at 0
// means records written before a first boot are still delivered.
func (q *Queue) EnsureGroup(ctx context.Context) error {
	err := q.client.XGroupCreateMkStream(ctx, q.cfg.Key, q.cfg.Group, "0").Err()
	if err != nil && strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}

// Enqueue durably appends a normalized record before any ingest attempt.
func (q *Queue) Enqueue(ctx context.Context, record decode.Record) (string, error) {
	payload, err := json.Marshal(record)
	if err != nil {
		return "", fmt.Errorf("stream: marshal record: %w", err)
	}
	id, err := q.client.XAdd(ctx, &redis.XAddArgs{
		Stream: q.cfg.Key,
		MaxLen: q.cfg.MaxLen,
		Approx: true,
		Values: []interface{}{
			payloadField, string(payload),
			"dup_suspect", strconv.FormatBool(record.DupSuspect),
			"dup_observation_count", strconv.FormatUint(record.DupObservationCount, 10),
		},
	}).Result()
	if err == nil {
		q.cfg.Counters.IncXAdded()
	}
	return id, err
}

// ReadNew reads only never-delivered messages for this one consumer.
func (q *Queue) ReadNew(ctx context.Context) ([]Message, error) {
	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.cfg.Group,
		Consumer: q.cfg.Consumer,
		Count:    q.cfg.BatchSize,
		Block:    q.cfg.Block,
		Streams:  []string{q.cfg.Key, ">"},
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return q.decodeStreams(streams)
}

// AutoClaim reassigns idle pending entries to this consumer. The caller must
// continue from next until it reaches 0-0 before ordinary reads begin.
func (q *Queue) AutoClaim(ctx context.Context, start string) ([]Message, string, error) {
	entries, next, err := q.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   q.cfg.Key,
		Group:    q.cfg.Group,
		Consumer: q.cfg.Consumer,
		MinIdle:  q.cfg.ClaimMinIdle,
		Start:    start,
		Count:    q.cfg.BatchSize,
	}).Result()
	if err != nil {
		return nil, "", err
	}
	messages, err := q.decodeEntries(entries)
	return messages, next, err
}

// Ack acknowledges exactly the IDs whose corresponding ingest results were
// successful. Callers must never acknowledge an unconfirmed message.
func (q *Queue) Ack(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	acked, err := q.client.XAck(ctx, q.cfg.Key, q.cfg.Group, ids...).Result()
	if err != nil {
		return err
	}
	if acked > 0 {
		q.cfg.Counters.AddXAcked(uint64(acked))
	}
	if acked != int64(len(ids)) {
		missing := int64(len(ids)) - acked
		if missing > 0 {
			q.cfg.Counters.AddXAckShortfall(uint64(missing))
		}
		if q.cfg.Logger != nil {
			q.cfg.Logger.Warn("stream acknowledgements already absent from pending list", "confirmed", acked, "requested", len(ids))
		}
	}
	return nil
}

func (q *Queue) decodeStreams(streams []redis.XStream) ([]Message, error) {
	var entries []redis.XMessage
	for _, stream := range streams {
		entries = append(entries, stream.Messages...)
	}
	return q.decodeEntries(entries)
}

func (q *Queue) decodeEntries(entries []redis.XMessage) ([]Message, error) {
	messages := make([]Message, 0, len(entries))
	for _, entry := range entries {
		raw, ok := entry.Values[payloadField]
		if !ok {
			return nil, fmt.Errorf("stream: entry %s has no payload", entry.ID)
		}
		payload, ok := payloadString(raw)
		if !ok {
			return nil, fmt.Errorf("stream: entry %s payload is not a string", entry.ID)
		}
		var record decode.Record
		if err := json.Unmarshal([]byte(payload), &record); err != nil {
			return nil, fmt.Errorf("stream: decode entry %s: %w", entry.ID, err)
		}
		dupSuspect, err := boolValue(entry.Values["dup_suspect"])
		if err != nil {
			return nil, fmt.Errorf("stream: entry %s has invalid dup_suspect: %w", entry.ID, err)
		}
		observationCount, err := uintValue(entry.Values["dup_observation_count"])
		if err != nil {
			return nil, fmt.Errorf("stream: entry %s has invalid dup observation count: %w", entry.ID, err)
		}
		messages = append(messages, Message{
			ID:                  entry.ID,
			Record:              record,
			DupSuspect:          dupSuspect,
			DupObservationCount: observationCount,
		})
	}
	return messages, nil
}

func payloadString(value interface{}) (string, bool) {
	switch value := value.(type) {
	case string:
		return value, true
	case []byte:
		return string(value), true
	default:
		return "", false
	}
}

func boolValue(value interface{}) (bool, error) {
	if value == nil {
		return false, nil
	}
	raw, ok := payloadString(value)
	if !ok {
		return false, errors.New("not a string")
	}
	return strconv.ParseBool(raw)
}

func uintValue(value interface{}) (uint64, error) {
	if value == nil {
		return 0, nil
	}
	raw, ok := payloadString(value)
	if !ok {
		return 0, errors.New("not a string")
	}
	return strconv.ParseUint(raw, 10, 64)
}
