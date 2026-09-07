package decode_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/fillwire/internal/decode"
	"github.com/mgh3326/fillwire/internal/stream"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

func TestT8ValidationDrop(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*ws.Event)
	}{
		{name: "zero quantity", mutate: func(event *ws.Event) { event.Execution.Qty = "0" }},
		{name: "zero price", mutate: func(event *ws.Event) { event.Execution.Price = "0" }},
		{name: "empty order", mutate: func(event *ws.Event) { event.Execution.OrderNo = "   " }},
	} {
		t.Run(test.name, func(t *testing.T) {
			mini := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
			defer client.Close()
			counters := decode.NewCounters()
			queue, err := stream.New(client, stream.Config{Key: "fills:kis", MaxLen: 10, Group: "fillwire-ingest", Consumer: "consumer", BatchSize: 1, Counters: counters})
			if err != nil {
				t.Fatal(err)
			}
			event := fixtureEvent()
			test.mutate(&event)
			decoder := decode.New(decode.Config{AccountMode: "live", Venue: "krx", DupTrackMax: 10, Counters: counters})
			if record, ok := decoder.Decode(event); ok {
				if _, err := queue.Enqueue(context.Background(), record); err != nil {
					t.Fatal(err)
				}
			}
			length, err := client.XLen(context.Background(), "fills:kis").Result()
			if err != nil && err != redis.Nil {
				t.Fatal(err)
			}
			if length != 0 {
				t.Fatalf("XADD count = %d, want 0", length)
			}
			if got := decoder.Dropped(); got != 1 {
				t.Fatalf("drop count = %d, want 1", got)
			}
		})
	}
}

func TestT9MidnightBoundary(t *testing.T) {
	event := fixtureEvent()
	event.Execution.FilledAt = "235959"
	event.ReceivedAt = time.Date(2026, time.September, 8, 0, 0, 3, 0, time.FixedZone("KST", 9*60*60))
	decoder := decode.New(decode.Config{AccountMode: "live", Venue: "krx", DupTrackMax: 10})
	record, ok := decoder.Decode(event)
	if !ok {
		t.Fatal("midnight execution was dropped")
	}
	want := time.Date(2026, time.September, 7, 23, 59, 59, 0, time.FixedZone("KST", 9*60*60))
	if !record.FilledAt.Equal(want) {
		t.Fatalf("filled_at = %s, want %s", record.FilledAt.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func TestT13FillSeqUsesAllFields(t *testing.T) {
	first := fixtureEvent()
	second := fixtureEvent()
	second.Fields[13] = "3" // Same order/qty/price/second; another KIS wire field differs.
	second.Execution.Filled = "3"
	decoder := decode.New(decode.Config{AccountMode: "live", Venue: "krx", DupTrackMax: 10})
	firstRecord, ok := decoder.Decode(first)
	if !ok {
		t.Fatal("first execution was dropped")
	}
	secondRecord, ok := decoder.Decode(second)
	if !ok {
		t.Fatal("second execution was dropped")
	}
	if firstRecord.FillSeq == secondRecord.FillSeq {
		t.Fatalf("fill_seq collision = %d; all decrypted fields must participate", firstRecord.FillSeq)
	}
}

func TestT14DuplicateSuspectStreamEntry(t *testing.T) {
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer client.Close()
	counters := decode.NewCounters()
	queue, err := stream.New(client, stream.Config{Key: "fills:kis", MaxLen: 10, Group: "fillwire-ingest", Consumer: "consumer", BatchSize: 1, Counters: counters})
	if err != nil {
		t.Fatal(err)
	}
	decoder := decode.New(decode.Config{AccountMode: "live", Venue: "krx", DupTrackMax: 10, Counters: counters})
	first, ok := decoder.Decode(fixtureEvent())
	if !ok {
		t.Fatal("first execution was dropped")
	}
	second, ok := decoder.Decode(fixtureEvent())
	if !ok {
		t.Fatal("second execution was dropped")
	}
	if first.DupSuspect || first.DupObservationCount != 1 {
		t.Fatalf("first record duplicate metadata = suspect:%t count:%d", first.DupSuspect, first.DupObservationCount)
	}
	if !second.DupSuspect || second.DupObservationCount != 2 {
		t.Fatalf("second record duplicate metadata = suspect:%t count:%d", second.DupSuspect, second.DupObservationCount)
	}
	if _, err := queue.Enqueue(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	entries, err := client.XRange(context.Background(), "fills:kis", "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("stream entries = %d, want 2", len(entries))
	}
	if got := entries[0].Values["dup_suspect"]; got != "false" {
		t.Fatalf("first dup_suspect = %#v, want false", got)
	}
	if got := entries[1].Values["dup_suspect"]; got != "true" {
		t.Fatalf("second dup_suspect = %#v, want true", got)
	}
	if got := entries[1].Values["dup_observation_count"]; got != "2" {
		t.Fatalf("second observation count = %#v, want 2", got)
	}
	if got := counters.Snapshot().DupSuspect; got != 1 {
		t.Fatalf("duplicate counter = %d, want 1", got)
	}
}

func TestT15DuplicateMetadataDoesNotChangeIdempotencyKey(t *testing.T) {
	decoder := decode.New(decode.Config{AccountMode: "live", Venue: "krx", DupTrackMax: 10})
	first, ok := decoder.Decode(fixtureEvent())
	if !ok {
		t.Fatal("first execution was dropped")
	}
	second, ok := decoder.Decode(fixtureEvent())
	if !ok {
		t.Fatal("second execution was dropped")
	}
	if first.FillSeq != second.FillSeq {
		t.Fatalf("fill_seq changed from %d to %d for duplicate observation", first.FillSeq, second.FillSeq)
	}
	if first.BrokerOrderID != second.BrokerOrderID {
		t.Fatalf("broker_order_id changed from %q to %q", first.BrokerOrderID, second.BrokerOrderID)
	}
}

func TestT17DuplicateTrackerIsBounded(t *testing.T) {
	decoder := decode.New(decode.Config{AccountMode: "live", Venue: "krx", DupTrackMax: 2})
	for _, orderNo := range []string{"A123456789", "A123456790", "A123456791"} {
		event := fixtureEvent()
		event.Execution.OrderNo = orderNo
		event.Fields[2] = orderNo
		if _, ok := decoder.Decode(event); !ok {
			t.Fatalf("execution %s was dropped", orderNo)
		}
	}
	if got := decoder.TrackerSize(); got > 2 {
		t.Fatalf("tracker size = %d, exceeds configured max 2", got)
	}
}

func fixtureEvent() ws.Event {
	fields := []string{
		"HTS_EXAMPLE", "00000000", "A123456789", "0000000000", "02", "00", "00",
		"00", "005930", "3", "71200", "093015", "0", "2",
	}
	return ws.Event{
		TR:         ws.TRExecutionLive,
		Key:        fields[0],
		Fields:     fields,
		Raw:        []byte("0|H0STCNI0|1|HTS_EXAMPLE^00000000^A123456789^0000000000^02^00^00^00^005930^3^71200^093015^0^2"),
		ReceivedAt: time.Date(2026, time.September, 7, 9, 30, 20, 0, time.FixedZone("KST", 9*60*60)),
		Execution: &ws.Execution{
			OrderNo:  "A123456789",
			Symbol:   "005930",
			Side:     ws.SideBuy,
			Qty:      "3",
			Price:    "71200",
			FilledAt: "093015",
			Filled:   "2",
		},
	}
}
