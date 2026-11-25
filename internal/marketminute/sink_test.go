package marketminute_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/fillwire/internal/marketminute"
	"github.com/redis/go-redis/v9"
)

func TestRedisSinkKeysTheStreamByVenue(t *testing.T) {
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { client.Close() })
	sink, err := marketminute.NewRedisSink(client, 100)
	if err != nil {
		t.Fatal(err)
	}
	bar := marketminute.Bar{
		TimeUTC:        time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC),
		SessionDate:    time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC),
		Symbol:         "005930",
		Venue:          "NTX",
		SessionSegment: marketminute.SegmentNXTPre,
		Source:         marketminute.Source,
		Open:           "100", High: "101", Low: "99", Close: "100", Volume: "10",
	}
	if err := sink.Write(context.Background(), bar); err != nil {
		t.Fatal(err)
	}
	keys := mini.Keys()
	if len(keys) != 1 || keys[0] != "minute:kis:NTX:005930" {
		t.Fatalf("stream keys = %v, want minute:kis:NTX:005930", keys)
	}
}
