package kisfutures

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/testutil"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

// A trade replayed after the cumulative volume started over is older than the
// last one, even though its total is larger: it is not counted again.
func TestOlderReplayWithLargerTotalIsNotCounted(t *testing.T) {
	builder := newBuilder(goldenCalendar(t))
	first := trade(t, "A01612", kstAt(10, 0, 0), "400.10", 9, 9)
	builder.add(testContract, first)
	builder.add(testContract, trade(t, "A01612", kstAt(10, 1, 0), "400.20", 2, 5))
	if got := builder.add(testContract, first); got != duplicate {
		t.Errorf("replay = %v, want duplicate", got)
	}
	// Two trades of one second keep counting.
	if got := builder.add(testContract, trade(t, "A01612", kstAt(10, 1, 0), "400.30", 1, 6)); got != added {
		t.Errorf("same second trade = %v, want added", got)
	}
	bars, _ := builder.closed(kstAt(10, 3, 0))
	if len(bars) != 2 || bars[0].Volume != 9 || bars[1].Volume != 3 {
		t.Fatalf("bars = %+v", bars)
	}
}

// lostReply applies the first write and then reports it as failed, as a reply
// lost on the way back does.
type lostReply struct{ lost bool }

func (h *lostReply) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *lostReply) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *lostReply) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		switch cmd.Name() {
		case "xadd", "eval", "evalsha":
			if err == nil && !h.lost {
				h.lost = true
				err = errors.New("i/o timeout")
				cmd.SetErr(err)
			}
		}
		return err
	}
}

func TestRetryAfterALostReplyAppendsOnce(t *testing.T) {
	mr, reader := testutil.Redis(t)
	writer := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	hook := &lostReply{}
	writer.AddHook(hook)
	sink, err := NewRedisSink(writer, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	bar := Bar{TimeUTC: kstAt(9, 0, 0), Symbol: "F200.2612", Volume: 3, ReceivedAt: kstAt(9, 1, 10)}
	if err := sink.Write(context.Background(), bar); err == nil || !hook.lost {
		t.Fatalf("first write = %v, want the lost reply", err)
	}
	if err := sink.Write(context.Background(), bar); err != nil {
		t.Fatalf("retry = %v; a committed bar must not block the queue", err)
	}
	next := bar
	next.TimeUTC = kstAt(9, 1, 0)
	if err := sink.Write(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	other := bar
	other.Symbol = "MF200.2612"
	if err := sink.Write(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	for stream, want := range map[string]int64{"minute:kis:futures:F200.2612": 2, "minute:kis:futures:MF200.2612": 1} {
		if n := reader.XLen(context.Background(), stream).Val(); n != want {
			t.Errorf("%s has %d entries, want %d", stream, n, want)
		}
	}
}

// A write Redis refuses writes nothing and is retried in full.
func TestRefusedWriteIsRetried(t *testing.T) {
	mr, client := testutil.Redis(t)
	sink, err := NewRedisSink(redis.NewClient(&redis.Options{Addr: mr.Addr()}), 100)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	bar := Bar{TimeUTC: kstAt(9, 0, 0), Symbol: "F200.2612", Volume: 3, ReceivedAt: kstAt(9, 1, 10)}
	if err := mr.Set("minute:kis:futures:F200.2612", "not a stream"); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(context.Background(), bar); err == nil {
		t.Fatal("a write to a wrong-type key succeeded")
	}
	mr.Del("minute:kis:futures:F200.2612")
	if err := sink.Write(context.Background(), bar); err != nil {
		t.Fatal(err)
	}
	if n := client.XLen(context.Background(), "minute:kis:futures:F200.2612").Val(); n != 1 {
		t.Fatalf("%d entries, want 1", n)
	}
}

func TestRedisWriteErrorKeepsTheCause(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	sink, err := NewRedisSink(client, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	err = sink.Write(context.Background(), Bar{TimeUTC: kstAt(9, 0, 0), Symbol: "F200.2612"})
	if !errors.Is(err, redis.ErrClosed) {
		t.Fatalf("err = %v, want the Redis cause", err)
	}
}

// The client decrypts an encrypted frame into the event's fields and keeps the
// ciphertext in Raw: the trade is read from the fields.
func TestEncryptedFrameIsReadFromTheDecryptedFields(t *testing.T) {
	f := newFixture(t, "2026-11-24T09:00:00+09:00")
	f.reconcile()
	plain := event(TRDayTrade, "A01612", "400.10", 3, 3, kstAt(9, 0, 1))
	encrypted := ws.Event{TR: TRDayTrade, ReceivedAt: plain.ReceivedAt, Raw: []byte("1|H0IFCNT0|001|c2VjcmV0Y2lwaGVydGV4dA=="),
		Fields: strings.Split(record(TRDayTrade, "A01612", "090001", "400.10", 3, 3), "^")}
	f.lane.Handle(encrypted)
	f.clock.Set(kstAt(9, 1, 10).Add(time.Second))
	f.lane.flush(context.Background())
	if len(f.sink.bars) != 1 || f.sink.bars[0].Open != "400.10" || f.sink.bars[0].Volume != 3 {
		t.Fatalf("bars = %+v, stats %+v", f.sink.bars, f.lane.Stats())
	}
}
