package decode_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/fillwire/internal/decode"
	"github.com/mgh3326/fillwire/internal/stream"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

// Real H0STCNI0 layout (26 fields). Account, customer and holder values are
// placeholders; nothing here comes from a real account.
const (
	fRctfCls  = 5
	fCntgQty  = 9
	fCntgUnpr = 10
	fCntgHour = 11
	fRfusYN   = 12
	fCntgYN   = 13
	fAcptYN   = 14
	fOderQty  = 16
	fOderPrc  = 25
)

// fillFrame is a 3-share fill at 71,200 of an order for 5 shares at 71,500,
// so the fill columns (9, 10) differ from the order columns (16, 25).
func fillFrame() []string {
	return []string{
		"HTS_EXAMPLE", "00000000", "A123456789", "0000000000", "02", "0", "00",
		"0", "005930", "3", "71200", "093015", "0", "2", "2", "00000", "5", "",
		"0", "1", "Y", "", "10", "", "EXAMPLE", "71500",
	}
}

// acceptFrame is the order-accept notice for the order behind fillFrame. KIS
// puts the order quantity and price into CNTG_QTY/CNTG_UNPR and the accept
// time into STCK_CNTG_HOUR, with CNTG_YN=1 and ACPT_YN=1.
func acceptFrame() []string {
	fields := fillFrame()
	fields[fCntgQty] = "5"
	fields[fCntgUnpr] = "71500"
	fields[fCntgHour] = "091256"
	fields[fCntgYN] = "1"
	fields[fAcptYN] = "1"
	return fields
}

// eventFromFrame builds the event exactly as go-kis does: Execution members
// come from the go-kis indices (kis/ws/execution.go idxOrderNo..idxFilled).
func eventFromFrame(tr string, fields []string) ws.Event {
	at := func(index int) string {
		if index < len(fields) {
			return fields[index]
		}
		return ""
	}
	side := ws.SideUnknown
	switch at(4) {
	case "01", "1", "S":
		side = ws.SideSell
	case "02", "2", "B":
		side = ws.SideBuy
	}
	return ws.Event{
		TR:         tr,
		Key:        at(0),
		Fields:     fields,
		Raw:        []byte("0|" + tr + "|1|" + strings.Join(fields, "^")),
		ReceivedAt: time.Date(2026, time.September, 30, 9, 30, 20, 0, time.FixedZone("KST", 9*60*60)),
		Execution: &ws.Execution{
			OrderNo:  at(2),
			Side:     side,
			Symbol:   at(8),
			Qty:      at(9),
			Price:    at(10),
			FilledAt: at(11),
			Filled:   at(13),
		},
	}
}

func newQueue(t *testing.T, counters *decode.Counters) (*stream.Queue, *redis.Client) {
	t.Helper()
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	queue, err := stream.New(client, stream.Config{Key: "fills:kis", MaxLen: 100, Group: "fillwire-ingest", Consumer: "consumer", BatchSize: 10, Counters: counters})
	if err != nil {
		t.Fatal(err)
	}
	return queue, client
}

// decodeAll runs every event through the decoder and enqueues what it accepts,
// the same order of operations as the ingress pipeline in cmd/fillwire.
func decodeAll(t *testing.T, decoder *decode.Decoder, queue *stream.Queue, events ...ws.Event) []decode.Record {
	t.Helper()
	var records []decode.Record
	for _, event := range events {
		record, ok := decoder.Decode(event)
		if !ok {
			continue
		}
		if _, err := queue.Enqueue(context.Background(), record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	return records
}

func streamLen(t *testing.T, client *redis.Client) int64 {
	t.Helper()
	length, err := client.XLen(context.Background(), "fills:kis").Result()
	if err != nil && err != redis.Nil {
		t.Fatal(err)
	}
	return length
}

func TestA1AcceptNoticeProducesNoFill(t *testing.T) {
	for _, tr := range []string{ws.TRExecutionLive, ws.TRExecutionVTS} {
		t.Run(tr, func(t *testing.T) {
			if got := decode.ClassifyNotice(tr, acceptFrame()); got != decode.NoticeOrder {
				t.Fatalf("classify(accept) = %q, want %q", got, decode.NoticeOrder)
			}
			counters := decode.NewCounters()
			queue, client := newQueue(t, counters)
			decoder := decode.New(decode.Config{AccountMode: "live", Venue: "krx", DupTrackMax: 10, Counters: counters})
			records := decodeAll(t, decoder, queue, eventFromFrame(tr, acceptFrame()))
			if len(records) != 0 || streamLen(t, client) != 0 {
				t.Fatalf("accept notice produced records=%d stream=%d, want 0 and 0", len(records), streamLen(t, client))
			}
			snapshot := counters.Snapshot()
			if snapshot.NonFillOrder != 1 || decoder.NonFill() != 1 {
				t.Fatalf("non-fill counters = %+v, want NonFillOrder 1", snapshot)
			}
			if snapshot.Dropped != 0 || snapshot.XAdded != 0 {
				t.Fatalf("counters = %+v, want no validation drop and no XADD", snapshot)
			}
		})
	}
}

func TestA2FillUsesFrameFillFieldsNotOrderFields(t *testing.T) {
	if frame := fillFrame(); frame[fOderQty] == frame[fCntgQty] || frame[fOderPrc] == frame[fCntgUnpr] {
		t.Fatal("fixture must keep order qty/price distinct from fill qty/price")
	}
	counters := decode.NewCounters()
	queue, client := newQueue(t, counters)
	decoder := decode.New(decode.Config{AccountMode: "live", Venue: "krx", DupTrackMax: 10, Counters: counters})
	// The accept arrives first, then the fill for the same order.
	records := decodeAll(t, decoder, queue,
		eventFromFrame(ws.TRExecutionLive, acceptFrame()),
		eventFromFrame(ws.TRExecutionLive, fillFrame()),
	)
	if len(records) != 1 || streamLen(t, client) != 1 {
		t.Fatalf("records=%d stream=%d, want exactly one fill", len(records), streamLen(t, client))
	}
	record := records[0]
	if record.FilledQty != "3" || record.FilledPrice != "71200" {
		t.Fatalf("fill qty/price = %s@%s, want 3@71200 from CNTG_QTY/CNTG_UNPR (order was 5@71500)", record.FilledQty, record.FilledPrice)
	}
	wantAt := time.Date(2026, time.September, 30, 9, 30, 15, 0, time.FixedZone("KST", 9*60*60))
	if !record.FilledAt.Equal(wantAt) {
		t.Fatalf("filled_at = %s, want fill time %s (accept was 09:12:56)", record.FilledAt, wantAt)
	}
	if record.BrokerOrderID != "A123456789" || record.Side != "buy" || record.Symbol != "005930" {
		t.Fatalf("record identity = %+v", record)
	}
	if got := counters.Snapshot().NonFillOrder; got != 1 {
		t.Fatalf("NonFillOrder = %d, want 1 (the accept)", got)
	}
}

func TestA3NonFillNoticesAreCountedNeverFilled(t *testing.T) {
	type counts struct{ order, reject, cancel, unknown uint64 }
	tests := []struct {
		name   string
		tr     string
		mutate func([]string) []string
		kind   decode.NoticeKind
		want   counts
	}{
		{"cntg empty", ws.TRExecutionLive, set(fCntgYN, ""), decode.NoticeUnknown, counts{unknown: 1}},
		{"cntg 0", ws.TRExecutionLive, set(fCntgYN, "0"), decode.NoticeUnknown, counts{unknown: 1}},
		{"cntg 3", ws.TRExecutionLive, set(fCntgYN, "3"), decode.NoticeUnknown, counts{unknown: 1}},
		{"cntg 9", ws.TRExecutionLive, set(fCntgYN, "9"), decode.NoticeUnknown, counts{unknown: 1}},
		{"cntg Y", ws.TRExecutionLive, set(fCntgYN, "Y"), decode.NoticeUnknown, counts{unknown: 1}},
		{"cntg 22", ws.TRExecutionLive, set(fCntgYN, "22"), decode.NoticeUnknown, counts{unknown: 1}},
		{"cntg 2x", ws.TRExecutionLive, set(fCntgYN, "2x"), decode.NoticeUnknown, counts{unknown: 1}},
		{"refused accept", ws.TRExecutionLive, chain(set(fCntgYN, "1"), set(fRfusYN, "1")), decode.NoticeRejected, counts{reject: 1}},
		{"refused with cntg 2", ws.TRExecutionLive, set(fRfusYN, "1"), decode.NoticeRejected, counts{reject: 1}},
		{"rfus unknown", ws.TRExecutionLive, set(fRfusYN, "N"), decode.NoticeUnknown, counts{unknown: 1}},
		{"rfus empty", ws.TRExecutionLive, set(fRfusYN, ""), decode.NoticeUnknown, counts{unknown: 1}},
		{"cancel confirm", ws.TRExecutionLive, chain(set(fCntgYN, "1"), set(fRctfCls, "2")), decode.NoticeCanceled, counts{cancel: 1}},
		{"cancel with cntg 2", ws.TRExecutionLive, set(fRctfCls, "2"), decode.NoticeCanceled, counts{cancel: 1}},
		{"modify confirm", ws.TRExecutionLive, chain(set(fCntgYN, "1"), set(fRctfCls, "1")), decode.NoticeOrder, counts{order: 1}},
		{"rctf unknown", ws.TRExecutionLive, set(fRctfCls, "3"), decode.NoticeUnknown, counts{unknown: 1}},
		{"fok ioc cancel", ws.TRExecutionLive, set(fAcptYN, "3"), decode.NoticeCanceled, counts{cancel: 1}},
		{"acpt unknown", ws.TRExecutionLive, set(fAcptYN, "4"), decode.NoticeUnknown, counts{unknown: 1}},
		{"acpt empty", ws.TRExecutionLive, set(fAcptYN, ""), decode.NoticeUnknown, counts{unknown: 1}},
		{"short frame without ACPT_YN", ws.TRExecutionLive, func(fields []string) []string { return fields[:fAcptYN] }, decode.NoticeUnknown, counts{unknown: 1}},
		{"overseas TR", "H0GSCNI0", func(fields []string) []string { return fields }, decode.NoticeUnknown, counts{unknown: 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fields := test.mutate(fillFrame())
			if got := decode.ClassifyNotice(test.tr, fields); got != test.kind {
				t.Fatalf("classify = %q, want %q", got, test.kind)
			}
			counters := decode.NewCounters()
			queue, client := newQueue(t, counters)
			decoder := decode.New(decode.Config{AccountMode: "live", Venue: "krx", DupTrackMax: 10, Counters: counters})
			records := decodeAll(t, decoder, queue, eventFromFrame(test.tr, fields))
			if len(records) != 0 || streamLen(t, client) != 0 {
				t.Fatalf("non-fill produced records=%d stream=%d", len(records), streamLen(t, client))
			}
			s := counters.Snapshot()
			got := counts{s.NonFillOrder, s.NonFillReject, s.NonFillCancel, s.NonFillUnknown}
			if got != test.want {
				t.Fatalf("non-fill counters = %+v, want %+v", got, test.want)
			}
		})
	}
}

func TestClassifyNoticeFillForms(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func([]string) []string
	}{
		{"canonical", func(fields []string) []string { return fields }},
		{"accept flag 1 on fill", set(fAcptYN, "1")},
		{"fill of modified order", set(fRctfCls, "1")},
		{"zero padded codes", chain(set(fRctfCls, "00"), set(fRfusYN, "00"), set(fCntgYN, "02"), set(fAcptYN, "02"))},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := decode.ClassifyNotice(ws.TRExecutionLive, test.mutate(fillFrame())); got != decode.NoticeFill {
				t.Fatalf("classify = %q, want fill", got)
			}
		})
	}
}

// deskRow is one of the four execution_ledger rows (#1172) that fillwire wrote
// from order accept notices on 2026-09-30. Only the stored fields are used:
// symbol, buy, 1 share, the order price, and the accept time recorded as
// filled_at. Rows 58052 and 58053 have no recorded time in the task note; any
// time inside the 09:12-11:35 KST window stands in, since classification does
// not read the time. Account and order numbers are placeholders.
type deskRow struct {
	id     int
	symbol string
	price  string
	hhmmss string
}

var deskRows = []deskRow{
	{58051, "005930", "259500", "091256"},
	{58052, "004020", "28900", "091300"},
	{58053, "034220", "8110", "091400"},
	{58064, "171090", "64500", "113505"},
}

func deskFrame(row deskRow) []string {
	return []string{
		"HTS_EXAMPLE", "00000000", "0000000000", "0000000000", "02", "0", "00",
		"0", row.symbol, "1", row.price, row.hhmmss, "0", "1", "1", "00000", "1", "",
		"0", "1", "Y", "", "10", "", "EXAMPLE", row.price,
	}
}

func TestA4DeskRowsClassifyAsNonFill(t *testing.T) {
	counters := decode.NewCounters()
	queue, client := newQueue(t, counters)
	decoder := decode.New(decode.Config{AccountMode: "live", Venue: "krx", DupTrackMax: 10, Counters: counters})
	for _, row := range deskRows {
		fields := deskFrame(row)
		if got := decode.ClassifyNotice(ws.TRExecutionLive, fields); got == decode.NoticeFill {
			t.Fatalf("desk row %d (%s 1@%s) classified as fill", row.id, row.symbol, row.price)
		}
		if records := decodeAll(t, decoder, queue, eventFromFrame(ws.TRExecutionLive, fields)); len(records) != 0 {
			t.Fatalf("desk row %d produced %d fill records", row.id, len(records))
		}
	}
	if got := streamLen(t, client); got != 0 {
		t.Fatalf("stream length = %d, want 0", got)
	}
	if got := counters.Snapshot().NonFillOrder; got != uint64(len(deskRows)) {
		t.Fatalf("NonFillOrder = %d, want %d", got, len(deskRows))
	}
}

func TestParsedExecutionMustMatchFrame(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*ws.Execution)
	}{
		{"qty from order column", func(execution *ws.Execution) { execution.Qty = "5" }},
		{"price from order column", func(execution *ws.Execution) { execution.Price = "71500" }},
		{"time", func(execution *ws.Execution) { execution.FilledAt = "091256" }},
		{"order number", func(execution *ws.Execution) { execution.OrderNo = "0000000000" }},
		{"symbol", func(execution *ws.Execution) { execution.Symbol = "000660" }},
		{"side", func(execution *ws.Execution) { execution.Side = ws.SideSell }},
		{"filled flag", func(execution *ws.Execution) { execution.Filled = "1" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			counters := decode.NewCounters()
			decoder := decode.New(decode.Config{AccountMode: "live", Venue: "krx", DupTrackMax: 10, Counters: counters})
			event := eventFromFrame(ws.TRExecutionLive, fillFrame())
			test.mutate(event.Execution)
			if record, ok := decoder.Decode(event); ok {
				t.Fatalf("disagreeing execution produced record %+v", record)
			}
			if got := counters.Snapshot().Dropped; got != 1 {
				t.Fatalf("Dropped = %d, want 1", got)
			}
		})
	}
}

func set(index int, value string) func([]string) []string {
	return func(fields []string) []string {
		fields[index] = value
		return fields
	}
}

func chain(steps ...func([]string) []string) func([]string) []string {
	return func(fields []string) []string {
		for _, step := range steps {
			fields = step(fields)
		}
		return fields
	}
}
