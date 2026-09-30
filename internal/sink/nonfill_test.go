package sink_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/mgh3326/fillwire/internal/decode"
)

// legacyAcceptRecord is what a pre-#1172 binary enqueued for an order accept
// notice: a well-formed record whose stored frame has CNTG_YN=1 and whose
// qty/price are the order's.
func legacyAcceptRecord(index int) decode.Record {
	record := fixtureRecord(index)
	fields := append([]string(nil), record.RawPayloadJSON.Fields...)
	fields[9], fields[10], fields[11], fields[13], fields[14] = "5", "71500", "091256", "1", "1"
	record.RawPayloadJSON.Fields = fields
	record.FilledQty, record.FilledPrice = "5", "71500"
	return record
}

type recordingIngest struct {
	mu     sync.Mutex
	posted [][]string // broker_order_id per request
	fail   handlerFailure
}

func (r *recordingIngest) handler(w http.ResponseWriter, request *http.Request) {
	var envelope struct {
		Fills []struct {
			BrokerOrderID string `json:"broker_order_id"`
		} `json:"fills"`
	}
	if err := json.NewDecoder(request.Body).Decode(&envelope); err != nil {
		r.fail.set(err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	ids := make([]string, 0, len(envelope.Fills))
	results := make([]string, 0, len(envelope.Fills))
	for index, fill := range envelope.Fills {
		ids = append(ids, fill.BrokerOrderID)
		results = append(results, fmt.Sprintf(`{"status":"inserted","row_id":%d,"reason":null}`, index+1))
	}
	r.mu.Lock()
	r.posted = append(r.posted, ids)
	r.mu.Unlock()
	_, _ = fmt.Fprintf(w, `{"source":"fillwire","source_run_id":null,"received":%d,"accepted":%d,"rejected":0,"results":[%s]}`, len(ids), len(ids), join(results))
}

func (r *recordingIngest) requests() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]string(nil), r.posted...)
}

func TestStoredNonFillEntriesAreAckedNeverPosted(t *testing.T) {
	client, queue, counters := testQueue(t, "consumer", 10, 0)
	ctx := context.Background()
	if err := queue.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	short := fixtureRecord(3)
	short.RawPayloadJSON.Fields = short.RawPayloadJSON.Fields[:14] // no ACPT_YN: unprovable
	for _, record := range []decode.Record{legacyAcceptRecord(1), fixtureRecord(2), short} {
		if _, err := queue.Enqueue(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	ingest := &recordingIngest{}
	server := httptest.NewServer(http.HandlerFunc(ingest.handler))
	defer server.Close()
	runner := testRunner(t, queue, server.URL, counters)

	messages, err := queue.ReadNew(ctx)
	if err != nil || len(messages) != 3 {
		t.Fatalf("read = %d messages, %v; want 3", len(messages), err)
	}
	if err := runner.DeliverOnce(ctx, messages); err != nil {
		t.Fatal(err)
	}
	ingest.fail.assert(t)
	requests := ingest.requests()
	if len(requests) != 1 || len(requests[0]) != 1 || requests[0][0] != "ORDER-002" {
		t.Fatalf("posted = %v, want exactly [[ORDER-002]]", requests)
	}
	waitPending(t, client, 0)
	if snapshot := counters.Snapshot(); snapshot.SinkNonFill != 2 || snapshot.XAcked != 3 {
		t.Fatalf("counters = %+v, want SinkNonFill 2 and XAcked 3", snapshot)
	}
}

func TestAllNonFillBatchPostsNothing(t *testing.T) {
	client, queue, counters := testQueue(t, "consumer", 10, 0)
	ctx := context.Background()
	if err := queue.EnsureGroup(ctx); err != nil {
		t.Fatal(err)
	}
	for index := 1; index <= 2; index++ {
		if _, err := queue.Enqueue(ctx, legacyAcceptRecord(index)); err != nil {
			t.Fatal(err)
		}
	}
	ingest := &recordingIngest{}
	server := httptest.NewServer(http.HandlerFunc(ingest.handler))
	defer server.Close()
	runner := testRunner(t, queue, server.URL, counters)

	messages, err := queue.ReadNew(ctx)
	if err != nil || len(messages) != 2 {
		t.Fatalf("read = %d messages, %v; want 2", len(messages), err)
	}
	if err := runner.DeliverOnce(ctx, messages); err != nil {
		t.Fatal(err)
	}
	if requests := ingest.requests(); len(requests) != 0 {
		t.Fatalf("posted = %v, want no ingest request", requests)
	}
	waitPending(t, client, 0)
	if got := counters.Snapshot().SinkNonFill; got != 2 {
		t.Fatalf("SinkNonFill = %d, want 2", got)
	}
}
