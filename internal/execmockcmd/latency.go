package execmockcmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/fillwire/internal/exec"
	"github.com/mgh3326/fillwire/internal/exec/e1"
	"github.com/redis/go-redis/v9"
)

// The latency report (hk 6068, design 6033 K1) measures how long each child's
// fill evidence takes to appear on each path. It only observes: the wrappers
// below call the wrapped function exactly as before, in the same order, and
// add no request. The one added read is a Redis XRANGE on the existing fills
// stream, made after the run ends.

const (
	// noticeLookback widens the stream scan below the POST time, to absorb
	// clock skew between this host and Redis. A fill notice cannot precede its
	// order, so the margin only costs a few extra entries.
	noticeLookback = time.Minute
	noticePage     = 500
	noticeMaxPages = 20
	flushTimeout   = 15 * time.Second
)

// noticeRead returns the earliest time fillwire received a KIS fill notice for
// one broker order, or false when it holds none.
type noticeRead func(ctx context.Context, brokerOrderID string, since time.Time) (time.Time, bool, error)

// stamp is one observation. Local observations carry the monotonic offset from
// the run start; a KIS notice time was taken by another process, so only its
// wall time exists.
type stamp struct {
	Wall   time.Time `json:"wall"`
	MonoNS *int64    `json:"mono_ns,omitempty"`
}

type deltasNS struct {
	PostToKISNotice        *int64 `json:"post_to_kis_notice,omitempty"`
	PostToE1Fills          *int64 `json:"post_to_e1_fills,omitempty"`
	PostToVTSReadback      *int64 `json:"post_to_vts_readback,omitempty"`
	KISNoticeToE1Fills     *int64 `json:"kis_notice_to_e1_fills,omitempty"`
	KISNoticeToVTSReadback *int64 `json:"kis_notice_to_vts_readback,omitempty"`
}

type latencyBody struct {
	IntentID       string   `json:"intent_id"`
	ChildKey       string   `json:"child_key"`
	Action         string   `json:"action"`
	Status         string   `json:"status"`
	BrokerOrderID  string   `json:"broker_order_id,omitempty"`
	Post           stamp    `json:"post"`
	KISNotice      *stamp   `json:"kis_notice"`
	E1Fills        *stamp   `json:"e1_fills"`
	VTSReadback    *stamp   `json:"vts_readback"`
	KISNoticeError string   `json:"kis_notice_error,omitempty"`
	DeltasNS       deltasNS `json:"deltas_ns"`
}

type childLatency struct {
	intentID, key, action, status, brokerOrderID string
	post                                         time.Time
	e1, vts                                      *time.Time
}

// latencyTracker records, per child sent in this run, the POST time and the
// first time each evidence path showed a fill. Evidence for orders from an
// earlier run of the same journal is not tracked.
type latencyTracker struct {
	mu       sync.Mutex
	now      func() time.Time
	base     time.Time
	children []*childLatency
	byKey    map[string]*childLatency
	byOrder  map[string]*childLatency
	notice   noticeRead
}

func newLatencyTracker(now func() time.Time, notice noticeRead) *latencyTracker {
	return &latencyTracker{now: now, base: now(), byKey: map[string]*childLatency{}, byOrder: map[string]*childLatency{}, notice: notice}
}

type latencyGateway struct {
	exec.Gateway
	t *latencyTracker
}

// Gateway times each Send. The POST time is taken just before the request.
func (t *latencyTracker) Gateway(g exec.Gateway) exec.Gateway { return latencyGateway{g, t} }

func (g latencyGateway) Send(c exec.Child) (exec.Outcome, error) {
	at := g.t.now()
	out, err := g.Gateway.Send(c)
	g.t.recordSend(c, out, err, at)
	return out, err
}

func (t *latencyTracker) recordSend(c exec.Child, out exec.Outcome, err error, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, dup := t.byKey[c.Key]; dup {
		return
	}
	status := out.Status
	if err != nil {
		status = exec.StatusUncertain
	}
	child := &childLatency{intentID: c.IntentID, key: c.Key, action: c.Action, status: status, post: at, brokerOrderID: exec.NormalizeOrderNumber(out.BrokerOrderID)}
	t.children = append(t.children, child)
	t.byKey[c.Key] = child
	if child.brokerOrderID != "" {
		t.byOrder[child.brokerOrderID] = child
	}
}

// Ledger stamps the first E1 /fills snapshot that lists a fill for a child's
// order. The stamp is the time the response arrived.
func (t *latencyTracker) Ledger(read ledgerRead) ledgerRead {
	return func(ctx context.Context) (e1.FillsSnapshot, error) {
		s, err := read(ctx)
		at := t.now()
		if err != nil {
			return s, err
		}
		t.mu.Lock()
		defer t.mu.Unlock()
		for _, o := range s.Orders {
			if len(o.Fills) == 0 {
				continue
			}
			if child := t.byOrder[exec.NormalizeOrderNumber(o.BrokerOrderID)]; child != nil && child.e1 == nil {
				child.e1 = &at
			}
		}
		return s, nil
	}
}

// Broker stamps the first VTS readback that reports a filled quantity for a
// child's order, whether or not the ledger agrees yet.
func (t *latencyTracker) Broker(read brokerRead) brokerRead {
	return func(ctx context.Context, o exec.BrokerOrder) (brokerView, error) {
		v, err := read(ctx, o)
		at := t.now()
		if err != nil || v.Filled <= 0 {
			return v, err
		}
		t.mu.Lock()
		defer t.mu.Unlock()
		if child := t.byKey[o.Key]; child != nil && child.vts == nil {
			child.vts = &at
		}
		return v, err
	}
}

// Flush writes one JSON line per child, in POST order. It looks the KIS notice
// time up once per accepted order, here and not during the run, so a slow
// Redis cannot delay a poll.
func (t *latencyTracker) Flush(w io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()
	t.mu.Lock()
	defer t.mu.Unlock()
	enc := json.NewEncoder(w)
	var errs []error
	for _, c := range t.children {
		line := t.line(c)
		if t.notice != nil && c.brokerOrderID != "" {
			lookup, cancelLookup := context.WithTimeout(ctx, liveReadTimeout)
			at, ok, err := t.notice(lookup, c.brokerOrderID, c.post)
			cancelLookup()
			switch {
			case err != nil:
				line.KISNoticeError = "fill stream unavailable"
			case ok:
				line.KISNotice = &stamp{Wall: at.UTC()}
				t.deltas(&line, c, &at)
			}
		}
		if err := enc.Encode(struct {
			Latency latencyBody `json:"latency"`
		}{line}); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (t *latencyTracker) local(at *time.Time) *stamp {
	if at == nil {
		return nil
	}
	m := at.Sub(t.base).Nanoseconds()
	return &stamp{Wall: at.UTC(), MonoNS: &m}
}

func (t *latencyTracker) line(c *childLatency) latencyBody {
	line := latencyBody{
		IntentID: c.intentID, ChildKey: c.key, Action: c.action, Status: c.status, BrokerOrderID: c.brokerOrderID,
		Post: *t.local(&c.post), E1Fills: t.local(c.e1), VTSReadback: t.local(c.vts),
	}
	t.deltas(&line, c, nil)
	return line
}

func since(from, to *time.Time) *int64 {
	if from == nil || to == nil {
		return nil
	}
	d := to.Sub(*from).Nanoseconds()
	return &d
}

func (t *latencyTracker) deltas(line *latencyBody, c *childLatency, kis *time.Time) {
	line.DeltasNS.PostToE1Fills = since(&c.post, c.e1)
	line.DeltasNS.PostToVTSReadback = since(&c.post, c.vts)
	if kis != nil {
		line.DeltasNS.PostToKISNotice = since(&c.post, kis)
		line.DeltasNS.KISNoticeToE1Fills = since(kis, c.e1)
		line.DeltasNS.KISNoticeToVTSReadback = since(kis, c.vts)
	}
}

// streamNoticeRead finds the earliest fill-notice receive time for one broker
// order in fillwire's Redis fills stream, a read-only XRANGE on the Redis
// execmock already opens. Stream ids start with the XADD millisecond time, so
// the scan starts at the POST time. Only the order id and the receive time are
// read from each entry.
func streamNoticeRead(r redis.Cmdable, key string) noticeRead {
	return func(ctx context.Context, brokerOrderID string, since time.Time) (time.Time, bool, error) {
		want := exec.NormalizeOrderNumber(brokerOrderID)
		start := strconv.FormatInt(since.Add(-noticeLookback).UnixMilli(), 10)
		var first time.Time
		found := false
		for range noticeMaxPages {
			entries, err := r.XRangeN(ctx, key, start, "+", noticePage).Result()
			if err != nil {
				return time.Time{}, false, err
			}
			for _, m := range entries {
				payload, _ := m.Values["payload"].(string)
				var rec struct {
					BrokerOrderID string `json:"broker_order_id"`
					Raw           struct {
						ReceivedAt time.Time `json:"received_at"`
					} `json:"raw_payload_json"`
				}
				if json.Unmarshal([]byte(payload), &rec) != nil || exec.NormalizeOrderNumber(rec.BrokerOrderID) != want || rec.Raw.ReceivedAt.IsZero() {
					continue
				}
				if !found || rec.Raw.ReceivedAt.Before(first) {
					first, found = rec.Raw.ReceivedAt, true
				}
			}
			if len(entries) < noticePage {
				return first, found, nil
			}
			if start, err = nextStreamID(entries[len(entries)-1].ID); err != nil {
				return time.Time{}, false, err
			}
		}
		return time.Time{}, false, errors.New("fill stream scan bound reached")
	}
}

// nextStreamID returns the id right after id, an inclusive XRANGE start that
// works on every Redis version.
func nextStreamID(id string) (string, error) {
	ms, seq, ok := strings.Cut(id, "-")
	n, err := strconv.ParseUint(seq, 10, 64)
	if !ok || err != nil || ms == "" {
		return "", fmt.Errorf("unexpected stream id %q", id)
	}
	return ms + "-" + strconv.FormatUint(n+1, 10), nil
}
