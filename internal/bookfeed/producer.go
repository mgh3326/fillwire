package bookfeed

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/mgh3326/fillwire/internal/book"
	"github.com/mgh3326/go-kis/kis"
	"github.com/mgh3326/go-kis/kis/ws"
	"github.com/redis/go-redis/v9"
)

type RESTReader interface {
	Read(context.Context, string, string, map[string]string, any) error
}

func Poll(ctx context.Context, r RESTReader, symbol string, now func() time.Time) (Snapshot, error) {
	var response struct {
		kis.Envelope
		Output map[string]string `json:"output1"`
	}
	err := r.Read(ctx, "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn", "FHKST01010200", map[string]string{"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}, &response)
	if err != nil {
		return Snapshot{}, errors.New("bookfeed: asking-price read failed")
	}
	s := Snapshot{Symbol: symbol, Received: now(), Source: "rest"}
	s.Exchange, err = ExchangeTime(response.Output["aspr_acpt_hour"], s.Received)
	if err != nil {
		return s, err
	}
	for i := range 5 {
		n := strconv.Itoa(i + 1)
		for _, f := range levelFields(&s, i, "bidp"+n, "bidp_rsqn"+n, "askp"+n, "askp_rsqn"+n) {
			*f.dst, err = strconv.ParseInt(response.Output[f.key], 10, 64)
			if err != nil {
				return s, errors.New("bookfeed: invalid asking-price level")
			}
		}
	}
	return s, s.Validate()
}

func FromEvent(ev ws.Event) ([]Snapshot, error) {
	frame, err := book.ParseFrame(ev.Raw)
	if err != nil {
		return nil, err
	}
	out := make([]Snapshot, 0, len(frame.Books))
	for _, b := range frame.Books {
		exchange, err := ExchangeTime(b.Hour, ev.ReceivedAt)
		if err != nil {
			return nil, err
		}
		s := Snapshot{Symbol: b.Symbol, Exchange: exchange, Received: ev.ReceivedAt, Source: "ws"}
		copy(s.Bids[:], b.Bids[:5])
		copy(s.Asks[:], b.Asks[:5])
		if err := s.Validate(); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// Run consumes book events from the existing fills websocket. Only mock uses
// REST fallback, checked every 2s when no valid websocket snapshot arrived in
// the preceding 4s. A bad read publishes nothing, never a restamped old book.
func Run(ctx context.Context, r redis.Cmdable, rest RESTReader, symbol string, mock bool, events <-chan ws.Event, warn func(error)) error {
	if !SymbolOK(symbol) {
		return errors.New("bookfeed: invalid symbol")
	}
	timer := time.NewTicker(2 * time.Second)
	defer timer.Stop()
	var lastWS time.Time
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, open := <-events:
			if !open {
				return nil
			}
			snapshots, err := FromEvent(ev)
			if err != nil {
				warn(err)
				continue
			}
			for _, s := range snapshots {
				if s.Symbol != symbol {
					continue
				}
				if err := Publish(ctx, r, s); err != nil {
					warn(fmt.Errorf("bookfeed: publish failed"))
					continue
				}
				lastWS = s.Received
			}
		case now := <-timer.C:
			if !mock || rest == nil || now.Sub(lastWS) <= 4*time.Second {
				continue
			}
			s, err := Poll(ctx, rest, symbol, time.Now)
			if err == nil {
				err = Publish(ctx, r, s)
			}
			if err != nil {
				warn(err)
			}
		}
	}
}
