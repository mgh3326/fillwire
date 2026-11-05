package e1

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/exec"
	"github.com/mgh3326/fillwire/internal/jsonx"
)

// FillsSnapshot matches auto_trader PR #2273 ac5e8676f04f4f5bcdb3f6d2c978a55bc60afbbd.
// It is a full effective snapshot, not an incremental fill stream.
type FillsSnapshot struct {
	IntentID string        `json:"intent_id"`
	Orders   []LedgerOrder `json:"orders"`
}
type LedgerOrder struct {
	BrokerOrderID string       `json:"broker_order_id"`
	CumulativeQty int64        `json:"cumulative_qty"`
	Fills         []LedgerFill `json:"fills"`
}
type LedgerFill struct {
	FillID      string    `json:"fill_id"`
	Side        exec.Side `json:"side"`
	Qty         int64     `json:"qty"`
	Price       string    `json:"price"`
	FilledAt    time.Time `json:"filled_at"`
	CommittedAt time.Time `json:"committed_at"`
}

func (f LedgerFill) Execution() (exec.Fill, error) {
	p, ok := new(big.Rat).SetString(f.Price)
	if !ok || !p.IsInt() || !p.Num().IsInt64() || p.Sign() <= 0 || f.FillID == "" || f.Qty <= 0 || f.Qty > 1_000_000 || f.FilledAt.IsZero() || f.CommittedAt.IsZero() {
		return exec.Fill{}, errors.New("e1: invalid or fractional-KRW ledger fill")
	}
	price := p.Num().Int64()
	if price > 10_000_000 {
		return exec.Fill{}, errors.New("e1: ledger price outside KR equity range")
	}
	return exec.Fill{FillID: f.FillID, Qty: f.Qty, Price: price, AtNS: f.FilledAt.UnixNano()}, nil
}

func strict(data []byte, out any) error {
	switch err := jsonx.DecodeStrict(data, out); {
	case errors.Is(err, jsonx.ErrTrailingData):
		return errors.New("e1: trailing read response")
	case err != nil:
		return errors.New("e1: invalid read response")
	}
	return nil
}

func (c *Client) read(ctx context.Context, suffix string, out any) error {
	if c.mode != Send || c.http == nil || !strings.HasSuffix(c.url, "/children") {
		return ErrNoReconcile
	}
	endpoint := strings.TrimSuffix(c.url, "/children") + "/intents/" + c.intent.IntentID + suffix
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return errors.New("e1: invalid read endpoint")
	}
	req.Header.Set(c.header, c.token)
	response, err := c.http.Do(req)
	if err != nil {
		return errors.New("e1: read transport failed")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return errors.New("e1: read HTTP failure")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 4<<20+1))
	if err != nil || len(data) > 4<<20 {
		return errors.New("e1: read response exceeds limit")
	}
	return strict(data, out)
}

func (c *Client) Fills(ctx context.Context) (FillsSnapshot, error) {
	var s FillsSnapshot
	if err := c.read(ctx, "/fills", &s); err != nil {
		return s, err
	}
	if s.IntentID != c.intent.IntentID || s.Orders == nil {
		return s, errors.New("e1: missing or mismatched fill snapshot identity")
	}
	ids := map[string]bool{}
	orders := map[string]bool{}
	for _, o := range s.Orders {
		number := strings.TrimLeft(o.BrokerOrderID, "0")
		if number == "" || orders[number] || o.CumulativeQty < 0 || o.Fills == nil {
			return s, errors.New("e1: invalid ledger order")
		}
		orders[number] = true
		var total int64
		for _, f := range o.Fills {
			if ids[f.FillID] || f.Side != c.intent.Side {
				return s, errors.New("e1: conflicting fill identity")
			}
			ids[f.FillID] = true
			if _, err := f.Execution(); err != nil {
				return s, err
			}
			total += f.Qty
		}
		if total != o.CumulativeQty {
			return s, errors.New("e1: ledger cumulative quantity mismatch")
		}
	}
	return s, nil
}

func (c *Client) query(intentID string) (exec.View, error) {
	if intentID != c.intent.IntentID {
		return exec.View{}, ErrNoReconcile
	}
	var s struct {
		IntentID   string            `json:"intent_id"`
		Reconciled bool              `json:"reconciled"`
		View       *exec.View        `json:"view"`
		Unresolved []json.RawMessage `json:"unresolved"`
		Children   []json.RawMessage `json:"children"`
	}
	if err := c.read(context.Background(), "", &s); err != nil {
		return exec.View{}, err
	}
	if s.IntentID != intentID || !s.Reconciled || s.View == nil || len(s.Unresolved) > 0 || s.View.FilledQty < 0 {
		return exec.View{}, ErrNoReconcile
	}
	return *s.View, nil
}
