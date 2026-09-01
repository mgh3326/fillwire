package kismockread

import (
	"encoding/json"
	"testing"
	"time"
)

func TestOverseasOrderHistoryEvidenceParsesReferenceFTFields(t *testing.T) {
	var body map[string]json.RawMessage
	if err := json.Unmarshal([]byte(`{
		"rt_cd":"0",
		"output1":[{
			"odno":"US-42",
			"sll_buy_dvsn_cd":"02",
			"pdno":"BRK/B",
			"ft_ord_qty":"001",
			"ft_ord_unpr3":"1.00",
			"ord_dt":"20260901",
			"ord_tmd":"210100"
		}]
	}`), &body); err != nil {
		t.Fatal(err)
	}
	orders, err := overseasOrders(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 1 {
		t.Fatalf("orders=%#v", orders)
	}
	got := orders[0]
	if got.BrokerOrderID != "US-42" || got.Side != "buy" || got.StockCode != "BRK/B" || got.Quantity != "001" || got.Price != "1.00" {
		t.Fatalf("order=%+v", got)
	}
	wantTime := time.Date(2026, 9, 1, 12, 1, 0, 0, time.UTC)
	if !got.OrderedAt.Equal(wantTime) {
		t.Fatalf("ordered at=%s, want=%s", got.OrderedAt, wantTime)
	}
}

func TestOverseasOrderHistoryEvidenceRejectsMissingFTTerms(t *testing.T) {
	var body map[string]json.RawMessage
	if err := json.Unmarshal([]byte(`{
		"rt_cd":"0",
		"output1":[{
			"odno":"US-42",
			"sll_buy_dvsn_cd":"02",
			"pdno":"AAPL",
			"ord_qty":"1",
			"ord_unpr":"1",
			"ord_dt":"20260901",
			"ord_tmd":"210100"
		}]
	}`), &body); err != nil {
		t.Fatal(err)
	}
	if _, err := overseasOrders(body); err == nil || err.Code != CodeResponseInvalid {
		t.Fatalf("missing overseas ft fields error=%v", err)
	}
}
