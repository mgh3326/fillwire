package e1_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/mgh3326/fillwire/internal/exec/e1"
)

func TestRecordedCommittedFillsContract(t *testing.T) {
	data, err := os.ReadFile("testdata/fills_recorded.json")
	if err != nil {
		t.Fatal(err)
	}
	in := fixtureIntent(t)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/execution-intents/intents/"+in.IntentID+"/fills" || r.Header.Get(e1.DefaultTokenHeader) != "test-token" || r.Header.Get(e1.ConfirmHeader) != "" {
			t.Error("wrong ledger read binding")
		}
		w.Write(data)
	}))
	defer s.Close()
	c, err := e1.New(sendConfig(s.URL+"/execution-intents/children"), in, e1.Options{Token: tokenSource(t, true)})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := c.Fills(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f, err := snapshot.Orders[0].Fills[0].Execution()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Orders[0].BrokerOrderID != "000123" || snapshot.Orders[0].CumulativeQty != 2 || f.Qty != 2 || f.Price != 70000 || f.FillID != "42" {
		t.Fatalf("snapshot %+v", snapshot)
	}
}

func TestFillsRejectsChangedOrInvalidContract(t *testing.T) {
	data, err := os.ReadFile("testdata/fills_recorded.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"http_failure", "wrong_intent", "missing_orders", "fractional_price", "wrong_side", "cumulative_mismatch", "duplicate_fill", "unknown_field"} {
		t.Run(kind, func(t *testing.T) {
			body := string(data)
			status := 200
			switch kind {
			case "http_failure":
				status = 503
			case "wrong_intent":
				body = strings.ReplaceAll(body, fixtureIntent(t).IntentID, "other")
			case "missing_orders":
				body = `{"intent_id":"` + fixtureIntent(t).IntentID + `"}`
			case "fractional_price":
				body = strings.ReplaceAll(body, "70000.00000000", "70000.12500000")
			case "wrong_side":
				body = strings.ReplaceAll(body, `"buy"`, `"sell"`)
			case "cumulative_mismatch":
				body = strings.ReplaceAll(body, `"cumulative_qty": 2`, `"cumulative_qty": 3`)
			case "duplicate_fill":
				var snapshot e1.FillsSnapshot
				json.Unmarshal(data, &snapshot)
				snapshot.Orders[0].Fills = append(snapshot.Orders[0].Fills, snapshot.Orders[0].Fills[0])
				snapshot.Orders[0].CumulativeQty = 4
				b, _ := json.Marshal(snapshot)
				body = string(b)
			case "unknown_field":
				body = strings.Replace(body, `"orders":`, `"extra":true,"orders":`, 1)
			}
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status); io.WriteString(w, body) }))
			defer s.Close()
			c, err := e1.New(sendConfig(s.URL+"/execution-intents/children"), fixtureIntent(t), e1.Options{Token: tokenSource(t, true)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Fills(context.Background()); err == nil {
				t.Fatal("accepted invalid ledger response")
			}
		})
	}
}

func TestQueryUsesReconciledViewOnly(t *testing.T) {
	in := fixtureIntent(t)
	for _, reconciled := range []bool{true, false} {
		body := `{"intent_id":"` + in.IntentID + `","reconciled":true,"view":{"filled_qty":2},"children":[],"unresolved":[]}`
		if !reconciled {
			body = strings.Replace(body, `"reconciled":true`, `"reconciled":false`, 1)
		}
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" || r.URL.Path != "/execution-intents/intents/"+in.IntentID {
				t.Error("wrong query path")
			}
			io.WriteString(w, body)
		}))
		c, err := e1.New(sendConfig(s.URL+"/execution-intents/children"), in, e1.Options{Token: tokenSource(t, true)})
		if err != nil {
			t.Fatal(err)
		}
		v, err := c.Query(in.IntentID)
		if reconciled && (err != nil || v.FilledQty != 2) {
			t.Fatalf("view %+v %v", v, err)
		}
		if !reconciled && err == nil {
			t.Fatal("used unresolved view")
		}
		s.Close()
	}
}
