package exec_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/fillwire/internal/exec"
)

// e1Fields is every field of ExecutionIntentV1 in auto_trader
// app/schemas/execution_intent.py (ROB-1440, merge dba07a882), and of its
// nested InventoryConstraint.
var (
	e1Fields = []string{
		"schema_version", "intent_id", "strategy", "account", "venue", "symbol", "side",
		"max_quantity", "max_notional_krw", "limit_price_krw", "max_slippage_bps",
		"expires_at", "inventory", "policy_version", "model_version", "authorization_ref",
	}
	e1InventoryFields = []string{"position_at_authorization", "min_position_after", "max_position_after"}
)

func keys(m map[string]any) []string { return slices.Sorted(maps.Keys(m)) }

func TestIntentRoundTripsE1FieldNames(t *testing.T) {
	data, err := os.ReadFile("testdata/e1_intent.json")
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]any
	if err := json.Unmarshal(data, &want); err != nil {
		t.Fatal(err)
	}
	equal(t, keys(want), slices.Sorted(slices.Values(e1Fields)))
	equal(t, keys(want["inventory"].(map[string]any)), slices.Sorted(slices.Values(e1InventoryFields)))

	in, err := exec.ParseIntent(data)
	if err != nil {
		t.Fatal(err)
	}
	out, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(want)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("round trip differs:\n got %s\nwant %s", gotJSON, wantJSON)
	}
	if in.Cap() != 70_300 || in.TargetQty() != 10 {
		t.Fatalf("cap %d target %d", in.Cap(), in.TargetQty())
	}
}

func TestParseIntentRefuses(t *testing.T) {
	base, _ := os.ReadFile("testdata/e1_intent.json")
	cases := map[string]string{
		"unknown field":    `"venue":"krx","extra":1`,
		"schema version":   `"schema_version":2`,
		"account":          `"account":"kis_live"`,
		"side":             `"side":"short"`,
		"symbol":           `"symbol":"5930"`,
		"notional cap":     `"max_notional_krw":5000001`,
		"instrument type":  `"venue":"krx","instrument_type":"krx_option"`,
		"no quantity fits": `"max_notional_krw":70000`,
		"inventory range":  `"inventory":{"position_at_authorization":0,"min_position_after":5,"max_position_after":1}`,
	}
	repl := map[string]string{
		"unknown field": `"venue":"krx"`, "schema version": `"schema_version":1`, "account": `"account":"kis_mock"`,
		"side": `"side":"buy"`, "symbol": `"symbol":"005930"`, "notional cap": `"max_notional_krw":1000000`,
		"instrument type": `"venue":"krx"`, "no quantity fits": `"max_notional_krw":1000000`,
		"inventory range": `"inventory":{"position_at_authorization":0,"min_position_after":0,"max_position_after":10}`,
	}
	for name, field := range cases {
		data := strings.Replace(string(base), repl[name], field, 1)
		if data == string(base) {
			t.Fatalf("%s: replacement did not apply", name)
		}
		if _, err := exec.ParseIntent([]byte(data)); !errors.Is(err, exec.ErrIntent) {
			t.Errorf("%s: err %v, want ErrIntent", name, err)
		}
	}
}

func TestTickGrid(t *testing.T) {
	for _, c := range []struct{ p, floor, ceil int64 }{
		{1_999, 1_999, 1_999}, {4_997, 4_995, 5_000}, {70_150, 70_100, 70_200}, {499_700, 499_500, 500_000},
	} {
		if f, cl := exec.FloorTick(c.p), exec.CeilTick(c.p); f != c.floor || cl != c.ceil {
			t.Errorf("%d: floor %d ceil %d, want %d %d", c.p, f, cl, c.floor, c.ceil)
		}
	}
}

func TestParseIntentRefusesTrailingData(t *testing.T) {
	base, _ := os.ReadFile("testdata/e1_intent.json")
	base = bytes.TrimRight(base, " \t\r\n")
	for _, tail := range []string{"}", "]", "{}", "x", " {}"} {
		if _, err := exec.ParseIntent(append(slices.Clone(base), tail...)); !errors.Is(err, exec.ErrIntent) {
			t.Errorf("tail %q: err %v, want ErrIntent", tail, err)
		}
	}
	if _, err := exec.ParseIntent(append(slices.Clone(base), "\n  \n"...)); err != nil {
		t.Errorf("trailing whitespace: %v", err)
	}
}
