package executioncontracts

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
)

func TestExecutionCommandV1HasOnlyApprovedWireFields(t *testing.T) {
	command := ExecutionCommandV1{
		SchemaVersion: ExecutionCommandV1SchemaVersion,
		CommandID:     "command-1",
		AccountScope:  AccountScopeKISMock,
		Side:          "buy",
		StockCode:     "005930",
		Quantity:      "001",
		Price:         "070000",
		OrderType:     "limit",
		IssuedAt:      "2026-09-01T12:00:00Z",
	}
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"account_scope", "command_id", "issued_at", "order_type", "price",
		"quantity", "schema_version", "side", "stock_code",
	}
	got := make([]string, 0, len(fields))
	for key := range fields {
		got = append(got, key)
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("command fields = %v, want %v", got, want)
	}
	if _, isString := fields["quantity"].(string); !isString {
		t.Fatal("quantity must stay a JSON string")
	}
	if _, isString := fields["price"].(string); !isString {
		t.Fatal("price must stay a JSON string")
	}
}

func TestExecutionReceiptDispositionIsClosed(t *testing.T) {
	for _, disposition := range []ExecutionDisposition{
		DispositionNotCreated,
		DispositionAccepted,
		DispositionUnknown,
	} {
		if !disposition.Valid() {
			t.Fatalf("valid disposition rejected: %q", disposition)
		}
	}
	if ExecutionDisposition("RETRY").Valid() {
		t.Fatal("unknown disposition was accepted")
	}
	if _, err := json.Marshal(ExecutionDisposition("RETRY")); err == nil {
		t.Fatal("unknown disposition marshaled")
	}
	var parsed ExecutionDisposition
	if err := json.Unmarshal([]byte(`"RETRY"`), &parsed); err == nil {
		t.Fatal("unknown disposition unmarshaled")
	}

	receipt := ExecutionReceiptV1{
		SchemaVersion: ExecutionReceiptV1SchemaVersion,
		CommandID:     "command-1",
		Disposition:   DispositionAccepted,
		RecordedAt:    "2026-09-01T12:00:00Z",
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, present := fields["broker_order_id"]; present {
		t.Fatal("empty optional broker_order_id was encoded")
	}
	if _, present := fields["error_code"]; present {
		t.Fatal("empty optional error_code was encoded")
	}
}

func TestAccountScopesAreNamedPaperBackends(t *testing.T) {
	if AccountScopeKISMock != "kis_mock" {
		t.Fatalf("KIS account scope = %q", AccountScopeKISMock)
	}
	if AccountScopeKISMockUS != "kis_mock_us" {
		t.Fatalf("KIS US account scope = %q", AccountScopeKISMockUS)
	}
	if AccountScopeAlpacaPaperCrypto != "alpaca_paper_crypto" {
		t.Fatalf("Alpaca account scope = %q", AccountScopeAlpacaPaperCrypto)
	}
	if AccountScopeKISMock == AccountScopeKISMockUS ||
		AccountScopeKISMock == AccountScopeAlpacaPaperCrypto ||
		AccountScopeKISMockUS == AccountScopeAlpacaPaperCrypto {
		t.Fatal("account scopes must be distinct")
	}
}
