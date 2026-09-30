package kismockedge

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
)

func TestMaskedRejectionRemovesEverySecretShape(t *testing.T) {
	config := testBrokerConfig()
	token := "cached-token-for-test"
	masker := newSecretMasker(config, token)
	body := map[string]json.RawMessage{
		"rt_cd":  json.RawMessage(`"1"`),
		"msg_cd": json.RawMessage(`"APBK0957"`),
		"msg1":   json.RawMessage(`"order refused for 12345678-01 (12345678) token cached-token-for-test key app-key-for-test secret app-secret-for-test"`),
	}
	rejection := maskedRejection(http.StatusOK, body, masker)
	if rejection == nil {
		t.Fatal("rejection missing")
	}
	if rejection.RtCd != "1" || rejection.MsgCd != "APBK0957" {
		t.Fatalf("fields = %#v", rejection)
	}
	for _, secret := range []string{"12345678", "12345678-01", token, config.AppKey, config.AppSecret} {
		if strings.Contains(rejection.Msg1, secret) {
			t.Fatalf("secret %q survived in msg1 %q", secret, rejection.Msg1)
		}
	}
	if !strings.Contains(rejection.Msg1, redactedValue) {
		t.Fatalf("no redaction marker in %q", rejection.Msg1)
	}
}

func TestMaskedRejectionStatusOnlyWhenBodyUnparseable(t *testing.T) {
	rejection := maskedRejection(http.StatusBadGateway, nil, newSecretMasker(testBrokerConfig(), "tok"))
	if rejection == nil || rejection.HTTPStatus != http.StatusBadGateway {
		t.Fatalf("rejection = %#v", rejection)
	}
	if rejection.RtCd != "" || rejection.MsgCd != "" || rejection.Msg1 != "" {
		t.Fatalf("fields present on unparseable body: %#v", rejection)
	}
	if maskedRejection(0, nil, newSecretMasker(testBrokerConfig(), "tok")) != nil {
		t.Fatal("no HTTP response must yield no rejection")
	}
}

func TestMaskedRejectionBoundsFields(t *testing.T) {
	masker := newSecretMasker(testBrokerConfig(), "tok")
	long := strings.Repeat("가", 1000)
	body := map[string]json.RawMessage{"msg1": json.RawMessage(`"` + long + `"`)}
	rejection := maskedRejection(http.StatusOK, body, masker)
	if got := len([]rune(rejection.Msg1)); got != maxRejectionMsgLength {
		t.Fatalf("msg1 runes = %d, want %d", got, maxRejectionMsgLength)
	}
}

func TestMaskedRejectionDropsNonStringFields(t *testing.T) {
	masker := newSecretMasker(testBrokerConfig(), "tok")
	// A serialized composite would let escaped secret material bypass the
	// text masker: \u0031\u0032... decodes to account digits only after JSON
	// decoding, so a raw object/array/number must never reach the output.
	escapedAccount := `\u0031\u0032\u0033\u0034\u0035\u0036\u0037\u0038` // 12345678
	body := map[string]json.RawMessage{
		"rt_cd":  json.RawMessage(`{"account":"` + escapedAccount + `-01"}`),
		"msg_cd": json.RawMessage(`12345`),
		"msg1":   json.RawMessage(`["denied", {"acct":"` + escapedAccount + `"}]`),
	}
	rejection := maskedRejection(http.StatusOK, body, masker)
	if rejection == nil {
		t.Fatal("rejection missing")
	}
	for name, field := range map[string]string{"rt_cd": rejection.RtCd, "msg_cd": rejection.MsgCd, "msg1": rejection.Msg1} {
		if field != nonStringFieldMarker {
			t.Fatalf("%s = %q, want %q", name, field, nonStringFieldMarker)
		}
	}
	encoded, err := json.Marshal(rejection)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, leaked := range []string{"12345678", escapedAccount} {
		if strings.Contains(string(encoded), leaked) {
			t.Fatalf("secret material %q survived in %s", leaked, encoded)
		}
	}
	if rejection.HTTPStatus != http.StatusOK {
		t.Fatalf("http_status = %d", rejection.HTTPStatus)
	}
}

func TestMaskedRejectionDropsNestedStringField(t *testing.T) {
	masker := newSecretMasker(testBrokerConfig(), "tok")
	body := map[string]json.RawMessage{
		"msg1": json.RawMessage(`{"outer":{"inner":"12345678-01"}}`),
	}
	rejection := maskedRejection(http.StatusOK, body, masker)
	if rejection.Msg1 != nonStringFieldMarker {
		t.Fatalf("nested object msg1 = %q, want %q", rejection.Msg1, nonStringFieldMarker)
	}
}

func TestMaskedRejectionDropsNullAndBoolFields(t *testing.T) {
	masker := newSecretMasker(testBrokerConfig(), "tok")
	body := map[string]json.RawMessage{
		"rt_cd":  json.RawMessage(`null`),
		"msg_cd": json.RawMessage(` true `),
	}
	rejection := maskedRejection(http.StatusOK, body, masker)
	if rejection.RtCd != nonStringFieldMarker || rejection.MsgCd != nonStringFieldMarker {
		t.Fatalf("non-string fields = %#v", rejection)
	}
}

func TestBrokerSendAttachesMaskedRejection(t *testing.T) {
	transport := &countingTransport{respond: func(*http.Request) (*http.Response, error) {
		return testHTTPResponse(http.StatusOK, `{"rt_cd":"1","msg_cd":"APBK0957","msg1":"balance check failed for 12345678-01 bearer cached-token-for-test"}`), nil
	}}
	broker := testKISMockBroker(transport)
	prepared, code := broker.Prepare(context.Background(), testCommand("rej-1"))
	if prepared == nil || code != "" {
		t.Fatalf("prepare code=%q", code)
	}
	result := prepared.Send(context.Background())
	if result.Accepted || result.ErrorCode != ErrorBrokerUnknown {
		t.Fatalf("result = %#v", result)
	}
	if result.Rejection == nil {
		t.Fatal("rejection missing")
	}
	if result.Rejection.HTTPStatus != http.StatusOK || result.Rejection.RtCd != "1" || result.Rejection.MsgCd != "APBK0957" {
		t.Fatalf("rejection = %#v", result.Rejection)
	}
	for _, secret := range []string{"12345678", "cached-token-for-test", "app-key-for-test", "app-secret-for-test"} {
		if strings.Contains(result.Rejection.Msg1, secret) {
			t.Fatalf("secret %q survived in %#v", secret, result.Rejection)
		}
	}
}

func TestBrokerSendAttachesRejectionOn5xxAndNonJSON(t *testing.T) {
	transport := &countingTransport{respond: func(*http.Request) (*http.Response, error) {
		return testHTTPResponse(http.StatusBadGateway, `<html>gateway 12345678-01</html>`), nil
	}}
	broker := testKISMockBroker(transport)
	prepared, _ := broker.Prepare(context.Background(), testCommand("rej-5xx"))
	result := prepared.Send(context.Background())
	if result.ErrorCode != ErrorBroker5xx || result.Rejection == nil || result.Rejection.HTTPStatus != http.StatusBadGateway {
		t.Fatalf("result = %#v", result)
	}
	if strings.Contains(result.Rejection.Msg1, "12345678") {
		t.Fatalf("account survived: %#v", result.Rejection)
	}
}

func TestRejectionReachesReceiptButNotStore(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "edge.sqlite"))
	transport := &countingTransport{respond: func(*http.Request) (*http.Response, error) {
		return testHTTPResponse(http.StatusOK, `{"rt_cd":"1","msg_cd":"APBK0957","msg1":"denied 12345678-01"}`), nil
	}}
	service := newTestService(store, testKISMockBroker(transport), true)
	receipt, err := service.Process(context.Background(), testCommand("rej-store"))
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if receipt.Disposition != executioncontracts.DispositionUnknown || receipt.ErrorCode != ErrorBrokerUnknown {
		t.Fatalf("receipt = %#v", receipt)
	}
	if receipt.Rejection == nil || receipt.Rejection.RtCd != "1" || strings.Contains(receipt.Rejection.Msg1, "12345678") {
		t.Fatalf("rejection = %#v", receipt.Rejection)
	}
	stored, found, err := store.Find(context.Background(), "rej-store")
	if err != nil || !found {
		t.Fatalf("find: %v %v", found, err)
	}
	if stored.Rejection != nil {
		t.Fatalf("rejection must never persist: %#v", stored.Rejection)
	}
}
