package kismockedge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	"github.com/mgh3326/broker-edge/internal/kismockread"
)

const (
	mockOrderPath       = "/uapi/domestic-stock/v1/trading/order-cash"
	brokerResponseLimit = 1024 * 1024
)

// Broker prepares an account-scoped paper order and exposes one non-retryable
// send step. Each implementation owns only its own credential lookup and
// request construction. Separating preparation from Send lets Service commit
// its shared pending marker immediately before the irreversible HTTP boundary.
type Broker interface {
	Prepare(context.Context, executioncontracts.ExecutionCommandV1) (PreparedBroker, string)
}

// PreparedBroker sends exactly one prepared broker HTTP request.
type PreparedBroker interface {
	Send(context.Context) BrokerResult
}

// BrokerResult contains only facts suitable for a receipt. A non-accepted
// result is deliberately mapped to UNKNOWN by Service after Send begins.
type BrokerResult struct {
	Accepted      bool
	BrokerOrderID string
	ErrorCode     string
}

// KISMockBroker is the real VTS-only broker implementation.
type KISMockBroker struct {
	Transport  http.RoundTripper
	LoadConfig ConfigLoader
	Tokens     TokenLoader
}

// Prepare lazily loads the KIS-only cached-token dependencies after Service has
// completed its shared pre-send validation and placement gate. No Alpaca path
// can reach either dependency because scope routing selects its own Broker.
func (broker KISMockBroker) Prepare(
	ctx context.Context,
	command executioncontracts.ExecutionCommandV1,
) (PreparedBroker, string) {
	if command.AccountScope != executioncontracts.AccountScopeKISMock {
		return nil, ErrorInvalidCommand
	}
	if broker.LoadConfig == nil || broker.Tokens == nil {
		return nil, ErrorStorageFailure
	}
	config, configCode := broker.LoadConfig()
	if configCode != "" {
		return nil, configCode
	}
	token, tokenCode := broker.Tokens.Load(ctx, config)
	if tokenCode != "" {
		return nil, tokenCode
	}
	return broker.prepareWithCredentials(ctx, config, command, token)
}

// prepareWithCredentials validates all KIS local request construction before
// the pending marker. Its URL and client both reuse kis-mock-read's HTTPS host
// pin and redirect refusal; no KIS live host is representable here.
func (broker KISMockBroker) prepareWithCredentials(
	ctx context.Context,
	config kismockread.Config,
	command executioncontracts.ExecutionCommandV1,
	token string,
) (PreparedBroker, string) {
	baseURL, err := url.Parse(config.BaseURL)
	if err != nil || kismockread.ValidatePinnedURL(baseURL) != nil ||
		!safeHeaderText(config.AppKey) || !safeHeaderText(config.AppSecret) || !safeHeaderText(token) || token == "" {
		return nil, ErrorInvalidCommand
	}
	cano, productCode, validAccount := splitAccountNo(config.AccountNo)
	if !validAccount {
		return nil, ErrorInvalidCommand
	}
	trID := ""
	switch command.Side {
	case "buy":
		trID = "VTTC0012U"
	case "sell":
		trID = "VTTC0011U"
	default:
		return nil, ErrorInvalidCommand
	}
	body, err := json.Marshal(mockPlaceRequest{
		CANO:           cano,
		AccountProduct: productCode,
		StockCode:      command.StockCode,
		OrderDivision:  "00", // limit only: cap and tick check are then enforceable.
		Quantity:       command.Quantity,
		Price:          command.Price,
		ExchangeRoute:  "KRX",
	})
	if err != nil {
		return nil, ErrorInvalidCommand
	}
	requestURL := &url.URL{
		Scheme: baseURL.Scheme,
		Host:   baseURL.Host,
		Path:   mockOrderPath,
	}
	if kismockread.ValidatePinnedURL(requestURL) != nil {
		return nil, ErrorInvalidCommand
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL.String(), bytes.NewReader(body))
	if err != nil {
		return nil, ErrorInvalidCommand
	}
	request.Header.Set("content-type", "application/json")
	request.Header.Set("authorization", "Bearer "+token)
	request.Header.Set("appkey", config.AppKey)
	request.Header.Set("appsecret", config.AppSecret)
	request.Header.Set("tr_id", trID)
	request.Header.Set("custtype", "P")
	return &preparedKISMockBroker{
		client:  kismockread.NewPinnedHTTPClient(broker.Transport, config.Timeout),
		request: request,
	}, ""
}

type mockPlaceRequest struct {
	CANO           string `json:"CANO"`
	AccountProduct string `json:"ACNT_PRDT_CD"`
	StockCode      string `json:"PDNO"`
	OrderDivision  string `json:"ORD_DVSN"`
	Quantity       string `json:"ORD_QTY"`
	Price          string `json:"ORD_UNPR"`
	ExchangeRoute  string `json:"EXCG_ID_DVSN_CD"`
}

type preparedKISMockBroker struct {
	client  *http.Client
	request *http.Request
	mu      sync.Mutex
	sent    bool
}

func (prepared *preparedKISMockBroker) Send(ctx context.Context) BrokerResult {
	if prepared == nil || prepared.client == nil || prepared.request == nil {
		return BrokerResult{ErrorCode: ErrorBrokerUnknown}
	}
	prepared.mu.Lock()
	if prepared.sent {
		prepared.mu.Unlock()
		return BrokerResult{ErrorCode: ErrorBrokerUnknown}
	}
	prepared.sent = true
	prepared.mu.Unlock()
	request := prepared.request.Clone(ctx)
	// Clone does not clone Body. The prepared request is intentionally sent one
	// time only; there is no retry or replay path in this type.
	response, err := prepared.client.Do(request)
	if err != nil {
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() {
			return BrokerResult{ErrorCode: ErrorBrokerTimeout}
		}
		return BrokerResult{ErrorCode: ErrorBrokerUnknown}
	}
	if response == nil {
		return BrokerResult{ErrorCode: ErrorBrokerUnknown}
	}
	defer response.Body.Close()
	if response.StatusCode >= http.StatusInternalServerError {
		return BrokerResult{ErrorCode: ErrorBroker5xx}
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return BrokerResult{ErrorCode: ErrorBrokerUnknown}
	}

	body, err := readBrokerBody(response.Body)
	if err != nil {
		return BrokerResult{ErrorCode: ErrorBrokerUnknown}
	}
	resultCode, present := body["rt_cd"]
	if !present {
		return BrokerResult{ErrorCode: ErrorBrokerUnknown}
	}
	var resultCodeText string
	if json.Unmarshal(resultCode, &resultCodeText) != nil || resultCodeText != "0" {
		return BrokerResult{ErrorCode: ErrorBrokerUnknown}
	}
	orderID := brokerOrderID(body)
	if orderID == "" {
		return BrokerResult{ErrorCode: ErrorBrokerUnknown}
	}
	return BrokerResult{Accepted: true, BrokerOrderID: orderID}
}

func readBrokerBody(reader io.Reader) (map[string]json.RawMessage, error) {
	limited := io.LimitReader(reader, brokerResponseLimit+1)
	raw, err := io.ReadAll(limited)
	if err != nil || len(raw) > brokerResponseLimit {
		return nil, errors.New("broker response invalid")
	}
	var body map[string]json.RawMessage
	if json.Unmarshal(raw, &body) != nil {
		return nil, errors.New("broker response invalid")
	}
	return body, nil
}

func brokerOrderID(body map[string]json.RawMessage) string {
	rawOutput, present := body["output"]
	if !present {
		return ""
	}
	var output map[string]json.RawMessage
	if json.Unmarshal(rawOutput, &output) != nil {
		return ""
	}
	for _, key := range []string{"ODNO", "ORD_NO"} {
		rawID, present := output[key]
		if !present {
			continue
		}
		var orderID string
		if json.Unmarshal(rawID, &orderID) == nil && strings.TrimSpace(orderID) != "" {
			return orderID
		}
	}
	return ""
}

func splitAccountNo(value string) (string, string, bool) {
	cleaned := strings.TrimSpace(value)
	if len(cleaned) == 11 && cleaned[8] == '-' {
		cleaned = cleaned[:8] + cleaned[9:]
	}
	if len(cleaned) == 8 && allDigits(cleaned, 8) {
		return cleaned, "01", true
	}
	if len(cleaned) == 10 && allDigits(cleaned, 10) {
		return cleaned[:8], cleaned[8:], true
	}
	return "", "", false
}

func safeHeaderText(value string) bool {
	return !strings.ContainsAny(value, "\r\n")
}
