package kismockedge

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	"github.com/mgh3326/broker-edge/internal/kismockread"
)

const (
	mockUSOrderPath  = "/uapi/overseas-stock/v1/trading/order"
	mockUSCancelPath = "/uapi/overseas-stock/v1/trading/order-rvsecncl"
	mockUSExchange   = "NASD"
)

// KISMockUSBroker is the VTS-only NASD equity implementation. It is separate
// from KISMockBroker so the domestic request body and TR surface cannot change
// as this overseas scope evolves.
type KISMockUSBroker struct {
	Transport  http.RoundTripper
	LoadConfig ConfigLoader
	Tokens     TokenLoader
}

func (broker KISMockUSBroker) Prepare(
	ctx context.Context,
	command executioncontracts.ExecutionCommandV1,
) (PreparedBroker, string) {
	if command.AccountScope != executioncontracts.AccountScopeKISMockUS ||
		broker.LoadConfig == nil || broker.Tokens == nil {
		return nil, ErrorInvalidCommand
	}
	if code := validateKISMockUSCommand(command); code != "" {
		return nil, code
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

// prepareWithCredentials mirrors auto_trader's overseas order body exactly
// for this fixed NASD scope. The source uses no hashkey on this route, so no
// hashkey capability or header is introduced here.
func (broker KISMockUSBroker) prepareWithCredentials(
	ctx context.Context,
	config kismockread.Config,
	command executioncontracts.ExecutionCommandV1,
	token string,
) (PreparedBroker, string) {
	baseURL, err := url.Parse(config.BaseURL)
	if err != nil || kismockread.ValidatePinnedURL(baseURL) != nil || token == "" ||
		!safeHeaderText(config.AppKey) || !safeHeaderText(config.AppSecret) || !safeHeaderText(token) {
		return nil, ErrorInvalidCommand
	}
	cano, productCode, validAccount := splitAccountNo(config.AccountNo)
	if !validAccount {
		return nil, ErrorInvalidCommand
	}
	trID := ""
	sellType := ""
	switch command.Side {
	case "buy":
		trID = "VTTT1002U"
	case "sell":
		// auto_trader selects the US-specific VTS sell TR for NASD/NYSE/AMEX.
		trID = "VTTT1001U"
		sellType = "00"
	default:
		return nil, ErrorInvalidCommand
	}
	body, err := json.Marshal(mockUSPlaceRequest{
		CANO:           cano,
		AccountProduct: productCode,
		Exchange:       mockUSExchange,
		StockCode:      toKISMockUSSymbol(command.StockCode),
		Quantity:       command.Quantity,
		Price:          command.Price,
		ContactNumber:  "",
		ManagedOrderNo: "",
		SellType:       sellType,
		OrderServer:    "0",
		OrderDivision:  "00",
	})
	if err != nil {
		return nil, ErrorInvalidCommand
	}
	requestURL := &url.URL{Scheme: baseURL.Scheme, Host: baseURL.Host, Path: mockUSOrderPath}
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

type mockUSPlaceRequest struct {
	CANO           string `json:"CANO"`
	AccountProduct string `json:"ACNT_PRDT_CD"`
	Exchange       string `json:"OVRS_EXCG_CD"`
	StockCode      string `json:"PDNO"`
	Quantity       string `json:"ORD_QTY"`
	Price          string `json:"OVRS_ORD_UNPR"`
	ContactNumber  string `json:"CTAC_TLNO"`
	ManagedOrderNo string `json:"MGCO_APTM_ODNO"`
	SellType       string `json:"SLL_TYPE"`
	OrderServer    string `json:"ORD_SVR_DVSN_CD"`
	OrderDivision  string `json:"ORD_DVSN"`
}

// PrepareCancel mirrors auto_trader's overseas cancellation TR. Price is
// deliberately absent from the precondition and body: the reference sends an
// explicit OVRS_ORD_UNPR of zero for a cancellation.
func (broker KISMockUSBroker) PrepareCancel(ctx context.Context, target CancelTarget) (PreparedCancelBroker, string) {
	if target.AccountScope != executioncontracts.AccountScopeKISMockUS || target.BrokerOrderID == "" ||
		target.StockCode == "" || target.Quantity == "" || broker.LoadConfig == nil || broker.Tokens == nil {
		return nil, ErrorInvalidCommand
	}
	config, code := broker.LoadConfig()
	if code != "" {
		return nil, code
	}
	token, code := broker.Tokens.Load(ctx, config)
	if code != "" {
		return nil, code
	}
	baseURL, err := url.Parse(config.BaseURL)
	if err != nil || kismockread.ValidatePinnedURL(baseURL) != nil || token == "" ||
		!safeHeaderText(config.AppKey) || !safeHeaderText(config.AppSecret) || !safeHeaderText(token) {
		return nil, ErrorInvalidCommand
	}
	cano, productCode, validAccount := splitAccountNo(config.AccountNo)
	if !validAccount {
		return nil, ErrorInvalidCommand
	}
	body, err := json.Marshal(mockUSCancelRequest{
		CANO:               cano,
		AccountProduct:     productCode,
		Exchange:           mockUSExchange,
		StockCode:          toKISMockUSSymbol(target.StockCode),
		OriginalOrderNo:    target.BrokerOrderID,
		RevisionCancelCode: "02",
		Quantity:           target.Quantity,
		Price:              "0",
		ManagedOrderNo:     "",
		OrderServer:        "0",
	})
	if err != nil {
		return nil, ErrorInvalidCommand
	}
	requestURL := &url.URL{Scheme: baseURL.Scheme, Host: baseURL.Host, Path: mockUSCancelPath}
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
	request.Header.Set("tr_id", "VTTT1004U")
	request.Header.Set("custtype", "P")
	return &preparedKISMockCancel{
		client:  kismockread.NewPinnedHTTPClient(broker.Transport, config.Timeout),
		request: request,
	}, ""
}

type mockUSCancelRequest struct {
	CANO               string `json:"CANO"`
	AccountProduct     string `json:"ACNT_PRDT_CD"`
	Exchange           string `json:"OVRS_EXCG_CD"`
	StockCode          string `json:"PDNO"`
	OriginalOrderNo    string `json:"ORGN_ODNO"`
	RevisionCancelCode string `json:"RVSE_CNCL_DVSN_CD"`
	Quantity           string `json:"ORD_QTY"`
	Price              string `json:"OVRS_ORD_UNPR"`
	ManagedOrderNo     string `json:"MGCO_APTM_ODNO"`
	OrderServer        string `json:"ORD_SVR_DVSN_CD"`
}

func toKISMockUSSymbol(symbol string) string {
	return strings.ReplaceAll(symbol, ".", "/")
}
