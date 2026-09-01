package kismockedge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
)

const (
	// AlpacaPaperCryptoHost is the sole authority that this backend can use.
	AlpacaPaperCryptoHost      = "paper-api.alpaca.markets"
	AlpacaPaperCryptoBaseURL   = "https://paper-api.alpaca.markets"
	AlpacaPaperCryptoOrderPath = "/v2/orders"

	// AlpacaLiveTradingHost and AlpacaLiveTradingBaseURL are intentionally
	// compile-time forbidden values, mirroring auto_trader's
	// FORBIDDEN_TRADING_BASE_URLS protection. No environment setting can
	// select, weaken, or override the paper host pin.
	AlpacaLiveTradingHost    = "api.alpaca.markets"
	AlpacaLiveTradingBaseURL = "https://api.alpaca.markets"

	// AlpacaPaperCryptoSymbolBTCUSD and AlpacaPaperUSStockSymbolAAPL are the
	// complete smoke-contract allowlist (contract v1.2: one US-stock paper
	// order added; same paper account, same $10 notional cap).
	AlpacaPaperCryptoSymbolBTCUSD = "BTC/USD"
	AlpacaPaperUSStockSymbolAAPL  = "AAPL"
)

var (
	errAlpacaRedirectBlocked = errors.New("alpaca redirect blocked")
	errAlpacaPinnedBlocked   = errors.New("alpaca pinned request blocked")
)

// AlpacaPaperCryptoConfig contains only process-local paper credentials. The
// base URL is fixed by ConfigFromEnv and is retained here only so direct tests
// can prove that a live host is rejected before transport.
type AlpacaPaperCryptoConfig struct {
	BaseURL   string
	APIKey    string
	APISecret string
	Timeout   time.Duration
}

// AlpacaPaperCryptoConfigLoader keeps secret lookup lazy, after the shared
// placement gate and all local command validation have passed.
type AlpacaPaperCryptoConfigLoader func() (AlpacaPaperCryptoConfig, string)

// AlpacaPaperCryptoConfigFromEnv reads only the documented static paper key
// names. It never reads a host setting, token cache, Redis setting, or live
// credential name, and returns only a safe error code.
func AlpacaPaperCryptoConfigFromEnv(lookup func(string) string) (AlpacaPaperCryptoConfig, string) {
	if lookup == nil {
		lookup = func(string) string { return "" }
	}
	config := AlpacaPaperCryptoConfig{
		BaseURL:   AlpacaPaperCryptoBaseURL,
		APIKey:    strings.TrimSpace(lookup("ALPACA_PAPER_CRYPTO_API_KEY")),
		APISecret: strings.TrimSpace(lookup("ALPACA_PAPER_CRYPTO_API_SECRET")),
		Timeout:   10 * time.Second,
	}
	if config.APIKey == "" || config.APISecret == "" ||
		!safeHeaderText(config.APIKey) || !safeHeaderText(config.APISecret) {
		return AlpacaPaperCryptoConfig{}, ErrorConfigurationMissing
	}
	return config, ""
}

// AlpacaPaperCryptoBroker is the paper-only Alpaca Broker implementation.
// It has no token cache or Redis dependency because Alpaca paper API keys are
// static request credentials.
type AlpacaPaperCryptoBroker struct {
	Transport  http.RoundTripper
	LoadConfig AlpacaPaperCryptoConfigLoader
}

// Prepare performs all Alpaca credential lookup and request construction
// before Service creates its durable pending marker.
func (broker AlpacaPaperCryptoBroker) Prepare(
	ctx context.Context,
	command executioncontracts.ExecutionCommandV1,
) (PreparedBroker, string) {
	if broker.LoadConfig == nil {
		return nil, ErrorStorageFailure
	}
	config, configCode := broker.LoadConfig()
	if configCode != "" {
		return nil, configCode
	}
	return broker.prepareWithConfig(ctx, config, command)
}

func (broker AlpacaPaperCryptoBroker) prepareWithConfig(
	ctx context.Context,
	config AlpacaPaperCryptoConfig,
	command executioncontracts.ExecutionCommandV1,
) (PreparedBroker, string) {
	if code := validateAlpacaPaperCryptoCommand(command); code != "" {
		return nil, code
	}
	if code := validateAlpacaPaperCryptoOrderCaps(command); code != "" {
		return nil, code
	}
	baseURL, err := url.Parse(config.BaseURL)
	if err != nil || !validAlpacaPaperCryptoURL(baseURL) ||
		config.APIKey == "" || config.APISecret == "" ||
		!safeHeaderText(config.APIKey) || !safeHeaderText(config.APISecret) {
		return nil, ErrorInvalidCommand
	}
	body, err := json.Marshal(alpacaPaperCryptoOrderRequest{
		Symbol:      command.StockCode,
		Quantity:    command.Quantity,
		Side:        command.Side,
		Type:        "limit",
		LimitPrice:  command.Price,
		TimeInForce: "gtc",
	})
	if err != nil {
		return nil, ErrorInvalidCommand
	}
	requestURL := &url.URL{
		Scheme: baseURL.Scheme,
		Host:   baseURL.Host,
		Path:   AlpacaPaperCryptoOrderPath,
	}
	if !validAlpacaPaperCryptoURL(requestURL) {
		return nil, ErrorInvalidCommand
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL.String(), bytes.NewReader(body))
	if err != nil {
		return nil, ErrorInvalidCommand
	}
	request.Header.Set("content-type", "application/json")
	request.Header.Set("APCA-API-KEY-ID", config.APIKey)
	request.Header.Set("APCA-API-SECRET-KEY", config.APISecret)
	return &preparedAlpacaPaperCryptoBroker{
		client:  newAlpacaPaperCryptoPinnedHTTPClient(broker.Transport, config.Timeout),
		request: request,
	}, ""
}

type alpacaPaperCryptoOrderRequest struct {
	Symbol      string `json:"symbol"`
	Quantity    string `json:"qty"`
	Side        string `json:"side"`
	Type        string `json:"type"`
	LimitPrice  string `json:"limit_price"`
	TimeInForce string `json:"time_in_force"`
}

type preparedAlpacaPaperCryptoBroker struct {
	client  *http.Client
	request *http.Request
	mu      sync.Mutex
	sent    bool
}

// Send makes one paper-only HTTP attempt. Once it begins, all failures remain
// UNKNOWN through Service; this method returns only safe categorization codes.
func (prepared *preparedAlpacaPaperCryptoBroker) Send(ctx context.Context) BrokerResult {
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
	// Clone does not clone Body. This prepared request is intentionally sent
	// once; there is no retry, replay, or cancellation route in this backend.
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
	rawID, present := body["id"]
	if !present {
		return BrokerResult{ErrorCode: ErrorBrokerUnknown}
	}
	var orderID string
	if json.Unmarshal(rawID, &orderID) != nil || strings.TrimSpace(orderID) == "" {
		return BrokerResult{ErrorCode: ErrorBrokerUnknown}
	}
	if _, present := body["status"]; !present {
		return BrokerResult{ErrorCode: ErrorBrokerUnknown}
	}
	return BrokerResult{Accepted: true, BrokerOrderID: orderID}
}

func validAlpacaPaperCryptoURL(requestURL *url.URL) bool {
	if requestURL == nil || requestURL.User != nil || requestURL.Fragment != "" ||
		!strings.EqualFold(requestURL.Scheme, "https") {
		return false
	}
	// The explicit live constant keeps the refusal readable even though the
	// allow-only paper host check below rejects every other authority as well.
	if strings.EqualFold(requestURL.Host, AlpacaLiveTradingHost) {
		return false
	}
	return strings.EqualFold(requestURL.Host, AlpacaPaperCryptoHost)
}

type alpacaPaperCryptoPinningTransport struct {
	base http.RoundTripper
}

func (transport alpacaPaperCryptoPinningTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil || !validAlpacaPaperCryptoURL(request.URL) {
		return nil, errAlpacaPinnedBlocked
	}
	return transport.base.RoundTrip(request)
}

// newAlpacaPaperCryptoPinnedHTTPClient refuses redirects and repeats the
// scheme/host pin inside RoundTrip immediately before transport can send.
func newAlpacaPaperCryptoPinnedHTTPClient(base http.RoundTripper, timeout time.Duration) *http.Client {
	if base == nil {
		// Never inherit ambient proxy routing for credential-bearing requests.
		if defaultTransport, ok := http.DefaultTransport.(*http.Transport); ok {
			cloned := defaultTransport.Clone()
			cloned.Proxy = nil
			base = cloned
		} else {
			base = &http.Transport{Proxy: nil}
		}
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &http.Client{
		Transport: alpacaPaperCryptoPinningTransport{base: base},
		Timeout:   timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return errAlpacaRedirectBlocked
		},
	}
}
