package kismockread

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

// DomesticOrder is the minimum non-sensitive evidence exposed to the local
// reconciliation path. It is intentionally not used by the CLI, whose output
// remains count-only.
type DomesticOrder struct {
	BrokerOrderID string
	Side          string
	StockCode     string
	Quantity      string
	Price         string
	OrderedAt     time.Time
}

// DomesticOrderHistory reads one KIS VTS order day through the same pinned,
// GET-only VTTC8001R route used by the kis-mock-read CLI.
func (executor Executor) DomesticOrderHistory(ctx context.Context, config Config, day string) ([]DomesticOrder, *SafeError) {
	input := ReadRequest{Operation: OperationDomesticOrderHistory, FromDate: day, ToDate: day, Side: "00"}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	if err := validateReadRequest(&input); err != nil {
		return nil, err
	}
	cacheKey, err := TokenCacheKey(config.BaseURL, config.AppKey)
	if err != nil {
		return nil, err
	}
	now := time.Now
	if executor.Now != nil {
		now = executor.Now
	}
	accessToken, err := LoadCachedToken(ctx, executor.TokenGetter, cacheKey, now())
	if err != nil {
		return nil, err
	}
	cano, accountProductCode, err := splitAccountNo(config.AccountNo)
	if err != nil {
		return nil, err
	}
	spec, _ := LookupReadSpec(OperationDomesticOrderHistory)
	return executeDomesticOrderHistoryPages(ctx, NewPinnedHTTPClient(executor.Transport, config.Timeout), config, input, spec, cano, accountProductCode, accessToken)
}

func executeDomesticOrderHistoryPages(ctx context.Context, client requestDoer, config Config, input ReadRequest, spec ReadSpec, cano, accountProductCode, accessToken string) ([]DomesticOrder, *SafeError) {
	// requestDoer keeps this loop mechanically aligned with executePages while
	// permitting the reconciliation-specific body parser.
	var orders []DomesticOrder
	cursorFK, cursorNK, continuation := "", "", ""
	for page := 1; page <= spec.MaximumPages; page++ {
		requestURL, err := buildReadURL(config.BaseURL, spec, input, cano, accountProductCode, cursorFK, cursorNK)
		if err != nil {
			return nil, err
		}
		request, buildErr := newReadRequest(ctx, requestURL.String(), config, accessToken, spec.TRID, continuation)
		if buildErr != nil {
			return nil, safeError(CodeRequestBlocked)
		}
		response, requestErr := client.Do(request)
		if requestErr != nil {
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			if errors.Is(requestErr, errRedirectBlocked) {
				return nil, safeError(CodeRedirectBlocked)
			}
			return nil, safeError(CodeRequestFailed)
		}
		if response == nil {
			return nil, safeError(CodeRequestFailed)
		}
		body, responseErr := readBrokerResponse(response)
		if responseErr != nil {
			return nil, responseErr
		}
		pageOrders, parseErr := domesticOrders(body)
		if parseErr != nil {
			return nil, parseErr
		}
		orders = append(orders, pageOrders...)
		hasNext, nextFK, nextNK, cursorErr := nextCursor(spec, response.Header, body)
		if cursorErr != nil {
			return nil, cursorErr
		}
		if !hasNext {
			return orders, nil
		}
		if nextNK == "" || nextNK == cursorNK || page == spec.MaximumPages {
			return nil, safeError(CodeResponseInvalid)
		}
		cursorFK, cursorNK, continuation = nextFK, nextNK, "N"
	}
	return nil, safeError(CodeResponseInvalid)
}

type requestDoer interface {
	Do(*http.Request) (*http.Response, error)
}

func newReadRequest(ctx context.Context, requestURL string, config Config, accessToken, trID, continuation string) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, err
	}
	if ValidatePinnedURL(request.URL) != nil {
		return nil, errors.New("request blocked")
	}
	request.Header.Set("authorization", "Bearer "+accessToken)
	request.Header.Set("appkey", config.AppKey)
	request.Header.Set("appsecret", config.AppSecret)
	request.Header.Set("tr_id", trID)
	request.Header.Set("custtype", "P")
	request.Header.Set("tr_cont", continuation)
	return request, nil
}

func domesticOrders(body map[string]json.RawMessage) ([]DomesticOrder, *SafeError) {
	rawRecords, present := body["output1"]
	if !present {
		return nil, safeError(CodeResponseInvalid)
	}
	var records []map[string]json.RawMessage
	if json.Unmarshal(rawRecords, &records) != nil {
		return nil, safeError(CodeResponseInvalid)
	}
	orders := make([]DomesticOrder, 0, len(records))
	for _, record := range records {
		order, err := domesticOrder(record)
		if err != nil {
			return nil, err
		}
		orders = append(orders, order)
	}
	return orders, nil
}

func domesticOrder(record map[string]json.RawMessage) (DomesticOrder, *SafeError) {
	value := func(names ...string) (string, bool) {
		for key, raw := range record {
			for _, name := range names {
				if strings.EqualFold(key, name) {
					var text string
					if json.Unmarshal(raw, &text) == nil {
						return strings.TrimSpace(text), true
					}
				}
			}
		}
		return "", false
	}
	orderID, okID := value("odno", "ord_no")
	sideCode, okSide := value("sll_buy_dvsn_cd")
	stockCode, okStock := value("pdno")
	quantity, okQuantity := value("ord_qty")
	price, okPrice := value("ord_unpr")
	date, okDate := value("ord_dt")
	clock, okClock := value("ord_tmd")
	if !okID || !okSide || !okStock || !okQuantity || !okPrice || !okDate || !okClock ||
		orderID == "" || !allDigitsAtMost(orderID, 20) || !allDigits(stockCode, 6) ||
		!allDigitsAtMost(quantity, 128) || !allDigitsAtMost(price, 128) || !validDate(date) || !allDigits(clock, 6) {
		return DomesticOrder{}, safeError(CodeResponseInvalid)
	}
	side := ""
	switch sideCode {
	case "01", "sell":
		side = "sell"
	case "02", "buy":
		side = "buy"
	default:
		return DomesticOrder{}, safeError(CodeResponseInvalid)
	}
	orderedAt, err := time.ParseInLocation("20060102150405", date+clock, time.FixedZone("KST", 9*60*60))
	if err != nil {
		return DomesticOrder{}, safeError(CodeResponseInvalid)
	}
	return DomesticOrder{BrokerOrderID: orderID, Side: side, StockCode: stockCode, Quantity: quantity, Price: price, OrderedAt: orderedAt.UTC()}, nil
}


// OverseasOrder is the minimum non-sensitive evidence exposed to the US
// reconciliation path. It mirrors the ft_* fields returned by the overseas
// daily-order history TR and deliberately carries no account data or payload.
type OverseasOrder struct {
	BrokerOrderID string
	Side          string
	StockCode     string
	Quantity      string
	Price         string
	OrderedAt     time.Time
}

// OverseasOrderHistory reads one KIS VTS NASD daily-order history through the
// mock-available VTTS3035R route. The VTS pending-order TR is intentionally
// not used because auto_trader rejects it in mock mode.
func (executor Executor) OverseasOrderHistory(ctx context.Context, config Config, day string) ([]OverseasOrder, *SafeError) {
	input := ReadRequest{Operation: OperationOverseasOrderHistory, FromDate: day, ToDate: day, Side: "00", Exchange: "NASD"}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	if err := validateReadRequest(&input); err != nil {
		return nil, err
	}
	cacheKey, err := TokenCacheKey(config.BaseURL, config.AppKey)
	if err != nil {
		return nil, err
	}
	now := time.Now
	if executor.Now != nil {
		now = executor.Now
	}
	accessToken, err := LoadCachedToken(ctx, executor.TokenGetter, cacheKey, now())
	if err != nil {
		return nil, err
	}
	cano, accountProductCode, err := splitAccountNo(config.AccountNo)
	if err != nil {
		return nil, err
	}
	spec, _ := LookupReadSpec(OperationOverseasOrderHistory)
	return executeOverseasOrderHistoryPages(ctx, NewPinnedHTTPClient(executor.Transport, config.Timeout), config, input, spec, cano, accountProductCode, accessToken)
}

func executeOverseasOrderHistoryPages(ctx context.Context, client requestDoer, config Config, input ReadRequest, spec ReadSpec, cano, accountProductCode, accessToken string) ([]OverseasOrder, *SafeError) {
	orders := make([]OverseasOrder, 0)
	seenOrderIDs := make(map[string]struct{})
	cursorFK, cursorNK, continuation := "", "", ""
	for page := 1; page <= spec.MaximumPages; page++ {
		requestURL, err := buildReadURL(config.BaseURL, spec, input, cano, accountProductCode, cursorFK, cursorNK)
		if err != nil {
			return nil, err
		}
		request, buildErr := newReadRequest(ctx, requestURL.String(), config, accessToken, spec.TRID, continuation)
		if buildErr != nil {
			return nil, safeError(CodeRequestBlocked)
		}
		response, requestErr := client.Do(request)
		if requestErr != nil {
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			if errors.Is(requestErr, errRedirectBlocked) {
				return nil, safeError(CodeRedirectBlocked)
			}
			return nil, safeError(CodeRequestFailed)
		}
		if response == nil {
			return nil, safeError(CodeRequestFailed)
		}
		body, responseErr := readBrokerResponse(response)
		if responseErr != nil {
			return nil, responseErr
		}
		pageOrders, parseErr := overseasOrders(body)
		if parseErr != nil {
			return nil, parseErr
		}
		for _, order := range pageOrders {
			// auto_trader identifies an overseas daily-history row by ODNO and
			// documents that KIS can repeat it over continuation pages.
			if _, seen := seenOrderIDs[order.BrokerOrderID]; seen {
				continue
			}
			seenOrderIDs[order.BrokerOrderID] = struct{}{}
			orders = append(orders, order)
		}
		hasNext, nextFK, nextNK, cursorErr := nextCursor(spec, response.Header, body)
		if cursorErr != nil {
			return nil, cursorErr
		}
		if !hasNext {
			return orders, nil
		}
		if nextNK == "" || nextNK == cursorNK || page == spec.MaximumPages {
			return nil, safeError(CodeResponseInvalid)
		}
		cursorFK, cursorNK, continuation = nextFK, nextNK, "N"
	}
	return nil, safeError(CodeResponseInvalid)
}

func overseasOrders(body map[string]json.RawMessage) ([]OverseasOrder, *SafeError) {
	rawRecords, present := body["output1"]
	if !present {
		return nil, safeError(CodeResponseInvalid)
	}
	var records []map[string]json.RawMessage
	if json.Unmarshal(rawRecords, &records) != nil {
		return nil, safeError(CodeResponseInvalid)
	}
	orders := make([]OverseasOrder, 0, len(records))
	for _, record := range records {
		order, err := overseasOrder(record)
		if err != nil {
			return nil, err
		}
		orders = append(orders, order)
	}
	return orders, nil
}

func overseasOrder(record map[string]json.RawMessage) (OverseasOrder, *SafeError) {
	value := func(names ...string) (string, bool) {
		for key, raw := range record {
			for _, name := range names {
				if strings.EqualFold(key, name) {
					var text string
					if json.Unmarshal(raw, &text) == nil {
						return strings.TrimSpace(text), true
					}
				}
			}
		}
		return "", false
	}
	orderID, okID := value("odno", "ord_no")
	sideCode, okSide := value("sll_buy_dvsn_cd")
	stockCode, okStock := value("pdno")
	quantity, okQuantity := value("ft_ord_qty")
	price, okPrice := value("ft_ord_unpr3")
	date, okDate := value("ord_dt")
	clock, okClock := value("ord_tmd")
	if !okID || !okSide || !okStock || !okQuantity || !okPrice || !okDate || !okClock ||
		!validOpaqueOrderID(orderID) || !validOverseasStockCode(stockCode) ||
		!allDigitsAtMost(quantity, 128) || !validDecimalText(price) || !validDate(date) || !allDigits(clock, 6) {
		return OverseasOrder{}, safeError(CodeResponseInvalid)
	}
	side := ""
	switch sideCode {
	case "01", "sell":
		side = "sell"
	case "02", "buy":
		side = "buy"
	default:
		return OverseasOrder{}, safeError(CodeResponseInvalid)
	}
	orderedAt, err := time.ParseInLocation("20060102150405", date+clock, time.FixedZone("KST", 9*60*60))
	if err != nil {
		return OverseasOrder{}, safeError(CodeResponseInvalid)
	}
	return OverseasOrder{BrokerOrderID: orderID, Side: side, StockCode: stockCode, Quantity: quantity, Price: price, OrderedAt: orderedAt.UTC()}, nil
}

func validOpaqueOrderID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character == 0x7f {
			return false
		}
	}
	return true
}

func validDecimalText(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	dot := -1
	for index := 0; index < len(value); index++ {
		character := value[index]
		switch {
		case character >= '0' && character <= '9':
		case character == '.' && dot == -1:
			dot = index
		default:
			return false
		}
	}
	return dot != 0 && dot != len(value)-1
}
