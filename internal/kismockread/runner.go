package kismockread

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const brokerResponseLimit = 1024 * 1024

// ReadRequest contains only inputs for the closed operation set.
type ReadRequest struct {
	Operation Operation
	FromDate  string
	ToDate    string
	StockCode string
	Side      string
	OrderNo   string
	Exchange  string
	Currency  string
}

// Result is deliberately a summary, not a raw broker payload.
type Result struct {
	Operation Operation
	TRID      string
	Pages     int
	Records   int
}

// Executor has only a cache reader and a transport injection seam for tests.
// The transport is always wrapped by NewPinnedHTTPClient before use.
type Executor struct {
	TokenGetter RedisGetter
	Transport   http.RoundTripper
	Now         func() time.Time
}

// Execute reads an already-issued cached token and performs one bounded,
// allowlisted, GET-only VTS operation. It never retries through issuance.
func (executor Executor) Execute(ctx context.Context, config Config, input ReadRequest) (Result, *SafeError) {
	spec, found := LookupReadSpec(input.Operation)
	if !found {
		return Result{}, safeError(CodeInvalidInput)
	}
	if err := validateConfig(config); err != nil {
		return Result{}, err
	}
	if err := validateReadRequest(&input); err != nil {
		return Result{}, err
	}

	cacheKey, err := TokenCacheKey(config.BaseURL, config.AppKey)
	if err != nil {
		return Result{}, err
	}
	now := time.Now
	if executor.Now != nil {
		now = executor.Now
	}
	accessToken, err := LoadCachedToken(ctx, executor.TokenGetter, cacheKey, now())
	if err != nil {
		return Result{}, err
	}

	cano, accountProductCode, err := splitAccountNo(config.AccountNo)
	if err != nil {
		return Result{}, err
	}
	client := NewPinnedHTTPClient(executor.Transport, config.Timeout)
	return executePages(ctx, client, config, input, spec, cano, accountProductCode, accessToken)
}

func validateConfig(config Config) *SafeError {
	if config.BaseURL == "" || config.AppKey == "" || config.AppSecret == "" || config.AccountNo == "" {
		return safeError(CodeConfigurationMissing)
	}
	baseURL, parseErr := url.Parse(config.BaseURL)
	if parseErr != nil || ValidatePinnedURL(baseURL) != nil ||
		!safeHeaderText(config.AppKey) || !safeHeaderText(config.AppSecret) {
		return safeError(CodeRequestBlocked)
	}
	_, _, accountErr := splitAccountNo(config.AccountNo)
	return accountErr
}

func validateReadRequest(input *ReadRequest) *SafeError {
	if input == nil {
		return safeError(CodeInvalidInput)
	}
	if input.Operation == OperationDomesticOrderHistory {
		if !validDate(input.FromDate) || !validDate(input.ToDate) || input.FromDate > input.ToDate {
			return safeError(CodeInvalidInput)
		}
		if input.Side == "" {
			input.Side = "00"
		}
		if input.Side != "00" && input.Side != "01" && input.Side != "02" {
			return safeError(CodeInvalidInput)
		}
		if input.StockCode != "" && !allDigits(input.StockCode, 6) {
			return safeError(CodeInvalidInput)
		}
		if input.OrderNo != "" && (!allDigitsAtMost(input.OrderNo, 20)) {
			return safeError(CodeInvalidInput)
		}
	}
	if input.Operation == OperationOverseasBalance {
		if input.Exchange == "" {
			input.Exchange = "NASD"
		}
		if input.Currency == "" {
			input.Currency = "USD"
		}
		if input.Exchange != "NASD" || input.Currency != "USD" {
			return safeError(CodeInvalidInput)
		}
	}
	return nil
}

func validDate(value string) bool {
	if !allDigits(value, 8) {
		return false
	}
	parsed, err := time.Parse("20060102", value)
	return err == nil && parsed.Format("20060102") == value
}

func allDigits(value string, length int) bool {
	return len(value) == length && allDigitsAtMost(value, length)
}

func allDigitsAtMost(value string, maximum int) bool {
	if value == "" || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func executePages(
	ctx context.Context,
	client *http.Client,
	config Config,
	input ReadRequest,
	spec ReadSpec,
	cano string,
	accountProductCode string,
	accessToken string,
) (Result, *SafeError) {
	result := Result{Operation: spec.Operation, TRID: spec.TRID}
	cursorFK := ""
	cursorNK := ""
	continuation := ""

	for page := 1; page <= spec.MaximumPages; page++ {
		requestURL, err := buildReadURL(config.BaseURL, spec, input, cano, accountProductCode, cursorFK, cursorNK)
		if err != nil {
			return Result{}, err
		}
		request, buildErr := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
		if buildErr != nil {
			return Result{}, safeError(CodeRequestBlocked)
		}
		request.Header.Set("authorization", "Bearer "+accessToken)
		request.Header.Set("appkey", config.AppKey)
		request.Header.Set("appsecret", config.AppSecret)
		request.Header.Set("tr_id", spec.TRID)
		request.Header.Set("custtype", "P")
		request.Header.Set("tr_cont", continuation)

		// This explicit check is intentionally adjacent to Do; pinningTransport
		// repeats it inside RoundTrip as the final defense against URL mutation.
		if ValidatePinnedURL(request.URL) != nil {
			return Result{}, safeError(CodeRequestBlocked)
		}
		response, requestErr := client.Do(request)
		if requestErr != nil {
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			if errors.Is(requestErr, errRedirectBlocked) {
				return Result{}, safeError(CodeRedirectBlocked)
			}
			return Result{}, safeError(CodeRequestFailed)
		}
		if response == nil {
			return Result{}, safeError(CodeRequestFailed)
		}
		body, responseErr := readBrokerResponse(response)
		if responseErr != nil {
			return Result{}, responseErr
		}

		result.Pages++
		records, responseErr := responseRecords(body)
		if responseErr != nil {
			return Result{}, responseErr
		}
		result.Records += records

		hasNext, nextFK, nextNK, responseErr := nextCursor(spec, response.Header, body)
		if responseErr != nil {
			return Result{}, responseErr
		}
		if !hasNext {
			return result, nil
		}
		if nextNK == "" || nextNK == cursorNK || page == spec.MaximumPages {
			return Result{}, safeError(CodeResponseInvalid)
		}
		cursorFK = nextFK
		cursorNK = nextNK
		continuation = "N"
	}
	return Result{}, safeError(CodeResponseInvalid)
}

func buildReadURL(
	baseURL string,
	spec ReadSpec,
	input ReadRequest,
	cano string,
	accountProductCode string,
	cursorFK string,
	cursorNK string,
) (*url.URL, *SafeError) {
	base, err := url.Parse(baseURL)
	if err != nil || ValidatePinnedURL(base) != nil || !strings.HasPrefix(spec.Path, "/uapi/") {
		return nil, safeError(CodeRequestBlocked)
	}
	query := readQuery(spec, input, cano, accountProductCode, cursorFK, cursorNK)
	requestURL := &url.URL{
		Scheme:   base.Scheme,
		Host:     base.Host,
		Path:     spec.Path,
		RawQuery: query.Encode(),
	}
	if ValidatePinnedURL(requestURL) != nil {
		return nil, safeError(CodeRequestBlocked)
	}
	return requestURL, nil
}

func readQuery(
	spec ReadSpec,
	input ReadRequest,
	cano string,
	accountProductCode string,
	cursorFK string,
	cursorNK string,
) url.Values {
	query := url.Values{
		"CANO":               {cano},
		"ACNT_PRDT_CD":       {accountProductCode},
		spec.RequestCursorFK: {cursorFK},
		spec.RequestCursorNK: {cursorNK},
	}

	switch spec.Operation {
	case OperationDomesticBalance:
		query.Set("AFHR_FLPR_YN", "N")
		query.Set("OFL_YN", "")
		query.Set("INQR_DVSN", "00")
		query.Set("UNPR_DVSN", "01")
		query.Set("FUND_STTL_ICLD_YN", "N")
		query.Set("FNCG_AMT_AUTO_RDPT_YN", "N")
		query.Set("PRCS_DVSN", "01")
	case OperationOverseasBalance:
		query.Set("OVRS_EXCG_CD", input.Exchange)
		query.Set("TR_CRCY_CD", input.Currency)
	case OperationDomesticOrderHistory:
		query.Set("INQR_STRT_DT", input.FromDate)
		query.Set("INQR_END_DT", input.ToDate)
		query.Set("SLL_BUY_DVSN_CD", input.Side)
		query.Set("PDNO", input.StockCode)
		query.Set("CCLD_DVSN", "00")
		query.Set("INQR_DVSN", "00")
		query.Set("INQR_DVSN_3", "00")
		query.Set("INQR_DVSN_1", "")
		query.Set("ORD_GNO_BRNO", "")
		query.Set("ODNO", input.OrderNo)
		query.Set("EXCG_ID_DVSN_CD", "ALL")
	}
	return query
}

func readBrokerResponse(response *http.Response) (map[string]json.RawMessage, *SafeError) {
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		if response.StatusCode >= http.StatusMultipleChoices && response.StatusCode < http.StatusBadRequest {
			return nil, safeError(CodeRedirectBlocked)
		}
		return nil, safeError(CodeRequestFailed)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, brokerResponseLimit))
	var body map[string]json.RawMessage
	if decoder.Decode(&body) != nil {
		return nil, safeError(CodeResponseInvalid)
	}
	resultCode, present := body["rt_cd"]
	var resultCodeText string
	if !present || json.Unmarshal(resultCode, &resultCodeText) != nil {
		return nil, safeError(CodeResponseInvalid)
	}
	if resultCodeText != "0" {
		return nil, safeError(CodeBrokerRejected)
	}
	return body, nil
}

func responseRecords(body map[string]json.RawMessage) (int, *SafeError) {
	rawRecords, present := body["output1"]
	if !present {
		return 0, safeError(CodeResponseInvalid)
	}
	var records []json.RawMessage
	if json.Unmarshal(rawRecords, &records) != nil {
		return 0, safeError(CodeResponseInvalid)
	}
	return len(records), nil
}

func nextCursor(spec ReadSpec, header http.Header, body map[string]json.RawMessage) (bool, string, string, *SafeError) {
	if spec.ContinuationUsesHeader {
		switch strings.ToUpper(strings.TrimSpace(header.Get("tr_cont"))) {
		case "", "D", "E":
			return false, "", "", nil
		case "F", "M":
			// Continue below and require both cursor fields.
		default:
			return false, "", "", safeError(CodeResponseInvalid)
		}
	}

	nextFK, fkPresent, err := responseString(body, spec.ResponseCursorFK)
	if err != nil {
		return false, "", "", err
	}
	nextNK, nkPresent, err := responseString(body, spec.ResponseCursorNK)
	if err != nil {
		return false, "", "", err
	}
	if spec.ContinuationUsesHeader && (!fkPresent || !nkPresent) {
		return false, "", "", safeError(CodeResponseInvalid)
	}
	if !spec.ContinuationUsesHeader && (!fkPresent || !nkPresent || nextNK == "") {
		return false, "", "", nil
	}
	return true, nextFK, nextNK, nil
}

func responseString(body map[string]json.RawMessage, key string) (string, bool, *SafeError) {
	raw, present := body[key]
	if !present {
		return "", false, nil
	}
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return "", false, safeError(CodeResponseInvalid)
	}
	return value, true, nil
}
