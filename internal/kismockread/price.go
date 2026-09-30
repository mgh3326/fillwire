package kismockread

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"time"
)

const (
	// domesticPricePath is the KIS current-price inquiry route. The route is
	// GET-only; it is a quotations surface, not a trading mutation.
	domesticPricePath = "/uapi/domestic-stock/v1/quotations/inquire-price"
	// domesticPriceTRID is the inquire-price TR id. KIS accepts the same id on
	// the VTS mock authority as on production.
	domesticPriceTRID = "FHKST01010100"
)

// DomesticPriceBand is the daily-band subset of the inquire-price output.
// Field mapping (output object of FHKST01010100): stck_prpr is the last
// traded price, stck_mxpr the upper limit, stck_llam the lower limit, and
// stck_sdpr the band's base (standard) price — the previous close the
// exchange multiplied by 0.7/1.3. Every value is a decimal digit string.
type DomesticPriceBand struct {
	LastPrice  string
	UpperLimit string
	LowerLimit string
	BasePrice  string
}

// DomesticPriceBand reads one symbol's current price and daily band through
// the same pinned, GET-only, cached-token machinery as the order-history
// read paths. It never issues or refreshes a token.
func (executor Executor) DomesticPriceBand(ctx context.Context, config Config, stockCode string) (DomesticPriceBand, *SafeError) {
	if err := validateConfig(config); err != nil {
		return DomesticPriceBand{}, err
	}
	if !allDigits(stockCode, 6) {
		return DomesticPriceBand{}, safeError(CodeInvalidInput)
	}
	cacheKey, err := TokenCacheKey(config.BaseURL, config.AppKey)
	if err != nil {
		return DomesticPriceBand{}, err
	}
	now := time.Now
	if executor.Now != nil {
		now = executor.Now
	}
	accessToken, err := LoadCachedToken(ctx, executor.TokenGetter, cacheKey, now())
	if err != nil {
		return DomesticPriceBand{}, err
	}
	return executeDomesticPrice(ctx, NewPinnedHTTPClient(executor.Transport, config.Timeout), config, stockCode, accessToken)
}

func executeDomesticPrice(ctx context.Context, client requestDoer, config Config, stockCode, accessToken string) (DomesticPriceBand, *SafeError) {
	base, err := url.Parse(config.BaseURL)
	if err != nil || ValidatePinnedURL(base) != nil {
		return DomesticPriceBand{}, safeError(CodeRequestBlocked)
	}
	requestURL := &url.URL{
		Scheme: base.Scheme,
		Host:   base.Host,
		Path:   domesticPricePath,
		RawQuery: url.Values{
			"FID_COND_MRKT_DIV_CODE": {"J"},
			"FID_INPUT_ISCD":         {stockCode},
		}.Encode(),
	}
	if ValidatePinnedURL(requestURL) != nil {
		return DomesticPriceBand{}, safeError(CodeRequestBlocked)
	}
	request, buildErr := newReadRequest(ctx, requestURL.String(), config, accessToken, domesticPriceTRID, "")
	if buildErr != nil {
		return DomesticPriceBand{}, safeError(CodeRequestBlocked)
	}
	response, requestErr := client.Do(request)
	if requestErr != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		if errors.Is(requestErr, errRedirectBlocked) {
			return DomesticPriceBand{}, safeError(CodeRedirectBlocked)
		}
		return DomesticPriceBand{}, safeError(CodeRequestFailed)
	}
	if response == nil {
		return DomesticPriceBand{}, safeError(CodeRequestFailed)
	}
	body, responseErr := readBrokerResponse(response)
	if responseErr != nil {
		return DomesticPriceBand{}, responseErr
	}
	return domesticPriceBand(body)
}

// domesticPriceBand extracts only the numeric price fields of the output
// object. A present-but-non-numeric field is a malformed response, not a
// missing one, so the caller never prices an order off garbage.
func domesticPriceBand(body map[string]json.RawMessage) (DomesticPriceBand, *SafeError) {
	rawOutput, present := body["output"]
	if !present {
		return DomesticPriceBand{}, safeError(CodeResponseInvalid)
	}
	var output map[string]json.RawMessage
	if json.Unmarshal(rawOutput, &output) != nil {
		return DomesticPriceBand{}, safeError(CodeResponseInvalid)
	}
	band := DomesticPriceBand{}
	fields := []struct {
		key  string
		dest *string
	}{
		{"stck_prpr", &band.LastPrice},
		{"stck_mxpr", &band.UpperLimit},
		{"stck_llam", &band.LowerLimit},
		{"stck_sdpr", &band.BasePrice},
	}
	for _, field := range fields {
		raw, present := output[field.key]
		if !present {
			continue
		}
		var text string
		if json.Unmarshal(raw, &text) != nil || (text != "" && !allDigitsAtMost(text, 16)) {
			return DomesticPriceBand{}, safeError(CodeResponseInvalid)
		}
		*field.dest = text
	}
	return band, nil
}
