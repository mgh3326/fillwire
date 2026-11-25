package marketminute

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/fillwire/internal/kst"
	"github.com/mgh3326/fillwire/internal/netpolicy"
)

// Closed failure classes reported for a target. They never carry an upstream
// message or a credential.
const (
	classFetchError        = "fetch_error"
	classRateLimited       = "rate_limited"
	classTokenUnavailable  = "token_unavailable"
	classMalformedResponse = "malformed_response"
	classCanceled          = "canceled"
	classCredentialHost    = "credential_host"
)

var (
	// ErrRateLimited reports HTTP 429 or KIS's GET rate-limit message code.
	ErrRateLimited = errors.New("marketminute: KIS rate limited the read")
	// ErrResponseRefused reports a non-zero KIS rt_cd for another reason.
	ErrResponseRefused = errors.New("marketminute: KIS refused the read")
	// ErrMalformedResponse reports a payload this reader cannot parse.
	ErrMalformedResponse = errors.New("marketminute: KIS response is malformed")
	// ErrTokenUnavailable reports that the read-only token source had no token.
	ErrTokenUnavailable = errors.New("marketminute: KIS access token unavailable")
	// ErrCredentialHost reports a host that would carry the KIS credentials
	// over cleartext or to an unpinned destination.
	ErrCredentialHost = errors.New("marketminute: credential host must be https or loopback")
)

// ErrorClass maps an error to the closed report vocabulary.
func ErrorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return classCanceled
	case errors.Is(err, ErrRateLimited):
		return classRateLimited
	case errors.Is(err, ErrTokenUnavailable), errors.Is(err, errTokenExpired):
		return classTokenUnavailable
	case errors.Is(err, ErrMalformedResponse):
		return classMalformedResponse
	case errors.Is(err, ErrCredentialHost):
		return classCredentialHost
	default:
		return classFetchError
	}
}

// DefaultHost is KIS live. Mock uses the paper host.
const (
	DefaultHost = "https://openapi.koreainvestment.com:9443"
	MockHost    = "https://openapivts.koreainvestment.com:29443"
)

// tokenExpiredCodes are the KIS message codes answered when the cached access
// token expired or was rejected. The reader asks its token source once more
// and retries the GET once; it never issues a token.
var tokenExpiredCodes = map[string]bool{"EGW00123": true, "EGW00121": true}

// rateLimitMsgCode is the KIS GET rate-limit answer.
const rateLimitMsgCode = "EGW00201"

// TokenSource returns the cached KIS access token. Implementations must be
// read-only: Redis GET or gatewayd fetch, never issuance.
type TokenSource func(context.Context) (string, error)

// ValidateHost refuses a host that would send the KIS bearer token, app key
// and app secret over cleartext or to an unsigned destination. https is
// allowed anywhere an operator deliberately points it; http only to loopback
// (local smoke and tests). Credentials, queries and fragments are refused.
func ValidateHost(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Hostname() == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return ErrCredentialHost
	}
	switch strings.ToLower(parsed.Scheme) {
	case "https":
		return nil
	case "http":
		if netpolicy.IsLoopbackHost(parsed.Hostname()) {
			return nil
		}
	}
	return ErrCredentialHost
}

// Client is the minute-chart REST reader. It allows exactly two GET paths and
// two transactions, the quotation checks quoted by Endpoint, and refuses every
// other request before it reaches the network.
type Client struct {
	Token     TokenSource
	AppKey    string
	AppSecret string
	HTTP      *http.Client
	Host      string
	Now       func() time.Time
	// Reject is called with the token the broker rejected before the one
	// retry. A gatewayd-fetch source uses it to ask for a reissue; a Redis
	// source leaves it nil because every Token call reads a fresh value.
	Reject func(context.Context, string) error
}

// Fetch performs one minute-chart GET.
func (c *Client) Fetch(ctx context.Context, target Target) FetchResult {
	if err := target.validate(); err != nil {
		return FetchResult{Err: ErrMalformedResponse}
	}
	if err := ValidateHost(c.host()); err != nil {
		return FetchResult{Err: err}
	}
	for attempt := range 2 {
		result, presented := c.fetchOnce(ctx, target)
		if attempt == 0 && errors.Is(result.Err, errTokenExpired) {
			if c.Reject != nil {
				if err := c.Reject(ctx, presented); err != nil {
					return FetchResult{Err: ErrTokenUnavailable}
				}
			}
			continue
		}
		return result
	}
	return FetchResult{Err: ErrTokenUnavailable}
}

var errTokenExpired = errors.New("marketminute: cached token expired")

func (c *Client) host() string {
	if c.Host == "" {
		return DefaultHost
	}
	return c.Host
}

func (c *Client) fetchOnce(ctx context.Context, target Target) (FetchResult, string) {
	host := c.host()
	query, path, tr := minuteRequest(target, c.now())
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(host, "/")+path+"?"+query.Encode(), nil)
	if err != nil {
		return FetchResult{Err: ErrMalformedResponse}, ""
	}
	if c.Token == nil {
		return FetchResult{Err: ErrTokenUnavailable}, ""
	}
	token, err := c.Token(ctx)
	if err != nil || token == "" || strings.ContainsAny(token, "\r\n") {
		return FetchResult{Err: ErrTokenUnavailable}, ""
	}
	if c.AppKey == "" || c.AppSecret == "" || strings.ContainsAny(c.AppKey+c.AppSecret, "\r\n") {
		return FetchResult{Err: ErrTokenUnavailable}, ""
	}
	request.Header.Set("authorization", "Bearer "+token)
	request.Header.Set("appkey", c.AppKey)
	request.Header.Set("appsecret", c.AppSecret)
	request.Header.Set("tr_id", tr)
	request.Header.Set("custtype", "P")

	client := http.Client{Timeout: 10 * time.Second}
	if c.HTTP != nil {
		client = *c.HTTP
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if client.Timeout <= 0 {
		client.Timeout = 10 * time.Second
	}
	response, err := client.Do(request)
	if err != nil {
		return FetchResult{Err: classedTransportError(ctx, err)}, token
	}
	defer response.Body.Close()
	retryAfter := parseRetryAfter(response.Header.Get("Retry-After"))
	if response.StatusCode != http.StatusOK {
		result := FetchResult{StatusCode: response.StatusCode, RetryAfter: retryAfter}
		switch {
		case response.StatusCode == http.StatusTooManyRequests:
			result.RateLimited = true
			result.Err = ErrRateLimited
		case response.StatusCode >= 500:
			result.Err = ErrResponseRefused
		default:
			result.Err = ErrResponseRefused
		}
		return result, token
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return FetchResult{StatusCode: response.StatusCode, Err: ErrMalformedResponse}, token
	}
	envelope := struct {
		RTCD    string           `json:"rt_cd"`
		MsgCD   string           `json:"msg_cd"`
		Output  []map[string]any `json:"output"`
		Output2 []map[string]any `json:"output2"`
	}{}
	if json.Unmarshal(body, &envelope) != nil {
		return FetchResult{StatusCode: response.StatusCode, Err: ErrMalformedResponse}, token
	}
	if envelope.RTCD != "0" {
		result := FetchResult{StatusCode: response.StatusCode}
		switch {
		case envelope.MsgCD == rateLimitMsgCode:
			result.RateLimited = true
			result.Err = ErrRateLimited
		case tokenExpiredCodes[envelope.MsgCD]:
			result.Err = errTokenExpired
		default:
			result.Err = ErrResponseRefused
		}
		return result, token
	}
	rows := envelope.Output2
	if len(rows) == 0 {
		rows = envelope.Output
	}
	wanted := len(rows)
	result := FetchResult{StatusCode: response.StatusCode, Rows: make([]FrameRow, 0, wanted)}
	for _, row := range rows {
		frame, ok := frameFromRow(target, row)
		if !ok {
			result.Err = ErrMalformedResponse
			return result, token
		}
		result.Rows = append(result.Rows, frame)
	}
	return result, token
}

func (c *Client) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func classedTransportError(ctx context.Context, _ error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return ErrResponseRefused
}

func parseRetryAfter(raw string) time.Duration {
	if raw == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(raw); err == nil {
		if wait := time.Until(when); wait > 0 {
			return wait
		}
	}
	return 0
}

// minuteRequest builds the GET query and transaction for one target. The
// parameters mirror auto_trader inquire_minute_chart (domestic) and
// inquire_overseas_minute_chart (overseas).
func minuteRequest(target Target, now time.Time) (url.Values, string, string) {
	query := url.Values{}
	switch target.Venue {
	case "KRX", "NTX":
		market := "J"
		if target.Venue == "NTX" {
			market = "NX"
		}
		local := now.In(kst.Zone)
		date := local.Format("20060102")
		symbol := target.Symbol
		if len(symbol) < 6 {
			symbol = strings.Repeat("0", 6-len(symbol)) + symbol
		}
		query.Set("FID_COND_MRKT_DIV_CODE", market)
		query.Set("FID_INPUT_ISCD", symbol)
		query.Set("FID_INPUT_HOUR_1", local.Format("150405"))
		query.Set("FID_INPUT_DATE_1", date)
		query.Set("FID_INPUT_DATE_2", date)
		query.Set("FID_INPUT_TIME_1", "01")
		query.Set("FID_INPUT_TIME_2", "01")
		query.Set("FID_PW_DATA_INCU_YN", "N")
		query.Set("FID_ETC_CLS_CODE", "")
		return query, DomesticPath, DomesticTR
	default:
		exchange := map[string]string{"NASD": "NAS", "NYSE": "NYS", "AMEX": "AMS"}[target.Venue]
		query.Set("AUTH", "")
		query.Set("EXCD", exchange)
		query.Set("SYMB", strings.ReplaceAll(target.Symbol, ".", "/"))
		query.Set("NMIN", "1")
		query.Set("PINC", "1")
		query.Set("NEXT", "")
		query.Set("NREC", "120")
		query.Set("FILL", "")
		query.Set("KEYB", "")
		return query, OverseasPath, OverseasTR
	}
}

// frameFromRow converts one raw page row into the common frame shape. An
// unsupported venue or an unreadable label is malformed.
func frameFromRow(target Target, row map[string]any) (FrameRow, bool) {
	switch target.Venue {
	case "KRX", "NTX":
		label := joinLabel(text(row["stck_bsop_date"]), text(row["stck_cntg_hour"]))
		return FrameRow{
			DateTime: label,
			Open:     number(row["stck_oprc"]),
			High:     number(row["stck_hgpr"]),
			Low:      number(row["stck_lwpr"]),
			Close:    number(row["stck_prpr"]),
			Volume:   number(row["cntg_vol"]),
			Value:    number(row["acml_tr_pbmn"]),
		}, label != ""
	case "NASD", "NYSE", "AMEX":
		label := joinLabel(text(row["xymd"]), text(row["xhms"]))
		return FrameRow{
			DateTime: label,
			Open:     number(row["open"]),
			High:     number(row["high"]),
			Low:      number(row["low"]),
			Close:    number(row["last"]),
			Volume:   number(row["evol"]),
			Value:    number(row["eamt"]),
		}, label != ""
	default:
		return FrameRow{}, false
	}
}

func text(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return ""
	}
}

func number(value any) *string {
	raw := text(value)
	if raw == "" {
		return nil
	}
	return &raw
}

// joinLabel builds the frame label "YYYY-MM-DD HH:MM" from the two KIS fields.
// A short overseas time (HHMMSS) is zero padded; seconds are kept so the parser
// drops them like Python's minute truncation.
func joinLabel(date, clock string) string {
	if len(date) != 8 || len(clock) < 4 {
		return ""
	}
	for index := range date {
		if date[index] < '0' || date[index] > '9' {
			return ""
		}
	}
	hour, minute := clock[:2], clock[2:4]
	if !digits(hour) || !digits(minute) {
		return ""
	}
	return fmt.Sprintf("%s-%s-%s %s:%s", date[:4], date[4:6], date[6:8], hour, minute)
}

func digits(raw string) bool {
	for index := range raw {
		if raw[index] < '0' || raw[index] > '9' {
			return false
		}
	}
	return raw != ""
}
