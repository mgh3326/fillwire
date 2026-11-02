// Package kisread exposes only the quote and mock order-history GET transactions.
package kisread

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/go-kis/kis"
)

// Client allows exactly two GET paths. go-kis's pinned Read allowlist lacks
// asking-price quotes and the current mock order-history transaction ID. This
// adapter reuses cached tokens and has no token issuance or mutation path.
type Client struct {
	mu                sync.Mutex
	next              time.Time
	Mock              bool
	AppKey, AppSecret string
	Token             func() string
	HTTP              *http.Client
}

func (c *Client) Read(ctx context.Context, path, tr string, query map[string]string, out any) error {
	quotation := path == "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn" && tr == "FHKST01010200"
	history := c.Mock && path == "/uapi/domestic-stock/v1/trading/inquire-daily-ccld" && tr == "VTTC0081R"
	if !quotation && !history {
		return errors.New("kisread: read endpoint refused")
	}
	// Reserve at most one request per second from this mock read client,
	// leaving room for the separate producer's 2s quotation poll.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Mock {
		if delay := time.Until(c.next); delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}
		c.next = time.Now().Add(time.Second)
	}
	host := kis.HostLive
	if c.Mock {
		host = kis.HostVTS
	}
	q := url.Values{}
	for k, v := range query {
		q.Set(k, v)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, host+path+"?"+q.Encode(), nil)
	if err != nil {
		return errors.New("kisread: invalid read request")
	}
	if c.Token == nil {
		return errors.New("kisread: cached token required")
	}
	token := c.Token()
	if token == "" || c.AppKey == "" || c.AppSecret == "" || strings.ContainsAny(token+c.AppKey+c.AppSecret, "\r\n") {
		return errors.New("kisread: credentials unavailable")
	}
	req.Header.Set("authorization", "Bearer "+token)
	req.Header.Set("appkey", c.AppKey)
	req.Header.Set("appsecret", c.AppSecret)
	req.Header.Set("tr_id", tr)
	req.Header.Set("custtype", "P")
	hc := http.Client{Timeout: time.Second}
	if c.HTTP != nil {
		hc = *c.HTTP
	}
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if hc.Timeout <= 0 {
		hc.Timeout = time.Second
	}
	resp, err := hc.Do(req)
	if err != nil {
		return errors.New("kisread: read transport failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return errors.New("kisread: read HTTP failure")
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return errors.New("kisread: read response unreadable")
	}
	var envelope kis.Envelope
	if json.Unmarshal(data, &envelope) != nil || envelope.RTCD != "0" {
		return errors.New("kisread: read refused")
	}
	if json.Unmarshal(data, out) != nil {
		return errors.New("kisread: read response invalid")
	}
	return nil
}
