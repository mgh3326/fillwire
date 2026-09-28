// Package alert sends bounded, rate-limited operational Telegram alerts.
package alert

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const DefaultRateLimit = 10 * time.Minute
const sendTimeout = 3 * time.Second

// Alerter reports one failure class without exposing credentials or approval keys.
type Alerter interface {
	Alert(context.Context, string, string)
}

type Telegram struct {
	token, chat, endpoint string
	client                *http.Client
	logger                *slog.Logger
	interval              time.Duration
	now                   func() time.Time
	mu                    sync.Mutex
	last                  map[string]time.Time
}

func FromEnv(logger *slog.Logger, interval time.Duration) *Telegram {
	if interval <= 0 {
		interval = DefaultRateLimit
	}
	a := &Telegram{token: os.Getenv("FILLWIRE_ALERT_TELEGRAM_BOT_TOKEN"), chat: os.Getenv("FILLWIRE_ALERT_TELEGRAM_CHAT_ID"), client: &http.Client{Timeout: sendTimeout}, logger: logger, interval: interval, now: time.Now, last: map[string]time.Time{}}
	if a.token == "" || a.chat == "" {
		logger.Error("Telegram alerting disabled: FILLWIRE_ALERT_TELEGRAM_BOT_TOKEN or FILLWIRE_ALERT_TELEGRAM_CHAT_ID is unset")
	} else {
		a.endpoint = "https://api.telegram.org/bot" + a.token + "/sendMessage"
	}
	return a
}

func (a *Telegram) SetRateLimit(interval time.Duration) {
	if interval <= 0 {
		return
	}
	a.mu.Lock()
	a.interval = interval
	a.mu.Unlock()
}

// Alert is best effort. A failed send never changes process exit behavior.
func (a *Telegram) Alert(ctx context.Context, class, message string) {
	if a == nil || a.endpoint == "" {
		return
	}
	a.mu.Lock()
	if last, ok := a.last[class]; ok && a.now().Sub(last) < a.interval {
		a.mu.Unlock()
		return
	}
	a.last[class] = a.now()
	a.mu.Unlock()
	requestCtx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	body := url.Values{"chat_id": {a.chat}, "text": {message}}
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, a.endpoint, strings.NewReader(body.Encode()))
	if err == nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		var response *http.Response
		response, err = a.client.Do(req)
		if err == nil {
			defer response.Body.Close()
			if response.StatusCode < 200 || response.StatusCode >= 300 {
				err = errors.New("non-success status")
			} else {
				var result struct {
					OK bool `json:"ok"`
				}
				if json.NewDecoder(response.Body).Decode(&result) != nil || !result.OK {
					err = errors.New("unsuccessful Telegram response")
				}
			}
		}
	}
	if err != nil && a.logger != nil {
		a.logger.Error("Telegram alert delivery failed", "class", class)
	}
}
