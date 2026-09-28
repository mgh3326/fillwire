package alert

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/fillwire/internal/reader"
	"github.com/redis/go-redis/v9"
)

func TestTelegramSendSuppressionAndFailure(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "POST" || r.URL.Path != "/botfake-token/sendMessage" {
			t.Errorf("Telegram path = %s", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.Form.Get("chat_id") != "fake-chat" || r.Form.Get("text") != "synthetic failure" {
			t.Error("Telegram form changed")
		}
		if calls.Load() == 3 {
			w.WriteHeader(500)
			return
		}
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer server.Close()
	var logs strings.Builder
	a := &Telegram{token: "fake-token", chat: "fake-chat", endpoint: server.URL + "/botfake-token/sendMessage", client: server.Client(), logger: slog.New(slog.NewTextHandler(&logs, nil)), interval: 10 * time.Minute, now: time.Now, last: map[string]time.Time{}}
	a.Alert(context.Background(), "configuration", "synthetic failure")
	a.Alert(context.Background(), "configuration", "synthetic failure")
	a.Alert(context.Background(), "transient", "synthetic failure")
	if calls.Load() != 2 {
		t.Fatalf("rate suppression calls = %d, want 2", calls.Load())
	}
	a.Alert(context.Background(), "session", "synthetic failure")
	if calls.Load() != 3 {
		t.Fatalf("failed alert calls = %d, want 3", calls.Load())
	}
	if !strings.Contains(logs.String(), "Telegram alert delivery failed") {
		t.Fatal("alert failure was not logged")
	}
	if strings.Contains(logs.String(), "fake-token") || strings.Contains(logs.String(), "fake-chat") {
		t.Fatal("alert log leaked credential")
	}
}
func TestMissingTelegramEnvironmentDisablesWithOneLoudLog(t *testing.T) {
	t.Setenv("FILLWIRE_ALERT_TELEGRAM_BOT_TOKEN", "")
	t.Setenv("FILLWIRE_ALERT_TELEGRAM_CHAT_ID", "")
	var logs strings.Builder
	a := FromEnv(slog.New(slog.NewTextHandler(&logs, nil)), DefaultRateLimit)
	a.Alert(context.Background(), "configuration", "synthetic failure")
	if strings.Count(logs.String(), "Telegram alerting disabled") != 1 {
		t.Fatalf("disabled log count = %d", strings.Count(logs.String(), "Telegram alerting disabled"))
	}
}

type fakeApprovalHTTP struct {
	url    string
	client *http.Client
}

func (i fakeApprovalHTTP) Issue(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, i.url+"/oauth2/Approval", strings.NewReader(`{"appkey":"fixture","secretkey":"fixture"}`))
	if err != nil {
		return "", err
	}
	response, err := i.client.Do(req)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return "", reader.ErrApprovalUnavailable
	}
	return "new-key", nil
}
func TestRecoverableApprovalFailureUsesTelegramRateLimit(t *testing.T) {
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer client.Close()
	var approvalCalls, telegramCalls atomic.Int32
	approvalServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { approvalCalls.Add(1); w.WriteHeader(503) }))
	defer approvalServer.Close()
	telegramServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if telegramCalls.Add(1) == 2 {
			w.WriteHeader(500)
			return
		}
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer telegramServer.Close()
	fakeNow := time.Now()
	var logs strings.Builder
	a := &Telegram{token: "fixture-bot", chat: "fixture-chat", endpoint: telegramServer.URL + "/botfixture-bot/sendMessage", client: telegramServer.Client(), logger: slog.New(slog.NewTextHandler(&logs, nil)), interval: 10 * time.Minute, now: func() time.Time { return fakeNow }, last: map[string]time.Time{}}
	p, err := reader.NewApprovalProviderConfig(client, fakeApprovalHTTP{approvalServer.URL, approvalServer.Client()}, "", "live", reader.DefaultRefreshMargin, nil)
	if err != nil {
		t.Fatal(err)
	}
	p.SetTransientAlert(func() {
		a.Alert(context.Background(), "transient", "fillwire approval refresh failed; retrying while cached key remains valid")
	})
	mini.Set("kis:websocket:approval_key", "still-live")
	mini.SetTTL("kis:websocket:approval_key", 30*time.Minute)
	for i := 0; i < 3; i++ {
		key, err := p.ApprovalKey(context.Background())
		if err != nil || key != "still-live" {
			t.Fatalf("recoverable approval %d = %q, %v", i, key, err)
		}
	}
	if telegramCalls.Load() != 1 {
		t.Fatalf("Telegram sends inside one interval = %d, want 1", telegramCalls.Load())
	}
	fakeNow = fakeNow.Add(10 * time.Minute)
	key, err := p.ApprovalKey(context.Background())
	if err != nil || key != "still-live" {
		t.Fatalf("failed Telegram delivery changed key result = %q, %v", key, err)
	}
	if telegramCalls.Load() != 2 || approvalCalls.Load() != 4 {
		t.Fatalf("Telegram sends=%d REST attempts=%d, want 2 and 4", telegramCalls.Load(), approvalCalls.Load())
	}
	if !strings.Contains(logs.String(), "Telegram alert delivery failed") || strings.Contains(logs.String(), "fixture-bot") || strings.Contains(logs.String(), "still-live") {
		t.Fatal("Telegram failure log missing or leaked values")
	}
}
