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
