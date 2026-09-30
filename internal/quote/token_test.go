package quote

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

const testTokenKey = "toss:oauth:0123456789abcdef:access_token"

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time                         { return c.now }
func (c fixedClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type commandHook struct {
	mu    sync.Mutex
	names []string
}

func (h *commandHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *commandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.mu.Lock()
		h.names = append(h.names, strings.ToLower(cmd.Name()))
		h.mu.Unlock()
		return next(ctx, cmd)
	}
}
func (h *commandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.mu.Lock()
		for _, cmd := range cmds {
			h.names = append(h.names, strings.ToLower(cmd.Name()))
		}
		h.mu.Unlock()
		return next(ctx, cmds)
	}
}

func TestCachedTokenReadsOnlyWithGET(t *testing.T) {
	mini := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mini.Addr()})
	defer client.Close()
	hook := &commandHook{}
	client.AddHook(hook)
	now := time.Date(2026, 9, 30, 13, 0, 0, 0, KST)
	source, err := NewCachedToken(client, testTokenKey, fixedClock{now: now})
	if err != nil {
		t.Fatal(err)
	}
	payload := func(token string, expiresAt time.Time) string {
		return fmt.Sprintf(`{"access_token": %q, "expires_at": %d.25}`, token, expiresAt.Unix())
	}
	for _, test := range []struct {
		name, value string
		want        string
		wantErr     error
	}{
		{"fresh", payload("live-token", now.Add(time.Hour)), "live-token", nil},
		{"inside expiry buffer", payload("old-token", now.Add(119*time.Second)), "", ErrTokenUnavailable},
		{"expired", payload("old-token", now.Add(-time.Minute)), "", ErrTokenUnavailable},
		{"no expiry", `{"access_token":"x"}`, "", ErrTokenUnavailable},
		{"empty token", payload("", now.Add(time.Hour)), "", ErrTokenUnavailable},
		{"not json", `x`, "", ErrTokenUnavailable},
		{"expires_at as string", `{"access_token":"x","expires_at":"9999999999"}`, "", ErrTokenUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			mini.Set(testTokenKey, test.value)
			token, err := source.Token(context.Background())
			if token != test.want || !errors.Is(err, test.wantErr) {
				t.Fatalf("Token = %q, %v; want %q, %v", token, err, test.want, test.wantErr)
			}
		})
	}
	mini.Del(testTokenKey)
	if _, err := source.Token(context.Background()); !errors.Is(err, ErrTokenUnavailable) {
		t.Fatalf("missing key = %v", err)
	}
	mini.Set(testTokenKey, payload("secret-value", now.Add(time.Hour)))
	mini.SetError("ERR synthetic")
	if _, err := source.Token(context.Background()); err == nil || strings.Contains(err.Error(), "secret-value") {
		t.Fatalf("Redis error = %v", err)
	}
	mini.SetError("")
	hook.mu.Lock()
	defer hook.mu.Unlock()
	for _, name := range hook.names {
		if name == "hello" || name == "client" {
			continue // connection handshake
		}
		if name != "get" {
			t.Fatalf("token source issued %s; only GET is allowed", name)
		}
	}
	if value, _ := mini.Get(testTokenKey); !strings.Contains(value, "secret-value") {
		t.Fatal("token cache entry changed")
	}
}

func TestValidTokenKey(t *testing.T) {
	for key, want := range map[string]bool{
		testTokenKey:                                      true,
		"toss:oauth:0123456789abcdef:lock":                false,
		"toss:oauth:0123456789ABCDEF:access_token":        false,
		"toss:oauth:0123456789abcde:access_token":         false,
		"toss:ratelimit:0123456789abcdef:MARKET_DATA":     false,
		"kis:websocket:approval_key":                      false,
		" toss:oauth:0123456789abcdef:access_token":       false,
		"toss:oauth:0123456789abcdef:access_token\n":      false,
		"toss:oauth:0123456789abcdef0:access_token":       false,
		"prefix:toss:oauth:0123456789abcdef:access_token": false,
	} {
		if got := ValidTokenKey(key); got != want {
			t.Fatalf("ValidTokenKey(%q) = %t, want %t", key, got, want)
		}
	}
	if _, err := NewCachedToken(nil, testTokenKey, nil); err == nil {
		t.Fatal("nil client accepted")
	}
}
