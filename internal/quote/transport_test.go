package quote

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func wsURL(server *httptest.Server) string { return "ws" + strings.TrimPrefix(server.URL, "http") }

func TestDialerRefusesAnyEndpointButToss(t *testing.T) {
	for _, endpoint := range []string{"wss://openapi-ws.tossinvest.com/ws/v2", "wss://example.invalid/ws/v1", "ws://openapi-ws.tossinvest.com/ws/v1", ""} {
		if _, err := NewDialer().Dial(context.Background(), endpoint, "t"); !errors.Is(err, errEndpoint) {
			t.Fatalf("Dial(%q) = %v, want errEndpoint", endpoint, err)
		}
	}
}

func TestDialURLSendsBearerAndRoundTripsFrames(t *testing.T) {
	var header atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header.Store(r.Header.Get("Authorization"))
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		kind, data, err := conn.Read(r.Context())
		if err != nil || kind != websocket.MessageText || string(data) != "PING" {
			return
		}
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"pong"}`))
		_, _, _ = conn.Read(r.Context())
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	transport, err := dialURL(ctx, wsURL(server), "fixture-token")
	if err != nil {
		t.Fatal(err)
	}
	if got := header.Load(); got != "Bearer fixture-token" {
		t.Fatalf("Authorization = %v", got)
	}
	if err := transport.Write(ctx, []byte("PING")); err != nil {
		t.Fatal(err)
	}
	data, err := transport.Read(ctx)
	if err != nil || string(data) != `{"type":"pong"}` {
		t.Fatalf("read = %q, %v", data, err)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDialURLMapsHandshakeRefusals(t *testing.T) {
	for status, want := range map[int]error{http.StatusUnauthorized: ErrUnauthorized, http.StatusForbidden: ErrForbidden} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
		_, err := dialURL(context.Background(), wsURL(server), "secret-token")
		server.Close()
		if !errors.Is(err, want) || strings.Contains(err.Error(), "secret-token") {
			t.Fatalf("HTTP %d = %v, want %v without the token", status, err, want)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer server.Close()
	if _, err := dialURL(context.Background(), wsURL(server), "secret-token"); err == nil || strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("HTTP 503 = %v", err)
	}
}

func TestDialURLNeverFollowsARedirect(t *testing.T) {
	var hit atomic.Bool
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit.Store(true)
		conn, err := websocket.Accept(w, r, nil)
		if err == nil {
			conn.CloseNow()
		}
	}))
	defer other.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusFound)
	}))
	defer redirect.Close()
	if _, err := dialURL(context.Background(), wsURL(redirect), "secret-token"); err == nil {
		t.Fatal("redirected handshake succeeded")
	}
	if hit.Load() {
		t.Fatal("the redirect target received the handshake (and the token)")
	}
}
