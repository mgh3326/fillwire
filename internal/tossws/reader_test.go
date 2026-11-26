package tossws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/websocket"
	"github.com/mgh3326/fillwire/internal/recording"
	"github.com/mgh3326/fillwire/internal/tokenfetch"
)

const fakeToken = "synthetic-toss-ws-token"

type testTokens struct{}

func (testTokens) Current(context.Context) (tokenfetch.Token, bool, error) {
	return tokenfetch.NewToken(fakeToken, time.Now().Add(time.Hour), "test"), false, nil
}

func (testTokens) Reissue(context.Context, string) (tokenfetch.Token, error) {
	return tokenfetch.Token{}, errors.New("unexpected token reissue")
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/doc-" + name)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestFakeServerRecordsDocumentedOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	order := fixture(t, "order.json")
	// Send the documented event with an extra field and precede it with a
	// malformed event (numeric quantity). Both traverse the actual socket.
	order = bytes.Replace(order, []byte(`"event": "FILL"`), []byte(`"future": {"value":9007199254740993}, "event":"FILL"`), 1)
	bad := bytes.Replace(order, []byte(`"quantity": "10"`), []byte(`"quantity": 10`), 1)
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		if req.Header.Get("Authorization") != "Bearer "+fakeToken {
			t.Error("missing expected Authorization")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		ws, err := websocket.Accept(w, req, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer ws.CloseNow()
		kind, raw, err := ws.Read(req.Context())
		var declaration []struct {
			ID    string
			Type  string
			Codes []string
		}
		if err != nil || kind != websocket.MessageText || json.Unmarshal(raw, &declaration) != nil || len(declaration) != 2 ||
			declaration[0].ID == "" || declaration[1].Type != "personal:order" ||
			len(declaration[1].Codes) != 1 || declaration[1].Codes[0] != "3" {
			t.Errorf("invalid declaration: %s, %v", raw, err)
			return
		}
		ack := fmt.Appendf(nil, `{"type":"subscriptions","id":%q,"subscribed":["personal:order:3"],"rejected":[]}`, declaration[0].ID)
		leaked := bytes.Replace(order, []byte(`"event":"FILL"`), []byte(`"event":"FILL", "echo":"synthetic-toss-ws-tok\u0065n"`), 1)
		for _, frame := range [][]byte{ack, bad, leaked, order} {
			if err := ws.Write(req.Context(), websocket.MessageText, frame); err != nil {
				t.Error(err)
				return
			}
		}
		_, _, _ = ws.Read(req.Context())
	}))
	defer server.Close()

	root := t.TempDir()
	sink, err := recording.NewSink(root, "toss-order")
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	reader, err := New(Config{AccountSeq: "3", Tokens: testTokens{}, Logger: slog.New(slog.NewTextHandler(&logs, nil)),
		Dial: func(ctx context.Context, endpoint string, headers http.Header) (Socket, int, error) {
			if endpoint != Endpoint {
				t.Errorf("unexpected endpoint %q", endpoint)
			}
			ws, _, err := websocket.Dial(ctx, "wss"+strings.TrimPrefix(server.URL, "https"), &websocket.DialOptions{HTTPClient: server.Client(), HTTPHeader: headers})
			return ws, 0, err
		},
		Append: func(r Record) error {
			err := sink.Append("toss-order", "3", r.ReceivedAt, r)
			if r.Kind == "order" {
				cancel()
			}
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	if err := reader.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(root, "toss-order", "*", "3.jsonl"))
	if err != nil || len(paths) != 1 || calls.Load() != 1 {
		t.Fatalf("paths %v, connections %d, error %v", paths, calls.Load(), err)
	}
	file, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(file), []byte("\n"))
	if len(lines) != 2 {
		t.Fatalf("expected 2 records, got %d", len(lines))
	}
	var marker, event Record
	if json.Unmarshal(lines[0], &marker) != nil || json.Unmarshal(lines[1], &event) != nil {
		t.Fatal("invalid JSONL")
	}
	if marker.Kind != "resync_needed" || marker.SessionID == "" || event.SessionID != marker.SessionID ||
		event.Kind != "order" || event.Topic != "personal:order:3" ||
		event.ReceivedAt.Before(before) || event.ReceivedAt.After(time.Now()) {
		t.Fatalf("unexpected records: %+v, %+v", marker, event)
	}
	decoded := parseFrame(event.Data)
	orderFields, _ := decoded["order"].(object)
	if decoded["event"] != "FILL" || decoded["accountSeq"] != "3" || orderFields["symbol"] != "AAPL" ||
		orderFields["quantity"] != "10" || !bytes.Contains(event.Raw, []byte("9007199254740993")) {
		t.Fatalf("unexpected data: %s", event.Data)
	}
	if reader.malformed.Load() != 2 || strings.Contains(logs.String(), fakeToken) || bytes.Contains(file, []byte("synthetic-toss-ws")) {
		t.Fatal("malformed frames or credential handling incorrect")
	}
}

type fakeSocket struct {
	read  func(context.Context) ([]byte, error)
	write func([]byte) error
	close func()
}

func (s fakeSocket) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	raw, err := s.read(ctx)
	return websocket.MessageText, raw, err
}
func (s fakeSocket) Write(_ context.Context, kind websocket.MessageType, raw []byte) error {
	if kind != websocket.MessageText {
		return errors.New("expected text")
	}
	return s.write(raw)
}
func (s fakeSocket) CloseNow() error { s.close(); return nil }

func requestID(raw []byte) string {
	var declaration []map[string]string
	// Only the first element has a string for every value.
	var items []json.RawMessage
	_ = json.Unmarshal(raw, &items)
	if len(items) > 0 {
		declaration = make([]map[string]string, 1)
		_ = json.Unmarshal(items[0], &declaration[0])
		return declaration[0]["id"]
	}
	return ""
}

func TestRejectedOrInvalidAckStops(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
		want    error
	}{
		{"account-not-found", `{"type":"subscriptions","id":%q,"subscribed":[],"rejected":[{"target":"personal:order:3","code":"account-not-found","message":"` + fakeToken + `"}]}`, ErrAccountNotFound},
		{"wrong-topic", `{"type":"subscriptions","id":%q,"subscribed":["personal:order:4"],"rejected":[]}`, ErrAck},
		{"missing-rejected", `{"type":"subscriptions","id":%q,"subscribed":["personal:order:3"]}`, ErrAck},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var id string
			var records []Record
			var logs bytes.Buffer
			reader, err := New(Config{AccountSeq: "3", Tokens: testTokens{},
				Logger: slog.New(slog.NewTextHandler(&logs, nil)),
				Append: func(r Record) error { records = append(records, r); return nil },
				Dial: func(context.Context, string, http.Header) (Socket, int, error) {
					return fakeSocket{
						read:  func(context.Context) ([]byte, error) { return fmt.Appendf(nil, tc.payload, id), nil },
						write: func(raw []byte) error { id = requestID(raw); return nil },
						close: func() {},
					}, 0, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = reader.Run(t.Context()); !errors.Is(err, tc.want) || strings.Contains(err.Error(), fakeToken) {
				t.Fatalf("expected sanitized %v, got %v", tc.want, err)
			}
			raw, _ := json.Marshal(records)
			if len(records) != 1 || strings.Contains(logs.String()+string(raw), fakeToken) {
				t.Fatal("unexpected records or credential in error-path output")
			}
		})
	}
}

func TestPingAndReconnectWithFakeClock(t *testing.T) {
	ping := fixture(t, "ping.txt")
	pong := fixture(t, "pong.json")
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var markers []Record
		var logs bytes.Buffer
		var dials int
		closed := true
		reader, err := New(Config{AccountSeq: "3", Tokens: testTokens{}, Clock: wallClock{},
			Logger: slog.New(slog.NewTextHandler(&logs, nil)),
			Append: func(r Record) error { markers = append(markers, r); return nil },
			Dial: func(context.Context, string, http.Header) (Socket, int, error) {
				if !closed {
					t.Error("next connection opened before prior socket closed")
				}
				closed = false
				dials++
				session := dials
				frames := make(chan []byte, 2)
				declared := time.Now()
				return fakeSocket{
					write: func(raw []byte) error {
						if bytes.Equal(raw, ping) {
							if time.Since(declared) != 60*time.Second {
								t.Errorf("PING delay was %v", time.Since(declared))
							}
							frames <- pong
							close(frames)
						} else {
							frames <- fmt.Appendf(nil, `{"type":"subscriptions","id":%q,"subscribed":["personal:order:3"],"rejected":[]}`, requestID(raw))
						}
						return nil
					},
					read: func(ctx context.Context) ([]byte, error) {
						select {
						case frame, ok := <-frames:
							if !ok {
								return nil, errors.New("simulated close: " + fakeToken)
							}
							if session == 2 {
								cancel()
							}
							return frame, nil
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					},
					close: func() { closed = true },
				}, 0, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := reader.Run(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("run: %v", err)
		}
		if len(markers) != 2 || markers[0].SessionID == markers[1].SessionID ||
			markers[1].ReceivedAt.Sub(markers[0].ReceivedAt) < 61*time.Second || !closed || dials != 2 ||
			markers[0].Kind != "resync_needed" || markers[1].Kind != "resync_needed" {
			t.Fatalf("unexpected session lifecycle: %+v, dials %d, closed %v", markers, dials, closed)
		}
		if !strings.Contains(logs.String(), "possible connection eviction") || strings.Contains(logs.String(), fakeToken) {
			t.Fatal("expected sanitized possible-eviction log")
		}
	})
}
