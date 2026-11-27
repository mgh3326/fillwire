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
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/websocket"
	"github.com/mgh3326/fillwire/internal/quote"
	"github.com/mgh3326/fillwire/internal/recording"
	"github.com/mgh3326/fillwire/internal/tokenfetch"
)

const fakeToken = "synthetic-toss-ws-token"

type testTokens struct{}

func (testTokens) Current(context.Context) (tokenfetch.Token, bool, error) {
	return tokenfetch.NewToken(fakeToken, time.Now().Add(time.Hour), "test"), false, nil
}

func (testTokens) Refresh(context.Context) (tokenfetch.Token, error) {
	return tokenfetch.Token{}, errors.New("unexpected token refresh")
}

func (testTokens) Reissue(context.Context, string) (tokenfetch.Token, error) {
	return tokenfetch.Token{}, errors.New("unexpected token replacement")
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

func TestDeclarationIncludesOrdersAndConfiguredQuoteChannels(t *testing.T) {
	declaration, topics, err := buildDeclaration("request-1", "3", []quote.Symbol{
		{Market: quote.MarketKR, Code: "005930"},
		{Market: quote.MarketUS, Code: "AAPL"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := `[{"id":"request-1"},{"codes":["3"],"type":"personal:order"},{"codes":["005930"],"type":"trade:kr"},{"codes":["005930"],"type":"orderbook:kr"},{"codes":["AAPL"],"type":"trade:us"},{"codes":["AAPL"],"type":"orderbook:us"}]`
	if string(declaration) != want {
		t.Fatalf("declaration = %s, want %s", declaration, want)
	}
	if strings.Join(topics, ",") != "personal:order:3,trade:kr:005930,orderbook:kr:005930,trade:us:AAPL,orderbook:us:AAPL" {
		t.Fatalf("topics = %v", topics)
	}
}

func TestDeclarationRejectsMoreThan100Subscriptions(t *testing.T) {
	symbols := make([]quote.Symbol, 50)
	for index := range symbols {
		symbols[index] = quote.Symbol{Market: quote.MarketKR, Code: fmt.Sprintf("%06d", index+1)}
	}
	if _, _, err := buildDeclaration("request-1", "3", symbols); err == nil {
		t.Fatal("declaration with 101 subscriptions was accepted")
	}
}

func TestReaderRecordsDocumentedQuoteFramesAndCountsMalformed(t *testing.T) {
	trade := fixture(t, "trade-us-asyncapi-example.json")
	book := fixture(t, "orderbook-kr-asyncapi-example.json")
	var cancel context.CancelFunc
	ctx, stop := context.WithCancel(context.Background())
	cancel = stop
	frames := make(chan []byte, 6)
	var declaration []byte
	var records []Record
	var recordsMu sync.Mutex
	var reader *Reader
	before := time.Now()
	reader, err := New(Config{
		AccountSeq: "3", Tokens: testTokens{},
		Symbols: []quote.Symbol{{Market: quote.MarketKR, Code: "005930"}, {Market: quote.MarketUS, Code: "AAPL"}},
		Append: func(record Record) error {
			recordsMu.Lock()
			records = append(records, record)
			if (record.Kind == "trade" || record.Kind == "orderbook") && !reader.Health() {
				t.Error("reader was unhealthy after subscription acknowledgement")
			}
			recordsMu.Unlock()
			if record.Kind == "trade" {
				cancel()
			}
			return nil
		},
		Dial: func(context.Context, string, http.Header) (Socket, int, error) {
			return fakeSocket{
				write: func(raw []byte) error {
					declaration = append([]byte(nil), raw...)
					_, topics, err := buildDeclaration(requestID(raw), "3", []quote.Symbol{{Market: quote.MarketKR, Code: "005930"}, {Market: quote.MarketUS, Code: "AAPL"}})
					if err != nil {
						return err
					}
					ack, _ := json.Marshal(map[string]any{"type": "subscriptions", "id": requestID(raw), "subscribed": topics, "rejected": []any{}})
					frames <- ack
					frames <- book
					frames <- []byte(`{"type":"message","topic":"orderbook:kr:005930","data":{}}`)
					frames <- []byte(`{"type":"message","topic":"orderbook:kr:005930","data":{"timestamp":"garbage","asks":[],"bids":[]}}`)
					frames <- []byte(`{"type":"message","topic":"trade:kr:005930","data":{"price":4}} trailing`)
					frames <- trade
					return nil
				},
				read: func(ctx context.Context) ([]byte, error) {
					select {
					case raw := <-frames:
						return raw, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				},
				close: func() {},
			}, 0, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() = %v", err)
	}
	if reader.Health() {
		t.Fatal("reader remained healthy after its connection closed")
	}
	receivedAfter := time.Now()
	if !bytes.Contains(declaration, []byte(`"type":"personal:order"`)) ||
		!bytes.Contains(declaration, []byte(`"type":"trade:us"`)) ||
		!bytes.Contains(declaration, []byte(`"type":"orderbook:kr"`)) {
		t.Fatalf("declaration missing a channel: %s", declaration)
	}
	recordsMu.Lock()
	defer recordsMu.Unlock()
	var quoteRecords []Record
	for _, record := range records {
		if record.Kind == "trade" || record.Kind == "orderbook" {
			quoteRecords = append(quoteRecords, record)
		}
	}
	if len(quoteRecords) != 2 || quoteRecords[0].Kind != "orderbook" || quoteRecords[1].Kind != "trade" ||
		!bytes.Equal(bytes.TrimSpace(quoteRecords[0].Raw), bytes.TrimSpace(book)) || !bytes.Equal(bytes.TrimSpace(quoteRecords[1].Raw), bytes.TrimSpace(trade)) {
		t.Fatalf("quote records = %+v", quoteRecords)
	}
	for _, record := range quoteRecords {
		if record.ReceivedAt.Before(before) || record.ReceivedAt.After(receivedAfter) || record.Topic == "" || record.SessionID == "" {
			t.Fatalf("quote record missing receipt metadata: %+v", record)
		}
	}
	if reader.malformed.Load() != 3 {
		t.Fatalf("malformed count = %d, want 3", reader.malformed.Load())
	}
}

func TestPartialQuoteRejectionKeepsPersonalOrderRecording(t *testing.T) {
	order := fixture(t, "order.json")
	trade := fixture(t, "trade-us-asyncapi-example.json")
	var id string
	var records []Record
	var recordsMu sync.Mutex
	var logs bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, err := New(Config{
		AccountSeq: "3", Tokens: testTokens{}, Symbols: []quote.Symbol{{Market: quote.MarketUS, Code: "AAPL"}},
		Logger: slog.New(slog.NewTextHandler(&logs, nil)),
		Append: func(record Record) error {
			recordsMu.Lock()
			records = append(records, record)
			recordsMu.Unlock()
			if record.Kind == "order" {
				cancel()
			}
			return nil
		},
		Dial: func(context.Context, string, http.Header) (Socket, int, error) {
			frames := make(chan []byte, 3)
			return fakeSocket{
				write: func(raw []byte) error {
					id = requestID(raw)
					ack := fmt.Appendf(nil, `{"type":"subscriptions","id":%q,"subscribed":["personal:order:3","trade:us:AAPL"],"rejected":[{"target":"orderbook:us:AAPL","code":"stock-not-found"}]}`, id)
					frames <- ack
					frames <- trade
					frames <- order
					return nil
				},
				read: func(ctx context.Context) ([]byte, error) {
					select {
					case frame := <-frames:
						return frame, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				},
				close: func() {},
			}, 0, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() = %v, want context cancellation after order record", err)
	}
	recordsMu.Lock()
	defer recordsMu.Unlock()
	var orderCount, quoteCount int
	for _, record := range records {
		switch record.Kind {
		case "order":
			orderCount++
		case "trade":
			quoteCount++
		}
	}
	if orderCount != 1 || quoteCount != 1 {
		t.Fatalf("records=%+v", records)
	}
	if !strings.Contains(logs.String(), "rejected_quotes=1") || !strings.Contains(logs.String(), "orderbook:us:AAPL") {
		t.Fatalf("rejected quote target was not counted and logged: %s", logs.String())
	}
}

func TestSlowQuoteRecorderDoesNotBlockOrderFrames(t *testing.T) {
	order := fixture(t, "order.json")
	trade := fixture(t, "trade-us-asyncapi-example.json")
	var id string
	quoteStarted := make(chan struct{})
	releaseQuote := make(chan struct{})
	orderRecorded := make(chan struct{})
	var releaseOnce sync.Once
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, err := New(Config{
		AccountSeq: "3", Tokens: testTokens{}, Symbols: []quote.Symbol{{Market: quote.MarketUS, Code: "AAPL"}},
		Append: func(record Record) error {
			if record.Kind == "trade" {
				close(quoteStarted)
				<-releaseQuote
			}
			if record.Kind == "order" {
				close(orderRecorded)
				releaseOnce.Do(func() { close(releaseQuote) })
				cancel()
			}
			return nil
		},
		Dial: func(context.Context, string, http.Header) (Socket, int, error) {
			frames := make(chan []byte, 3)
			return fakeSocket{
				write: func(raw []byte) error {
					id = requestID(raw)
					ack := fmt.Appendf(nil, `{"type":"subscriptions","id":%q,"subscribed":["personal:order:3","trade:us:AAPL","orderbook:us:AAPL"],"rejected":[]}`, id)
					frames <- ack
					frames <- trade
					frames <- order
					return nil
				},
				read: func(ctx context.Context) ([]byte, error) {
					select {
					case frame := <-frames:
						return frame, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				},
				close: func() {},
			}, 0, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- reader.Run(ctx) }()
	select {
	case <-quoteStarted:
	case <-time.After(time.Second):
		t.Fatal("quote recorder did not start")
	}
	select {
	case <-orderRecorded:
	case <-time.After(time.Second):
		cancel()
		releaseOnce.Do(func() { close(releaseQuote) })
		t.Fatal("slow quote recorder blocked the order frame")
	}
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() = %v, want context cancellation", err)
	}
}

func TestReaderRejectsMoreThan100Subscriptions(t *testing.T) {
	symbols := make([]quote.Symbol, 50)
	for i := range symbols {
		symbols[i] = quote.Symbol{Market: quote.MarketKR, Code: fmt.Sprintf("%06d", i)}
	}
	if _, err := New(Config{AccountSeq: "3", Tokens: testTokens{}, Append: func(Record) error { return nil }, Symbols: symbols}); err == nil {
		t.Fatal("configuration with 101 subscriptions was accepted")
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

func TestTransientReadFailuresReconnect(t *testing.T) {
	for _, scenario := range []string{"ack-timeout", "read-idle", "unknown-server-error"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
				defer cancel()
				var dials, closes int
				var id string
				var markers []Record
				var pings atomic.Int32
				reader, err := New(Config{AccountSeq: "3", Tokens: testTokens{},
					Append: func(r Record) error { markers = append(markers, r); return nil },
					Dial: func(context.Context, string, http.Header) (Socket, int, error) {
						dials++
						if dials == 2 {
							cancel()
						}
						acknowledged := false
						return fakeSocket{
							write: func(raw []byte) error {
								if string(raw) == "PING" {
									pings.Add(1)
								} else {
									id = requestID(raw)
								}
								return nil
							},
							read: func(ctx context.Context) ([]byte, error) {
								if dials == 1 && scenario != "ack-timeout" && !acknowledged {
									acknowledged = true
									return fmt.Appendf(nil, `{"type":"subscriptions","id":%q,"subscribed":["personal:order:3"],"rejected":[]}`, id), nil
								}
								if dials == 1 && scenario == "unknown-server-error" {
									return []byte(`{"type":"error","error":{"code":"unknown","message":"test"}}`), nil
								}
								<-ctx.Done()
								return nil, ctx.Err()
							},
							close: func() { closes++ },
						}, 0, nil
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := reader.Run(ctx); !errors.Is(err, context.Canceled) || dials != 2 || closes != 2 ||
					len(markers) != 2 || markers[1].Kind != "resync_needed" {
					t.Fatalf("run = %v, dials = %d, closes = %d, markers = %d", err, dials, closes, len(markers))
				}
				if scenario == "read-idle" && pings.Load() < 2 {
					t.Fatal("expected successful PING writes before idle reconnect")
				}
			})
		})
	}
}

type recoveryTokens struct {
	testTokens
	refreshes   int
	reissues    []string
	replacement string
	value       string
	err         error
}

func (s *recoveryTokens) Refresh(context.Context) (tokenfetch.Token, error) {
	s.refreshes++
	return tokenfetch.NewToken(s.value, time.Now().Add(time.Hour), "test"), s.err
}

func (s *recoveryTokens) Reissue(_ context.Context, rejected string) (tokenfetch.Token, error) {
	s.reissues = append(s.reissues, tokenfetch.Hash(rejected))
	if s.replacement == "" {
		return tokenfetch.Token{}, errors.New("unexpected token replacement")
	}
	return tokenfetch.NewToken(s.replacement, time.Now().Add(time.Hour), "test"), nil
}

func TestAuthRecoveryComparesGatewaydTokenAndBoundsReplacement(t *testing.T) {
	for _, scenario := range []string{"http-401", "revoked-before-ack", "revoked-after-ack", "replace-current", "same-token", "fetch-failure", "dial-failure", "after-window"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 11*time.Minute)
				defer cancel()
				tokens := &recoveryTokens{value: "replacement-token"}
				if scenario == "same-token" || scenario == "replace-current" {
					tokens.value = fakeToken
					tokens.replacement = fakeToken
					if scenario == "replace-current" {
						tokens.replacement = "replacement-token"
					}
				}
				if scenario == "after-window" {
					tokens.replacement = "replacement-token"
				}
				if scenario == "fetch-failure" {
					tokens.err = errors.New(fakeToken)
				}
				var dials int
				var firstDial time.Time
				var records []Record
				var logs bytes.Buffer
				reader, err := New(Config{AccountSeq: "3", Tokens: tokens,
					Logger: slog.New(slog.NewTextHandler(&logs, nil)),
					Append: func(r Record) error { records = append(records, r); return nil },
					Dial: func(_ context.Context, _ string, h http.Header) (Socket, int, error) {
						dials++
						want := fakeToken
						if dials == 1 {
							firstDial = time.Now()
						} else {
							want = tokens.value
							if scenario == "replace-current" {
								want = tokens.replacement
							}
							if time.Since(firstDial) < time.Second {
								t.Error("reconnected without backoff")
							}
						}
						if h.Get("Authorization") != "Bearer "+want {
							t.Error("unexpected handshake token")
						}
						revocationScenario := scenario == "revoked-before-ack" || scenario == "revoked-after-ack"
						if (dials == 1 && !revocationScenario) || scenario == "http-401" || scenario == "replace-current" {
							return nil, http.StatusUnauthorized, errors.New(fakeToken)
						}
						if scenario == "dial-failure" {
							return nil, 0, errors.New(fakeToken)
						}
						var id string
						firstRead := true
						pongs := 0
						return fakeSocket{
							write: func(raw []byte) error {
								if string(raw) != "PING" {
									id = requestID(raw)
								}
								return nil
							},
							read: func(ctx context.Context) ([]byte, error) {
								if firstRead && scenario != "revoked-before-ack" {
									firstRead = false
									return fmt.Appendf(nil, `{"type":"subscriptions","id":%q,"subscribed":["personal:order:3"],"rejected":[]}`, id), nil
								}
								if scenario == "after-window" && pongs < 10 {
									pongs++
									select {
									case <-ctx.Done():
										return nil, ctx.Err()
									case <-time.After(time.Minute):
										return []byte(`{"type":"pong"}`), nil
									}
								}
								return []byte(`{"type":"error","error":{"code":"auth-revoked","message":"` + fakeToken + `"}}`), nil
							},
							close: func() {},
						}, http.StatusSwitchingProtocols, nil
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				wantErr, wantDials, wantRefreshes := ErrAuthRevokedStop, 2, 1
				switch scenario {
				case "same-token":
					wantErr, wantDials = errFreshToken, 1
				case "fetch-failure":
					wantErr, wantDials = errToken, 1
				case "dial-failure":
					wantErr = errDial
				case "after-window":
					wantErr, wantRefreshes = errFreshToken, 2
				}
				runErr := reader.Run(ctx)
				wantReplacementHash := ""
				switch scenario {
				case "same-token", "replace-current":
					wantReplacementHash = tokenfetch.Hash(fakeToken)
				case "after-window":
					wantReplacementHash = tokenfetch.Hash("replacement-token")
				}
				if wantReplacementHash == "" {
					if len(tokens.reissues) != 0 {
						t.Fatalf("replacement calls = %d, want 0", len(tokens.reissues))
					}
				} else if len(tokens.reissues) != 1 || tokens.reissues[0] != wantReplacementHash {
					t.Fatalf("expected one replacement call with the rejected hash, got %d calls", len(tokens.reissues))
				}
				if !errors.Is(runErr, wantErr) || dials != wantDials || tokens.refreshes != wantRefreshes {
					t.Fatalf("run = %v, dials = %d, refreshes = %d", runErr, dials, tokens.refreshes)
				}
				var stops int
				for _, record := range records {
					if record.Kind == "auth_revoked_stop" {
						stops++
						if record.Topic != "personal:order:3" || record.SessionID == "" || record.ReceivedAt.IsZero() {
							t.Fatalf("invalid stop record: %+v", record)
						}
					}
				}
				if (wantErr == ErrAuthRevokedStop && stops != 1) || (wantErr != ErrAuthRevokedStop && stops != 0) {
					t.Fatalf("stop rows = %d", stops)
				}
				raw, _ := json.Marshal(records)
				for _, token := range []string{fakeToken, "replacement-token"} {
					if strings.Contains(string(raw)+logs.String()+runErr.Error(), token) {
						t.Fatal("credential appears in output")
					}
				}
			})
		})
	}
}

func TestQuoteRecorderFailureClosesConnection(t *testing.T) {
	trade := fixture(t, "trade-us-asyncapi-example.json")
	var reader *Reader
	healthyAtAppend := make(chan bool, 1)
	closed := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, err := New(Config{
		AccountSeq: "3", Tokens: testTokens{}, Symbols: []quote.Symbol{{Market: quote.MarketUS, Code: "AAPL"}},
		Append: func(Record) error { return nil },
		AppendQuote: func(Record) error {
			healthyAtAppend <- reader.Health()
			return errors.New("disk full")
		},
		Dial: func(context.Context, string, http.Header) (Socket, int, error) {
			frames := make(chan []byte, 2)
			return fakeSocket{
				write: func(raw []byte) error {
					frames <- fmt.Appendf(nil, `{"type":"subscriptions","id":%q,"subscribed":["personal:order:3","trade:us:AAPL","orderbook:us:AAPL"],"rejected":[]}`, requestID(raw))
					frames <- trade
					return nil
				},
				read: func(ctx context.Context) ([]byte, error) {
					select {
					case frame := <-frames:
						return frame, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				},
				close: func() { closed <- struct{}{} },
			}, 0, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- reader.Run(ctx) }()
	select {
	case err := <-runDone:
		if !errors.Is(err, ErrRecord) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Run() = %v, want the recorder failure alone", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("quote recorder failure did not close the connection")
	}
	if !<-healthyAtAppend || reader.Health() {
		t.Fatal("health should transition from connected to disconnected on recorder failure")
	}
	select {
	case <-closed:
	default:
		t.Fatal("failed recorder left socket open")
	}
}

func TestQuoteWriteFailureDegradesHealthUntilNextRun(t *testing.T) {
	trade := fixture(t, "trade-us-asyncapi-example.json")
	order := fixture(t, "order.json")
	var reader *Reader
	var dials atomic.Int32
	failQuotes := atomic.Bool{}
	failQuotes.Store(true)
	releaseFirstOrder := make(chan struct{})
	firstOrderAppend := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, err := New(Config{
		AccountSeq: "3", Tokens: testTokens{}, Symbols: []quote.Symbol{{Market: quote.MarketUS, Code: "AAPL"}},
		Append: func(record Record) error {
			if record.Kind != "order" {
				return nil
			}
			switch dials.Load() {
			case 1:
				firstOrderAppend <- struct{}{}
				<-releaseFirstOrder
				if reader.Health() {
					t.Error("reader reported healthy while quote records were dropped")
				}
				if !reader.healthy.Load() {
					t.Error("connection health cleared before the session ended")
				}
			case 2:
				if !reader.Health() {
					t.Error("reader did not recover after quote writes succeeded again")
				}
				cancel()
			}
			return nil
		},
		AppendQuote: func(Record) error {
			if failQuotes.Load() {
				return errors.New("disk full")
			}
			return nil
		},
		Dial: func(context.Context, string, http.Header) (Socket, int, error) {
			dials.Add(1)
			frames := make(chan []byte, 4)
			return fakeSocket{
				write: func(raw []byte) error {
					if bytes.Equal(raw, []byte("PING")) {
						return nil
					}
					frames <- fmt.Appendf(nil, `{"type":"subscriptions","id":%q,"subscribed":["personal:order:3","trade:us:AAPL","orderbook:us:AAPL"],"rejected":[]}`, requestID(raw))
					frames <- trade
					frames <- order
					return nil
				},
				read: func(ctx context.Context) ([]byte, error) {
					select {
					case frame := <-frames:
						return frame, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				},
				close: func() {},
			}, 0, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- reader.Run(ctx) }()
	select {
	case <-firstOrderAppend:
	case <-time.After(5 * time.Second):
		t.Fatal("order record was not appended")
	}
	deadline := time.Now().Add(5 * time.Second)
	for reader.Health() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if reader.Health() {
		t.Fatal("quote write failure did not degrade reader health")
	}
	if !reader.healthy.Load() {
		t.Fatal("session ended before the degraded health check")
	}
	close(releaseFirstOrder)
	if err := <-runDone; !errors.Is(err, ErrRecord) {
		t.Fatalf("Run() = %v, want recording error", err)
	}
	if reader.droppedQuotes.Load() == 0 {
		t.Fatal("dropped quote records were not counted")
	}
	if reader.Health() {
		t.Fatal("reader recovered without a new run")
	}

	failQuotes.Store(false)
	go func() { runDone <- reader.Run(ctx) }()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("second Run() = %v, want context cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second run did not stop after the order record")
	}
	if dials.Load() != 2 {
		t.Fatalf("dials = %d, want 2", dials.Load())
	}
}
