package kiwoomws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type testSocket struct {
	writes [][]byte
	reads  [][]byte
}

func (s *testSocket) Read(context.Context) (websocket.MessageType, []byte, error) {
	if len(s.reads) > 0 {
		frame := s.reads[0]
		s.reads = s.reads[1:]
		return websocket.MessageText, frame, nil
	}
	return websocket.MessageText, nil, errors.New("closed")
}
func (s *testSocket) Write(_ context.Context, _ websocket.MessageType, b []byte) error {
	s.writes = append(s.writes, append([]byte(nil), b...))
	return nil
}
func (*testSocket) CloseNow() error { return nil }

type idleSocket struct {
	reads [][]byte
}

func (s *idleSocket) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	if len(s.reads) > 0 {
		frame := s.reads[0]
		s.reads = s.reads[1:]
		return websocket.MessageText, frame, nil
	}
	<-ctx.Done()
	return websocket.MessageText, nil, ctx.Err()
}
func (*idleSocket) Write(context.Context, websocket.MessageType, []byte) error { return nil }
func (*idleSocket) CloseNow() error                                            { return nil }

func TestMockEndpointPolicy(t *testing.T) {
	if err := ValidateEndpoint(MockWebSocketEndpoint); err != nil {
		t.Fatalf("mock endpoint rejected: %v", err)
	}
	dialed := false
	_, err := New(Config{
		Account: AccountKR, Endpoint: "wss://api.kiwoom.com:10000/api/websocket",
		Token:  func(context.Context) (string, error) { return "token", nil },
		Append: func(Frame) error { return nil },
		Dial:   func(context.Context, string) (Socket, error) { dialed = true; return &testSocket{}, nil },
	})
	if !errors.Is(err, ErrLiveEndpoint) {
		t.Fatalf("live endpoint error = %v", err)
	}
	if dialed {
		t.Fatal("live endpoint reached dialer")
	}
}

func TestBuildSubscriptionMatchesKiwoomDocs(t *testing.T) {
	for _, tc := range []struct {
		account     Account
		symbols     []Symbol
		wantItems   []any
		wantQuotes  []string
		wantAccount []string
	}{
		{AccountKR, []Symbol{{Code: "005930"}}, []any{"005930"}, []string{"0B", "0D", "0H", "1h"}, []string{"00", "04"}},
		{AccountUS, []Symbol{{Code: "NVDA", Exchange: "ND"}}, []any{map[string]any{"jmcode": "NVDA", "stex_tp": "ND"}}, []string{"FE", "FT"}, []string{"F4", "F5"}},
	} {
		payload, err := BuildSubscription(tc.account, tc.symbols)
		if err != nil {
			t.Fatal(err)
		}
		var got registration
		if err := json.Unmarshal(payload, &got); err != nil {
			t.Fatal(err)
		}
		if got.TRNM != "REG" || got.GroupNo != "1" || got.Refresh != "0" || len(got.Data) != 2 {
			t.Fatalf("registration header/data = %+v", got)
		}
		if !reflect.DeepEqual(got.Data[0].Item, tc.wantItems) || strings.Join(got.Data[0].Type, ",") != strings.Join(tc.wantQuotes, ",") {
			t.Errorf("quote registration = %+v", got.Data[0])
		}
		if !reflect.DeepEqual(got.Data[1].Item, []any{""}) || strings.Join(got.Data[1].Type, ",") != strings.Join(tc.wantAccount, ",") {
			t.Errorf("account registration = %+v", got.Data[1])
		}
	}
	withoutSymbols, err := BuildSubscription(AccountUS, nil)
	if err != nil {
		t.Fatal(err)
	}
	var accountOnly registration
	if err := json.Unmarshal(withoutSymbols, &accountOnly); err != nil || len(accountOnly.Data) != 1 || strings.Join(accountOnly.Data[0].Type, ",") != "F4,F5" {
		t.Fatalf("account-only subscription = %+v, err=%v", accountOnly, err)
	}
}

func TestBuildRemovalAndCombinedSymbolLimit(t *testing.T) {
	removal, err := BuildRemoval(AccountKR, []Symbol{{Code: "005930"}})
	if err != nil {
		t.Fatal(err)
	}
	var got registration
	if err := json.Unmarshal(removal, &got); err != nil || got.TRNM != "REMOVE" || got.Refresh != "1" {
		t.Fatalf("removal = %+v, err=%v", got, err)
	}
	removal, err = BuildRemoval(AccountUS, []Symbol{{Code: "NVDA", Exchange: "ND"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(removal, &got); err != nil || got.TRNM != "REMOVE" || !reflect.DeepEqual(got.Data[0].Item, []any{map[string]any{"jmcode": "NVDA", "stex_tp": "ND"}}) {
		t.Fatalf("US removal quote registration = %+v, err=%v", got, err)
	}
	if err := ValidateCombinedSymbols(makeSymbols(101), makeUSSymbols(100)); err == nil {
		t.Fatal("201 combined symbols accepted")
	}
	if err := ValidateCombinedSymbols(makeSymbols(100), makeUSSymbols(100)); err != nil {
		t.Fatalf("200 combined symbols rejected: %v", err)
	}
	if err := ValidateSymbols(AccountUS, []Symbol{{Code: "NVDA"}}); err == nil {
		t.Fatal("US symbol without exchange was accepted")
	}
}

func makeSymbols(n int) []Symbol {
	items := make([]Symbol, n)
	for i := range items {
		items[i] = Symbol{Code: "S" + string(rune('A'+i/26)) + string(rune('A'+i%26))}
	}
	return items
}

func makeUSSymbols(n int) []Symbol {
	items := makeSymbols(n)
	for i := range items {
		items[i].Exchange = "ND"
	}
	return items
}

func TestDocumentedFramesRecordedAndMalformedCounted(t *testing.T) {
	trade, err := os.ReadFile("testdata/kr_trade.json")
	if err != nil {
		t.Fatal(err)
	}
	var records []Frame
	r, err := New(Config{Account: AccountKR, Symbols: []Symbol{{Code: "005930"}}, Token: func(context.Context) (string, error) { return "mock-token", nil }, Append: func(f Frame) error { records = append(records, f); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	conn := &testSocket{}
	if err := r.handleFrame(conn, context.Background(), []byte(`{"not":"json frame"}`), "mock-token"); err != nil {
		t.Fatal(err)
	}
	if err := r.handleFrame(conn, context.Background(), trade, "mock-token"); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Account != AccountKR || !records[0].ReceivedAt.After(time.Time{}) || string(records[0].Raw) != string(trade) {
		t.Fatalf("recorded frames = %+v", records)
	}
	if got := r.Health().MalformedFrames; got != 1 {
		t.Fatalf("malformed count = %d, want 1", got)
	}
	ping := []byte(`{"trnm":"PING","data":"keepalive"}`)
	if err := r.handleFrame(conn, context.Background(), ping, "mock-token"); err != nil {
		t.Fatal(err)
	}
	if len(conn.writes) != 1 || string(conn.writes[0]) != string(ping) {
		t.Fatalf("PING response = %q", conn.writes)
	}
}

func TestDocumentedLoginAcknowledgementFrame(t *testing.T) {
	ack, err := os.ReadFile("testdata/login_ack.json")
	if err != nil {
		t.Fatal(err)
	}
	var recorded []Frame
	r, err := New(Config{Account: AccountKR, Token: func(context.Context) (string, error) { return "mock-token", nil }, Append: func(f Frame) error { recorded = append(recorded, f); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	conn := &testSocket{reads: [][]byte{ack}}
	if err := r.readUntilLogin(context.Background(), conn, "mock-token"); err != nil {
		t.Fatal(err)
	}
	if len(recorded) != 1 || !strings.Contains(string(recorded[0].Raw), `"trnm":"LOGIN"`) {
		t.Fatalf("login frames = %+v", recorded)
	}
}

func TestMalformedAndReflectedTokenAreDropped(t *testing.T) {
	var records int
	r, err := New(Config{Account: AccountUS, Token: func(context.Context) (string, error) { return "secret-token", nil }, Append: func(Frame) error { records++; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	conn := &testSocket{}
	for _, frame := range []string{
		`{`,
		`[]`,
		`{"trnm":"REAL","token":"secret-token"}`,
		`{"trnm":"REAL","payload":{"nested":["secre\u0074-token"]}}`,
		`{"trnm":"REAL","payload":{"secre\u0074-token":"echo"}}`,
	} {
		if err := r.handleFrame(conn, context.Background(), []byte(frame), "secret-token"); err != nil {
			t.Fatal(err)
		}
	}
	if records != 0 || r.Health().MalformedFrames != 5 {
		t.Fatalf("records=%d malformed=%d", records, r.Health().MalformedFrames)
	}
}

func TestPermanentAuthAndRegistrationFailuresStopWithoutRetry(t *testing.T) {
	for _, scenario := range []string{"token", "login", "registration"} {
		t.Run(scenario, func(t *testing.T) {
			var tokenCalls, dials int
			var logs bytes.Buffer
			r, err := New(Config{
				Account: AccountKR,
				Token: func(context.Context) (string, error) {
					tokenCalls++
					if scenario == "token" {
						return "", errors.New("secret-token rejected")
					}
					return "secret-token", nil
				},
				Append: func(Frame) error { return nil },
				Logger: slog.New(slog.NewTextHandler(&logs, nil)),
				Dial: func(context.Context, string) (Socket, error) {
					dials++
					switch scenario {
					case "login":
						return &testSocket{reads: [][]byte{[]byte(`{"trnm":"LOGIN","return_code":1}`)}}, nil
					case "registration":
						return &testSocket{reads: [][]byte{
							[]byte(`{"trnm":"LOGIN","return_code":0}`),
							[]byte(`{"trnm":"REG","return_code":1}`),
						}}, nil
					default:
						return &testSocket{}, nil
					}
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			got := r.Run(context.Background())
			if got == nil || tokenCalls != 1 || dials != map[string]int{"token": 0, "login": 1, "registration": 1}[scenario] {
				t.Fatalf("Run() = %v, token calls = %d, dials = %d", got, tokenCalls, dials)
			}
			if strings.Contains(got.Error()+logs.String(), "secret-token") {
				t.Fatalf("credential leaked in error/log: %q %q", got, logs.String())
			}
			if scenario != "token" && !strings.Contains(logs.String(), "중단합니다") {
				t.Fatalf("permanent failure was not logged: %q", logs.String())
			}
		})
	}
}

func TestIdleReadReconnects(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var tokenCalls, dials int
	var logs bytes.Buffer
	r, err := New(Config{
		Account: AccountKR,
		Token: func(context.Context) (string, error) {
			tokenCalls++
			return "secret-token", nil
		},
		Append: func(Frame) error { return nil },
		Logger: slog.New(slog.NewTextHandler(&logs, nil)),
		Dial: func(context.Context, string) (Socket, error) {
			dials++
			if dials == 2 {
				cancel()
			}
			return &idleSocket{reads: [][]byte{[]byte(`{"trnm":"LOGIN","return_code":0}`)}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	r.readIdle = 5 * time.Millisecond
	if err := r.Run(ctx); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if tokenCalls != 2 || dials != 2 || !strings.Contains(logs.String(), "유휴 읽기 시간 초과") {
		t.Fatalf("idle read did not reconnect: token calls=%d dials=%d logs=%q", tokenCalls, dials, logs.String())
	}
	if strings.Contains(logs.String(), "secret-token") {
		t.Fatalf("credential leaked in log: %q", logs.String())
	}
}

func TestMalformedFramesAreLoggedAtPowerOfTwoCounts(t *testing.T) {
	var logs bytes.Buffer
	r, err := New(Config{
		Account: AccountKR,
		Token:   func(context.Context) (string, error) { return "secret-token", nil },
		Append:  func(Frame) error { return nil },
		Logger:  slog.New(slog.NewTextHandler(&logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	for range 8 {
		if err := r.handleFrame(&testSocket{}, context.Background(), []byte(`{`), "secret-token"); err != nil {
			t.Fatal(err)
		}
	}
	if got := r.Health().MalformedFrames; got != 8 {
		t.Fatalf("malformed count = %d, want 8", got)
	}
	for _, count := range []string{"malformed_frames=1", "malformed_frames=2", "malformed_frames=4", "malformed_frames=8"} {
		if !strings.Contains(logs.String(), count) {
			t.Errorf("missing %s log: %q", count, logs.String())
		}
	}
}
