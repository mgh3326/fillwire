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

type idleTestSocket struct{}

func (idleTestSocket) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	<-ctx.Done()
	return 0, nil, ctx.Err()
}
func (idleTestSocket) Write(context.Context, websocket.MessageType, []byte) error { return nil }
func (idleTestSocket) CloseNow() error                                            { return nil }

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
	for _, frame := range []string{`{`, `[]`, `{"trnm":"REAL","token":"secret-token"}`} {
		if err := r.handleFrame(conn, context.Background(), []byte(frame), "secret-token"); err != nil {
			t.Fatal(err)
		}
	}
	if records != 0 || r.Health().MalformedFrames != 3 {
		t.Fatalf("records=%d malformed=%d", records, r.Health().MalformedFrames)
	}
}

func TestEscapedReflectedTokenIsDropped(t *testing.T) {
	var records int
	var logs bytes.Buffer
	r, err := New(Config{Account: AccountUS, Token: func(context.Context) (string, error) { return "secret-token", nil }, Append: func(Frame) error { records++; return nil }, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	conn := &testSocket{}
	for _, frame := range []string{
		`{"trnm":"REAL","nested":{"value":"se\u0063ret-token"}}`,
		`{"trnm":"REAL","nested":{"se\u0063ret-token":"reflected"}}`,
	} {
		if err := r.handleFrame(conn, context.Background(), []byte(frame), "secret-token"); err != nil {
			t.Fatal(err)
		}
	}
	if records != 0 || r.Health().MalformedFrames != 2 {
		t.Fatalf("records=%d malformed=%d", records, r.Health().MalformedFrames)
	}
	if !strings.Contains(logs.String(), "malformed_frames=1") || !strings.Contains(logs.String(), "malformed_frames=2") {
		t.Fatalf("malformed counts were not logged during the session: %s", logs.String())
	}
}

func TestPermanentAuthFailuresStopTokenRefreshLoop(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reads [][]byte
		token func(context.Context) (string, error)
	}{
		{name: "token", token: func(context.Context) (string, error) { return "", errors.New("denied") }},
		{name: "login", reads: [][]byte{[]byte(`{"trnm":"LOGIN","return_code":1}`)}, token: func(context.Context) (string, error) { return "mock-token", nil }},
		{name: "registration", reads: [][]byte{[]byte(`{"trnm":"LOGIN","return_code":0}`), []byte(`{"trnm":"REG","return_code":1}`)}, token: func(context.Context) (string, error) { return "mock-token", nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tokenRequests := 0
			cfg := Config{
				Account: AccountKR,
				Token: func(ctx context.Context) (string, error) {
					tokenRequests++
					return tc.token(ctx)
				},
				Append: func(Frame) error { return nil },
				Dial: func(context.Context, string) (Socket, error) {
					return &testSocket{reads: append([][]byte(nil), tc.reads...)}, nil
				},
			}
			r, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Run(context.Background()); !errors.Is(err, ErrPermanentAuth) {
				t.Fatalf("Run error = %v, want permanent auth rejection", err)
			}
			if tokenRequests != 1 {
				t.Fatalf("token requests = %d, want 1", tokenRequests)
			}
		})
	}
}

func TestReadWithIdleLimit(t *testing.T) {
	_, _, err := readWithIdleLimit(context.Background(), idleTestSocket{}, 10*time.Millisecond)
	if !errors.Is(err, errReadIdle) {
		t.Fatalf("idle read error = %v, want %v", err, errReadIdle)
	}
}
