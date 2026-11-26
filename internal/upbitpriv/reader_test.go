package upbitpriv

import (
	"context"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func docTrade(t *testing.T) string {
	t.Helper()
	// Field subset from https://docs.upbit.com/kr/reference/websocket-myorder.
	raw, err := os.ReadFile("testdata/doc-trade.json")
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestMyOrderFill(t *testing.T) {
	raw := docTrade(t)
	at := time.Unix(1800000000, 0)
	got, err := decodeFrame([]byte(raw), at)
	want := Record{
		ReceivedAt: at.UTC(), Broker: "upbit", Venue: "upbit_krw", Instrument: "crypto",
		Symbol: "BTC", RawSymbol: "KRW-BTC", Side: "buy", Currency: "KRW",
		BrokerOrderID: "ac2dc2a3-fce9-40a2-a4f6-5987c25c438f",
		TradeUUID:     "68315169-fba4-4175-ade3-aff14a616657", Identifier: "test-1",
		FillSeq: 834460042, FilledPrice: 0.001453, FilledQty: 30925891.29839369,
		FilledAt: time.UnixMilli(1710751590421).UTC(),
	}
	if err != nil || got == nil || *got != want {
		t.Fatalf("got %+v, %v, want %+v", got, err, want)
	}
	sell := strings.NewReplacer(
		`"BID"`, `"ASK"`,
		`"executed_volume": 30925891.29839369`, `"executed_volume": 99999999`,
	).Replace(raw)
	got, err = decodeFrame([]byte(sell), at)
	if err != nil || got == nil || got.Side != "sell" || got.FilledQty != want.FilledQty {
		t.Fatalf("second partial fill: %+v, %v", got, err)
	}
	for _, replacement := range [][2]string{
		{`0.001453,`, `"0.001453",`},
		{"30925891.29839369", "0"},
		{`"BID"`, `"unknown"`},
		{"1710751590421", "null"},
		{"1710751590421", "9223372036854775807"},
		{`"68315169-fba4-4175-ade3-aff14a616657"`, "null"},
	} {
		record, err := decodeFrame([]byte(strings.Replace(raw, replacement[0], replacement[1], 1)), at)
		if record != nil || !errors.Is(err, errMalformed) {
			t.Fatalf("replacement %q: %+v, %v", replacement, record, err)
		}
	}
}

func TestAuthToken(t *testing.T) {
	token := authToken("fixture-access", "fixture-signing-key", "fixture-nonce")
	pieces := strings.Split(token, ".")
	if len(pieces) != 3 {
		t.Fatal("expected three JWT parts")
	}
	for i, want := range []map[string]string{
		{"alg": "HS512", "typ": "JWT"},
		{"access_key": "fixture-access", "nonce": "fixture-nonce"},
	} {
		raw, err := base64.RawURLEncoding.DecodeString(pieces[i])
		var got map[string]string
		if err != nil || json.Unmarshal(raw, &got) != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("part %d: got %v, want %v", i, got, want)
		}
	}
	mac := hmac.New(sha512.New, []byte("fixture-signing-key"))
	_, _ = io.WriteString(mac, strings.Join(pieces[:2], "."))
	if pieces[2] != base64.RawURLEncoding.EncodeToString(mac.Sum(nil)) {
		t.Fatal("JWT signature mismatch")
	}
}

type scriptedSocket struct {
	frames  []string
	written string
	closed  bool
}

func (s *scriptedSocket) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	if len(s.frames) == 0 {
		return 0, nil, io.EOF
	}
	raw := s.frames[0]
	s.frames = s.frames[1:]
	return websocket.MessageBinary, []byte(raw), nil
}

func (s *scriptedSocket) Write(_ context.Context, kind websocket.MessageType, raw []byte) error {
	if kind != websocket.MessageText {
		return errors.New("expected text subscription")
	}
	s.written = string(raw)
	return nil
}
func (s *scriptedSocket) Ping(context.Context) error { return nil }
func (s *scriptedSocket) CloseNow() error {
	s.closed = true
	return nil
}

func TestReconnectAndDropMalformed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	raw := docTrade(t)
	first := &scriptedSocket{frames: []string{
		"{", `{"status":"UP"}`,
		strings.Replace(raw, `"trade"`, `"done"`, 1),
		strings.Replace(raw, `"test-1"`, `"fixture-\u0073igning-key"`, 1),
	}}
	second := &scriptedSocket{}
	third := &scriptedSocket{frames: []string{raw}}
	var records []Record
	reader, err := New(Config{AccessKey: "fixture-access", SecretKey: "fixture-signing-key", Append: func(record Record) error {
		records = append(records, record)
		cancel()
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	sockets := []*scriptedSocket{first, second, third}
	tokens := map[string]bool{}
	reader.connect = func(_ context.Context, token string) (socket, error) {
		if tokens[token] || len(tokens) >= len(sockets) {
			t.Fatal("expected one fresh token per connection")
		}
		conn := sockets[len(tokens)]
		tokens[token] = true
		return conn, nil
	}
	var delays []time.Duration
	reader.sleep = func(_ context.Context, d time.Duration) error {
		delays = append(delays, d)
		return nil
	}
	before := time.Now()
	if err := reader.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	if !reflect.DeepEqual(delays, []time.Duration{time.Second, 2 * time.Second}) {
		t.Fatalf("retry delays: %v", delays)
	}
	if health := reader.Health(); health.Connected || health.MalformedFrames != 2 {
		t.Fatalf("health: %+v", health)
	}
	if len(records) != 1 || records[0].ReceivedAt.Before(before) || records[0].ReceivedAt.After(time.Now()) {
		t.Fatalf("records: %+v", records)
	}
	for _, conn := range sockets {
		var request []map[string]string
		err := json.Unmarshal([]byte(conn.written), &request)
		if !conn.closed || err != nil || len(request) != 3 || request[0]["ticket"] == "" ||
			request[1]["type"] != "myOrder" || request[2]["format"] != "DEFAULT" {
			t.Fatalf("closed=%v subscription=%s", conn.closed, conn.written)
		}
	}
}
