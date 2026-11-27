package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/mgh3326/fillwire/internal/quote"
	"github.com/mgh3326/fillwire/internal/tokenfetch"
	"github.com/mgh3326/fillwire/internal/tossws"
)

func TestTossWSConfigExplicitOptIn(t *testing.T) {
	for _, config := range []string{"", "[toss_ws]\nenabled = false"} {
		settings, err := resolveTossWSSettings([]byte(config), gatewaydSettings{})
		if err != nil || settings.Enabled {
			t.Fatalf("reader must default off, enabled = %v, error = %v", settings.Enabled, err)
		}
	}
	const on = "[toss_ws]\nenabled = true\naccount_seq = '3'\ndirectory = 'test-output'"
	const fakeCredential = "fake-gatewayd-test-credential"
	t.Setenv("FILLWIRE_TEST_TOSS_GATEWAYD", fakeCredential)
	symbolsFile := filepath.Join(t.TempDir(), "symbols.txt")
	if err := os.WriteFile(symbolsFile, []byte("kr 005930\nus AAPL\n"), 0600); err != nil {
		t.Fatal(err)
	}
	gateway := gatewaydSettings{URL: "http://127.0.0.1:8080", TokenEnv: "FILLWIRE_TEST_TOSS_GATEWAYD", TokenHeader: "Authorization"}
	withSymbols := on + "\nsymbols_file = '" + symbolsFile + "'"
	settings, err := resolveTossWSSettings([]byte(withSymbols), gateway)
	if err != nil || !settings.Enabled || settings.AccountSeq != "3" || settings.Directory != "test-output" || settings.credential != fakeCredential || len(settings.symbols) != 2 {
		t.Fatal("opt-in did not resolve the expected account, directory and gatewayd settings")
	}
	for _, config := range []string{strings.Replace(on, "'3'", "'3/4'", 1), on + "\nendpoint = 'wss://elsewhere.invalid'", on + "\nsymbols_file = '/missing/symbols'"} {
		if _, err := resolveTossWSSettings([]byte(config), gateway); err == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
	tooManySymbols := filepath.Join(t.TempDir(), "too-many-symbols.txt")
	var contents strings.Builder
	for index := 0; index < 50; index++ {
		fmt.Fprintf(&contents, "kr %06d\n", index+1)
	}
	if err := os.WriteFile(tooManySymbols, []byte(contents.String()), 0600); err != nil {
		t.Fatal(err)
	}
	tooMany := on + "\nsymbols_file = '" + tooManySymbols + "'"
	if _, err := resolveTossWSSettings([]byte(tooMany), gateway); err == nil {
		t.Fatal("configuration with 101 trade/orderbook subscriptions was accepted")
	}
	gateway.URL = "http://gateway.invalid"
	if _, err := resolveTossWSSettings([]byte(on), gateway); err == nil {
		t.Fatal("untrusted credential transport accepted")
	}
}

func TestTossWSModuleStop(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		stop bool
	}{
		{"record-only", tossws.ErrRecord, false},
		{"forbidden", tossws.ErrForbidden, true},
		{"account-not-found", tossws.ErrAccountNotFound, true},
		{"rejected", tossws.ErrRejected, true},
		{"ack", tossws.ErrAck, true},
		{"auth-revoked-stop", tossws.ErrAuthRevokedStop, true},
		{"forbidden-with-record", errors.Join(tossws.ErrForbidden, tossws.ErrRecord), true},
		{"account-not-found-with-record", errors.Join(tossws.ErrAccountNotFound, tossws.ErrRecord), true},
		{"rejected-with-record", errors.Join(tossws.ErrRejected, tossws.ErrRecord), true},
		{"ack-with-record", errors.Join(tossws.ErrAck, tossws.ErrRecord), true},
		{"auth-stop-with-record", errors.Join(tossws.ErrAuthRevokedStop, tossws.ErrRecord), true},
		{"generic", errors.New("dial failed"), true},
		{"generic-with-record", errors.Join(errors.New("dial failed"), tossws.ErrRecord), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tossWSModuleStop(tc.err); got != tc.stop {
				t.Fatalf("tossWSModuleStop(%v) = %v, want %v", tc.err, got, tc.stop)
			}
		})
	}
}

type tossTestTokens struct{}

func (tossTestTokens) Current(context.Context) (tokenfetch.Token, bool, error) {
	return tokenfetch.NewToken("test-token", time.Now().Add(time.Hour), "test"), false, nil
}

func (tossTestTokens) Refresh(context.Context) (tokenfetch.Token, error) {
	return tokenfetch.Token{}, errors.New("unexpected token refresh")
}

func (tossTestTokens) Reissue(context.Context, string) (tokenfetch.Token, error) {
	return tokenfetch.Token{}, errors.New("unexpected token replacement")
}

type tossTestSocket struct {
	write func([]byte) error
	read  func(context.Context) ([]byte, error)
}

func (s tossTestSocket) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	data, err := s.read(ctx)
	return websocket.MessageText, data, err
}

func (s tossTestSocket) Write(_ context.Context, _ websocket.MessageType, data []byte) error {
	return s.write(data)
}

func (tossTestSocket) CloseNow() error { return nil }

// tossTestDeclarationID returns the request id of a subscription declaration.
func tossTestDeclarationID(t *testing.T, raw []byte) string {
	t.Helper()
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil || len(items) == 0 {
		t.Fatalf("declaration %q is not a JSON array", raw)
	}
	var head struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(items[0], &head); err != nil {
		t.Fatal(err)
	}
	return head.ID
}

// TestTossWSDegradedHealthSurvivesSupervisorRestart drives the supervisor's
// toss_ws module through runTossWS: a failed quote write restarts the Reader,
// and the new Reader must stay degraded until one of its own quote writes
// succeeds.
func TestTossWSDegradedHealthSurvivesSupervisorRestart(t *testing.T) {
	trade, err := os.ReadFile("../../internal/tossws/testdata/doc-trade-us-asyncapi-example.json")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	settings := tossWSSettings{
		tossWSFileConfig: tossWSFileConfig{Enabled: true, AccountSeq: "3", Directory: t.TempDir()},
		symbols:          []quote.Symbol{{Market: quote.MarketUS, Code: "AAPL"}},
	}
	quoteHealth := tossws.NewQuoteHealth()
	var failQuotes atomic.Bool
	failQuotes.Store(true)
	var dials atomic.Int32
	sessions := make(chan chan []byte, 2)
	acked := make(chan int32, 2)
	module := &streamModule{name: "toss_ws", start: func(ctx context.Context, observe func(func() bool)) error {
		return runTossWS(ctx, settings, logger, observe, tossWSDeps{
			quote:  quoteHealth,
			tokens: tossTestTokens{},
			appendQuote: func(tossws.Record) error {
				if failQuotes.Load() {
					return errors.New("disk full")
				}
				return nil
			},
			dial: func(context.Context, string, http.Header) (tossws.Socket, int, error) {
				session := dials.Add(1)
				frames := make(chan []byte, 4)
				sessions <- frames
				return tossTestSocket{
					write: func(raw []byte) error {
						if bytes.Equal(raw, []byte("PING")) {
							return nil
						}
						frames <- fmt.Appendf(nil, `{"type":"subscriptions","id":%q,"subscribed":["personal:order:3","trade:us:AAPL","orderbook:us:AAPL"],"rejected":[]}`, tossTestDeclarationID(t, raw))
						if session == 1 {
							frames <- trade
						}
						acked <- session
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
				}, 0, nil
			},
		})
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slot := &moduleSlot{name: "toss_ws", Module: module}
	sup := newSupervisor([]*moduleSlot{slot}, logger)
	sup.retryMin, sup.retryMax = time.Millisecond, 10*time.Millisecond
	supDone := make(chan struct{})
	go func() {
		sup.run(ctx)
		close(supDone)
	}()

	// Session 1's trade frame fails the quote write, which ends the Reader
	// attempt; the supervisor then starts session 2.
	<-sessions
	secondFrames := <-sessions
	deadline := time.After(5 * time.Second)
	for session := int32(0); session != 2; {
		select {
		case session = <-acked:
		case <-deadline:
			t.Fatal("restarted Reader did not acknowledge its subscription")
		}
	}
	if slot.snapshot().Restarts == 0 {
		t.Fatal("supervisor did not restart toss_ws after the quote write failure")
	}
	if !quoteHealth.Degraded() {
		t.Fatal("quote write failure did not mark the shared quote health degraded")
	}
	// The new session is acknowledged and alive; it must not report healthy
	// while its quote writes have not succeeded.
	for end := time.Now().Add(100 * time.Millisecond); time.Now().Before(end); {
		if module.Health() {
			t.Fatal("restarted Reader reported healthy before any quote write succeeded")
		}
		time.Sleep(time.Millisecond)
	}

	failQuotes.Store(false)
	secondFrames <- trade
	recovered := time.Now().Add(5 * time.Second)
	for !module.Health() && time.Now().Before(recovered) {
		time.Sleep(time.Millisecond)
	}
	if !module.Health() {
		t.Fatal("restarted Reader did not recover after a quote write succeeded")
	}
	if quoteHealth.Degraded() {
		t.Fatal("successful quote write left the shared quote health degraded")
	}

	cancel()
	select {
	case <-supDone:
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not stop after cancellation")
	}
}
