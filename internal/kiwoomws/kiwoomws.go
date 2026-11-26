// Package kiwoomws records Kiwoom mock account and quote WebSocket frames.
package kiwoomws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

const (
	MockWebSocketEndpoint = "wss://mockapi.kiwoom.com:10000/api/websocket"
	MockRESTEndpoint      = "https://mockapi.kiwoom.com"
	NameKR                = "kiwoom-kr"
	NameUS                = "kiwoom-us"
	MaxSymbols            = 200
	ioTimeout             = 10 * time.Second
)

var (
	ErrLiveEndpoint = errors.New("키움 모의 웹소켓 호스트만 허용됩니다")
	ErrAppend       = errors.New("키움 웹소켓 프레임 기록에 실패했습니다")
	symbolPattern   = regexp.MustCompile(`^[A-Za-z0-9._-]{1,16}$`)
)

type Account string

const (
	AccountKR Account = "kr"
	AccountUS Account = "us"
)

// Symbol identifies a quote symbol. US symbols require the Kiwoom exchange
// code; KR symbols leave Exchange empty.
type Symbol struct {
	Code     string `toml:"code" json:"jmcode"`
	Exchange string `toml:"exchange" json:"stex_tp,omitempty"`
}

type Frame struct {
	Account    Account         `json:"account"`
	ReceivedAt time.Time       `json:"received_at"`
	Raw        json.RawMessage `json:"raw"`
}

type Health struct {
	Connected       bool
	MalformedFrames uint64
}

type Socket interface {
	Read(context.Context) (websocket.MessageType, []byte, error)
	Write(context.Context, websocket.MessageType, []byte) error
	CloseNow() error
}

type Dialer func(context.Context, string) (Socket, error)

type Config struct {
	Account  Account
	Endpoint string
	Symbols  []Symbol
	Token    func(context.Context) (string, error)
	Append   func(Frame) error
	Dial     Dialer
	Logger   *slog.Logger
}

type Reader struct {
	cfg       Config
	running   atomic.Bool
	connected atomic.Bool
	malformed atomic.Uint64
}

func New(cfg Config) (*Reader, error) {
	if cfg.Endpoint == "" {
		cfg.Endpoint = MockWebSocketEndpoint
	}
	if err := ValidateEndpoint(cfg.Endpoint); err != nil {
		return nil, err
	}
	if cfg.Account != AccountKR && cfg.Account != AccountUS {
		return nil, errors.New("키움 웹소켓 계정은 kr 또는 us여야 합니다")
	}
	if cfg.Token == nil || cfg.Append == nil {
		return nil, errors.New("키움 웹소켓에는 토큰 발급기와 기록기가 필요합니다")
	}
	if err := ValidateSymbols(cfg.Account, cfg.Symbols); err != nil {
		return nil, err
	}
	if cfg.Dial == nil {
		cfg.Dial = dial
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Reader{cfg: cfg}, nil
}

// ValidateEndpoint allows only the fixed Kiwoom mock WebSocket endpoint.
// It is called by New before any dialer can be invoked.
func ValidateEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "wss" || u.Host != "mockapi.kiwoom.com:10000" || u.Path != "/api/websocket" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return ErrLiveEndpoint
	}
	return nil
}

func ValidateSymbols(account Account, symbols []Symbol) error {
	if len(symbols) > MaxSymbols {
		return fmt.Errorf("키움 웹소켓 설정 종목 수가 %d개를 초과했습니다", MaxSymbols)
	}
	seen := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		if !symbolPattern.MatchString(symbol.Code) {
			return errors.New("키움 웹소켓 종목 코드가 잘못되었습니다")
		}
		if account == AccountKR && symbol.Exchange != "" {
			return errors.New("키움 국내주식 종목에는 거래소 코드가 없어야 합니다")
		}
		if account == AccountUS && symbol.Exchange != "ND" && symbol.Exchange != "NY" && symbol.Exchange != "NA" {
			return errors.New("키움 미국주식 거래소 코드는 ND, NY 또는 NA여야 합니다")
		}
		if account != AccountKR && account != AccountUS {
			return errors.New("키움 웹소켓 계정은 kr 또는 us여야 합니다")
		}
		if _, ok := seen[symbol.Code]; ok {
			return errors.New("키움 웹소켓 종목 코드가 중복되었습니다")
		}
		seen[symbol.Code] = struct{}{}
	}
	return nil
}

func ValidateCombinedSymbols(kr, us []Symbol) error {
	if len(kr)+len(us) > MaxSymbols {
		return fmt.Errorf("키움 국내·미국 웹소켓 종목 합계가 %d개를 초과했습니다", MaxSymbols)
	}
	if err := ValidateSymbols(AccountKR, kr); err != nil {
		return err
	}
	return ValidateSymbols(AccountUS, us)
}

func (r *Reader) Name() string {
	if r.cfg.Account == AccountUS {
		return NameUS
	}
	return NameKR
}

func (r *Reader) Health() Health {
	return Health{Connected: r.connected.Load(), MalformedFrames: r.malformed.Load()}
}

func dial(ctx context.Context, endpoint string) (Socket, error) {
	// Validate again at the final network boundary so a future caller cannot
	// replace the mock endpoint between construction and dialing.
	if err := ValidateEndpoint(endpoint); err != nil {
		return nil, err
	}
	conn, _, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{
		HTTPClient: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	})
	if err != nil {
		return nil, errors.New("Kiwoom mock WebSocket dial failed")
	}
	conn.SetReadLimit(1 << 20)
	return conn, nil
}

func (r *Reader) Run(ctx context.Context) error {
	if !r.running.CompareAndSwap(false, true) {
		return errors.New("키움 웹소켓 reader가 이미 실행 중입니다")
	}
	defer r.running.Store(false)
	delay := time.Second
	for ctx.Err() == nil {
		started := time.Now()
		err := r.session(ctx)
		r.connected.Store(false)
		if errors.Is(err, ErrAppend) {
			return err
		}
		if ctx.Err() != nil {
			break
		}
		if time.Since(started) > time.Minute {
			delay = time.Second
		}
		r.cfg.Logger.Warn("키움 모의 웹소켓 재접속 중", "account", r.cfg.Account, "reason", safeReason(err), "delay", delay, "malformed_frames", r.malformed.Load())
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		delay = min(2*delay, 30*time.Second)
	}
	return nil
}

func safeReason(err error) string {
	if err == nil {
		return "연결 종료"
	}
	return "연결 또는 프로토콜 오류"
}

func (r *Reader) session(ctx context.Context) error {
	token, err := r.cfg.Token(ctx)
	if err != nil || strings.TrimSpace(token) == "" {
		return errors.New("키움 모의 OAuth 토큰을 사용할 수 없습니다")
	}
	dialCtx, cancel := context.WithTimeout(ctx, ioTimeout)
	conn, err := r.cfg.Dial(dialCtx, r.cfg.Endpoint)
	cancel()
	if err != nil {
		return errors.New("키움 모의 웹소켓 연결에 실패했습니다")
	}
	defer conn.CloseNow()
	login, _ := json.Marshal(map[string]string{"trnm": "LOGIN", "token": token})
	if err := write(ctx, conn, login); err != nil {
		return errors.New("키움 모의 웹소켓 로그인 전송에 실패했습니다")
	}
	if err := r.readUntilLogin(ctx, conn, token); err != nil {
		return err
	}
	subscription, err := BuildSubscription(r.cfg.Account, r.cfg.Symbols)
	if err != nil {
		return err
	}
	if err := write(ctx, conn, subscription); err != nil {
		return errors.New("키움 모의 웹소켓 구독 전송에 실패했습니다")
	}
	r.connected.Store(true)
	defer r.connected.Store(false)
	for {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			return errors.New("키움 모의 웹소켓 수신이 중단되었습니다")
		}
		if err := r.handleFrame(conn, ctx, raw, token); err != nil {
			return err
		}
	}
}

func (r *Reader) readUntilLogin(ctx context.Context, conn Socket, token string) error {
	loginCtx, cancel := context.WithTimeout(ctx, ioTimeout)
	defer cancel()
	for {
		_, raw, err := conn.Read(loginCtx)
		if err != nil {
			return errors.New("키움 모의 웹소켓 로그인 확인에 실패했습니다")
		}
		if err := r.handleFrame(conn, loginCtx, raw, token); err != nil {
			return err
		}
		var head struct {
			TRNM       string `json:"trnm"`
			ReturnCode *int   `json:"return_code"`
		}
		if json.Unmarshal(raw, &head) != nil || head.TRNM != "LOGIN" {
			continue
		}
		if head.ReturnCode == nil || *head.ReturnCode != 0 {
			return errors.New("키움 모의 웹소켓 로그인이 거부되었습니다")
		}
		return nil
	}
}

func (r *Reader) handleFrame(conn Socket, ctx context.Context, raw []byte, token string) error {
	var frame map[string]json.RawMessage
	var head struct {
		TRNM string `json:"trnm"`
	}
	if json.Unmarshal(raw, &frame) != nil || frame == nil || json.Unmarshal(raw, &head) != nil || strings.TrimSpace(head.TRNM) == "" || (token != "" && strings.Contains(string(raw), token)) {
		r.malformed.Add(1)
		return nil
	}
	if err := r.cfg.Append(Frame{Account: r.cfg.Account, ReceivedAt: time.Now().UTC(), Raw: append(json.RawMessage(nil), raw...)}); err != nil {
		return ErrAppend
	}
	if head.TRNM == "PING" {
		if err := write(ctx, conn, raw); err != nil {
			return errors.New("키움 모의 웹소켓 PING 응답 전송에 실패했습니다")
		}
	}
	if head.TRNM == "REG" || head.TRNM == "REMOVE" {
		var ack struct {
			ReturnCode *int `json:"return_code"`
		}
		if json.Unmarshal(raw, &ack) != nil || ack.ReturnCode == nil || *ack.ReturnCode != 0 {
			return errors.New("키움 모의 웹소켓 구독이 거부되었습니다")
		}
	}
	return nil
}

func write(ctx context.Context, conn Socket, payload []byte) error {
	writeCtx, cancel := context.WithTimeout(ctx, ioTimeout)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, payload)
}
