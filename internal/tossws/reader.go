// Package tossws records Toss personal order events without acting on them.
package tossws

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	mathrand "math/rand/v2"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/mgh3326/fillwire/internal/quote"
	"github.com/mgh3326/fillwire/internal/tokenfetch"
)

const (
	Endpoint         = quote.Endpoint
	MaxSubscriptions = 100
	pingInterval     = 60 * time.Second
	ioTimeout        = 10 * time.Second
	readIdle         = 3 * pingInterval
	authWindow       = 10 * time.Minute
	quoteQueueSize   = 2048
)

var (
	ErrAccountNotFound = errors.New("Toss WS subscription rejected: account-not-found")
	ErrRejected        = errors.New("Toss WS subscription rejected")
	ErrAck             = errors.New("Toss WS invalid subscription acknowledgement")
	ErrAuthRevokedStop = errors.New("Toss WS stopped after a second authentication rejection")
	ErrForbidden       = errors.New("Toss WS handshake forbidden: HTTP 403")
	ErrRecord          = errors.New("Toss WS JSONL append failed")
	errFreshToken      = errors.New("Toss WS gatewayd did not provide a different token")
	errToken           = errors.New("Toss WS gatewayd token unavailable")
	errAckTimeout      = errors.New("Toss WS subscription acknowledgement timed out")
	errReadIdle        = errors.New("Toss WS inbound traffic timed out")
	errServer          = errors.New("Toss WS unrecognized server error")
	errUnauthorized    = errors.New("Toss WS handshake unauthorized: HTTP 401")
	errAuthRevoked     = errors.New("Toss WS token revoked")
	errDial            = errors.New("Toss WS handshake failed")
	errWrite           = errors.New("Toss WS declaration write failed")
	errClose           = errors.New("Toss WS unexpected close; possible connection eviction")
	errShutdown        = errors.New("Toss WS server-shutdown")
	errPing            = errors.New("Toss WS PING write failed")
	errRate            = errors.New("Toss WS rate-limit-exceeded")
)

type Socket interface {
	Read(context.Context) (websocket.MessageType, []byte, error)
	Write(context.Context, websocket.MessageType, []byte) error
	CloseNow() error
}

type Dialer func(ctx context.Context, endpoint string, headers http.Header) (Socket, int, error)

type TokenSource interface {
	Current(context.Context) (tokenfetch.Token, bool, error)
	Refresh(context.Context) (tokenfetch.Token, error)
}

type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time                         { return time.Now() }
func (wallClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type Config struct {
	AccountSeq  string
	Symbols     []quote.Symbol
	Tokens      TokenSource
	Append      func(Record) error
	AppendQuote func(Record) error
	Dial        Dialer
	Clock       Clock
	Logger      *slog.Logger
}

type Reader struct {
	cfg            Config
	decoder        *quote.Decoder
	topics         []string
	running        atomic.Bool
	healthy        atomic.Bool
	malformed      atomic.Uint64
	rejectedQuotes atomic.Uint64
	droppedQuotes  atomic.Uint64
	quoteRecords   chan Record
}

// Health reports whether this reader has an acknowledged live connection.
func (r *Reader) Health() bool { return r.healthy.Load() }

func New(cfg Config) (*Reader, error) {
	if !ValidAccountSeq(cfg.AccountSeq) || cfg.Tokens == nil || cfg.Append == nil {
		return nil, errors.New("Toss WS requires account_seq, gatewayd token source and recorder")
	}
	_, topics, err := buildDeclaration("validation", cfg.AccountSeq, cfg.Symbols)
	if err != nil {
		return nil, err
	}
	if cfg.Dial == nil {
		cfg.Dial = dial
	}
	if cfg.Clock == nil {
		cfg.Clock = wallClock{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.AppendQuote == nil {
		cfg.AppendQuote = cfg.Append
	}
	return &Reader{cfg: cfg, decoder: quote.NewDecoder(cfg.Symbols), topics: topics}, nil
}

// buildDeclaration creates Toss's full-replacement subscription declaration.
// One personal-order subscription is always present; each configured symbol
// consumes one trade and one orderbook subscription.
func buildDeclaration(id, account string, symbols []quote.Symbol) ([]byte, []string, error) {
	if !ValidAccountSeq(account) {
		return nil, nil, errors.New("Toss WS requires a valid account_seq")
	}
	if 1+2*len(symbols) > MaxSubscriptions {
		return nil, nil, fmt.Errorf("Toss WS subscription count exceeds %d", MaxSubscriptions)
	}
	seen := make(map[quote.Symbol]struct{}, len(symbols))
	byMarket := map[string][]string{}
	for _, symbol := range symbols {
		if !quote.ValidSymbol(symbol) {
			return nil, nil, errors.New("Toss WS contains an invalid quote symbol")
		}
		if _, ok := seen[symbol]; ok {
			return nil, nil, errors.New("Toss WS contains a duplicate quote symbol")
		}
		seen[symbol] = struct{}{}
		byMarket[symbol.Market] = append(byMarket[symbol.Market], symbol.Code)
	}
	declaration := []any{
		map[string]string{"id": id},
		map[string]any{"type": channel, "codes": []string{account}},
	}
	topics := []string{channel + ":" + account}
	for _, market := range []string{quote.MarketKR, quote.MarketUS} {
		codes := byMarket[market]
		if len(codes) == 0 {
			continue
		}
		for _, kind := range []string{"trade", "orderbook"} {
			declaration = append(declaration, map[string]any{"type": kind + ":" + market, "codes": codes})
			for _, code := range codes {
				topics = append(topics, kind+":"+market+":"+code)
			}
		}
	}
	payload, err := json.Marshal(declaration)
	if err != nil {
		return nil, nil, err
	}
	return payload, topics, nil
}

func dial(ctx context.Context, endpoint string, headers http.Header) (Socket, int, error) {
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{
		HTTPClient: client, HTTPHeader: headers,
	})
	if err != nil {
		if response != nil {
			return nil, response.StatusCode, err
		}
		return nil, 0, err
	}
	conn.SetReadLimit(64 << 10)
	return conn, response.StatusCode, nil
}

func (r *Reader) Run(ctx context.Context) (runErr error) {
	if !r.running.CompareAndSwap(false, true) {
		return errors.New("Toss WS reader is already running")
	}
	defer r.running.Store(false)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r.quoteRecords = make(chan Record, quoteQueueSize)
	quoteWriterDone := make(chan struct{})
	quoteWriteFailed := false
	go func() {
		defer close(quoteWriterDone)
		for record := range r.quoteRecords {
			if quoteWriteFailed {
				r.dropQuote("recorder stopped")
				continue
			}
			if err := r.cfg.AppendQuote(record); err != nil {
				quoteWriteFailed = true
				r.dropQuote("append failed")
				cancel()
			}
		}
	}()
	defer func() {
		close(r.quoteRecords)
		<-quoteWriterDone
		r.quoteRecords = nil
		if quoteWriteFailed {
			if runErr == context.Canceled {
				runErr = ErrRecord
			} else {
				runErr = errors.Join(runErr, ErrRecord)
			}
		}
	}()
	var rejected string
	var recoveryStarted time.Time
	delay := time.Second
	for ctx.Err() == nil {
		recovering := rejected != ""
		token, err := r.token(ctx, rejected)
		if recovering && err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if err == nil {
			start := r.cfg.Clock.Now()
			if recovering {
				recoveryStarted = start
				rejected = ""
			}
			id := rand.Text()
			value := token.Value()
			conn, dialErr := r.connect(ctx, value)
			err = dialErr
			if err == nil {
				err = r.session(ctx, conn, value, id)
			}
			if ctx.Err() != nil {
				break
			}
			if r.cfg.Clock.Now().Sub(start) >= pingInterval {
				delay = time.Second
			}
			switch err {
			case errUnauthorized, errAuthRevoked:
				now := r.cfg.Clock.Now()
				if !recoveryStarted.IsZero() && now.Sub(recoveryStarted) < authWindow {
					if err := r.cfg.Append(Record{
						Kind: "auth_revoked_stop", ReceivedAt: now.UTC(), SessionID: id,
						Topic: channel + ":" + r.cfg.AccountSeq,
					}); err != nil {
						return errors.Join(ErrAuthRevokedStop, ErrRecord)
					}
					return ErrAuthRevokedStop
				}
				rejected = value
			default:
				if recovering && dialErr != nil {
					return dialErr
				}
			}
		}
		if ctx.Err() != nil {
			break
		}
		switch err {
		case ErrAccountNotFound, ErrRejected, ErrAck, ErrForbidden, ErrRecord:
			return err
		}
		r.cfg.Logger.Warn("Toss WS reconnecting", "reason", err, "malformed_frames", r.malformed.Load())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.cfg.Clock.After(delay + mathrand.N(delay/4)):
		}
		delay = min(2*delay, 30*time.Second)
	}
	return ctx.Err()
}

func (r *Reader) token(ctx context.Context, rejected string) (tokenfetch.Token, error) {
	var token tokenfetch.Token
	var err error
	if rejected == "" {
		token, _, err = r.cfg.Tokens.Current(ctx)
	} else {
		token, err = r.cfg.Tokens.Refresh(ctx)
	}
	if err != nil || token.Value() == "" {
		return tokenfetch.Token{}, errToken
	}
	if token.Value() == rejected {
		return tokenfetch.Token{}, errFreshToken
	}
	return token, nil
}

func (r *Reader) connect(ctx context.Context, token string) (Socket, error) {
	headers := http.Header{"Authorization": {"Bearer " + token}}
	dialCtx, cancel := context.WithTimeout(ctx, ioTimeout)
	conn, status, err := r.cfg.Dial(dialCtx, Endpoint, headers)
	cancel()
	if err != nil {
		switch status {
		case http.StatusUnauthorized:
			return nil, errUnauthorized
		case http.StatusForbidden:
			return nil, ErrForbidden
		default:
			return nil, errDial
		}
	}
	return conn, nil
}

func (r *Reader) session(ctx context.Context, conn Socket, token, id string) error {
	defer conn.CloseNow()
	topic := channel + ":" + r.cfg.AccountSeq
	if err := r.cfg.Append(Record{Kind: "resync_needed", ReceivedAt: r.cfg.Clock.Now().UTC(), SessionID: id, Topic: topic}); err != nil {
		return ErrRecord
	}

	payload, topics, err := buildDeclaration(id, r.cfg.AccountSeq, r.cfg.Symbols)
	if err != nil {
		return err
	}
	if write(ctx, conn, payload) != nil {
		return errWrite
	}
	firstPing := r.cfg.Clock.After(pingInterval)
	ackCtx, cancel := context.WithTimeout(ctx, ioTimeout)
	err = r.ack(ackCtx, conn, id, topics)
	cancel()
	if err != nil {
		return err
	}
	r.healthy.Store(true)
	defer r.healthy.Store(false)

	sessionCtx, stop := context.WithCancelCause(ctx)
	pingDone := make(chan struct{})
	go func() {
		defer close(pingDone)
		r.keepalive(sessionCtx, stop, conn, firstPing)
	}()
	defer func() {
		stop(nil)
		<-pingDone
	}()
	for {
		readCtx, cancel := context.WithTimeout(sessionCtx, readIdle)
		kind, raw, err := conn.Read(readCtx)
		timedOut := errors.Is(readCtx.Err(), context.DeadlineExceeded)
		cancel()
		at := r.cfg.Clock.Now().UTC()
		if err != nil {
			if context.Cause(sessionCtx) == errPing {
				return errPing
			}
			if timedOut {
				return errReadIdle
			}
			return errClose
		}
		frame := parseFrame(raw)
		switch {
		case kind != websocket.MessageText:
		case frame["type"] == "pong":
			continue
		case frame["type"] == "error":
			if err := serverError(frame); err != nil {
				return err
			}
		case frame["type"] == "message":
			topicValue, _ := frame["topic"].(string)
			if quoteTopic(topicValue) {
				recorded, err := r.recordQuote(raw, topicValue, at, id, token)
				if err != nil {
					return err
				}
				if recorded {
					continue
				}
				r.dropMalformed()
				continue
			}
			if orderData(frame, topic, r.cfg.AccountSeq) && !containsToken(raw, token) {
				data, _ := json.Marshal(frame["data"])
				if err := r.cfg.Append(Record{Kind: "order", ReceivedAt: at, SessionID: id, Topic: topic, Data: data, Raw: raw}); err != nil {
					return ErrRecord
				}
				continue
			}
		}
		r.dropMalformed()
	}
}

func quoteTopic(topic string) bool {
	return strings.HasPrefix(topic, "trade:") || strings.HasPrefix(topic, "orderbook:")
}

func (r *Reader) recordQuote(raw []byte, topic string, at time.Time, id, token string) (bool, error) {
	frame := r.decoder.Decode(raw)
	if frame.Kind != quote.FrameMessage || (frame.Drop != "" && frame.Drop != quote.DropOutOfSession && frame.Drop != quote.DropEmptyBook && frame.Drop != quote.DropNoTimestamp) || containsToken(raw, token) {
		return false, nil
	}
	record := Record{Kind: strings.SplitN(topic, ":", 2)[0], ReceivedAt: at, SessionID: id, Topic: topic, Raw: raw}
	select {
	case r.quoteRecords <- record:
	default:
		r.dropQuote("queue full")
	}
	return true, nil
}

func (r *Reader) ack(ctx context.Context, conn Socket, id string, topics []string) error {
	for {
		kind, raw, err := conn.Read(ctx)
		if err != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return errAckTimeout
			}
			return errClose
		}
		frame := parseFrame(raw)
		if kind != websocket.MessageText || frame == nil {
			r.dropMalformed()
			continue
		}
		switch frame["type"] {
		case "error":
			err := serverError(frame)
			if err == errServer {
				// Before acknowledgement, other server errors reject the declaration.
				return ErrAck
			}
			if err != nil {
				return err
			}
			r.dropMalformed()
		case "subscriptions":
			subscribed, subOK := frame["subscribed"].([]any)
			rejected, rejectOK := frame["rejected"].([]any)
			if frame["id"] != id || !subOK || !rejectOK {
				return ErrAck
			}
			want := make(map[string]struct{}, len(topics))
			for _, topic := range topics {
				want[topic] = struct{}{}
			}
			seen := make(map[string]struct{}, len(topics))
			for _, item := range rejected {
				rejection, _ := item.(object)
				topic, topicOK := rejection["target"].(string)
				code, codeOK := rejection["code"].(string)
				if !topicOK || !codeOK || code == "" {
					return ErrAck
				}
				if _, expected := want[topic]; !expected {
					return ErrAck
				}
				if _, duplicate := seen[topic]; duplicate {
					return ErrAck
				}
				seen[topic] = struct{}{}
				if topic == channel+":"+r.cfg.AccountSeq && code == "account-not-found" {
					return ErrAccountNotFound
				}
				if topic == channel+":"+r.cfg.AccountSeq {
					return ErrRejected
				}
				count := r.rejectedQuotes.Add(1)
				r.cfg.Logger.Warn("Toss quote subscription rejected", "target", topic, "code", code, "rejected_quotes", count)
			}
			for _, item := range subscribed {
				topic, ok := item.(string)
				if !ok {
					return ErrAck
				}
				if _, ok := want[topic]; !ok {
					return ErrAck
				}
				if _, duplicate := seen[topic]; duplicate {
					return ErrAck
				}
				seen[topic] = struct{}{}
			}
			personalOrder := channel + ":" + r.cfg.AccountSeq
			if _, ok := seen[personalOrder]; !ok {
				return ErrAck
			}
			if len(seen) != len(want) {
				return ErrAck
			}
			return nil
		default:
			r.dropMalformed()
		}
	}
}

func serverError(frame object) error {
	body, _ := frame["error"].(object)
	if !stringsPresent(body, "code", "message") {
		return nil
	}
	switch body["code"] {
	case "auth-revoked":
		return errAuthRevoked
	case "server-shutdown":
		return errShutdown
	case "rate-limit-exceeded", "internal-error":
		return errRate
	default:
		return errServer
	}
}

func write(ctx context.Context, conn Socket, payload []byte) error {
	writeCtx, cancel := context.WithTimeout(ctx, ioTimeout)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, payload)
}

func (r *Reader) keepalive(ctx context.Context, stop context.CancelCauseFunc, conn Socket, next <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-next:
			next = r.cfg.Clock.After(pingInterval)
			if write(ctx, conn, []byte("PING")) != nil {
				stop(errPing)
				return
			}
		}
	}
}

func (r *Reader) dropMalformed() {
	count := r.malformed.Add(1)
	// Log early and at powers of two during a sustained malformed burst.
	if count&(count-1) == 0 {
		r.cfg.Logger.Warn("Toss WS malformed frame dropped", "malformed_frames", count)
	}
}

func (r *Reader) dropQuote(reason string) {
	count := r.droppedQuotes.Add(1)
	if count == 1 || count&(count-1) == 0 {
		r.cfg.Logger.Warn("Toss quote record dropped", "reason", reason, "dropped_quotes", count)
	}
}

// Examine decoded strings so JSON escapes cannot hide an echoed credential.
func containsToken(raw []byte, token string) bool {
	if bytes.Contains(raw, []byte(token)) {
		return true
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	for {
		item, err := decoder.Token()
		if err != nil {
			return false
		}
		if value, ok := item.(string); ok && strings.Contains(value, token) {
			return true
		}
	}
}
