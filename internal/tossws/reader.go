// Package tossws records Toss personal order events without acting on them.
package tossws

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
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
	Endpoint     = quote.Endpoint
	pingInterval = 60 * time.Second
	ioTimeout    = 10 * time.Second
	readIdle     = 3 * pingInterval
	reissueGap   = time.Minute
)

var (
	ErrAccountNotFound  = errors.New("Toss WS subscription rejected: account-not-found")
	ErrRejected         = errors.New("Toss WS subscription rejected")
	ErrAck              = errors.New("Toss WS invalid subscription acknowledgement")
	ErrAuthAfterReissue = errors.New("Toss WS replacement token rejected")
	ErrForbidden        = errors.New("Toss WS handshake forbidden: HTTP 403")
	ErrRecord           = errors.New("Toss WS JSONL append failed")
	errToken            = errors.New("Toss WS gatewayd token unavailable")
	errAckTimeout       = errors.New("Toss WS subscription acknowledgement timed out")
	errReadIdle         = errors.New("Toss WS inbound traffic timed out")
	errServer           = errors.New("Toss WS unrecognized server error")
	errAuth             = errors.New("Toss WS token rejected")
	errDial             = errors.New("Toss WS handshake failed")
	errWrite            = errors.New("Toss WS declaration write failed")
	errClose            = errors.New("Toss WS unexpected close; possible connection eviction")
	errShutdown         = errors.New("Toss WS server-shutdown")
	errPing             = errors.New("Toss WS PING write failed")
	errRate             = errors.New("Toss WS rate-limit-exceeded")
)

type Socket interface {
	Read(context.Context) (websocket.MessageType, []byte, error)
	Write(context.Context, websocket.MessageType, []byte) error
	CloseNow() error
}

type Dialer func(ctx context.Context, endpoint string, headers http.Header) (Socket, int, error)

type TokenSource interface {
	Current(context.Context) (tokenfetch.Token, bool, error)
	Reissue(context.Context, string) (tokenfetch.Token, error)
}

type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time                         { return time.Now() }
func (wallClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type Config struct {
	AccountSeq string
	Tokens     TokenSource
	Append     func(Record) error
	Dial       Dialer
	Clock      Clock
	Logger     *slog.Logger
}

type Reader struct {
	cfg       Config
	running   atomic.Bool
	malformed atomic.Uint64
}

func New(cfg Config) (*Reader, error) {
	if !ValidAccountSeq(cfg.AccountSeq) || cfg.Tokens == nil || cfg.Append == nil {
		return nil, errors.New("Toss WS requires account_seq, gatewayd token source and recorder")
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
	return &Reader{cfg: cfg}, nil
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

func (r *Reader) Run(ctx context.Context) error {
	if !r.running.CompareAndSwap(false, true) {
		return errors.New("Toss WS reader is already running")
	}
	defer r.running.Store(false)
	var rejected string
	var nextReissue time.Time
	var replacementUsed bool
	delay := time.Second
	for ctx.Err() == nil {
		var token tokenfetch.Token
		err := errToken
		if rejected == "" || !r.cfg.Clock.Now().Before(nextReissue) {
			if rejected != "" {
				// Count the attempt even when gatewayd reports a failure.
				nextReissue = r.cfg.Clock.Now().Add(reissueGap)
			}
			token, err = r.token(ctx, rejected)
		}
		if err == nil {
			replacementUsed = replacementUsed || rejected != ""
			rejected = ""
			start := r.cfg.Clock.Now()
			err = r.session(ctx, token.Value())
			if r.cfg.Clock.Now().Sub(start) >= pingInterval {
				delay = time.Second
				replacementUsed = false
			}
			if err == errAuth {
				if replacementUsed {
					return ErrAuthAfterReissue
				}
				rejected = token.Value()
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
		token, err = r.cfg.Tokens.Reissue(ctx, rejected)
	}
	if err != nil || token.Value() == "" {
		return tokenfetch.Token{}, errToken
	}
	return token, nil
}

func (r *Reader) session(ctx context.Context, token string) error {
	headers := http.Header{"Authorization": {"Bearer " + token}}
	dialCtx, cancel := context.WithTimeout(ctx, ioTimeout)
	conn, status, err := r.cfg.Dial(dialCtx, Endpoint, headers)
	cancel()
	if err != nil {
		switch status {
		case http.StatusUnauthorized:
			return errAuth
		case http.StatusForbidden:
			return ErrForbidden
		default:
			return errDial
		}
	}
	defer conn.CloseNow()
	id := rand.Text()
	topic := channel + ":" + r.cfg.AccountSeq
	if err := r.cfg.Append(Record{Kind: "resync_needed", ReceivedAt: r.cfg.Clock.Now().UTC(), SessionID: id, Topic: topic}); err != nil {
		return ErrRecord
	}

	declaration := []map[string]any{{"id": id}, {"type": channel, "codes": []string{r.cfg.AccountSeq}}}
	payload, _ := json.Marshal(declaration)
	if write(ctx, conn, payload) != nil {
		return errWrite
	}
	firstPing := r.cfg.Clock.After(pingInterval)
	ackCtx, cancel := context.WithTimeout(ctx, ioTimeout)
	err = r.ack(ackCtx, conn, id, topic)
	cancel()
	if err != nil {
		return err
	}

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
		case frame["type"] == "message" && orderData(frame, topic, r.cfg.AccountSeq) && !containsToken(raw, token):
			data, _ := json.Marshal(frame["data"])
			if err := r.cfg.Append(Record{Kind: "order", ReceivedAt: at, SessionID: id, Topic: topic, Data: data, Raw: raw}); err != nil {
				return ErrRecord
			}
			continue
		}
		r.dropMalformed()
	}
}

func (r *Reader) ack(ctx context.Context, conn Socket, id, topic string) error {
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
			if err := serverError(frame); err != nil {
				return err
			}
			r.dropMalformed()
		case "subscriptions":
			subscribed, subOK := frame["subscribed"].([]any)
			rejected, rejectOK := frame["rejected"].([]any)
			if frame["id"] != id || !subOK || !rejectOK {
				return ErrAck
			}
			for _, item := range rejected {
				rejection, _ := item.(object)
				if rejection["code"] == "account-not-found" {
					return ErrAccountNotFound
				}
			}
			if len(rejected) != 0 {
				return ErrRejected
			}
			if len(subscribed) != 1 || subscribed[0] != topic {
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
		return errAuth
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
