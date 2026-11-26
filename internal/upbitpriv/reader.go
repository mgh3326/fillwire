package upbitpriv

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/mgh3326/fillwire/internal/ctxwait"
)

const (
	Name         = "upbit-fills"
	endpoint     = "wss://api.upbit.com/websocket/v1/private"
	ioTimeout    = 10 * time.Second
	pingInterval = 30 * time.Second
)

var errAppend = errors.New("Upbit WS JSONL append failed")

type Config struct {
	AccessKey string
	SecretKey string
	Append    func(Record) error
	Logger    *slog.Logger
}

type Health struct {
	Connected       bool
	MalformedFrames uint64
}

type socket interface {
	Read(context.Context) (websocket.MessageType, []byte, error)
	Write(context.Context, websocket.MessageType, []byte) error
	Ping(context.Context) error
	CloseNow() error
}

type Reader struct {
	cfg       Config
	connect   func(context.Context, string) (socket, error)
	sleep     func(context.Context, time.Duration) error
	running   atomic.Bool
	connected atomic.Bool
	malformed atomic.Uint64
}

func New(cfg Config) (*Reader, error) {
	if strings.TrimSpace(cfg.AccessKey) == "" || strings.TrimSpace(cfg.SecretKey) == "" || cfg.Append == nil {
		return nil, errors.New("Upbit WS requires UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY and a recorder")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Reader{cfg: cfg, connect: dial, sleep: ctxwait.Sleep}, nil
}

func (r *Reader) Name() string { return Name }

func (r *Reader) Health() Health {
	return Health{Connected: r.connected.Load(), MalformedFrames: r.malformed.Load()}
}

func dial(ctx context.Context, token string) (socket, error) {
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	conn, _, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{
		HTTPClient: client, HTTPHeader: http.Header{"Authorization": {"Bearer " + token}},
	})
	if err != nil {
		return nil, errors.New("Upbit WS handshake failed")
	}
	conn.SetReadLimit(64 << 10)
	return conn, nil
}

func (r *Reader) Run(ctx context.Context) error {
	if !r.running.CompareAndSwap(false, true) {
		return errors.New("Upbit WS reader is already running")
	}
	defer r.running.Store(false)
	delay := time.Second
	for ctx.Err() == nil {
		start := time.Now()
		err := r.session(ctx)
		if err == errAppend {
			return err
		}
		if ctx.Err() != nil {
			break
		}
		if time.Since(start) >= 2*pingInterval {
			delay = time.Second
		}
		r.cfg.Logger.Warn("Upbit WS reconnecting", "reason", err, "delay", delay)
		if err := r.sleep(ctx, delay); err != nil {
			return err
		}
		delay = min(2*delay, 30*time.Second)
	}
	return ctx.Err()
}

func (r *Reader) session(ctx context.Context) error {
	id, err := uuid.NewRandom()
	if err != nil {
		return errors.New("Upbit WS nonce generation failed")
	}
	token := authToken(r.cfg.AccessKey, r.cfg.SecretKey, id.String())
	dialCtx, cancelDial := context.WithTimeout(ctx, ioTimeout)
	conn, err := r.connect(dialCtx, token)
	cancelDial()
	if err != nil {
		return errors.New("Upbit WS connection failed")
	}
	defer conn.CloseNow()

	subscribe := `[{"ticket":"` + id.String() + `"},{"type":"myOrder"},{"format":"DEFAULT"}]`
	writeCtx, cancelWrite := context.WithTimeout(ctx, ioTimeout)
	err = conn.Write(writeCtx, websocket.MessageText, []byte(subscribe))
	cancelWrite()
	if err != nil {
		return errors.New("Upbit WS subscription write failed")
	}
	r.connected.Store(true)
	defer r.connected.Store(false)

	sessionCtx, stop := context.WithCancel(ctx)
	pingDone := make(chan struct{})
	go func() {
		defer close(pingDone)
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-sessionCtx.Done():
				return
			case <-ticker.C:
				pingCtx, cancel := context.WithTimeout(sessionCtx, ioTimeout)
				err := conn.Ping(pingCtx)
				cancel()
				if err != nil {
					stop()
					return
				}
			}
		}
	}()
	defer func() {
		stop()
		<-pingDone
	}()

	for {
		_, raw, err := conn.Read(sessionCtx)
		at := time.Now()
		if err != nil {
			return errors.New("Upbit WS read stopped")
		}
		record, err := decodeFrame(raw, at)
		if err == errRejected {
			return err
		}
		if err != nil || (record != nil && r.containsCredential(record, token)) {
			count := r.malformed.Add(1)
			if count&(count-1) == 0 {
				r.cfg.Logger.Warn("Upbit WS malformed frame dropped", "malformed_frames", count)
			}
			continue
		}
		if record != nil {
			if err := r.cfg.Append(*record); err != nil {
				return errAppend
			}
		}
	}
}

func (r *Reader) containsCredential(record *Record, token string) bool {
	for _, value := range []string{record.RawSymbol, record.BrokerOrderID, record.TradeUUID, record.Identifier} {
		if strings.Contains(value, token) || strings.Contains(value, r.cfg.AccessKey) || strings.Contains(value, r.cfg.SecretKey) {
			return true
		}
	}
	return false
}
