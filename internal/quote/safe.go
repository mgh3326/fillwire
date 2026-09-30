package quote

import (
	"context"
	"errors"
	"log/slog"
	"sync"
)

// errContainedPanic replaces a panic raised by the lane's injected dialer,
// transport, or token source.
var errContainedPanic = errors.New("quote: panic contained in websocket dependency")

// errSecondConnection refuses a dial while the lane still holds a connection.
var errSecondConnection = errors.New("quote: refusing a second Toss websocket while one is open")

// The wrappers below turn a panic in an injected dependency into an ordinary
// error, so it cannot crash the process and take the fills pipeline down.

type safeDialer struct {
	dialer Dialer
	logger *slog.Logger
}

func (d safeDialer) Dial(ctx context.Context, endpoint, token string) (transport Transport, err error) {
	defer contain(d.logger, "dial", &err)
	inner, err := d.dialer.Dial(ctx, endpoint, token)
	if err != nil || inner == nil {
		if err == nil {
			err = errors.New("quote: dialer returned no transport")
		}
		return nil, err
	}
	return safeTransport{transport: inner, logger: d.logger}, nil
}

type safeTransport struct {
	transport Transport
	logger    *slog.Logger
}

func (t safeTransport) Read(ctx context.Context) (data []byte, err error) {
	defer contain(t.logger, "read", &err)
	return t.transport.Read(ctx)
}

func (t safeTransport) Write(ctx context.Context, data []byte) (err error) {
	defer contain(t.logger, "write", &err)
	return t.transport.Write(ctx, data)
}

func (t safeTransport) Close() (err error) {
	defer contain(t.logger, "close", &err)
	return t.transport.Close()
}

type safeToken struct {
	source TokenSource
	logger *slog.Logger
}

func (s safeToken) Token(ctx context.Context) (token string, err error) {
	defer contain(s.logger, "token", &err)
	return s.source.Token(ctx)
}

func contain(logger *slog.Logger, operation string, err *error) {
	if recovered := recover(); recovered != nil {
		if logger != nil {
			logger.Error("quote dependency panic contained", "operation", operation)
		}
		*err = errContainedPanic
	}
}

// singleConnection enforces that the lane holds at most one Toss websocket.
// Toss allows two connections per account and closes the oldest when a third
// opens; fillwire uses exactly one and leaves the other as a spare. A dial is
// refused until the previous transport is closed (break before make).
type singleConnection struct {
	dialer Dialer
	mu     sync.Mutex
	open   bool
}

func (s *singleConnection) Dial(ctx context.Context, endpoint, token string) (Transport, error) {
	s.mu.Lock()
	if s.open {
		s.mu.Unlock()
		return nil, errSecondConnection
	}
	s.open = true
	s.mu.Unlock()
	transport, err := s.dialer.Dial(ctx, endpoint, token)
	if err != nil {
		s.release()
		return nil, err
	}
	return &releasingTransport{Transport: transport, release: s.release}, nil
}

func (s *singleConnection) release() {
	s.mu.Lock()
	s.open = false
	s.mu.Unlock()
}

type releasingTransport struct {
	Transport
	once    sync.Once
	release func()
}

func (t *releasingTransport) Close() error {
	err := t.Transport.Close()
	t.once.Do(t.release)
	return err
}
