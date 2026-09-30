package quote

import (
	"context"
	"errors"
	"log/slog"

	"github.com/mgh3326/go-kis/kis/ws"
)

// errContainedPanic replaces a panic raised by the lane's injected dialer,
// transport, or approval provider.
var errContainedPanic = errors.New("quote: panic contained in websocket dependency")

// go-kis calls the dialer, transport, and approval provider from its own
// reader and reconnect goroutines, where the lane's guard cannot reach. These
// wrappers turn a panic in any of them into an ordinary error, which go-kis
// already handles as a failed dial, read, write, or approval, so it cannot
// crash the process and take the fills pipeline down. A panic inside go-kis
// itself is outside this boundary.

type safeDialer struct {
	dialer ws.Dialer
	logger *slog.Logger
}

func (d safeDialer) Dial(ctx context.Context, endpoint string) (transport ws.Transport, err error) {
	defer contain(d.logger, "dial", &err)
	inner, err := d.dialer.Dial(ctx, endpoint)
	if err != nil || inner == nil {
		if err == nil {
			err = errors.New("quote: dialer returned no transport")
		}
		return nil, err
	}
	return safeTransport{transport: inner, logger: d.logger}, nil
}

type safeTransport struct {
	transport ws.Transport
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

func (t safeTransport) WriteControl(ctx context.Context, kind int, data []byte) (err error) {
	defer contain(t.logger, "write control", &err)
	return t.transport.WriteControl(ctx, kind, data)
}

func (t safeTransport) Close() (err error) {
	defer contain(t.logger, "close", &err)
	return t.transport.Close()
}

type safeApproval struct {
	provider ws.ApprovalKeyProvider
	logger   *slog.Logger
}

func (a safeApproval) ApprovalKey(ctx context.Context) (key string, err error) {
	defer contain(a.logger, "approval", &err)
	return a.provider.ApprovalKey(ctx)
}

func (a safeApproval) Reissue(ctx context.Context) (key string, err error) {
	defer contain(a.logger, "approval reissue", &err)
	return a.provider.Reissue(ctx)
}

func contain(logger *slog.Logger, operation string, err *error) {
	if recovered := recover(); recovered != nil {
		if logger != nil {
			logger.Error("quote websocket dependency panic contained", "operation", operation)
		}
		*err = errContainedPanic
	}
}
