package quote

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

// Endpoint is the only websocket this package dials (Toss AsyncAPI 1.2.2,
// servers.production).
const Endpoint = "wss://openapi-ws.tossinvest.com/ws/v1"

// readLimit bounds one frame. Toss realtime frames are small JSON objects.
const readLimit = 1 << 20

var (
	// ErrUnauthorized is the handshake's HTTP 401: the access token is
	// missing, invalid, or expired.
	ErrUnauthorized = errors.New("quote: Toss websocket refused the access token (HTTP 401)")
	// ErrForbidden is the handshake's HTTP 403: this host is not on the Toss
	// allowed-IP list.
	ErrForbidden = errors.New("quote: Toss websocket refused this host (HTTP 403, allowed IP)")

	errEndpoint = errors.New("quote: refusing to dial a websocket other than the Toss endpoint")
)

// Transport is one open websocket, reduced to what the lane needs. Read and
// Write may run concurrently; the lane serialises its own writes.
type Transport interface {
	Read(ctx context.Context) ([]byte, error)
	Write(ctx context.Context, data []byte) error
	Close() error
}

// Dialer opens a Transport authenticated with token.
type Dialer interface {
	Dial(ctx context.Context, endpoint, token string) (Transport, error)
}

// NewDialer returns the production dialer, built on github.com/coder/websocket.
func NewDialer() Dialer { return coderDialer{} }

type coderDialer struct{}

func (coderDialer) Dial(ctx context.Context, endpoint, token string) (Transport, error) {
	if endpoint != Endpoint {
		return nil, errEndpoint
	}
	return dialURL(ctx, endpoint, token)
}

// dialURL performs the handshake. Only Dial, after the endpoint check, calls
// it in production; tests call it against a loopback fake server.
func dialURL(ctx context.Context, endpoint, token string) (Transport, error) {
	client := &http.Client{
		// Never follow a redirect: it could carry the Authorization header to
		// another host.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Timeout:       0,
	}
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{
		HTTPClient: client,
		HTTPHeader: http.Header{"Authorization": {"Bearer " + token}},
	})
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		if response != nil {
			switch response.StatusCode {
			case http.StatusUnauthorized:
				return nil, ErrUnauthorized
			case http.StatusForbidden:
				return nil, ErrForbidden
			default:
				return nil, fmt.Errorf("quote: Toss websocket handshake failed (HTTP %d)", response.StatusCode)
			}
		}
		// The library error names the endpoint, never the token.
		return nil, fmt.Errorf("quote: Toss websocket dial failed: %w", err)
	}
	conn.SetReadLimit(readLimit)
	return &coderTransport{conn: conn}, nil
}

type coderTransport struct{ conn *websocket.Conn }

func (t *coderTransport) Read(ctx context.Context) ([]byte, error) {
	_, data, err := t.conn.Read(ctx)
	return data, err
}

func (t *coderTransport) Write(ctx context.Context, data []byte) error {
	return t.conn.Write(ctx, websocket.MessageText, data)
}

func (t *coderTransport) Close() error {
	done := make(chan struct{})
	go func() {
		_ = t.conn.Close(websocket.StatusNormalClosure, "")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(closeTimeout):
		_ = t.conn.CloseNow()
	}
	return nil
}
