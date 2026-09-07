package reader

import (
	"context"
	"errors"
	"testing"

	"github.com/mgh3326/go-kis/kis/ws"
)

type closedEventSession struct{ events chan ws.Event }

func (s *closedEventSession) Events() <-chan ws.Event { return s.events }

func (*closedEventSession) Subscribe(context.Context, string, string) error { return nil }

func (*closedEventSession) Close() error { return nil }

func TestT19UnexpectedEventCloseReturnsGenericFailure(t *testing.T) {
	events := make(chan ws.Event)
	close(events)
	reader := New(Config{
		Endpoint: "live",
		HTSID:    "HTS_EXAMPLE",
		dial: func(context.Context, ws.Config) (session, error) {
			return &closedEventSession{events: events}, nil
		},
	})
	err := reader.Run(context.Background(), make(chan ws.Event, 1))
	if !errors.Is(err, ErrEventsClosed) {
		t.Fatalf("Reader.Run error = %v, want ErrEventsClosed", err)
	}
	if errors.Is(err, ws.ErrSessionOccupied) {
		t.Fatalf("unexpected close error = %v must not be ErrSessionOccupied", err)
	}
	if got := ProcessExitCode(err); got == 0 || got == ExitCodeSessionOccupied {
		t.Fatalf("exit code = %d, want generic non-zero non-%d", got, ExitCodeSessionOccupied)
	}
}
