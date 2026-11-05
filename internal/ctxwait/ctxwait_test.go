package ctxwait

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSleep(t *testing.T) {
	if err := Sleep(context.Background(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Sleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
	if err := Sleep(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled zero: %v", err)
	}
	if err := Sleep(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
}
