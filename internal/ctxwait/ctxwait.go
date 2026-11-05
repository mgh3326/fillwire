// Package ctxwait is the one cancellable sleep the retry loops share.
package ctxwait

import (
	"context"
	"time"
)

// Sleep waits for d or until ctx is done, whichever comes first. It returns
// ctx.Err() if ctx ended the wait and nil otherwise. A non-positive d returns
// at once unless ctx is already done.
func Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
