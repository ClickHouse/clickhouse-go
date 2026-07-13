package clickhouse

import (
	"context"
	"sync"
)

// contextWatchdog is a helper function to run a callback when the context is done.
// It has a cancellation function to prevent the callback from running.
// Useful for interrupting some logic when the context is done,
// but you want to not bother about context cancellation if your logic is already done.
//
// The returned cancel function guarantees that once it returns, the callback
// either already completed or will never run — even when the context is
// cancelled concurrently with the cancel call.
//
// Example:
// stopCW := contextWatchdog(ctx, func() { /* do something */ })
// // do something else
// defer stopCW()
func contextWatchdog(ctx context.Context, callback func()) (cancel func()) {
	exit := make(chan struct{})

	var mu sync.Mutex
	stopped := false

	go func() {
		select {
		case <-exit:
			return
		case <-ctx.Done():
			mu.Lock()
			defer mu.Unlock()
			if stopped {
				return
			}
			callback()
		}
	}()

	return func() {
		mu.Lock()
		stopped = true
		mu.Unlock()
		close(exit)
	}
}
