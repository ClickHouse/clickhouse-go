package clickhouse

import "context"

// contextWatchdog is a helper function to run a callback when the context is done.
// It has a cancellation function to prevent the callback from running.
// The cancellation function blocks until the watchdog goroutine exits. When it
// returns, the callback has either not run and never will, or has completed.
// Useful for interrupting some logic when the context is done,
// but you want to not bother about context cancellation if your logic is already done.
// Example:
// stopCW := contextWatchdog(ctx, func() { /* do something */ })
// // do something else
// defer stopCW()
func contextWatchdog(ctx context.Context, callback func()) (cancel func()) {
	exit := make(chan struct{})
	done := make(chan struct{})

	// The goroutine exits when cancel is called or after callback returns.
	go func() {
		defer close(done)
		select {
		case <-exit:
			return
		case <-ctx.Done():
			// The caller may have stopped the watchdog at the same time the
			// context ended; the caller finishing its work wins.
			select {
			case <-exit:
				return
			default:
			}
			callback()
			return
		}
	}()

	return func() {
		close(exit)
		<-done
	}
}
