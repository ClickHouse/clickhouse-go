package clickhouse

import "context"

// contextWatchdog is a helper function to run a callback when the context is done.
// It has a cancellation function to prevent the callback from running.
// Useful for interrupting some logic when the context is done,
// but you want to not bother about context cancellation if your logic is already done.
// Example:
// stopCW := contextWatchdog(ctx, func() { /* do something */ })
// // do something else
// defer stopCW()
//
// The watchdog goroutine exits when either the context is done or the returned
// cancel function is called. cancel blocks until the goroutine has exited, so
// once it returns the callback is either finished or will never run; a callback
// that has not started by the time cancel is called is skipped even if the
// context is already done.
func contextWatchdog(ctx context.Context, callback func()) (cancel func()) {
	exit := make(chan struct{})
	done := make(chan struct{})

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
