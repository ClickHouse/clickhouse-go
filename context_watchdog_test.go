package clickhouse

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestContextWatchdog(t *testing.T) {
	t.Run("callback should be called once", func(t *testing.T) {
		called := atomic.Int32{}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		stopCW := contextWatchdog(ctx, func() {
			called.Add(1)
		})

		<-ctx.Done()

		// Give it some more time to make sure watch dog has enough time to
		// call callback multiple times
		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, int32(1), called.Load(), "callback should be called only once")

		stopCW()
		assert.Equal(t, int32(1), called.Load(), "callback should be called only once even after stopping watchdog")
	})

	t.Run("callback should not be called during normal exit before context cancellation", func(t *testing.T) {
		called := atomic.Int32{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stopCW := contextWatchdog(ctx, func() {
			called.Add(1)
		})
		stopCW() // normal exit

		assert.Equal(t, int32(0), called.Load(), "callback should not be called during normal exit")
	})

	t.Run("No goroutines should be left out after stopping ContextWatchdog", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			// when context is cancelled
			called := atomic.Int32{}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()

			stopCW := contextWatchdog(ctx, func() {
				called.Add(1)
			})

			<-ctx.Done()

			// Give it some more time to make sure watch dog has enough time to
			// call callback multiple times
			time.Sleep(100 * time.Millisecond)
			assert.Equal(t, int32(1), called.Load(), "callback should be called only once")

			stopCW()
			assert.Equal(t, int32(1), called.Load(), "callback should be called only once even after stopping watchdog")

			synctest.Wait()
		})

		synctest.Test(t, func(t *testing.T) {
			// during normal exit
			called := atomic.Int32{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			stopCW := contextWatchdog(ctx, func() {
				called.Add(1)
			})
			stopCW() // normal exit

			assert.Equal(t, int32(0), called.Load(), "callback should not be called during normal exit")

			synctest.Wait()
		})
	})

	t.Run("stop should wait for a running callback to complete", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			started := make(chan struct{})
			release := make(chan struct{})
			completed := atomic.Bool{}

			stopCW := contextWatchdog(ctx, func() {
				close(started)
				<-release
				completed.Store(true)
			})

			cancel()
			<-started

			stopped := make(chan struct{})
			go func() {
				stopCW()
				close(stopped)
			}()

			// stopCW must block while the callback runs.
			synctest.Wait()
			select {
			case <-stopped:
				t.Fatal("stopCW returned before the callback completed")
			default:
			}

			close(release)
			<-stopped
			assert.True(t, completed.Load(), "callback should complete before stopCW returns")
		})
	})
}
