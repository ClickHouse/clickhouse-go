package clickhouse

import (
	"context"
	"runtime"
	"strconv"
	"strings"
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
		if isGo124OrLess(t) {
			t.Skipf("Skipping test as it involves incompatible Go version %s to use `synctest` package", runtime.Version())
		}

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
}

func isGo124OrLess(t *testing.T) bool {
	t.Helper()

	// always in "go1.[minor]" format.
	v := runtime.Version()
	parts := strings.FieldsFunc(v, func(r rune) bool {
		return r == '.'
	})

	if len(parts) != 2 {
		return false
	}

	minor, err := strconv.Atoi(parts[0])
	assert.NoError(t, err)
	if minor <= 24 {
		return true
	}

	return false
}
