package issues

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// lateWatchdogCtx delays Done() only when it is read from the goroutine that
// batch.Send's context watchdog starts. It models the watchdog goroutine not
// being scheduled until Send has stopped it: by the time the delayed Done()
// returns, the caller has cancelled the context, so the watchdog select can
// still pick the Done case and run its callback.
type lateWatchdogCtx struct {
	context.Context
	delay time.Duration
}

func (c lateWatchdogCtx) Done() <-chan struct{} {
	pcs := make([]uintptr, 16)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, "contextWatchdog") {
			time.Sleep(c.delay)
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Done()
}

// Test2076BatchSendWatchdogDoesNotPoisonPool reproduces
// https://github.com/ClickHouse/clickhouse-go/issues/2076. A batch context
// cancelled right after Send() returned could make the Send watchdog close the
// TCP connection after Send had returned it to the pool, so the next query on
// the pool failed with a closed network connection.
//
// The watchdog only exists on the native protocol batch path, so this test
// does not run for HTTP.
func Test2076BatchSendWatchdogDoesNotPoisonPool(t *testing.T) {
	testEnv, err := GetIssuesTestEnvironment()
	require.NoError(t, err)

	opts := clickhouse_tests.ClientOptionsFromEnv(testEnv, clickhouse.Settings{}, false)
	opts.MaxOpenConns = 1
	opts.MaxIdleConns = 1
	conn, err := clickhouse_tests.GetConnectionWithOptions(&opts)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		batchCtx, cancel := context.WithCancel(ctx)
		b, err := conn.PrepareBatch(lateWatchdogCtx{Context: batchCtx, delay: 100 * time.Millisecond}, "INSERT INTO function null('x UInt64')")
		require.NoError(t, err, "round %d: prepare batch", i)
		require.NoError(t, b.Append(uint64(i)), "round %d: append", i)
		require.NoError(t, b.Send(), "round %d: send", i)
		cancel()

		// The query must not inherit a connection the watchdog closed. The
		// sleep keeps it in flight while the delayed watchdog fires.
		var got uint8
		require.NoError(t, conn.QueryRow(ctx, "SELECT sleep(0.5)").Scan(&got), "round %d: query after send", i)
	}
}

// Test2076BatchSendDeadlineKeepsPoolUsable sends a batch whose context
// deadline ends while Send is running and then reuses the pool. Whatever the
// deadline does to Send, the next query must get a working connection.
// Like the test above it only covers the native protocol, where the watchdog
// lives.
func Test2076BatchSendDeadlineKeepsPoolUsable(t *testing.T) {
	testEnv, err := GetIssuesTestEnvironment()
	require.NoError(t, err)

	opts := clickhouse_tests.ClientOptionsFromEnv(testEnv, clickhouse.Settings{}, false)
	opts.MaxOpenConns = 1
	opts.MaxIdleConns = 1
	conn, err := clickhouse_tests.GetConnectionWithOptions(&opts)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	ctx := context.Background()

	sent := 0
	for i := 0; i < 10; i++ {
		batchCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		b, err := conn.PrepareBatch(batchCtx, "INSERT INTO function null('x UInt64')")
		if err != nil {
			// The deadline can end before the batch is prepared.
			cancel()
			continue
		}
		require.NoError(t, b.Append(uint64(i)), "round %d: append", i)
		if err := b.Send(); err != nil {
			t.Logf("round %d: send returned %v", i, err)
		} else {
			sent++
		}
		cancel()

		var got uint8
		require.NoError(t, conn.QueryRow(ctx, "SELECT 1").Scan(&got), "round %d: query after send", i)
	}
	require.NotZero(t, sent, "the deadline never let a batch send")
}
