package issues

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test2076 verifies that cancelling a batch context after Send does not close a pooled connection.
// See https://github.com/ClickHouse/clickhouse-go/issues/2076.
func Test2076(t *testing.T) {
	env, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)

	conn, err := clickhouse_tests.GetConnectionTCPWithOptions(
		testSet,
		clickhouse_tests.TestClientDefaultSettings(env),
		nil,
		nil,
		func(o *clickhouse.Options) {
			o.MaxOpenConns = 1
			o.MaxIdleConns = 1
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close connection: %v", err)
		}
	})

	bg := context.Background()
	tableName := fmt.Sprintf("test_2076_%d", time.Now().UnixNano())
	require.NoError(t, conn.Exec(bg, fmt.Sprintf("CREATE TABLE %s (x UInt64) ENGINE = Null", tableName)))
	t.Cleanup(func() {
		if err := conn.Exec(bg, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
			t.Errorf("drop table: %v", err)
		}
	})

	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithCancel(bg)
		batch, err := conn.PrepareBatch(lateWatchdogCtx{ctx}, fmt.Sprintf("INSERT INTO %s", tableName))
		require.NoErrorf(t, err, "round %d", i)
		require.NoErrorf(t, batch.Append(uint64(i)), "round %d", i)
		require.NoErrorf(t, batch.Send(), "round %d", i)
		cancel()

		var x uint8
		// Keep the pooled connection in use while the late watchdog can fire.
		require.NoErrorf(t, conn.QueryRow(bg, "SELECT sleep(0.2)").Scan(&x), "round %d", i)
	}
}

type lateWatchdogCtx struct{ context.Context }

func (c lateWatchdogCtx) Done() <-chan struct{} {
	pcs := make([]uintptr, 16)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, "contextWatchdog") {
			time.Sleep(50 * time.Millisecond)
			break
		}
		if !more {
			break
		}
	}
	return c.Context.Done()
}
