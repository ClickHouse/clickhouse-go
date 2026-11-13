package issues

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test2076BatchSendDeadlineKeepsPoolUsable is a pool usability soak for the
// scenario in https://github.com/ClickHouse/clickhouse-go/issues/2076: a batch
// whose context deadline ends while Send is running. Whatever the deadline
// does to each Send, the next query on the pool must get a working connection.
//
// This test does not pin the watchdog fix. A real server cannot force the
// interleaving: Send returns only after the watchdog goroutine has been
// joined, so the callback cannot fire after Send has returned. The
// deterministic unit test TestBatchSendWatchdogClosedConnNotReleasedAsHealthy
// in the root package covers the fix instead.
//
// The watchdog only exists on the native protocol batch path, so this test
// does not run for HTTP.
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

	for i := 0; i < 10; i++ {
		batchCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		b, err := conn.PrepareBatch(batchCtx, "INSERT INTO function null('x UInt64')")
		if err != nil {
			// The deadline can end before the batch is prepared.
			cancel()
			continue
		}
		require.NoError(t, b.Append(uint64(i)), "round %d: append", i)
		if err := b.Send(); err != nil {
			// Either outcome for Send is fine. What this test asserts is
			// that the pool still serves the next query.
			t.Logf("round %d: send returned %v", i, err)
		}
		cancel()

		var got uint8
		require.NoError(t, conn.QueryRow(ctx, "SELECT 1").Scan(&got), "round %d: query after send", i)
	}
}
