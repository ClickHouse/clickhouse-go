package issues

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func Test1229(t *testing.T) {
	const (
		queryTimeout = 4 * time.Second
		concurrency  = 100
	)

	var (
		conn, err = clickhouse_tests.GetConnectionTCPWithOptions("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		}, func(o *clickhouse.Options) {
			o.MaxOpenConns = concurrency
			o.MaxIdleConns = concurrency
		})
	)
	require.NoError(t, err)
	ctx := context.Background()
	const ddl = "CREATE TABLE IF NOT EXISTS test_1229 (`test1` String, `test2` String) Engine = MergeTree ORDER BY tuple()"
	require.NoError(t, conn.Exec(ctx, ddl))

	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_1229"))
	}()

	const insertQuery = "INSERT INTO test_1229 VALUES ('test1value%d', 'test2value%d')"
	for i := 0; i < concurrency; i++ {
		withTimeoutCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		require.NoError(t, conn.Exec(withTimeoutCtx, fmt.Sprintf(insertQuery, i, i)))
		cancel()
	}

	wg := new(sync.WaitGroup)
	const selectQuery = "SELECT test1, test2 FROM test_1229"
	errTestQueryTimeout := fmt.Errorf("Test1229: query budget %s exceeded", queryTimeout)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			withTimeoutCtx, cancel := context.WithTimeoutCause(ctx, queryTimeout, errTestQueryTimeout)
			defer cancel()
			rows, err := conn.Query(withTimeoutCtx, selectQuery)
			// Query returns nil rows on every error path, so Close only after a successful Query.
			if err != nil {
				if !queryBudgetExceeded(withTimeoutCtx, err) {
					require.NoErrorf(t, err, "query failed; ctx cause=%v", context.Cause(withTimeoutCtx))
				}
				return
			}
			if closeErr := rows.Close(); closeErr != nil && !queryBudgetExceeded(withTimeoutCtx, closeErr) {
				require.NoErrorf(t, closeErr, "rows.Close failed; ctx cause=%v", context.Cause(withTimeoutCtx))
			}
		}()
	}

	wg.Wait()

	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		openConnections := conn.Stats().Open
		assert.Zerof(ct, openConnections, "open connections: %d", openConnections)
	}, time.Second*5, time.Millisecond*10)

	// sleepEachRow runs for about a second, so this budget hits the timeout path
	// on a machine fast enough for the selects above to finish within 4s.
	t.Run("streaming query budget", func(t *testing.T) {
		const budget = 300 * time.Millisecond
		budgetCtx, cancel := context.WithTimeout(ctx, budget)
		defer cancel()

		const streamQuery = "SELECT sleepEachRow(0.001) FROM system.numbers LIMIT 1000 SETTINGS max_block_size=10"
		rows, err := conn.Query(budgetCtx, streamQuery)
		if err != nil {
			if !queryBudgetExceeded(budgetCtx, err) {
				require.NoErrorf(t, err, "query failed; ctx cause=%v", context.Cause(budgetCtx))
			}
		} else if closeErr := rows.Close(); closeErr != nil && !queryBudgetExceeded(budgetCtx, closeErr) {
			require.NoErrorf(t, closeErr, "rows.Close failed; ctx cause=%v", context.Cause(budgetCtx))
		}

		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			openConnections := conn.Stats().Open
			assert.Zerof(ct, openConnections, "open connections: %d", openConnections)
		}, time.Second*5, time.Millisecond*10)
	})
}

// queryBudgetExceeded reports whether err is the client query budget expiring:
// the query context is done or its deadline has elapsed, and err is a timeout.
// Cancellation requires a done context. processImpl wraps the socket error,
// so match it with errors.Is and errors.As.
func queryBudgetExceeded(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return ctx.Err() != nil
	}
	if ctx.Err() == nil {
		// The socket deadline can expire before the context timer runs.
		deadline, ok := ctx.Deadline()
		if !ok || time.Now().Before(deadline) {
			return false
		}
	}
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
