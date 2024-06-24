package issues

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test1229(t *testing.T) {
	const queryTimeout = 2 * time.Second

	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)
	ctx := context.Background()
	const ddl = "CREATE TABLE IF NOT EXISTS test_1229 (`test1` String, `test2` String) Engine = Memory"
	require.NoError(t, conn.Exec(ctx, ddl))

	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_1229"))
	}()

	const insertQuery = "INSERT INTO test_1229 VALUES ('test1value%d', 'test2value%d')"
	for i := 0; i < 100; i++ {
		withTimeoutCtx, cancel := context.WithTimeout(ctx, queryTimeout)
		require.NoError(t, conn.Exec(withTimeoutCtx, fmt.Sprintf(insertQuery, i, i)))
		cancel()
	}

	wg := new(sync.WaitGroup)
	const selectQuery = "SELECT test1, test2 FROM test_1229"
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			withTimeoutCtx, cancel := context.WithTimeout(ctx, queryTimeout)
			defer cancel()
			rows, err := conn.Query(withTimeoutCtx, selectQuery)
			require.NoError(t, err)
			require.NoError(t, rows.Close())
		}()
	}

	wg.Wait()

	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		openConnections := conn.Stats().Open
		assert.Zerof(ct, openConnections, "open connections: %d", openConnections)
	}, time.Second*5, time.Millisecond*10)
}
