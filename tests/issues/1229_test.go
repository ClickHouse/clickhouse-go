package issues

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func Test1229(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time":             60,
			"allow_experimental_object_type": true,
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
		withTimeoutCtx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
		require.NoError(t, conn.Exec(withTimeoutCtx, fmt.Sprintf(insertQuery, i, i)))
		cancel()
	}

	wg := new(sync.WaitGroup)
	const selectQuery = "SELECT test1, test2 FROM test_1229"
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			withTimeoutCtx, cancel := context.WithTimeout(ctx, time.Millisecond*10)
			defer cancel()
			_, _ = conn.Query(withTimeoutCtx, selectQuery)
		}()
	}

	wg.Wait()

	openConnections := conn.Stats().Open
	require.Equal(t, 0, openConnections)
}
