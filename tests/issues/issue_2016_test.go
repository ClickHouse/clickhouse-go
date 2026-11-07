package issues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test2016 verifies that appending the rows of a query to a batch returns the error of a query that fails in the
// middle of its result, instead of leaving the batch with the rows that were read before the error.
// See https://github.com/ClickHouse/clickhouse-go/issues/2016.
func Test2016(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	conn, err := clickhouse_tests.GetConnection("issues", t, clickhouse.Native, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	ctx := context.Background()

	const tableName = "test_2016"
	require.NoError(t, conn.Exec(ctx, "CREATE TABLE "+tableName+" (x UInt64) Engine MergeTree() ORDER BY x"))
	t.Cleanup(func() {
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName); err != nil {
			t.Logf("DROP TABLE %s failed: %v", tableName, err)
		}
	})

	// Small blocks, so that the first ones arrive before the exception.
	rows, err := conn.Query(ctx, "SELECT number FROM numbers(1000000) WHERE NOT throwIf(number = 500000) SETTINGS max_block_size = 1000, max_threads = 1")
	require.NoError(t, err)
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+tableName)
	require.NoError(t, err)

	err = batch.Append(rows)
	var exception *clickhouse.Exception
	require.ErrorAs(t, err, &exception)
	require.Equal(t, int32(395), exception.Code) // FUNCTION_THROW_IF_VALUE_IS_NON_ZERO
}
