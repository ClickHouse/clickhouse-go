package issues

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func Test1801(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	conn, err := clickhouse_tests.TestClientWithDefaultOptions(testEnv, clickhouse.Settings{})
	require.NoError(t, err)

	require.NoError(t, conn.Exec(context.Background(), `CREATE TABLE test_1801
	(
		ID UInt64,
		build_config LowCardinality(String),
		allocation_id String
	)
	ENGINE = MergeTree
	ORDER BY ID;`), "Create table failed")
	t.Cleanup(func() {
		conn.Exec(context.Background(), "DROP TABLE IF EXISTS test_1801")
	})

	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO test_1801")
	require.NoError(t, err, "PrepareBatch failed")

	for i := range 10 {
		require.NoError(t, batch.Append(uint64(i), "config_value", "alloc_123"), "Append failed")
	}

	require.NoError(t, batch.Send())
}

func Test1801ManyRows(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	conn, err := clickhouse_tests.TestClientWithDefaultOptions(testEnv, clickhouse.Settings{})
	require.NoError(t, err)

	require.NoError(t, conn.Exec(context.Background(), `CREATE TABLE test_1801_many
	(
		ID UInt64,
		build_config LowCardinality(String),
		allocation_id String
	)
	ENGINE = MergeTree
	ORDER BY ID;`), "Create table failed")
	t.Cleanup(func() {
		conn.Exec(context.Background(), "DROP TABLE IF EXISTS test_1801_many")
	})

	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO test_1801_many")
	require.NoError(t, err, "PrepareBatch failed")

	for i := range 500 {
		require.NoError(t, batch.Append(uint64(i), "config_value", "alloc_123"), "Append failed at row %d", i)
	}

	require.NoError(t, batch.Send())

	var count uint64
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT COUNT() FROM test_1801_many").Scan(&count))
	require.Equal(t, uint64(500), count)
}

func Test1801FlushReuse(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	conn, err := clickhouse_tests.TestClientWithDefaultOptions(testEnv, clickhouse.Settings{})
	require.NoError(t, err)

	require.NoError(t, conn.Exec(context.Background(), `CREATE TABLE test_1801_flush
	(
		Col1 LowCardinality(String)
	)
	ENGINE = MergeTree
	ORDER BY tuple();`), "Create table failed")
	t.Cleanup(func() {
		conn.Exec(context.Background(), "DROP TABLE IF EXISTS test_1801_flush")
	})

	for batchNum := range 3 {
		batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO test_1801_flush")
		require.NoError(t, err, "PrepareBatch failed for batch %d", batchNum)

		for i := range 100 {
			require.NoError(t, batch.Append("value_"+string(rune('A'+i%26))), "Append failed at batch %d, row %d", batchNum, i)
		}

		require.NoError(t, batch.Send(), "Send failed for batch %d", batchNum)
	}

	var count uint64
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT COUNT() FROM test_1801_flush").Scan(&count))
	require.Equal(t, uint64(300), count)
}
