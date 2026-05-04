package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func Test1801(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	conn, err := clickhouse_tests.TestClientWithDefaultSettings(testEnv)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	require.NoError(t, conn.Exec(context.Background(), `CREATE TABLE test_1801
	(
		ID UInt64,
		build_config LowCardinality(String),
		allocation_id String
	)
	ENGINE = MergeTree
	ORDER BY ID;`), "Create table failed")
	t.Cleanup(func() {
		if err := conn.Exec(context.Background(), "DROP TABLE IF EXISTS test_1801"); err != nil {
			t.Logf("DROP TABLE test_1801 failed: %v", err)
		}
	})

	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO test_1801")
	require.NoError(t, err, "PrepareBatch failed")

	for i := range 10 {
		require.NoError(t, batch.Append(uint64(i), "config_value", "alloc_123"), "Append failed")
	}

	require.NoError(t, batch.Send())

	var count uint64
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT COUNT() FROM test_1801").Scan(&count))
	require.Equal(t, uint64(10), count)
}

func Test1801ManyRows(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	conn, err := clickhouse_tests.TestClientWithDefaultSettings(testEnv)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	require.NoError(t, conn.Exec(context.Background(), `CREATE TABLE test_1801_many
	(
		ID UInt64,
		build_config LowCardinality(String),
		allocation_id String
	)
	ENGINE = MergeTree
	ORDER BY ID;`), "Create table failed")
	t.Cleanup(func() {
		if err := conn.Exec(context.Background(), "DROP TABLE IF EXISTS test_1801_many"); err != nil {
			t.Logf("DROP TABLE test_1801_many failed: %v", err)
		}
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
	conn, err := clickhouse_tests.TestClientWithDefaultSettings(testEnv)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	require.NoError(t, conn.Exec(context.Background(), `CREATE TABLE test_1801_flush
	(
		Col1 LowCardinality(String)
	)
	ENGINE = MergeTree
	ORDER BY tuple();`), "Create table failed")
	t.Cleanup(func() {
		if err := conn.Exec(context.Background(), "DROP TABLE IF EXISTS test_1801_flush"); err != nil {
			t.Logf("DROP TABLE test_1801_flush failed: %v", err)
		}
	})

	for batchNum := range 3 {
		batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO test_1801_flush")
		require.NoError(t, err, "PrepareBatch failed for batch %d", batchNum)

		for i := range 100 {
			require.NoError(t, batch.Append(fmt.Sprintf("batch_%d_value_%c", batchNum, 'A'+i%26)), "Append failed at batch %d, row %d", batchNum, i)
		}

		require.NoError(t, batch.Send(), "Send failed for batch %d", batchNum)
	}

	var count uint64
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT COUNT() FROM test_1801_flush").Scan(&count))
	require.Equal(t, uint64(300), count)
}

func Test1801FlushReuseHTTP(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)

	protocols := []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP}
	for _, protocol := range protocols {
		t.Run(fmt.Sprintf("%v", protocol), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			tableName := fmt.Sprintf("test_1801_flush_%v", protocol)
			require.NoError(t, conn.Exec(context.Background(), fmt.Sprintf(`CREATE TABLE %s
			(
				Col1 LowCardinality(String)
			)
			ENGINE = MergeTree
			ORDER BY tuple();`, tableName)), "Create table failed")
			t.Cleanup(func() {
				if err := conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
					t.Logf("DROP TABLE %s failed: %v", tableName, err)
				}
			})

			for batchNum := range 3 {
				batch, err := conn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s", tableName))
				require.NoError(t, err, "PrepareBatch failed for batch %d", batchNum)

				for i := range 100 {
					require.NoError(t, batch.Append(fmt.Sprintf("batch_%d_value_%c", batchNum, 'A'+i%26)), "Append failed at batch %d, row %d", batchNum, i)
				}

				require.NoError(t, batch.Send(), "Send failed for batch %d", batchNum)
			}

			var count uint64
			require.NoError(t, conn.QueryRow(context.Background(), fmt.Sprintf("SELECT COUNT() FROM %s", tableName)).Scan(&count))
			require.Equal(t, uint64(300), count)
		})
	}
}
