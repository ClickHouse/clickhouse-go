package issues

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func Test1297(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	conn, err := clickhouse_tests.TestClientWithDefaultOptions(testEnv, clickhouse.Settings{
		"flatten_nested": "0",
	})
	require.NoError(t, err)

	require.NoError(t, conn.Exec(context.Background(), `CREATE TABLE test_1297
(
    Id UInt8,
    Device LowCardinality(String),
    Nestme Nested(
    	Id UInt32,
		TestLC LowCardinality(String),
		Test String
	)
)
ENGINE = MergeTree
ORDER BY Id;`), "Create table failed")
	t.Cleanup(func() {
		conn.Exec(context.Background(), "DROP TABLE IF EXISTS test_1297")
	})

	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO test_1297")
	require.NoError(t, err, "PrepareBatch failed")

	require.NoError(t, batch.Append(uint8(1), "pc", []any{[]any{1, "test LC 1", "test"}, []any{2, "test LC 2", "test"}}), "Append failed")
	require.NoError(t, batch.Send())
}
