package issues

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func Test1356(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	conn, err := clickhouse_tests.TestClientWithDefaultSettings(testEnv)
	require.NoError(t, err)

	require.NoError(t, conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS test_1356 (Col String) Engine MergeTree() ORDER BY tuple()`))
	defer conn.Exec(context.Background(), "DROP TABLE test_1356")

	var testCases = map[string]clickhouse.Settings{
		"async_insert=0": {
			"async_insert": 0,
		},
		"async_insert=1": {
			"async_insert": 1,
		},
	}

	for name, tc := range testCases {
		settings := tc

		t.Run(name, func(t *testing.T) {
			require.NoError(t, conn.Exec(context.Background(), "TRUNCATE TABLE test_1356"))

			ctx := clickhouse.Context(context.Background(), clickhouse.WithParameters(clickhouse.Parameters{
				"p1": "Hello world",
			}), clickhouse.WithSettings(settings))

			require.NoError(t, conn.Exec(ctx, "INSERT INTO test_1356 (Col) VALUES ({p1:String})"))

			row := conn.QueryRow(context.Background(), "SELECT * FROM test_1356")
			require.NoError(t, row.Err())
			var col string
			require.NoError(t, row.Scan(&col))
			require.Equal(t, "Hello world", col)
		})
	}
}
