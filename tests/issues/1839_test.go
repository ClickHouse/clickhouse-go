package issues

import (
	"context"
	"testing"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func Test1839(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	conn, err := clickhouse_tests.TestClientWithDefaultSettings(testEnv)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	require.NoError(t, conn.Exec(t.Context(), `
			CREATE TABLE test_1839 (
		  col Enum8('a\'b' = 1, 'c\'d' = 2, 'a\'b\'c' = 3)
		) ENGINE = MergeTree ORDER BY tuple()
		`), "Create table failed")

	t.Cleanup(func() {
		if err := conn.Exec(context.Background(), "DROP TABLE IF EXISTS test_1839"); err != nil {
			t.Logf("DROP TABLE test_1839 failed: %v", err)
		}
	})

	batch, err := conn.PrepareBatch(t.Context(), "INSERT INTO test_1839")
	require.NoError(t, err, "PrepareBatch failed")
	require.NoError(t, batch.Append("a'b"), "Append failed for %q", "a'b")
	require.NoError(t, batch.Append("c'd"), "Append failed for %q", "c'd")
	require.NoError(t, batch.Append("a'b'c"), "Append failed for %q", "a'b'c")
	require.NoError(t, batch.Send(), "Send failed")

	rows, err := conn.Query(t.Context(), "SELECT col FROM test_1839 ORDER BY col")
	require.NoError(t, err, "SELECT col failed")
	defer rows.Close()

	var results []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		results = append(results, s)
	}
	require.NoError(t, rows.Err(), "Scan failed")
	require.Equal(t, []string{"a'b", "c'd", "a'b'c"}, results)
}
