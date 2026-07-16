package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test1919 verifies that PrepareBatch preserves an inline SETTINGS clause placed after
// the column list instead of silently dropping it from the query sent to the server.
// See https://github.com/ClickHouse/clickhouse-go/issues/1919.
func Test1919(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("%v", protocol), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			tableName := fmt.Sprintf("test_1919_%v", protocol)
			require.NoError(t, conn.Exec(context.Background(), fmt.Sprintf(`CREATE TABLE %s
			(
				col1 UInt64,
				col2 String
			)
			ENGINE = MergeTree
			ORDER BY col1;`, tableName)), "Create table failed")
			t.Cleanup(func() {
				if err := conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
					t.Logf("DROP TABLE %s failed: %v", tableName, err)
				}
			})

			// A valid SETTINGS clause after the column list must be accepted and the
			// rows inserted. Before the fix the clause was dropped from the normalized
			// query, so this only exercises the happy path staying intact.
			batch, err := conn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s (col1, col2) SETTINGS async_insert=0", tableName))
			require.NoError(t, err, "PrepareBatch with SETTINGS after column list failed")
			for i := range 10 {
				require.NoError(t, batch.Append(uint64(i), "value"))
			}
			require.NoError(t, batch.Send())

			var count uint64
			require.NoError(t, conn.QueryRow(context.Background(), fmt.Sprintf("SELECT count() FROM %s", tableName)).Scan(&count))
			require.Equal(t, uint64(10), count)

			// The SETTINGS clause must actually reach the server: an unknown setting has
			// to surface as an error rather than being silently dropped. Before the fix
			// the clause was stripped, so the insert succeeded and no error was raised.
			send := func(query string) error {
				b, err := conn.PrepareBatch(context.Background(), query)
				if err != nil {
					return err
				}
				if err = b.Append(uint64(1), "value"); err != nil {
					return err
				}
				return b.Send()
			}
			err = send(fmt.Sprintf("INSERT INTO %s (col1, col2) SETTINGS nonexistent_setting_for_test_1919=1", tableName))
			require.ErrorContains(t, err, "nonexistent_setting_for_test_1919")
		})
	}
}
