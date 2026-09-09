package issues

import (
	"context"
	"database/sql"
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

			// A trailing `;` statement terminator on the inline SETTINGS form must be
			// tolerated. Before the fix it was folded into the normalized query as
			// "SETTINGS ...; FORMAT Native", which the server rejects, so the rows would
			// fail to insert.
			batchSemicolon, err := conn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s (col1, col2) SETTINGS async_insert=0;", tableName))
			require.NoError(t, err, "PrepareBatch with SETTINGS and trailing semicolon failed")
			for i := range 10 {
				require.NoError(t, batchSemicolon.Append(uint64(i), "value"))
			}
			require.NoError(t, batchSemicolon.Send())

			require.NoError(t, conn.QueryRow(context.Background(), fmt.Sprintf("SELECT count() FROM %s", tableName)).Scan(&count))
			require.Equal(t, uint64(20), count)

			// A single line comment after the inline SETTINGS form must not be folded
			// into the normalized query: it would comment out the FORMAT clause appended
			// after it, so the server would read the Native payload with its default
			// input format and the insert would fail.
			batchComment, err := conn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s (col1, col2) SETTINGS async_insert=0 -- inline comment", tableName))
			require.NoError(t, err, "PrepareBatch with SETTINGS and trailing comment failed")
			for i := range 10 {
				require.NoError(t, batchComment.Append(uint64(i), "value"))
			}
			require.NoError(t, batchComment.Send())

			require.NoError(t, conn.QueryRow(context.Background(), fmt.Sprintf("SELECT count() FROM %s", tableName)).Scan(&count))
			require.Equal(t, uint64(30), count)

			// A SETTINGS clause written over several lines must be captured as a whole
			// instead of being dropped, which is the original symptom of this issue.
			batchMultiline, err := conn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s (col1, col2) SETTINGS\n\tasync_insert=0,\n\twait_for_async_insert=0", tableName))
			require.NoError(t, err, "PrepareBatch with multiline SETTINGS failed")
			for i := range 10 {
				require.NoError(t, batchMultiline.Append(uint64(i), "value"))
			}
			require.NoError(t, batchMultiline.Send())

			require.NoError(t, conn.QueryRow(context.Background(), fmt.Sprintf("SELECT count() FROM %s", tableName)).Scan(&count))
			require.Equal(t, uint64(40), count)

			// Row data written after the inline SETTINGS clause is ignored: a batch sends
			// its rows in the request body, so the tuples must not be part of the query.
			// The server rejects them if they are.
			batchValues, err := conn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s (col1, col2) SETTINGS async_insert=0 values (1, 'a -- b')", tableName))
			require.NoError(t, err, "PrepareBatch with SETTINGS and row data failed")
			for i := range 10 {
				require.NoError(t, batchValues.Append(uint64(i), "value"))
			}
			require.NoError(t, batchValues.Send())

			require.NoError(t, conn.QueryRow(context.Background(), fmt.Sprintf("SELECT count() FROM %s", tableName)).Scan(&count))
			require.Equal(t, uint64(50), count)

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

			err = send(fmt.Sprintf("INSERT INTO %s (col1, col2) SETTINGS nonexistent_setting_for_test_1919=1 -- inline comment", tableName))
			require.ErrorContains(t, err, "nonexistent_setting_for_test_1919")

			err = send(fmt.Sprintf("INSERT INTO %s (col1, col2) SETTINGS\n\tasync_insert=0,\n\tnonexistent_setting_for_test_1919=1", tableName))
			require.ErrorContains(t, err, "nonexistent_setting_for_test_1919")
		})
	}

	// database/sql surface (clickhouse_std.go): Prepare routes through the same query
	// normalization, so the inline SETTINGS clause must behave identically there.
	t.Run("std", func(t *testing.T) {
		for _, useHTTP := range []bool{false, true} {
			proto := "native"
			if useHTTP {
				proto = "http"
			}
			t.Run(proto, func(t *testing.T) {
				opts := clickhouse_tests.ClientOptionsFromEnv(testEnv, nil, useHTTP)
				db, err := sql.Open("clickhouse", clickhouse_tests.OptionsToDSN(&opts))
				require.NoError(t, err)
				t.Cleanup(func() { db.Close() })

				tableName := fmt.Sprintf("test_1919_std_%s", proto)
				_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s
				(
					col1 UInt64,
					col2 String
				)
				ENGINE = MergeTree
				ORDER BY col1;`, tableName))
				require.NoError(t, err, "Create table failed")
				t.Cleanup(func() {
					if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
						t.Logf("DROP TABLE %s failed: %v", tableName, err)
					}
				})

				insert := func(query string) error {
					scope, err := db.Begin()
					if err != nil {
						return err
					}
					batch, err := scope.Prepare(query)
					if err != nil {
						return err
					}
					if _, err = batch.Exec(uint64(1), "value"); err != nil {
						return err
					}
					return scope.Commit()
				}

				require.NoError(t, insert(fmt.Sprintf("INSERT INTO %s (col1, col2) SETTINGS async_insert=0", tableName)))
				require.NoError(t, insert(fmt.Sprintf("INSERT INTO %s (col1, col2) SETTINGS async_insert=0 -- inline comment", tableName)))
				require.NoError(t, insert(fmt.Sprintf("INSERT INTO %s (col1, col2) SETTINGS\n\tasync_insert=0,\n\twait_for_async_insert=0", tableName)))
				require.NoError(t, insert(fmt.Sprintf("INSERT INTO %s (col1, col2) SETTINGS async_insert=0 values (1, 'a -- b')", tableName)))

				var count uint64
				require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT count() FROM %s", tableName)).Scan(&count))
				require.Equal(t, uint64(4), count)

				// The clause must reach the server rather than being silently dropped.
				err = insert(fmt.Sprintf("INSERT INTO %s (col1, col2) SETTINGS nonexistent_setting_for_test_1919=1", tableName))
				require.ErrorContains(t, err, "nonexistent_setting_for_test_1919")
			})
		}
	})
}
