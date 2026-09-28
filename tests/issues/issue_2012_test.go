package issues

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

// TestIssue2012_PrepareBatch_WithoutSpaceBeforeColumns is a regression test for
// https://github.com/ClickHouse/clickhouse-go/issues/2012:
// PrepareBatch("INSERT INTO t(a, b)") without a space between the table name and the
// opening parenthesis must extract the specified columns instead of failing to parse
// and falling back to all columns from DESCRIBE TABLE (which failed on HTTP transport).
// It also tests quoted table names containing parentheses.
func TestIssue2012_PrepareBatch_WithoutSpaceBeforeColumns(t *testing.T) {
	ctx := context.Background()
	protocols := []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP}

	t.Run("native and http driver", func(t *testing.T) {
		for _, protocol := range protocols {
			t.Run(protocol.String(), func(t *testing.T) {
				conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
				require.NoError(t, err)
				t.Cleanup(func() { conn.Close() })

				// Case 1: Standard table name with no space before columns on a table with an extra default column
				tableName := "issue_2012_" + protocol.String()
				require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName))
				require.NoError(t, conn.Exec(ctx, "CREATE TABLE "+tableName+" (col1 UInt32, col2 String, col3 UInt32 DEFAULT 42) ENGINE MergeTree ORDER BY col1"))
				t.Cleanup(func() { conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName) })

				batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+tableName+"(col1, col2)")
				require.NoError(t, err)
				require.NoError(t, batch.Append(uint32(1), "hello"))
				require.NoError(t, batch.Append(uint32(2), "world"))
				require.NoError(t, batch.Send())

				var (
					col1 uint32
					col2 string
					col3 uint32
				)
				require.NoError(t, conn.QueryRow(ctx, "SELECT col1, col2, col3 FROM "+tableName+" WHERE col1 = 1").Scan(&col1, &col2, &col3))
				assert.Equal(t, uint32(1), col1)
				assert.Equal(t, "hello", col2)
				assert.Equal(t, uint32(42), col3)

				// Case 2: Double-quoted table name containing parentheses
				quotedTable := "\"issue_2012(" + protocol.String() + ")\""
				require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTable))
				require.NoError(t, conn.Exec(ctx, "CREATE TABLE "+quotedTable+" (col1 UInt32, col2 String, col3 UInt32 DEFAULT 99) ENGINE MergeTree ORDER BY col1"))
				t.Cleanup(func() { conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTable) })

				batchQuoted, err := conn.PrepareBatch(ctx, "INSERT INTO "+quotedTable+"(col1, col2)")
				require.NoError(t, err)
				require.NoError(t, batchQuoted.Append(uint32(10), "quoted"))
				require.NoError(t, batchQuoted.Send())

				require.NoError(t, conn.QueryRow(ctx, "SELECT col1, col2, col3 FROM "+quotedTable+" WHERE col1 = 10").Scan(&col1, &col2, &col3))
				assert.Equal(t, uint32(10), col1)
				assert.Equal(t, "quoted", col2)
				assert.Equal(t, uint32(99), col3)

				// Case 3: Backtick-quoted table name containing parentheses
				backtickTable := "`issue_2012_bt(" + protocol.String() + ")`"
				require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+backtickTable))
				require.NoError(t, conn.Exec(ctx, "CREATE TABLE "+backtickTable+" (col1 UInt32, col2 String, col3 UInt32 DEFAULT 100) ENGINE MergeTree ORDER BY col1"))
				t.Cleanup(func() { conn.Exec(ctx, "DROP TABLE IF EXISTS "+backtickTable) })

				batchBT, err := conn.PrepareBatch(ctx, "INSERT INTO "+backtickTable+"(col1, col2)")
				require.NoError(t, err)
				require.NoError(t, batchBT.Append(uint32(20), "backtick"))
				require.NoError(t, batchBT.Send())

				require.NoError(t, conn.QueryRow(ctx, "SELECT col1, col2, col3 FROM "+backtickTable+" WHERE col1 = 20").Scan(&col1, &col2, &col3))
				assert.Equal(t, uint32(20), col1)
				assert.Equal(t, "backtick", col2)
				assert.Equal(t, uint32(100), col3)
			})
		}
	})

	t.Run("database/sql", func(t *testing.T) {
		useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
		require.NoError(t, err)

		for _, protocol := range protocols {
			t.Run(protocol.String(), func(t *testing.T) {
				db, err := clickhouse_std_tests.GetDSNConnection("issues", protocol, useSSL, nil)
				require.NoError(t, err)
				t.Cleanup(func() { db.Close() })

				tableName := "issue_2012_sql_" + protocol.String()
				_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tableName)
				require.NoError(t, err)
				_, err = db.ExecContext(ctx, "CREATE TABLE "+tableName+" (col1 UInt32, col2 String, col3 UInt32 DEFAULT 77) ENGINE MergeTree ORDER BY col1")
				require.NoError(t, err)
				t.Cleanup(func() { db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tableName) })

				stmt, err := db.PrepareContext(ctx, "INSERT INTO "+tableName+"(col1, col2)")
				require.NoError(t, err)
				defer stmt.Close()

				_, err = stmt.ExecContext(ctx, uint32(5), "std_sql")
				require.NoError(t, err)

				var (
					col1 uint32
					col2 string
					col3 uint32
				)
				require.NoError(t, db.QueryRowContext(ctx, "SELECT col1, col2, col3 FROM "+tableName+" WHERE col1 = 5").Scan(&col1, &col2, &col3))
				assert.Equal(t, uint32(5), col1)
				assert.Equal(t, "std_sql", col2)
				assert.Equal(t, uint32(77), col3)
			})
		}
	})
}
