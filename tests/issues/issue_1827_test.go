package issues

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

// Test1827 is a regression test for https://github.com/ClickHouse/clickhouse-go/issues/1827.
// PrepareBatch used to split the INSERT column list on every comma, so a column
// name containing a comma inside a backtick- or double-quoted identifier was
// split into multiple bogus columns and the batch failed. This verifies the
// end-to-end round-trip against a real server for both the native driver.Conn
// and database/sql APIs, and for both the backtick and double-quoted forms of
// a comma-containing identifier, on both protocols.
func Test1827(t *testing.T) {
	ctx := context.Background()
	quotedColumns := []struct {
		name string
		col  string
	}{
		{name: "backtick-quoted column", col: "`my_weird,col2`"},
		{name: "double-quoted column", col: `"my_weird,col2"`},
	}

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(protocol.String(), func(t *testing.T) {
			for _, tc := range quotedColumns {
				t.Run(tc.name, func(t *testing.T) {
					t.Run("native API", func(t *testing.T) {
						runIssue1827Native(t, ctx, protocol, tc.col)
					})
					t.Run("database/sql", func(t *testing.T) {
						runIssue1827Std(t, ctx, protocol, tc.col)
					})
				})
			}
		})
	}
}

func runIssue1827Native(t *testing.T, ctx context.Context, protocol clickhouse.Protocol, quotedCol string) {
	conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	t.Cleanup(func() { conn.Exec(ctx, "DROP TABLE IF EXISTS issue_1827") })
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf("CREATE TABLE issue_1827 (col1 UInt8, %s UInt8, col3 UInt8) ENGINE = Memory", quotedCol)))

	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO issue_1827 (col1, %s, col3)", quotedCol))
	require.NoError(t, err)
	require.NoError(t, batch.Append(uint8(1), uint8(2), uint8(3)))
	require.NoError(t, batch.Send())

	var got uint8
	require.NoError(t, conn.QueryRow(ctx, fmt.Sprintf("SELECT %s FROM issue_1827", quotedCol)).Scan(&got))
	require.Equal(t, uint8(2), got)
}

func runIssue1827Std(t *testing.T, ctx context.Context, protocol clickhouse.Protocol, quotedCol string) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)

	db, err := clickhouse_std_tests.GetDSNConnection("issues", protocol, useSSL, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	t.Cleanup(func() { db.ExecContext(ctx, "DROP TABLE IF EXISTS issue_1827") })
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE issue_1827 (col1 UInt8, %s UInt8, col3 UInt8) ENGINE = Memory", quotedCol))
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)
	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf("INSERT INTO issue_1827 (col1, %s, col3)", quotedCol))
	require.NoError(t, err)
	_, err = stmt.Exec(1, 2, 3)
	require.NoError(t, err)
	require.NoError(t, stmt.Close())
	require.NoError(t, tx.Commit())

	var got uint8
	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("SELECT %s FROM issue_1827", quotedCol)).Scan(&got))
	require.Equal(t, uint8(2), got)
}
