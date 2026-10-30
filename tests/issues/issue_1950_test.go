package issues

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

var issue1950Tables = []string{"issue_1950", "issue_1950_other"}

// Test1950 is a regression test for https://github.com/ClickHouse/clickhouse-go/issues/1950.
// PrepareBatch did not treat "//" as the start of a line comment, and it ended a block
// comment at the first "*/" even when that closed a comment nested inside it. An INSERT
// written in such a comment was parsed instead of the real statement, so the rows went to
// the table named in the comment without any error.
func Test1950(t *testing.T) {
	ctx := context.Background()
	queries := []struct {
		name  string
		query string
	}{
		{
			name:  "line comment",
			query: "// INSERT INTO issue_1950_other (col1, col2)\nINSERT INTO issue_1950 (col1, col2)",
		},
		{
			name:  "nested block comment",
			query: "/* old /* note */ INSERT INTO issue_1950_other (col1, col2) */ INSERT INTO issue_1950 (col1, col2)",
		},
	}

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(protocol.String(), func(t *testing.T) {
			for _, tc := range queries {
				t.Run(tc.name, func(t *testing.T) {
					t.Run("native API", func(t *testing.T) {
						runIssue1950Native(t, ctx, protocol, tc.query)
					})
					t.Run("database/sql", func(t *testing.T) {
						runIssue1950Std(t, ctx, protocol, tc.query)
					})
				})
			}
		})
	}
}

func runIssue1950Native(t *testing.T, ctx context.Context, protocol clickhouse.Protocol, query string) {
	conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	for _, table := range issue1950Tables {
		t.Cleanup(func() { conn.Exec(ctx, "DROP TABLE IF EXISTS "+table) })
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (col1 UInt8, col2 String) ENGINE = MergeTree ORDER BY tuple()", table)))
	}

	batch, err := conn.PrepareBatch(ctx, query)
	require.NoError(t, err)
	require.NoError(t, batch.Append(uint8(1), "a"))
	require.NoError(t, batch.Send())

	var count uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM issue_1950").Scan(&count))
	assert.Equal(t, uint64(1), count)
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM issue_1950_other").Scan(&count))
	assert.Equal(t, uint64(0), count)
}

func runIssue1950Std(t *testing.T, ctx context.Context, protocol clickhouse.Protocol, query string) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)

	db, err := clickhouse_std_tests.GetDSNConnection("issues", protocol, useSSL, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	for _, table := range issue1950Tables {
		t.Cleanup(func() { db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table) })
		_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (col1 UInt8, col2 String) ENGINE = MergeTree ORDER BY tuple()", table))
		require.NoError(t, err)
	}

	tx, err := db.Begin()
	require.NoError(t, err)
	t.Cleanup(func() { tx.Rollback() })
	stmt, err := tx.PrepareContext(ctx, query)
	require.NoError(t, err)
	_, err = stmt.Exec(uint8(1), "a")
	require.NoError(t, err)
	require.NoError(t, stmt.Close())
	require.NoError(t, tx.Commit())

	var count uint64
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count() FROM issue_1950").Scan(&count))
	assert.Equal(t, uint64(1), count)
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count() FROM issue_1950_other").Scan(&count))
	assert.Equal(t, uint64(0), count)
}
