package issues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test1952 verifies that PrepareBatch inserts into the right table when the INSERT query
// has the optional TABLE keyword before the table name. The HTTP protocol describes the
// table by the name taken from the query, so it failed with UNKNOWN_TABLE.
// See https://github.com/ClickHouse/clickhouse-go/issues/1952.
func Test1952(t *testing.T) {
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		ctx := context.Background()
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS issue_1952"))
		require.NoError(t, conn.Exec(ctx, "CREATE TABLE issue_1952 (a UInt8, b String) ENGINE MergeTree ORDER BY tuple()"))
		t.Cleanup(func() { conn.Exec(ctx, "DROP TABLE IF EXISTS issue_1952") })

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO TABLE issue_1952 (a, b)")
		require.NoError(t, err)
		require.NoError(t, batch.Append(uint8(1), "x"))
		require.NoError(t, batch.Send())

		var (
			a uint8
			b string
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT a, b FROM issue_1952").Scan(&a, &b))
		assert.Equal(t, uint8(1), a)
		assert.Equal(t, "x", b)
	})
}
