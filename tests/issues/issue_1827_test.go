package issues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test1827 is a regression test for https://github.com/ClickHouse/clickhouse-go/issues/1827.
// PrepareBatch used to split the INSERT column list on every comma, so a column
// name containing a comma inside a backtick-quoted identifier was split into
// multiple bogus columns and the batch failed. This verifies the end-to-end
// round-trip against a real server on both protocols.
func Test1827(t *testing.T) {
	ctx := context.Background()
	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(protocol.String(), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			t.Cleanup(func() { conn.Exec(ctx, "DROP TABLE IF EXISTS issue_1827") })
			require.NoError(t, conn.Exec(ctx, "CREATE TABLE issue_1827 (col1 UInt8, `my_weird,col2` UInt8, col3 UInt8) ENGINE = Memory"))

			batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_1827 (col1, `my_weird,col2`, col3)")
			require.NoError(t, err)
			require.NoError(t, batch.Append(uint8(1), uint8(2), uint8(3)))
			require.NoError(t, batch.Send())

			var got uint8
			require.NoError(t, conn.QueryRow(ctx, "SELECT `my_weird,col2` FROM issue_1827").Scan(&got))
			require.Equal(t, uint8(2), got)
		})
	}
}
