package tests

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBoolUInt8(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		require.NoError(t, err)

		ctx := context.Background()
		const ddl = `
			CREATE TABLE IF NOT EXISTS issue_1050 (
				  Col1 UInt8
				, Col2 UInt8                   
			) Engine MergeTree() ORDER BY tuple()
		`
		require.NoError(t, conn.Exec(ctx, ddl))
		defer func() {
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS issue_1050"))
		}()

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_1050 (Col1, Col2)")
		require.NoError(t, err)
		require.NoError(t, batch.Append(true, false))
		require.NoError(t, batch.Send())

		row := conn.QueryRow(ctx, "SELECT Col1, Col2 from issue_1050")
		require.NoError(t, err)

		var (
			col1 bool
			col2 bool
		)
		require.NoError(t, row.Scan(&col1, &col2))
		require.True(t, col1)
		require.False(t, col2)
	})
}
