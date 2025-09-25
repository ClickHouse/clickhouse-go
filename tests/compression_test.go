
package tests

import (
	"context"
	"github.com/ClickHouse/ch-go/compress"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestZSTDCompression(t *testing.T) {
	CompressionTest(t, compress.LevelZero, clickhouse.CompressionZSTD)
}

func TestLZ4Compression(t *testing.T) {
	CompressionTest(t, compress.Level(3), clickhouse.CompressionLZ4)
}

func TestLZ4HCCompression(t *testing.T) {
	CompressionTest(t, compress.LevelLZ4HCDefault, clickhouse.CompressionLZ4HC)
}

func TestNoCompression(t *testing.T) {
	CompressionTest(t, compress.LevelZero, clickhouse.CompressionNone)
}

func CompressionTest(t *testing.T, level compress.Level, method clickhouse.CompressionMethod) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: method,
			Level:  int(level),
		})
		ctx := context.Background()
		require.NoError(t, err)
		const ddl = `
		CREATE TABLE test_array (
			  Col1 Array(String)
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_array")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_array")
		require.NoError(t, err)
		var (
			col1Data = []string{"A", "b", "c"}
		)
		for i := 0; i < 100; i++ {
			require.NoError(t, batch.Append(col1Data))
		}
		require.NoError(t, batch.Send())
		rows, err := conn.Query(ctx, "SELECT * FROM test_array")
		require.NoError(t, err)
		for rows.Next() {
			var (
				col1 []string
			)
			require.NoError(t, rows.Scan(&col1))
			assert.Equal(t, col1Data, col1)
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}
