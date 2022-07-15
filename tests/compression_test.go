package tests

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestZSTDCompression(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionZSTD,
			},
			MaxOpenConns: 1,
		})
	)
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_array (
			  Col1 Array(String)
		) Engine Memory
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_array")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_array")
	require.NoError(t, err)
	var (
		col1Data = []string{"A", "b", "c"}
	)
	for i := 0; i < 10; i++ {
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

}
