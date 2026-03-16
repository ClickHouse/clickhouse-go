package tests

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBindArrayInsert(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})

		ctx := context.Background()
		require.NoError(t, err)
		const ddl = `
		CREATE TABLE test_bind_array_insert (
			  Col1 String
			, Col2 Array(String)
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_bind_array_insert")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		arrayData := []string{"a", "b", "c"}
		err = conn.Exec(ctx, "INSERT INTO test_bind_array_insert (Col1, Col2) VALUES (?, ?)",
			"abc123", arrayData)
		require.NoError(t, err)
	})
}
