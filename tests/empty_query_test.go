
package tests

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestEmptyQuery(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()
		require.NoError(t, err)
		const ddl = `
		CREATE TEMPORARY TABLE test_empty_query (
			  Col1 UInt8
			, Col2 Array(UInt8)
			, Col3 LowCardinality(String)
			, NestedCol  Nested (
				  First  UInt32
				, Second UInt32
			)
		)
		`
		require.NoError(t, conn.Exec(ctx, ddl))
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
		defer cancel()
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_empty_query")
		require.NoError(t, err)
		require.Equal(t, 0, batch.Rows())
		assert.NoError(t, batch.Send())
	})
}
