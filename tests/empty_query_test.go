package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestEmptyQuery(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		// TEMPORARY TABLE lives only inside a session. Native uses the stateful connection; HTTP
		// needs an explicit session_id (an HTTP-interface concept, so only set it for HTTP).
		var settings clickhouse.Settings
		if protocol == clickhouse.HTTP {
			settings = clickhouse.Settings{"session_id": t.Name()}
		}
		conn, err := GetNativeConnection(t, protocol, settings, nil, &clickhouse.Compression{
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
