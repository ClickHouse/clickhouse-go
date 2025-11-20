package tests

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestWithTotals(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		if protocol == clickhouse.HTTP {
			t.Skip("Only test Totals for Native")
		}

		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()
		require.NoError(t, err)
		const query = `
		SELECT
			number AS n
			, COUNT()
		FROM (
			SELECT number FROM system.numbers LIMIT 100
		) GROUP BY n WITH TOTALS
		`
		rows, err := conn.Query(ctx, query)
		require.NoError(t, err)

		var count int
		for rows.Next() {
			count++
			var (
				n uint64
				c uint64
			)
			require.NoError(t, rows.Scan(&n, &c))
		}
		require.Equal(t, 100, count)
		var (
			n, totals uint64
		)
		require.NoError(t, rows.Totals(&n, &totals))
		assert.Equal(t, uint64(0), n)
		assert.Equal(t, uint64(100), totals)
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}
