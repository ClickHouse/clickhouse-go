
package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestSimpleAggregateFunction(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, clickhouse.Settings{
			"allow_experimental_json_type": true,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()
		require.NoError(t, err)
		if !CheckMinServerServerVersion(conn, 21, 1, 0) {
			t.Skip(fmt.Errorf("unsupported clickhouse version"))
			return
		}
		const ddl = `
		CREATE TABLE test_simple_aggregate_function (
			  Col1 UInt64
			, Col2 SimpleAggregateFunction(sum, Double)
			, Col3 SimpleAggregateFunction(sumMap, Tuple(Array(Int16), Array(UInt64)))
			, Col4 SimpleAggregateFunction(anyLast, JSON)
		) Engine MergeTree() ORDER BY tuple()
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_simple_aggregate_function")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_simple_aggregate_function")
		require.NoError(t, err)
		var (
			col1Data = uint64(42)
			col2Data = float64(256.1)
			col3Data = []any{
				[]int16{1, 2, 3, 4, 5},
				[]uint64{1, 2, 3, 4, 5},
			}
			col4Data = map[string]any{
				"str": "hello world",
			}
		)
		require.NoError(t, batch.Append(col1Data, col2Data, col3Data, col4Data))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Send())
		var result struct {
			Col1 uint64
			Col2 float64
			Col3 []any
			Col4 map[string]any
		}
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_simple_aggregate_function").ScanStruct(&result))
		assert.Equal(t, col1Data, result.Col1)
		assert.Equal(t, col2Data, result.Col2)
		assert.Equal(t, col3Data, result.Col3)
		assert.Equal(t, col4Data, result.Col4)
	})
}
