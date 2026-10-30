package issues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column/orderedmap"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1976 covers nested Map columns that carry duplicate keys. A top-level
// orderedmap destination already preserved both pairs; Array(Map) and Map of Map
// used Interface.Row and collapsed them. Scanning into orderedmap keeps every pair.
func TestIssue1976(t *testing.T) {
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		ctx := context.Background()
		conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		if !clickhouse_tests.CheckMinServerServerVersion(conn, 21, 9, 0) {
			t.Skip("Map type requires ClickHouse 21.9+")
		}

		t.Run("server sends duplicate keys", func(t *testing.T) {
			var n uint64
			require.NoError(t, conn.QueryRow(ctx, "SELECT length(map('key', 'X', 'key', 'Y'))").Scan(&n))
			require.Equal(t, uint64(2), n)
		})

		t.Run("top-level orderedmap", func(t *testing.T) {
			var om orderedmap.Map[string, string]
			require.NoError(t, conn.QueryRow(ctx, "SELECT map('key', 'X', 'key', 'Y')").Scan(&om))
			require.Len(t, om, 2)
		})

		t.Run("Array(Map) orderedmap", func(t *testing.T) {
			var am []orderedmap.Map[string, string]
			require.NoError(t, conn.QueryRow(ctx, "SELECT [map('key', 'X', 'key', 'Y')]").Scan(&am))
			require.Len(t, am, 1)
			require.Len(t, am[0], 2)

			iter := am[0].Iterator()
			require.True(t, iter.Next())
			require.Equal(t, "key", iter.Key())
			require.Equal(t, "X", iter.Value())
			require.True(t, iter.Next())
			require.Equal(t, "key", iter.Key())
			require.Equal(t, "Y", iter.Value())
		})

		t.Run("Map of Map orderedmap", func(t *testing.T) {
			var mm orderedmap.Map[string, orderedmap.Map[string, string]]
			require.NoError(t, conn.QueryRow(ctx,
				"SELECT map('outer', map('key', 'X', 'key', 'Y'))",
			).Scan(&mm))
			require.Len(t, mm, 1)

			iter := mm.Iterator()
			require.True(t, iter.Next())
			require.Equal(t, "outer", iter.Key())
			nested, ok := iter.Value().(orderedmap.Map[string, string])
			require.True(t, ok)
			require.Len(t, nested, 2)
		})

		t.Run("Go map nested still collapses", func(t *testing.T) {
			var am []map[string]string
			require.NoError(t, conn.QueryRow(ctx, "SELECT [map('key', 'X', 'key', 'Y')]").Scan(&am))
			require.Len(t, am, 1)
			require.Equal(t, map[string]string{"key": "Y"}, am[0])
		})
	})
}
