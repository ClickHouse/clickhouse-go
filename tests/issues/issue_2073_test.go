package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func TestIssue2073_TupleElementNameWithSpace(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("%v", protocol), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			ctx := context.Background()

			tableName := fmt.Sprintf("test_2073_%v", protocol)
			require.NoError(t, conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (Col1 Tuple(`a b` Int64, `price (usd)` Float64)) Engine MergeTree() ORDER BY tuple()", tableName)))
			t.Cleanup(func() { _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) })

			mapData := map[string]any{"a b": int64(1), "price (usd)": float64(1.5)}
			batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", tableName))
			require.NoError(t, err)
			require.NoError(t, batch.Append(mapData))
			require.NoError(t, batch.Send())

			var gotMap map[string]any
			require.NoError(t, conn.QueryRow(ctx, fmt.Sprintf("SELECT Col1 FROM %s", tableName)).Scan(&gotMap))
			assert.Equal(t, mapData, gotMap)
		})
	}
}
