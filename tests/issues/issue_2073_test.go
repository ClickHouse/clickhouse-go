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

func TestIssue2073_QuotedNamesInCompositeTypes(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("%v", protocol), func(t *testing.T) {
			settings := clickhouse_tests.TestClientDefaultSettings(testEnv)
			settings["flatten_nested"] = 0
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, settings, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, conn.Close()) })
			if !clickhouse_tests.CheckMinServerServerVersion(conn, 24, 4, 0) {
				t.Skip("unsupported clickhouse version for Variant type")
			}
			ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
				"allow_experimental_variant_type": true,
			}))

			variant := clickhouse.NewVariantWithType(uint64(8), "UInt64")
			nested := []map[string]any{{"z)": int8(7), "value": "ok"}}
			for _, tc := range []struct {
				name     string
				query    string
				dest     any
				expected any
			}{
				{
					name:     "Variant retains the alternative after a quoted parenthesis",
					query:    "SELECT CAST(toUInt64(8) AS Variant(Tuple(`z)` Int8), UInt64))",
					dest:     new(clickhouse.Variant),
					expected: &variant,
				},
				{
					name:     "Nested retains both quoted and plain fields",
					query:    "SELECT CAST([(toInt8(7), 'ok')] AS Nested(`z)` Int8, value String))",
					dest:     new([]map[string]any),
					expected: &nested,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					require.NoError(t, conn.QueryRow(ctx, tc.query).Scan(tc.dest))
					assert.Equal(t, tc.expected, tc.dest)
				})
			}
		})
	}
}
