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

// Test2042 verifies that named Tuple elements the server backquotes in the type name
// (reserved words such as `values` and `from` since ClickHouse 26.5, names with spaces or
// leading digits on any version) are matched by their plain name when inserting from a
// struct or a map and when scanning back into them.
// See https://github.com/ClickHouse/clickhouse-go/issues/2042.
func Test2042(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("%v", protocol), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			ctx := context.Background()
			// https://github.com/ClickHouse/ClickHouse/pull/36544
			if !clickhouse_tests.CheckMinServerServerVersion(conn, 22, 5, 0) {
				t.Skip(fmt.Errorf("unsupported clickhouse version"))
				return
			}

			tableName := fmt.Sprintf("test_2042_%v", protocol)
			require.NoError(t, conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (Col1 Tuple(`values` Array(String), `from` String, `with space` UInt8, `56` String)) Engine MergeTree() ORDER BY tuple()", tableName)))
			t.Cleanup(func() {
				if err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
					t.Logf("DROP TABLE %s failed: %v", tableName, err)
				}
			})

			type element struct {
				Values []string `ch:"values"`
				From   string   `ch:"from"`
				Spaced uint8    `ch:"with space"`
				Num    string   `ch:"56"`
			}
			var (
				structData = element{Values: []string{"a", "b"}, From: "x", Spaced: 1, Num: "n"}
				mapData    = map[string]any{"values": []string{"c"}, "from": "y", "with space": uint8(2), "56": "m"}
			)
			batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", tableName))
			require.NoError(t, err)
			require.NoError(t, batch.Append(structData))
			require.NoError(t, batch.Append(mapData))
			require.Equal(t, 2, batch.Rows())
			require.NoError(t, batch.Send())

			var gotStruct element
			require.NoError(t, conn.QueryRow(ctx, fmt.Sprintf("SELECT Col1 FROM %s WHERE Col1.`from` = 'x'", tableName)).Scan(&gotStruct))
			assert.Equal(t, structData, gotStruct)
			var gotMap map[string]any
			require.NoError(t, conn.QueryRow(ctx, fmt.Sprintf("SELECT Col1 FROM %s WHERE Col1.`from` = 'y'", tableName)).Scan(&gotMap))
			assert.Equal(t, mapData, gotMap)
		})
	}
}
