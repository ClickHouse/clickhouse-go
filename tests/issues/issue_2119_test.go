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

// Test2119 verifies that Enum names with control characters, which the server writes as
// escape sequences in the type name, are appended and scanned as the names the server defines.
// See https://github.com/ClickHouse/clickhouse-go/issues/2119.
func Test2119(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)

	const enumValues = `'a\nb' = 1, 'c\td\te' = 2, 'f\0g' = 3, 'h\ri' = 4, 'j\bk' = 5, 'l\fm' = 6, 'n\\no' = 7, 'p\'q\'r' = 8, 'anb' = 9`
	names := []string{"a\nb", "c\td\te", "f\x00g", "h\ri", "j\bk", "l\fm", `n\no`, "p'q'r", "anb"}

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("%v", protocol), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			ctx := context.Background()

			tableName := fmt.Sprintf("test_2119_%v", protocol)
			require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
				"CREATE TABLE %s (Id UInt8, Col1 Enum8(%[2]s), Col2 Enum16(%[2]s), Col3 Array(Enum8(%[2]s)), Col4 Nullable(Enum8(%[2]s))) Engine MergeTree() ORDER BY Id",
				tableName, enumValues)))
			t.Cleanup(func() {
				if err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
					t.Logf("DROP TABLE %s failed: %v", tableName, err)
				}
			})

			batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", tableName))
			require.NoError(t, err)
			for i, name := range names {
				require.NoError(t, batch.Append(uint8(i+1), name, name, []string{name}, &name))
			}
			require.Equal(t, len(names), batch.Rows())
			require.NoError(t, batch.Send())

			// CAST(Col1, 'Int8') is the value the server stored for the name.
			rows, err := conn.Query(ctx, fmt.Sprintf("SELECT Id, CAST(Col1, 'Int8'), Col1, Col2, Col3, Col4 FROM %s ORDER BY Id", tableName))
			require.NoError(t, err)
			t.Cleanup(func() { rows.Close() })
			var count int
			for rows.Next() {
				var (
					id    uint8
					value int8
					col1  string
					col2  string
					col3  []string
					col4  *string
				)
				require.NoError(t, rows.Scan(&id, &value, &col1, &col2, &col3, &col4))
				name := names[id-1]
				assert.Equal(t, int8(id), value)
				assert.Equal(t, name, col1)
				assert.Equal(t, name, col2)
				assert.Equal(t, []string{name}, col3)
				require.NotNil(t, col4)
				assert.Equal(t, name, *col4)
				count++
			}
			require.NoError(t, rows.Err())
			assert.Equal(t, len(names), count)
		})
	}
}
