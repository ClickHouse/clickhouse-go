package issues

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test2166 verifies that a String value scanned into []byte or json.RawMessage is a copy
// the caller can write into, also when the value is one byte long.
// See https://github.com/ClickHouse/clickhouse-go/issues/2166.
func Test2166(t *testing.T) {
	columnTypes := []string{"String", "Nullable(String)", "LowCardinality(String)"}

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		conn, err := clickhouse_tests.GetConnection(testSet, t, protocol, nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		for _, columnType := range columnTypes {
			t.Run(fmt.Sprintf("%v/%s", protocol, columnType), func(t *testing.T) {
				query := fmt.Sprintf("SELECT CAST(arrayJoin(['A', 'A']) AS %s) AS value", columnType)

				rows, err := conn.Query(context.Background(), query)
				require.NoError(t, err)
				defer rows.Close()

				var values [][]byte
				for rows.Next() {
					var value []byte
					var raw json.RawMessage
					require.NoError(t, rows.Scan(&value))
					require.NoError(t, rows.Scan(&raw))
					values = append(values, value, raw)
				}
				require.NoError(t, rows.Err())
				require.Len(t, values, 4)

				for i := 1; i < len(values); i++ {
					require.NotSame(t, &values[0][0], &values[i][0], "scanned values share memory")
				}
				for _, value := range values {
					value[0] = 'a'
					require.Equal(t, []byte("a"), value)
				}
			})
		}
	}
}
