package issues

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test2161 verifies that nil pointers of *[]byte, *json.RawMessage, *sql.Null* and *time.Duration
// are inserted as NULL in Nullable columns and as the zero value in other columns, instead of panicking.
// See https://github.com/ClickHouse/clickhouse-go/issues/2161.
func Test2161(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("%v", protocol), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			ctx := context.Background()

			tableName := fmt.Sprintf("test_2161_%v", protocol)
			require.NoError(t, conn.Exec(ctx, fmt.Sprintf(
				"CREATE TABLE %s (Id UInt8, Col1 String, Col2 Nullable(String), Col3 Int64, Col4 Nullable(Int64), Col5 DateTime, Col6 Nullable(DateTime)) Engine MergeTree() ORDER BY Id",
				tableName)))
			t.Cleanup(func() {
				if err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
					t.Logf("DROP TABLE %s failed: %v", tableName, err)
				}
			})

			batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", tableName))
			require.NoError(t, err)
			require.NoError(t, batch.Append(uint8(1), (*[]byte)(nil), (*json.RawMessage)(nil),
				(*sql.NullInt64)(nil), (*time.Duration)(nil), (*sql.NullTime)(nil), (*sql.NullTime)(nil)))
			require.NoError(t, batch.Append(uint8(2), (*sql.NullString)(nil), (*[]byte)(nil),
				(*time.Duration)(nil), (*sql.NullInt64)(nil), (*sql.NullTime)(nil), (*sql.NullTime)(nil)))
			require.NoError(t, batch.AppendStruct(&struct {
				Id   uint8
				Col1 *[]byte
				Col2 *[]byte
				Col3 *sql.NullInt64
				Col4 *sql.NullInt64
				Col5 *sql.NullTime
				Col6 *sql.NullTime
			}{Id: 3}))
			require.Equal(t, 3, batch.Rows())
			require.NoError(t, batch.Send())

			rows, err := conn.Query(ctx, fmt.Sprintf("SELECT Id, Col1, Col2, Col3, Col4, toUnixTimestamp(Col5), Col6 FROM %s ORDER BY Id", tableName))
			require.NoError(t, err)
			t.Cleanup(func() { rows.Close() })
			var count int
			for rows.Next() {
				var (
					id   uint8
					col1 string
					col2 *string
					col3 int64
					col4 *int64
					col5 uint32
					col6 *time.Time
				)
				require.NoError(t, rows.Scan(&id, &col1, &col2, &col3, &col4, &col5, &col6))
				assert.Equal(t, "", col1)
				assert.Nil(t, col2)
				assert.Equal(t, int64(0), col3)
				assert.Nil(t, col4)
				assert.Equal(t, uint32(0), col5)
				assert.Nil(t, col6)
				count++
			}
			require.NoError(t, rows.Err())
			assert.Equal(t, 3, count)
		})
	}
}
