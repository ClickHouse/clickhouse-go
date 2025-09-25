
package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func setupTimeTest(t *testing.T) (context.Context, func(), clickhouse.Conn) {
	ctx := context.Background()
	conn := clickhouse_tests.GetNativeConnection(t, map[string]any{
		"enable_time_timet64_type": 1,
	})
	if !clickhouse_tests.CheckMinServerServerVersion(conn, 24, 6, 1) {
		t.Skip("Time/Time64 not supported on this ClickHouse version")
	}
	return ctx, func() {}, conn
}

func TestTimeAndTime64(t *testing.T) {
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol string) {
		ctx, cleanup, conn := setupTimeTest(t)
		defer cleanup()
		tableName := fmt.Sprintf("test_time_types_%d", time.Now().UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				t1 Time,
				t2 Time64(9),
				t3 Array(Time),
				t4 Array(Time64(9))
			) ENGINE = MergeTree() ORDER BY tuple()`, tableName)))
		defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		t1 := time.Date(0, 1, 1, 12, 34, 56, 0, time.UTC)
		t2 := time.Date(0, 1, 1, 23, 59, 59, 123456789, time.UTC)
		t3 := []time.Time{t1, t2}
		t4 := []time.Time{t2, t1}
		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (t1, t2, t3, t4)", tableName))
		require.NoError(t, err)
		require.NoError(t, batch.Append(t1, t2, t3, t4))
		require.NoError(t, batch.Send())

		var (
			outT1 time.Time
			outT2 time.Time
			outT3 []time.Time
			outT4 []time.Time
		)
		row := conn.QueryRow(ctx, fmt.Sprintf("SELECT t1, t2, t3, t4 FROM %s", tableName))
		require.NoError(t, row.Scan(&outT1, &outT2, &outT3, &outT4))
		require.Equal(t, t1, outT1)
		require.Equal(t, t2, outT2)
		require.Equal(t, t3, outT3)
		require.Equal(t, t4, outT4)
	})
}
