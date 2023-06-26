package issues

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/ext"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"
)

func Test990(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.HTTP, useSSL, nil)

	for i := 0; i < 10; i++ {
		externalTableNameA, externalTableNameB := fmt.Sprintf("external_table_%v_a", i), fmt.Sprintf("external_table_%v_b", i)
		table1, err := ext.NewTable(externalTableNameA,
			ext.Column("col1", "UInt8"),
			ext.Column("col2", "String"),
			ext.Column("col3", "DateTime"),
		)
		require.NoError(t, err)
		for i := 0; i < 10; i++ {
			assert.NoError(t, table1.Append(uint8(i), fmt.Sprintf("value_%d", i), time.Now()))
		}
		table2, err := ext.NewTable(externalTableNameB,
			ext.Column("col1", "UInt8"),
			ext.Column("col2", "String"),
			ext.Column("col3", "DateTime"),
		)
		require.NoError(t, err)
		for i := 0; i < 10; i++ {
			assert.NoError(t, table2.Append(uint8(i), fmt.Sprintf("value_%d", i), time.Now()))
		}
		require.NoError(t, err)

		ctx := clickhouse.Context(context.Background(),
			clickhouse.WithExternalTable(table1, table2),
		)

		rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %v", externalTableNameA))
		require.NoError(t, err)
		for rows.Next() {
			var (
				col1 uint8
				col2 string
				col3 time.Time
			)
			require.NoError(t, rows.Scan(&col1, &col2, &col3))
			t.Logf("row: col1=%d, col2=%s, col3=%s\n", col1, col2, col3)
		}
		rows.Close()

		var count uint64
		require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %v", externalTableNameA)).Scan(&count))
		assert.Equal(t, uint64(10), count)
		require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %v", externalTableNameB)).Scan(&count))
		assert.Equal(t, uint64(10), count)
		require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM (SELECT * FROM %v UNION ALL SELECT * FROM %v)", externalTableNameA, externalTableNameB)).Scan(&count))
		assert.Equal(t, uint64(20), count)
		require.NoError(t, conn.Ping())
	}

}
