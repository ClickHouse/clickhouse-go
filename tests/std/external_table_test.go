package std

import (
	"context"
	"fmt"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"github.com/stretchr/testify/assert"
)

func TestStdExternalTable(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			table1, err := ext.NewTable("std_external_table_1",
				ext.Column("col1", "UInt8"),
				ext.Column("col2", "String"),
				ext.Column("col3", "DateTime"),
			)
			require.NoError(t, err)
			for i := 0; i < 10; i++ {
				assert.NoError(t, table1.Append(uint8(i), fmt.Sprintf("value_%d", i), time.Now()))
			}
			table2, err := ext.NewTable("std_external_table_2",
				ext.Column("col1", "UInt8"),
				ext.Column("col2", "String"),
				ext.Column("col3", "DateTime"),
			)
			require.NoError(t, err)
			for i := 0; i < 10; i++ {
				assert.NoError(t, table2.Append(uint8(i), fmt.Sprintf("value_%d", i), time.Now()))
			}
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)
			ctx := clickhouse.Context(context.Background(),
				clickhouse.WithExternalTable(table1, table2),
			)
			rows, err := conn.QueryContext(ctx, "SELECT * FROM std_external_table_1")
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
			require.NoError(t, conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM std_external_table_1").Scan(&count))
			assert.Equal(t, uint64(10), count)
			require.NoError(t, conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM std_external_table_2").Scan(&count))
			assert.Equal(t, uint64(10), count)
			require.NoError(t, conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM (SELECT * FROM std_external_table_1 UNION ALL SELECT * FROM std_external_table_2)").Scan(&count))
			assert.Equal(t, uint64(20), count)
		})
	}
}
