package std

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/external"
	"github.com/stretchr/testify/assert"
)

func TestStdExternalTable(t *testing.T) {
	table1, err := external.NewTable("external_table_1",
		external.Column("col1", "UInt8"),
		external.Column("col2", "String"),
		external.Column("col3", "DateTime"),
	)
	if assert.NoError(t, err) {
		for i := 0; i < 10; i++ {
			assert.NoError(t, table1.Append(uint8(i), fmt.Sprintf("value_%d", i), time.Now()))
		}
	}
	table2, err := external.NewTable("external_table_2",
		external.Column("col1", "UInt8"),
		external.Column("col2", "String"),
		external.Column("col3", "DateTime"),
	)
	if assert.NoError(t, err) {
		for i := 0; i < 10; i++ {
			assert.NoError(t, table2.Append(uint8(i), fmt.Sprintf("value_%d", i), time.Now()))
		}
	}
	if conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) {
		ctx := clickhouse.Context(context.Background(),
			clickhouse.WithExternalTable(table1, table2),
		)
		if rows, err := conn.QueryContext(ctx, "SELECT * FROM external_table_1"); assert.NoError(t, err) {
			for rows.Next() {
				var (
					col1 uint8
					col2 string
					col3 time.Time
				)
				if err := rows.Scan(&col1, &col2, &col3); assert.NoError(t, err) {
					t.Logf("row: col1=%d, col2=%s, col3=%s\n", col1, col2, col3)
				}
			}
			rows.Close()
		}
		var count uint64
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM external_table_1").Scan(&count); assert.NoError(t, err) {
			assert.Equal(t, uint64(10), count)
		}
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM external_table_2").Scan(&count); assert.NoError(t, err) {
			assert.Equal(t, uint64(10), count)
		}
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM (SELECT * FROM external_table_1 UNION ALL SELECT * FROM external_table_2)").Scan(&count); assert.NoError(t, err) {
			assert.Equal(t, uint64(20), count)
		}
	}
}
