package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/ext"
)

func TestExternalTable(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		table1, err := ext.NewTable("external_table_1",
			ext.Column("col1", "UInt8"),
			ext.Column("col2", "String"),
			ext.Column("col3", "DateTime"),
		)
		if assert.NoError(t, err) {
			for i := 0; i < 10; i++ {
				assert.NoError(t, table1.Append(uint8(i), fmt.Sprintf("value_%d", i), time.Now()))
			}
		}
		table2, err := ext.NewTable("external_table_2",
			ext.Column("col1", "UInt8"),
			ext.Column("col2", "String"),
			ext.Column("col3", "DateTime"),
		)
		if assert.NoError(t, err) {
			for i := 0; i < 10; i++ {
				assert.NoError(t, table2.Append(uint8(i), fmt.Sprintf("value_%d", i), time.Now()))
			}
		}

		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		require.NoError(t, err)
		ctx := clickhouse.Context(context.Background(),
			clickhouse.WithExternalTable(table1, table2),
		)
		rows, err := conn.Query(ctx, "SELECT * FROM external_table_1")
		require.NoError(t, err)
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
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())

		var count uint64
		require.NoError(t, conn.QueryRow(ctx, "SELECT COUNT(*) FROM external_table_1").Scan(&count))
		assert.Equal(t, uint64(10), count)
		require.NoError(t, conn.QueryRow(ctx, "SELECT COUNT(*) FROM external_table_2").Scan(&count))
		assert.Equal(t, uint64(10), count)
		require.NoError(t, conn.QueryRow(ctx, "SELECT COUNT(*) FROM (SELECT * FROM external_table_1 UNION ALL SELECT * FROM external_table_2)").Scan(&count))
		assert.Equal(t, uint64(20), count)
	})
}

func TestExternalTableQuotedColumnNames(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		tbl, err := ext.NewTable("quoted_ext",
			ext.Column("test id", "UInt64"),
			ext.Column("`test id`", "UInt64"),
			ext.Column("back`tick", "UInt64"),
			ext.Column("x UInt8, y", "UInt64"),
			ext.Column("é", "UInt64"),
		)
		require.NoError(t, err)
		require.NoError(t, tbl.Append(uint64(42), uint64(43), uint64(44), uint64(45), uint64(46)))

		conn, err := GetNativeConnection(t, protocol, nil, nil, nil)
		require.NoError(t, err)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithExternalTable(tbl))
		rows, err := conn.Query(ctx, "SELECT * FROM quoted_ext")
		require.NoError(t, err)
		defer rows.Close()

		assert.Equal(t, []string{"test id", "`test id`", "back`tick", "x UInt8, y", "é"}, rows.Columns())
		require.True(t, rows.Next())

		var v1, v2, v3, v4, v5 uint64
		require.NoError(t, rows.Scan(&v1, &v2, &v3, &v4, &v5))
		assert.Equal(t, []uint64{42, 43, 44, 45, 46}, []uint64{v1, v2, v3, v4, v5})
		require.NoError(t, rows.Err())
	})
}
