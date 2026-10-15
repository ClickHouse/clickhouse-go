package issues

import (
	"context"
	"reflect"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func Test1730NullableDecimalDynamicScan(t *testing.T) {
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })

		ctx := context.Background()
		const ddl = `
		CREATE TABLE test_1730 (
			ID UInt8,
			Col1 Nullable(Decimal64(2))
		) Engine Memory
		`
		t.Cleanup(func() {
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_1730"))
		})

		require.NoError(t, conn.Exec(ctx, ddl))
		require.NoError(t, conn.Exec(ctx, `
		INSERT INTO test_1730 VALUES
			(1, NULL),
			(2, 123.1),
			(3, NULL),
			(4, 124.1),
			(5, NULL)
		`))

		rows, err := conn.Query(ctx, "SELECT Col1 FROM test_1730 ORDER BY ID")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, rows.Close()) })

		columnTypes := rows.ColumnTypes()
		require.Len(t, columnTypes, 1)
		dest := reflect.New(columnTypes[0].ScanType()).Interface()

		var got []string
		for rows.Next() {
			require.NoError(t, rows.Scan(dest))
			v := dest.(**decimal.Decimal)
			if *v == nil {
				got = append(got, "NULL")
				continue
			}
			got = append(got, (*v).String())
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []string{"NULL", "123.1", "NULL", "124.1", "NULL"}, got)
	})
}
