package std

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestStdDecimal(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)
			if !CheckMinServerVersion(conn, 21, 1, 0) {
				t.Skip(fmt.Errorf("unsupported clickhouse version"))
				return
			}
			const ddl = `
			CREATE TABLE std_test_decimal (
				Col1 Decimal32(5)
				, Col2 Decimal(18,5)
				, Col3 Nullable(Decimal(15,3))
				, Col4 Array(Decimal(15,3))
			) Engine MergeTree() ORDER BY tuple()
		`
			defer func() {
				conn.Exec("DROP TABLE std_test_decimal")
			}()
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO std_test_decimal")
			require.NoError(t, err)
			_, err = batch.Exec(
				decimal.New(25, 0),
				decimal.New(30, 0),
				decimal.New(35, 0),
				[]decimal.Decimal{
					decimal.New(25, 0),
					decimal.New(30, 0),
					decimal.New(35, 0),
				},
			)
			require.NoError(t, err)
			require.NoError(t, scope.Commit())
			var (
				col1 decimal.Decimal
				col2 decimal.Decimal
				col3 decimal.Decimal
				col4 []decimal.Decimal
			)
			rows, err := conn.Query("SELECT * FROM std_test_decimal")
			require.NoError(t, err)
			columnTypes, err := rows.ColumnTypes()
			require.NoError(t, err)
			for i, column := range columnTypes {
				switch i {
				case 0:
					nullable, nullableOk := column.Nullable()
					assert.False(t, nullable)
					assert.True(t, nullableOk)

					precision, scale, ok := column.DecimalSize()
					assert.Equal(t, int64(5), scale)
					assert.Equal(t, int64(9), precision)
					assert.True(t, ok)
				case 1:
					nullable, nullableOk := column.Nullable()
					assert.False(t, nullable)
					assert.True(t, nullableOk)

					precision, scale, ok := column.DecimalSize()
					assert.Equal(t, int64(5), scale)
					assert.Equal(t, int64(18), precision)
					assert.True(t, ok)
				case 2:
					nullable, nullableOk := column.Nullable()
					assert.True(t, nullable)
					assert.True(t, nullableOk)

					precision, scale, ok := column.DecimalSize()
					assert.Equal(t, int64(3), scale)
					assert.Equal(t, int64(15), precision)
					assert.True(t, ok)
				case 3:
					nullable, nullableOk := column.Nullable()
					assert.False(t, nullable)
					assert.True(t, nullableOk)

					precision, scale, ok := column.DecimalSize()
					assert.Equal(t, int64(3), scale)
					assert.Equal(t, int64(15), precision)
					assert.True(t, ok)
				}
			}
			for rows.Next() {
				require.NoError(t, rows.Scan(&col1, &col2, &col3, &col4))
				assert.True(t, decimal.New(25, 0).Equal(col1))
				assert.True(t, decimal.New(30, 0).Equal(col2))
				assert.True(t, decimal.New(35, 0).Equal(col3))
			}

			require.NoError(t, rows.Close())
			require.NoError(t, rows.Err())
		})
	}
}
