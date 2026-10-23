package issues

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func Test1730NullableDynamicScan(t *testing.T) {
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })

		ctx := context.Background()
		const ddl = `
		CREATE TABLE test_1730 (
			ID UInt8,
			Col1 Nullable(Decimal64(2)),
			Col2 Nullable(Int64),
			Col3 Nullable(String),
			Col4 Nullable(DateTime('UTC')),
			Col5 Nullable(UUID),
			Col6 LowCardinality(Nullable(String))
		) Engine Memory
		`
		t.Cleanup(func() {
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_1730"))
		})

		require.NoError(t, conn.Exec(ctx, ddl))
		require.NoError(t, conn.Exec(ctx, `
		INSERT INTO test_1730 VALUES
			(1, NULL, NULL, NULL, NULL, NULL, NULL),
			(2, 123.1, 42, 'a', '2024-01-02 03:04:05', '00000000-0000-0000-0000-000000000001', 'x'),
			(3, NULL, NULL, NULL, NULL, NULL, NULL),
			(4, 124.1, 43, 'b', '2024-01-02 03:04:06', '00000000-0000-0000-0000-000000000002', 'y'),
			(5, NULL, NULL, NULL, NULL, NULL, NULL)
		`))

		t.Run("pointer destinations from ScanType", func(t *testing.T) {
			rows, err := conn.Query(ctx, "SELECT Col1, Col2, Col3, Col4, Col5, Col6 FROM test_1730 ORDER BY ID")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rows.Close()) })

			// Destinations are reused across rows, so a NULL row must reset them to nil.
			columnTypes := rows.ColumnTypes()
			dest := make([]any, len(columnTypes))
			for i, ct := range columnTypes {
				dest[i] = reflect.New(ct.ScanType()).Interface()
			}

			var got [][]string
			for rows.Next() {
				require.NoError(t, rows.Scan(dest...))
				row := make([]string, len(dest))
				for i, d := range dest {
					v := reflect.ValueOf(d).Elem()
					require.Equal(t, reflect.Pointer, v.Kind(), "column %s", columnTypes[i].Name())
					if v.IsNil() {
						row[i] = "NULL"
						continue
					}
					row[i] = fmt.Sprint(v.Elem().Interface())
				}
				got = append(got, row)
			}
			require.NoError(t, rows.Err())

			null := []string{"NULL", "NULL", "NULL", "NULL", "NULL", "NULL"}
			require.Equal(t, [][]string{
				null,
				{"123.1", "42", "a", "2024-01-02 03:04:05 +0000 UTC", "00000000-0000-0000-0000-000000000001", "x"},
				null,
				{"124.1", "43", "b", "2024-01-02 03:04:06 +0000 UTC", "00000000-0000-0000-0000-000000000002", "y"},
				null,
			}, got)
		})

		t.Run("value destinations return an error on NULL", func(t *testing.T) {
			rows, err := conn.Query(ctx, "SELECT Col2, Col3, Col4, Col6 FROM test_1730 ORDER BY ID")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rows.Close()) })

			var (
				i64 int64
				str string
				ts  time.Time
				lc  string
			)
			for i := 0; rows.Next(); i++ {
				err := rows.Scan(&i64, &str, &ts, &lc)
				if i%2 == 0 {
					require.ErrorContains(t, err, "converting NULL to *int64 is unsupported")
					continue
				}
				require.NoError(t, err)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, int64(43), i64)
			require.Equal(t, "b", str)
			require.Equal(t, time.Date(2024, 1, 2, 3, 4, 6, 0, time.UTC), ts.UTC())
			require.Equal(t, "y", lc)

			// LowCardinality(Nullable(String)) shares the same NULL handling.
			var lcOnly string
			require.ErrorContains(t,
				conn.QueryRow(ctx, "SELECT Col6 FROM test_1730 WHERE ID = 1").Scan(&lcOnly),
				"converting NULL to *string is unsupported")
		})

		t.Run("Select into struct", func(t *testing.T) {
			var values []struct {
				Col2 int64 `ch:"Col2"`
			}
			require.ErrorContains(t,
				conn.Select(ctx, &values, "SELECT Col2 FROM test_1730 ORDER BY ID"),
				"converting NULL to *int64 is unsupported")

			var pointers []struct {
				Col2 *int64 `ch:"Col2"`
			}
			require.NoError(t, conn.Select(ctx, &pointers, "SELECT Col2 FROM test_1730 ORDER BY ID"))
			require.Len(t, pointers, 5)
			require.Nil(t, pointers[0].Col2)
			require.Equal(t, int64(42), *pointers[1].Col2)
			require.Nil(t, pointers[2].Col2)
			require.Equal(t, int64(43), *pointers[3].Col2)
			require.Nil(t, pointers[4].Col2)
		})
	})
}
