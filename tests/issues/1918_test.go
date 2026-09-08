package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhousetests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test1918 verifies that Enum8/Enum16 columns can be scanned into integer
// destinations (the underlying numeric ordinal), not only string destinations.
//
// Native and HTTP decode a result set through the same Enum8/Enum16 columns, so
// every case runs on both protocols. The tables are suffixed with the protocol
// to keep the two subtests independent.
func Test1918(t *testing.T) {
	clickhousetests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := clickhousetests.GetConnection("issues", t, protocol, nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		ctx := context.Background()

		table := fmt.Sprintf("test_1918_%s", protocol)
		nullableTable := table + "_nullable"

		require.NoError(t, conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table)))
		t.Cleanup(func() {
			require.NoError(t, conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table)))
		})
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				  c8  Enum8 ('a' = -5, 'b' = 0, 'c' = 42)
				, c16 Enum16('x' = -300, 'y' = 0, 'z' = 1000)
			) Engine MergeTree() ORDER BY tuple()
		`, table)))

		// Row 'c'/'z' carries the large-magnitude ordinals (42, 1000); row 'a'/'x'
		// carries the negative ordinals (-5, -300). 1000 and -300 fall outside the
		// int8 range, so they double as proof that wider destinations are not
		// truncated to int8.
		require.NoError(t, conn.Exec(ctx,
			fmt.Sprintf("INSERT INTO %s VALUES ('c', 'z'), ('a', 'x')", table)))

		t.Run("Enum8 into every signed integer width", func(t *testing.T) {
			var (
				i8  int8
				i16 int16
				i32 int32
				i64 int64
				i   int
			)
			require.NoError(t, conn.QueryRow(ctx,
				fmt.Sprintf("SELECT c8, c8, c8, c8, c8 FROM %s WHERE c8 = 'c'", table)).
				Scan(&i8, &i16, &i32, &i64, &i))
			assert.Equal(t, int8(42), i8)
			assert.Equal(t, int16(42), i16)
			assert.Equal(t, int32(42), i32)
			assert.Equal(t, int64(42), i64)
			assert.Equal(t, 42, i)
		})

		t.Run("Enum16 into signed integer widths >= int16", func(t *testing.T) {
			var (
				i16 int16
				i32 int32
				i64 int64
				i   int
			)
			require.NoError(t, conn.QueryRow(ctx,
				fmt.Sprintf("SELECT c16, c16, c16, c16 FROM %s WHERE c16 = 'z'", table)).
				Scan(&i16, &i32, &i64, &i))
			// 1000 does not fit in int8; the wider destinations must carry it losslessly.
			assert.Equal(t, int16(1000), i16)
			assert.Equal(t, int32(1000), i32)
			assert.Equal(t, int64(1000), i64)
			assert.Equal(t, 1000, i)
		})

		t.Run("negative ordinals preserve their sign", func(t *testing.T) {
			var (
				e8  int8
				e16 int16
				e   int
			)
			require.NoError(t, conn.QueryRow(ctx,
				fmt.Sprintf("SELECT c8, c16, c16 FROM %s WHERE c8 = 'a'", table)).
				Scan(&e8, &e16, &e))
			assert.Equal(t, int8(-5), e8)
			assert.Equal(t, int16(-300), e16)
			assert.Equal(t, -300, e)
		})

		t.Run("pointer-to-pointer integer destinations", func(t *testing.T) {
			var (
				p8  *int8
				p16 *int16
				p   *int
			)
			require.NoError(t, conn.QueryRow(ctx,
				fmt.Sprintf("SELECT c8, c16, c16 FROM %s WHERE c8 = 'a'", table)).
				Scan(&p8, &p16, &p))
			require.NotNil(t, p8)
			require.NotNil(t, p16)
			require.NotNil(t, p)
			assert.Equal(t, int8(-5), *p8)
			assert.Equal(t, int16(-300), *p16)
			assert.Equal(t, -300, *p)
		})

		t.Run("string destination is unchanged", func(t *testing.T) {
			var s8, s16 string
			require.NoError(t, conn.QueryRow(ctx,
				fmt.Sprintf("SELECT c8, c16 FROM %s WHERE c8 = 'c'", table)).
				Scan(&s8, &s16))
			assert.Equal(t, "c", s8)
			assert.Equal(t, "z", s16)
		})

		// Contrast case: int8 cannot hold every Enum16 ordinal (e.g. 1000), so an
		// int8 destination for Enum16 remains an unsupported (error) conversion
		// rather than silently truncating.
		t.Run("Enum16 into int8 stays an error (no silent truncation)", func(t *testing.T) {
			var i8 int8
			err := conn.QueryRow(ctx,
				fmt.Sprintf("SELECT c16 FROM %s WHERE c16 = 'z'", table)).
				Scan(&i8)
			require.ErrorContains(t, err, "converting Enum16 to *int8 is unsupported")
		})

		// Nullable(Enum) delegates element scanning to the underlying Enum column's
		// ScanRow, so integer destinations work there too; a NULL leaves a pointer
		// destination nil.
		t.Run("Nullable(Enum) into integer destinations", func(t *testing.T) {
			require.NoError(t, conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", nullableTable)))
			t.Cleanup(func() {
				require.NoError(t, conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", nullableTable)))
			})
			require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
				CREATE TABLE %s (
					  n8  Nullable(Enum8 ('a' = -5, 'c' = 42))
					, n16 Nullable(Enum16('z' = 1000))
				) Engine MergeTree() ORDER BY tuple()
			`, nullableTable)))
			require.NoError(t, conn.Exec(ctx,
				fmt.Sprintf("INSERT INTO %s VALUES ('c', 'z'), (NULL, NULL)", nullableTable)))

			var (
				v8  int8
				v16 int16
			)
			require.NoError(t, conn.QueryRow(ctx,
				fmt.Sprintf("SELECT n8, n16 FROM %s WHERE n8 IS NOT NULL", nullableTable)).
				Scan(&v8, &v16))
			assert.Equal(t, int8(42), v8)
			assert.Equal(t, int16(1000), v16)

			// A NULL must clear a pointer destination. Seed each pointer non-nil
			// first, otherwise the assertion passes whether or not ScanRow actually
			// cleared it (a nil-initialised pointer is already nil).
			seed8 := int8(7)
			p8 := &seed8
			require.NoError(t, conn.QueryRow(ctx,
				fmt.Sprintf("SELECT n8 FROM %s WHERE n8 IS NULL", nullableTable)).
				Scan(&p8))
			assert.Nil(t, p8, "NULL Nullable(Enum8) must clear the *int8 destination")

			seedInt := 7
			pInt := &seedInt
			require.NoError(t, conn.QueryRow(ctx,
				fmt.Sprintf("SELECT n8 FROM %s WHERE n8 IS NULL", nullableTable)).
				Scan(&pInt))
			assert.Nil(t, pInt, "NULL Nullable(Enum8) must clear the *int destination")
		})
	})
}
