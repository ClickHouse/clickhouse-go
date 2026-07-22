package issues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clickhousetests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test1918 verifies that Enum8/Enum16 columns can be scanned into integer
// destinations (the underlying numeric ordinal), not only string destinations.
func Test1918(t *testing.T) {
	testEnv, err := clickhousetests.GetTestEnvironment("issues")
	require.NoError(t, err)
	conn, err := clickhousetests.TestClientWithDefaultSettings(testEnv)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	ctx := context.Background()

	const ddl = `
		CREATE TABLE test_1918 (
			  c8  Enum8 ('a' = -5, 'b' = 0, 'c' = 42)
			, c16 Enum16('x' = -300, 'y' = 0, 'z' = 1000)
		) Engine MergeTree() ORDER BY tuple()
	`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_1918"))
	}()

	// Row 'c'/'z' carries the large-magnitude ordinals (42, 1000); row 'a'/'x'
	// carries the negative ordinals (-5, -300). 1000 and -300 fall outside the
	// int8 range, so they double as proof that wider destinations are not
	// truncated to int8.
	require.NoError(t, conn.Exec(ctx, "INSERT INTO test_1918 VALUES ('c', 'z'), ('a', 'x')"))

	t.Run("Enum8 into every signed integer width", func(t *testing.T) {
		var (
			i8  int8
			i16 int16
			i32 int32
			i64 int64
			i   int
		)
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT c8, c8, c8, c8, c8 FROM test_1918 WHERE c8 = 'c'").
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
			"SELECT c16, c16, c16, c16 FROM test_1918 WHERE c16 = 'z'").
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
			"SELECT c8, c16, c16 FROM test_1918 WHERE c8 = 'a'").
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
			"SELECT c8, c16, c16 FROM test_1918 WHERE c8 = 'a'").
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
			"SELECT c8, c16 FROM test_1918 WHERE c8 = 'c'").
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
			"SELECT c16 FROM test_1918 WHERE c16 = 'z'").
			Scan(&i8)
		require.Error(t, err)
	})

	// Nullable(Enum) delegates element scanning to the underlying Enum column's
	// ScanRow, so integer destinations work there too; a NULL leaves a pointer
	// destination nil.
	t.Run("Nullable(Enum) into integer destinations", func(t *testing.T) {
		require.NoError(t, conn.Exec(ctx, `
			CREATE TABLE test_1918_nullable (
				  n8  Nullable(Enum8 ('a' = -5, 'c' = 42))
				, n16 Nullable(Enum16('z' = 1000))
			) Engine MergeTree() ORDER BY tuple()
		`))
		defer func() {
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_1918_nullable"))
		}()
		require.NoError(t, conn.Exec(ctx,
			"INSERT INTO test_1918_nullable VALUES ('c', 'z'), (NULL, NULL)"))

		var (
			v8  int8
			v16 int16
		)
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT n8, n16 FROM test_1918_nullable WHERE n8 IS NOT NULL").
			Scan(&v8, &v16))
		assert.Equal(t, int8(42), v8)
		assert.Equal(t, int16(1000), v16)

		var p8 *int8
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT n8 FROM test_1918_nullable WHERE n8 IS NULL").
			Scan(&p8))
		assert.Nil(t, p8)
	})
}
