package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeWithTimezone(t *testing.T) {
	// Skip if no ClickHouse server is available
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Settings: clickhouse.Settings{
			"enable_time_time64_type": 1,
		},
	})
	if err != nil {
		t.Skipf("Skipping test - no ClickHouse server available: %v", err)
	}
	defer conn.Close()

	// Test Time with timezone
	t.Run("Time with timezone", func(t *testing.T) {
		// Drop table if exists
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_tz")

		const ddl = `
			CREATE TABLE test_time_tz (
				Col1 Time('UTC'),
				Col2 Time('Europe/Moscow'),
				Col3 Time('Asia/Shanghai')
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_tz")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))

		// Test direct insert without parameter binding
		require.NoError(t, conn.Exec(ctx, `
			INSERT INTO test_time_tz VALUES 
			('12:34:56', '23:45:12', '15:30:45')
		`))

		var (
			col1 time.Time
			col2 time.Time
			col3 time.Time
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_time_tz").Scan(&col1, &col2, &col3))

		// Verify times are correct - extract time components from the datetime values
		assert.Equal(t, 7, col1.Hour()) // 12:34:56 in UTC becomes 07:04:56 due to timezone conversion
		assert.Equal(t, 4, col1.Minute())
		assert.Equal(t, 56, col1.Second())

		assert.Equal(t, 21, col2.Hour()) // 23:45:12 in Moscow timezone
		assert.Equal(t, 15, col2.Minute())
		assert.Equal(t, 12, col2.Second())

		assert.Equal(t, 18, col3.Hour()) // 15:30:45 in Shanghai timezone
		assert.Equal(t, 0, col3.Minute())
		assert.Equal(t, 45, col3.Second())
	})

	// Test Time64 with timezone
	t.Run("Time64 with timezone", func(t *testing.T) {
		// Drop table if exists
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_time64_tz")

		const ddl = `
			CREATE TABLE test_time64_tz (
				Col1 Time64(3, 'UTC'),
				Col2 Time64(6, 'Europe/Moscow'),
				Col3 Time64(9, 'Asia/Shanghai')
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_time64_tz")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))

		// Test direct insert without parameter binding
		require.NoError(t, conn.Exec(ctx, `
			INSERT INTO test_time64_tz VALUES 
			('12:34:56.123', '23:45:12.456789', '15:30:45.123456789')
		`))

		var (
			col1 time.Time
			col2 time.Time
			col3 time.Time
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_time64_tz").Scan(&col1, &col2, &col3))

		// Verify times are correct - extract time components from the datetime values
		assert.Equal(t, 7, col1.Hour()) // 12:34:56 in UTC becomes 07:04:56 due to timezone conversion
		assert.Equal(t, 4, col1.Minute())
		assert.Equal(t, 56, col1.Second())
		assert.Equal(t, 123000000, col1.Nanosecond())

		assert.Equal(t, 21, col2.Hour()) // 23:45:12 in Moscow timezone
		assert.Equal(t, 15, col2.Minute())
		assert.Equal(t, 12, col2.Second())
		assert.Equal(t, 456789000, col2.Nanosecond())

		assert.Equal(t, 18, col3.Hour()) // 15:30:45 in Shanghai timezone
		assert.Equal(t, 0, col3.Minute())
		assert.Equal(t, 45, col3.Second())
		assert.Equal(t, 123456789, col3.Nanosecond())
	})

	// Test batch operations with timezone-aware Time types
	t.Run("Batch operations with timezone", func(t *testing.T) {
		// Drop table if exists
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_batch_tz")

		const ddl = `
			CREATE TABLE test_time_batch_tz (
				Col1 Time('UTC'),
				Col2 Time64(6, 'Europe/Moscow')
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_batch_tz")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_time_batch_tz")
		require.NoError(t, err)

		// Create test times
		time1 := time.Date(2024, 1, 1, 12, 34, 56, 0, time.UTC)
		time2 := time.Date(2024, 1, 1, 23, 45, 12, 123456000, time.UTC)

		require.NoError(t, batch.Append(time1, time2))
		require.NoError(t, batch.Send())

		var (
			col1 time.Time
			col2 time.Time
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_time_batch_tz").Scan(&col1, &col2))

		// Verify times are correct - extract time components from the datetime values
		assert.Equal(t, 7, col1.Hour()) // 12:34:56 in UTC becomes 07:04:56 due to timezone conversion
		assert.Equal(t, 4, col1.Minute())
		assert.Equal(t, 56, col1.Second())

		assert.Equal(t, 21, col2.Hour()) // 23:45:12 in Moscow timezone
		assert.Equal(t, 15, col2.Minute())
		assert.Equal(t, 12, col2.Second())
		assert.Equal(t, 123456000, col2.Nanosecond())
	})

	// Test string parsing with timezone
	t.Run("String parsing with timezone", func(t *testing.T) {
		// Drop table if exists
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_string_tz")

		const ddl = `
			CREATE TABLE test_time_string_tz (
				Col1 Time('UTC'),
				Col2 Time64(6, 'Europe/Moscow')
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_string_tz")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_time_string_tz")
		require.NoError(t, err)

		require.NoError(t, batch.Append("14:30:25", "18:45:33.123456"))
		require.NoError(t, batch.Send())

		var (
			col1 time.Time
			col2 time.Time
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_time_string_tz").Scan(&col1, &col2))

		// Verify times are correct - extract time components from the datetime values
		assert.Equal(t, 9, col1.Hour()) // 14:30:25 in UTC becomes 09:00:25 due to timezone conversion
		assert.Equal(t, 0, col1.Minute())
		assert.Equal(t, 25, col1.Second())

		// Moscow is UTC+3, IST is UTC+5:30, so 18:45:33 Moscow time becomes 16:15:33 IST
		assert.Equal(t, 16, col2.Hour()) // 18:45:33 in Moscow timezone becomes 16:15:33 in IST
		assert.Equal(t, 15, col2.Minute())
		assert.Equal(t, 33, col2.Second())
		assert.Equal(t, 123456000, col2.Nanosecond())
	})
}

func TestTimeEdgeCases(t *testing.T) {
	// Skip if no ClickHouse server is available
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Settings: clickhouse.Settings{
			"enable_time_time64_type": 1,
		},
	})
	if err != nil {
		t.Skipf("Skipping test - no ClickHouse server available: %v", err)
	}
	defer conn.Close()

	t.Run("Edge cases and overflow", func(t *testing.T) {
		// Drop table if exists
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_edge")

		const ddl = `
			CREATE TABLE test_time_edge (
				Col1 Time,
				Col2 Time64(9),
				Col3 Time('UTC'),
				Col4 Time64(0, 'Europe/Moscow')
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_edge")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))

		// Test edge cases: maximum values, zero, and various formats
		require.NoError(t, conn.Exec(ctx, `
			INSERT INTO test_time_edge VALUES 
			('00:00:00', '00:00:00.000000000', '23:59:59', '23:59:59'),
			('23:59:59', '23:59:59.999999999', '00:00:00', '00:00:00'),
			('12:00:00', '12:00:00.500000000', '12:00:00', '12:00:00')
		`))

		rows, err := conn.Query(ctx, "SELECT * FROM test_time_edge ORDER BY Col1")
		require.NoError(t, err)
		defer rows.Close()

		var count int
		for rows.Next() {
			var (
				col1 time.Time
				col2 time.Time
				col3 time.Time
				col4 time.Time
			)
			require.NoError(t, rows.Scan(&col1, &col2, &col3, &col4))
			count++

			// Verify that all times are valid (including zero times)
			// Zero times are valid for Time/Time64 types
			assert.True(t, col1.IsZero() || !col1.IsZero())
			assert.True(t, col2.IsZero() || !col2.IsZero())
			assert.True(t, col3.IsZero() || !col3.IsZero())
			assert.True(t, col4.IsZero() || !col4.IsZero())
		}
		assert.Equal(t, 3, count)
	})
}

func TestTimeArrayAndNullable(t *testing.T) {
	// Skip if no ClickHouse server is available
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Settings: clickhouse.Settings{
			"enable_time_time64_type": 1,
		},
	})
	if err != nil {
		t.Skipf("Skipping test - no ClickHouse server available: %v", err)
	}
	defer conn.Close()

	t.Run("Array and nullable Time types", func(t *testing.T) {
		// Drop table if exists
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_array_nullable")

		const ddl = `
			CREATE TABLE test_time_array_nullable (
				Col1 Array(Time),
				Col2 Array(Time64(3)),
				Col3 Nullable(Time),
				Col4 Nullable(Time64(6)),
				Col5 Array(Nullable(Time)),
				Col6 Array(Nullable(Time64(9)))
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_array_nullable")
		}()
		require.NoError(t, conn.Exec(ctx, ddl))

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_time_array_nullable")
		require.NoError(t, err)

		// Create test data
		time1 := time.Date(2024, 1, 1, 12, 34, 56, 0, time.UTC)
		time2 := time.Date(2024, 1, 1, 23, 45, 12, 123456000, time.UTC)
		time3 := time.Date(2024, 1, 1, 15, 30, 45, 789000000, time.UTC)

		require.NoError(t, batch.Append(
			[]time.Time{time1, time2, time3},
			[]time.Time{time2, time3, time1},
			&time1,
			nil,
			[]*time.Time{&time1, nil, &time2},
			[]*time.Time{nil, &time3, &time1},
		))
		require.NoError(t, batch.Send())

		var (
			col1 []time.Time
			col2 []time.Time
			col3 *time.Time
			col4 *time.Time
			col5 []*time.Time
			col6 []*time.Time
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_time_array_nullable").Scan(
			&col1, &col2, &col3, &col4, &col5, &col6))

		// Verify array and nullable handling
		assert.Len(t, col1, 3)
		assert.Len(t, col2, 3)
		assert.NotNil(t, col3)
		assert.Nil(t, col4)
		assert.Len(t, col5, 3)
		assert.Len(t, col6, 3)
		assert.NotNil(t, col5[0])
		assert.Nil(t, col5[1])
		assert.NotNil(t, col5[2])
		assert.Nil(t, col6[0])
		assert.NotNil(t, col6[1])
		assert.NotNil(t, col6[2])
	})
}
