package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTimeTest(t *testing.T, protocol clickhouse.Protocol) clickhouse.Conn {
	conn, err := GetNativeConnection(t, protocol, nil, nil, nil)
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 25, 6, 0) {
		t.Skip("Time type not supported on this ClickHouse version")
	}
	return conn
}

func TestTime(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupTimeTest(t, protocol)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"enable_time_time64_type": 1,
		}))

		tableName := fmt.Sprintf("test_time_%d", time.Now().UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				t1 Time
			) ENGINE = MergeTree() ORDER BY tuple()`, tableName)))
		defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		// Test basic Time value
		t1 := 12*time.Hour + 34*time.Minute + 56*time.Second
		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (t1) VALUES (?)", tableName))
		require.NoError(t, err)
		require.NoError(t, batch.Append(t1))
		require.NoError(t, batch.Send())

		var outT1 time.Duration
		row := conn.QueryRow(ctx, fmt.Sprintf("SELECT t1 FROM %s", tableName))
		require.NoError(t, row.Scan(&outT1))
		assert.Equal(t, t1, outT1)
	})
}

func TestTimeEdgeCases(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupTimeTest(t, protocol)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"enable_time_time64_type": 1,
		}))

		tableName := fmt.Sprintf("test_time_edge_%d", time.Now().UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				id UInt32,
				t1 Time
			) ENGINE = MergeTree() ORDER BY id`, tableName)))
		defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		testCases := []struct {
			name     string
			duration time.Duration
			expected time.Duration // Expected after truncation to seconds
		}{
			{"Zero", 0, 0},
			{"Midnight", 0, 0},
			{"One second", 1 * time.Second, 1 * time.Second},
			{"One minute", 1 * time.Minute, 1 * time.Minute},
			{"One hour", 1 * time.Hour, 1 * time.Hour},
			{"Midday", 12 * time.Hour, 12 * time.Hour},
			{"End of day", 23*time.Hour + 59*time.Minute + 59*time.Second, 23*time.Hour + 59*time.Minute + 59*time.Second},
			{"With milliseconds (truncated)", 10*time.Hour + 20*time.Minute + 30*time.Second + 123*time.Millisecond, 10*time.Hour + 20*time.Minute + 30*time.Second},
			{"With microseconds (truncated)", 15*time.Hour + 30*time.Minute + 45*time.Second + 999*time.Microsecond, 15*time.Hour + 30*time.Minute + 45*time.Second},
			{"With nanoseconds (truncated)", 8*time.Hour + 15*time.Minute + 25*time.Second + 999999999*time.Nanosecond, 8*time.Hour + 15*time.Minute + 25*time.Second},
		}

		// Insert all test cases
		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, t1) VALUES (?, ?)", tableName))
		require.NoError(t, err)
		for i, tc := range testCases {
			require.NoError(t, batch.Append(uint32(i), tc.duration))
		}
		require.NoError(t, batch.Send())

		// Verify all test cases
		for i, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				var outT1 time.Duration
				row := conn.QueryRow(ctx, fmt.Sprintf("SELECT t1 FROM %s WHERE id = %d", tableName, i))
				require.NoError(t, row.Scan(&outT1))
				assert.Equal(t, tc.expected, outT1, "Test case: %s", tc.name)
			})
		}
	})
}

func TestTimeArray(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupTimeTest(t, protocol)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"enable_time_time64_type": 1,
		}))

		tableName := fmt.Sprintf("test_time_array_%d", time.Now().UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				t1 Array(Time)
			) ENGINE = MergeTree() ORDER BY tuple()`, tableName)))
		defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		// Test array of Time values
		timeArray := []time.Duration{
			0,                            // Midnight
			1 * time.Hour,                // 1 AM
			6*time.Hour + 30*time.Minute, // 6:30 AM
			12 * time.Hour,               // Noon
			18*time.Hour + 45*time.Minute + 30*time.Second, // 6:45:30 PM
			23*time.Hour + 59*time.Minute + 59*time.Second, // 11:59:59 PM
		}

		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (t1) VALUES (?)", tableName))
		require.NoError(t, err)
		require.NoError(t, batch.Append(timeArray))
		require.NoError(t, batch.Send())

		var outT1 []time.Duration
		row := conn.QueryRow(ctx, fmt.Sprintf("SELECT t1 FROM %s", tableName))
		require.NoError(t, row.Scan(&outT1))

		// Time truncates to seconds
		expectedArray := make([]time.Duration, len(timeArray))
		for i, v := range timeArray {
			expectedArray[i] = v.Truncate(time.Second)
		}
		assert.Equal(t, expectedArray, outT1)
	})
}

func TestTimeNullable(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupTimeTest(t, protocol)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"enable_time_time64_type": 1,
		}))

		tableName := fmt.Sprintf("test_time_nullable_%d", time.Now().UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				id UInt32,
				t1 Nullable(Time)
			) ENGINE = MergeTree() ORDER BY id`, tableName)))
		defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		// Insert NULL value
		var tNull *time.Duration
		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, t1) VALUES (?, ?)", tableName))
		require.NoError(t, err)
		require.NoError(t, batch.Append(uint32(1), tNull))
		require.NoError(t, batch.Send())

		// Insert non-NULL value
		batch, err = conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, t1) VALUES (?, ?)", tableName))
		require.NoError(t, err)
		testTime := 10*time.Hour + 30*time.Minute + 45*time.Second
		require.NoError(t, batch.Append(uint32(2), testTime))
		require.NoError(t, batch.Send())

		// Verify NULL value
		var outT1Null *time.Duration
		row := conn.QueryRow(ctx, fmt.Sprintf("SELECT t1 FROM %s WHERE id = 1", tableName))
		require.NoError(t, row.Scan(&outT1Null))
		assert.Nil(t, outT1Null)

		// Verify non-NULL value
		var outT1 *time.Duration
		row = conn.QueryRow(ctx, fmt.Sprintf("SELECT t1 FROM %s WHERE id = 2", tableName))
		require.NoError(t, row.Scan(&outT1))
		require.NotNil(t, outT1)
		assert.Equal(t, testTime, *outT1)
	})
}

func TestTimeMultipleRows(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupTimeTest(t, protocol)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"enable_time_time64_type": 1,
		}))

		tableName := fmt.Sprintf("test_time_multi_%d", time.Now().UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				id UInt32,
				t1 Time
			) ENGINE = MergeTree() ORDER BY id`, tableName)))
		defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		// Insert multiple rows
		expectedTimes := []time.Duration{
			0,
			3*time.Hour + 15*time.Minute + 30*time.Second,
			6*time.Hour + 30*time.Minute + 45*time.Second,
			12 * time.Hour,
			18*time.Hour + 45*time.Minute + 15*time.Second,
			23*time.Hour + 59*time.Minute + 59*time.Second,
		}

		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, t1) VALUES (?, ?)", tableName))
		require.NoError(t, err)
		for i, tm := range expectedTimes {
			require.NoError(t, batch.Append(uint32(i), tm))
		}
		require.NoError(t, batch.Send())

		// Query all rows
		rows, err := conn.Query(ctx, fmt.Sprintf("SELECT id, t1 FROM %s ORDER BY id", tableName))
		require.NoError(t, err)
		defer rows.Close()

		i := 0
		for rows.Next() {
			var id uint32
			var outT1 time.Duration
			require.NoError(t, rows.Scan(&id, &outT1))
			assert.Equal(t, uint32(i), id)
			assert.Equal(t, expectedTimes[i], outT1)
			i++
		}
		assert.Equal(t, len(expectedTimes), i)
	})
}
