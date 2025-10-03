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

func setupTime64Test(t *testing.T, protocol clickhouse.Protocol) clickhouse.Conn {
	conn, err := GetNativeConnection(t, protocol, nil, nil, nil)
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 25, 6, 0) {
		t.Skip("Time64 type not supported on this ClickHouse version")
	}
	return conn
}

func TestTime64(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupTime64Test(t, protocol)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"enable_time_time64_type": 1,
		}))

		tableName := fmt.Sprintf("test_time64_%d", time.Now().UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				t1 Time64(9)
			) ENGINE = MergeTree() ORDER BY tuple()`, tableName)))
		defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		// Test basic Time64 value with nanosecond precision
		t1, err := time.ParseDuration("23h59m59s123456789ns")
		require.NoError(t, err)
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

func TestTime64Precision(t *testing.T) {
	precisionTests := []struct {
		name      string
		precision int
		duration  time.Duration
		expected  time.Duration
	}{
		{
			name:      "Time64(0) - second precision",
			precision: 0,
			duration:  10*time.Hour + 20*time.Minute + 30*time.Second + 999999999*time.Nanosecond,
			expected:  10*time.Hour + 20*time.Minute + 30*time.Second,
		},
		{
			name:      "Time64(3) - millisecond precision",
			precision: 3,
			duration:  10*time.Hour + 20*time.Minute + 30*time.Second + 123456789*time.Nanosecond,
			expected:  10*time.Hour + 20*time.Minute + 30*time.Second + 123000000*time.Nanosecond,
		},
		{
			name:      "Time64(6) - microsecond precision",
			precision: 6,
			duration:  10*time.Hour + 20*time.Minute + 30*time.Second + 123456789*time.Nanosecond,
			expected:  10*time.Hour + 20*time.Minute + 30*time.Second + 123456000*time.Nanosecond,
		},
		{
			name:      "Time64(9) - nanosecond precision",
			precision: 9,
			duration:  10*time.Hour + 20*time.Minute + 30*time.Second + 123456789*time.Nanosecond,
			expected:  10*time.Hour + 20*time.Minute + 30*time.Second + 123456789*time.Nanosecond,
		},
	}

	for _, tt := range precisionTests {
		t.Run(tt.name, func(t *testing.T) {
			TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
				conn := setupTime64Test(t, protocol)

				ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
					"enable_time_time64_type": 1,
				}))

				tableName := fmt.Sprintf("test_time64_prec_%d_%d", tt.precision, time.Now().UnixNano())
				require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
					CREATE TABLE %s (
						t1 Time64(%d)
					) ENGINE = MergeTree() ORDER BY tuple()`, tableName, tt.precision)))
				defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

				batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (t1) VALUES (?)", tableName))
				require.NoError(t, err)
				require.NoError(t, batch.Append(tt.duration))
				require.NoError(t, batch.Send())

				var outT1 time.Duration
				row := conn.QueryRow(ctx, fmt.Sprintf("SELECT t1 FROM %s", tableName))
				require.NoError(t, row.Scan(&outT1))
				assert.Equal(t, tt.expected, outT1)
			})
		})
	}
}

func TestTime64EdgeCases(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupTime64Test(t, protocol)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"enable_time_time64_type": 1,
		}))

		tableName := fmt.Sprintf("test_time64_edge_%d", time.Now().UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				id UInt32,
				t1 Time64(9)
			) ENGINE = MergeTree() ORDER BY id`, tableName)))
		defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		testCases := []struct {
			name     string
			duration time.Duration
		}{
			{"Zero", 0},
			{"Midnight", 0},
			{"One nanosecond", 1 * time.Nanosecond},
			{"One microsecond", 1 * time.Microsecond},
			{"One millisecond", 1 * time.Millisecond},
			{"One second", 1 * time.Second},
			{"One minute", 1 * time.Minute},
			{"One hour", 1 * time.Hour},
			{"Midday", 12 * time.Hour},
			{"Max nanoseconds", 999999999 * time.Nanosecond},
			{"Hour with max subseconds", 1*time.Hour + 999999999*time.Nanosecond},
			{"Complex time 1", 8*time.Hour + 15*time.Minute + 25*time.Second + 123456789*time.Nanosecond},
			{"Complex time 2", 15*time.Hour + 30*time.Minute + 45*time.Second + 987654321*time.Nanosecond},
			{"End of day precise", 23*time.Hour + 59*time.Minute + 59*time.Second + 999999999*time.Nanosecond},
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
				assert.Equal(t, tc.duration, outT1, "Test case: %s", tc.name)
			})
		}
	})
}

func TestTime64Array(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupTime64Test(t, protocol)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"enable_time_time64_type": 1,
		}))

		tableName := fmt.Sprintf("test_time64_array_%d", time.Now().UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				t1 Array(Time64(9))
			) ENGINE = MergeTree() ORDER BY tuple()`, tableName)))
		defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		// Test array of Time64 values with nanosecond precision
		time64Array := []time.Duration{
			0,                                       // Midnight
			1*time.Hour + 123456789*time.Nanosecond, // 1:00:00.123456789
			6*time.Hour + 30*time.Minute + 999999999*time.Nanosecond, // 6:30:00.999999999
			12 * time.Hour, // Noon
			18*time.Hour + 45*time.Minute + 30*time.Second + 500000000*time.Nanosecond, // 18:45:30.500000000
			23*time.Hour + 59*time.Minute + 59*time.Second + 999999999*time.Nanosecond, // 23:59:59.999999999
		}

		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (t1) VALUES (?)", tableName))
		require.NoError(t, err)
		require.NoError(t, batch.Append(time64Array))
		require.NoError(t, batch.Send())

		var outT1 []time.Duration
		row := conn.QueryRow(ctx, fmt.Sprintf("SELECT t1 FROM %s", tableName))
		require.NoError(t, row.Scan(&outT1))
		assert.Equal(t, time64Array, outT1)
	})
}

func TestTime64Nullable(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupTime64Test(t, protocol)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"enable_time_time64_type": 1,
		}))

		tableName := fmt.Sprintf("test_time64_nullable_%d", time.Now().UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				id UInt32,
				t1 Nullable(Time64(9))
			) ENGINE = MergeTree() ORDER BY id`, tableName)))
		defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		// Insert NULL value
		var null *time.Duration
		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, t1) VALUES (?, ?)", tableName))
		require.NoError(t, err)
		require.NoError(t, batch.Append(uint32(1), null))
		require.NoError(t, batch.Send())

		// Insert non-NULL value
		batch, err = conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, t1) VALUES (?, ?)", tableName))
		require.NoError(t, err)
		testTime := 10*time.Hour + 30*time.Minute + 45*time.Second + 123456789*time.Nanosecond
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

func TestTime64MultipleRows(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupTime64Test(t, protocol)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"enable_time_time64_type": 1,
		}))

		tableName := fmt.Sprintf("test_time64_multi_%d", time.Now().UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				id UInt32,
				t1 Time64(9)
			) ENGINE = MergeTree() ORDER BY id`, tableName)))
		defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		// Insert multiple rows with high precision
		expectedTimes := []time.Duration{
			0,
			3*time.Hour + 15*time.Minute + 30*time.Second + 111111111*time.Nanosecond,
			6*time.Hour + 30*time.Minute + 45*time.Second + 222222222*time.Nanosecond,
			12*time.Hour + 333333333*time.Nanosecond,
			18*time.Hour + 45*time.Minute + 15*time.Second + 444444444*time.Nanosecond,
			23*time.Hour + 59*time.Minute + 59*time.Second + 999999999*time.Nanosecond,
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
