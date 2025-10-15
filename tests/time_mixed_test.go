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

func setupTimeMixedTest(t *testing.T, protocol clickhouse.Protocol) clickhouse.Conn {
	conn, err := GetNativeConnection(t, protocol, nil, nil, nil)
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 25, 6, 0) {
		t.Skip("Time/Time64 types not supported on this ClickHouse version")
	}
	return conn
}

func TestTimeMixed(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupTimeMixedTest(t, protocol)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"enable_time_time64_type": 1,
		}))

		tableName := fmt.Sprintf("test_time_mixed_%d", time.Now().UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				t_time Time,
				t_time64_0 Time64(0),
				t_time64_3 Time64(3),
				t_time64_6 Time64(6),
				t_time64_9 Time64(9)
			) ENGINE = MergeTree() ORDER BY tuple()`, tableName)))
		defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		// Insert same base time in different precision columns
		baseTime := 12*time.Hour + 34*time.Minute + 56*time.Second + 123456789*time.Nanosecond

		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?, ?, ?)", tableName))
		require.NoError(t, err)
		require.NoError(t, batch.Append(baseTime, baseTime, baseTime, baseTime, baseTime))
		require.NoError(t, batch.Send())

		var (
			outTime     time.Duration
			outTime64_0 time.Duration
			outTime64_3 time.Duration
			outTime64_6 time.Duration
			outTime64_9 time.Duration
		)

		row := conn.QueryRow(ctx, fmt.Sprintf("SELECT t_time, t_time64_0, t_time64_3, t_time64_6, t_time64_9 FROM %s", tableName))
		require.NoError(t, row.Scan(&outTime, &outTime64_0, &outTime64_3, &outTime64_6, &outTime64_9))

		// Verify precision handling
		assert.Equal(t, baseTime.Truncate(time.Second), outTime, "Time should truncate to seconds")
		assert.Equal(t, baseTime.Truncate(time.Second), outTime64_0, "Time64(0) should truncate to seconds")
		assert.Equal(t, baseTime.Truncate(time.Millisecond), outTime64_3, "Time64(3) should truncate to milliseconds")
		assert.Equal(t, baseTime.Truncate(time.Microsecond), outTime64_6, "Time64(6) should truncate to microseconds")
		assert.Equal(t, baseTime, outTime64_9, "Time64(9) should preserve nanoseconds")
	})
}

func TestTimeMixedArrays(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupTimeMixedTest(t, protocol)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"enable_time_time64_type": 1,
		}))

		tableName := fmt.Sprintf("test_time_mixed_arrays_%d", time.Now().UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				arr_time Array(Time),
				arr_time64_3 Array(Time64(3)),
				arr_time64_9 Array(Time64(9))
			) ENGINE = MergeTree() ORDER BY tuple()`, tableName)))
		defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		// Create arrays with different precision values
		timeArray := []time.Duration{
			0,
			6*time.Hour + 30*time.Minute + 15*time.Second + 123456789*time.Nanosecond,
			12*time.Hour + 999999999*time.Nanosecond,
			18*time.Hour + 45*time.Minute + 30*time.Second + 500000000*time.Nanosecond,
		}

		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?)", tableName))
		require.NoError(t, err)
		require.NoError(t, batch.Append(timeArray, timeArray, timeArray))
		require.NoError(t, batch.Send())

		var (
			outArrTime     []time.Duration
			outArrTime64_3 []time.Duration
			outArrTime64_9 []time.Duration
		)

		row := conn.QueryRow(ctx, fmt.Sprintf("SELECT arr_time, arr_time64_3, arr_time64_9 FROM %s", tableName))
		require.NoError(t, row.Scan(&outArrTime, &outArrTime64_3, &outArrTime64_9))

		// Verify array precision handling
		for i, v := range timeArray {
			assert.Equal(t, v.Truncate(time.Second), outArrTime[i], "Array(Time) should truncate to seconds")
			assert.Equal(t, v.Truncate(time.Millisecond), outArrTime64_3[i], "Array(Time64(3)) should truncate to milliseconds")
			assert.Equal(t, v, outArrTime64_9[i], "Array(Time64(9)) should preserve nanoseconds")
		}
	})
}

func TestTimeMixedNullable(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupTimeMixedTest(t, protocol)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"enable_time_time64_type": 1,
		}))

		tableName := fmt.Sprintf("test_time_mixed_nullable_%d", time.Now().UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				id UInt32,
				t_time Nullable(Time),
				t_time64_3 Nullable(Time64(3)),
				t_time64_9 Nullable(Time64(9))
			) ENGINE = MergeTree() ORDER BY id`, tableName)))
		defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		// Insert row with all NULLs
		var (
			t1Null *time.Duration
			t2Null *time.Duration
			t3Null *time.Duration
		)
		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?, ?)", tableName))
		require.NoError(t, err)
		require.NoError(t, batch.Append(uint32(1), t1Null, t2Null, t3Null))
		require.NoError(t, batch.Send())

		// Insert row with all non-NULL values
		testTime := 15*time.Hour + 30*time.Minute + 45*time.Second + 123456789*time.Nanosecond
		batch, err = conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?, ?)", tableName))
		require.NoError(t, err)
		require.NoError(t, batch.Append(uint32(2), testTime, testTime, testTime))
		require.NoError(t, batch.Send())

		// Insert row with mixed NULL and non-NULL
		batch, err = conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?, ?)", tableName))
		require.NoError(t, err)
		require.NoError(t, batch.Append(uint32(3), testTime, t2Null, testTime))
		require.NoError(t, batch.Send())

		// Verify all NULLs
		var nullTime, nullTime64_3, nullTime64_9 *time.Duration
		row := conn.QueryRow(ctx, fmt.Sprintf("SELECT t_time, t_time64_3, t_time64_9 FROM %s WHERE id = 1", tableName))
		require.NoError(t, row.Scan(&nullTime, &nullTime64_3, &nullTime64_9))
		assert.Nil(t, nullTime)
		assert.Nil(t, nullTime64_3)
		assert.Nil(t, nullTime64_9)

		// Verify all non-NULL values
		var outTime, outTime64_3, outTime64_9 *time.Duration
		row = conn.QueryRow(ctx, fmt.Sprintf("SELECT t_time, t_time64_3, t_time64_9 FROM %s WHERE id = 2", tableName))
		require.NoError(t, row.Scan(&outTime, &outTime64_3, &outTime64_9))
		require.NotNil(t, outTime)
		require.NotNil(t, outTime64_3)
		require.NotNil(t, outTime64_9)
		assert.Equal(t, testTime.Truncate(time.Second), *outTime)
		assert.Equal(t, testTime.Truncate(time.Millisecond), *outTime64_3)
		assert.Equal(t, testTime, *outTime64_9)

		// Verify mixed NULL and non-NULL
		var mixedTime, mixedTime64_3, mixedTime64_9 *time.Duration
		row = conn.QueryRow(ctx, fmt.Sprintf("SELECT t_time, t_time64_3, t_time64_9 FROM %s WHERE id = 3", tableName))
		require.NoError(t, row.Scan(&mixedTime, &mixedTime64_3, &mixedTime64_9))
		require.NotNil(t, mixedTime)
		assert.Nil(t, mixedTime64_3)
		require.NotNil(t, mixedTime64_9)
		assert.Equal(t, testTime.Truncate(time.Second), *mixedTime)
		assert.Equal(t, testTime, *mixedTime64_9)
	})
}

func TestTimeMixedMultipleRows(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupTimeMixedTest(t, protocol)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"enable_time_time64_type": 1,
		}))

		tableName := fmt.Sprintf("test_time_mixed_multi_%d", time.Now().UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				id UInt32,
				t_time Time,
				t_time64_9 Time64(9)
			) ENGINE = MergeTree() ORDER BY id`, tableName)))
		defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		// Insert multiple rows with different times
		testData := []struct {
			id   uint32
			time time.Duration
		}{
			{1, 0},
			{2, 6*time.Hour + 30*time.Minute + 123456789*time.Nanosecond},
			{3, 12*time.Hour + 999999999*time.Nanosecond},
			{4, 18*time.Hour + 45*time.Minute + 30*time.Second + 500000000*time.Nanosecond},
			{5, 23*time.Hour + 59*time.Minute + 59*time.Second + 999999999*time.Nanosecond},
		}

		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?)", tableName))
		require.NoError(t, err)
		for _, td := range testData {
			require.NoError(t, batch.Append(td.id, td.time, td.time))
		}
		require.NoError(t, batch.Send())

		// Query and verify all rows
		rows, err := conn.Query(ctx, fmt.Sprintf("SELECT id, t_time, t_time64_9 FROM %s ORDER BY id", tableName))
		require.NoError(t, err)
		defer rows.Close()

		i := 0
		for rows.Next() {
			var id uint32
			var outTime, outTime64_9 time.Duration
			require.NoError(t, rows.Scan(&id, &outTime, &outTime64_9))

			assert.Equal(t, testData[i].id, id)
			assert.Equal(t, testData[i].time.Truncate(time.Second), outTime)
			assert.Equal(t, testData[i].time, outTime64_9)
			i++
		}
		assert.Equal(t, len(testData), i)
	})
}

func TestTimeMixedComplexTypes(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupTimeMixedTest(t, protocol)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"enable_time_time64_type": 1,
		}))

		tableName := fmt.Sprintf("test_time_mixed_complex_%d", time.Now().UnixNano())
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				arr_time Array(Time),
				arr_time64 Array(Time64(9)),
				nullable_time Nullable(Time),
				nullable_time64 Nullable(Time64(9))
			) ENGINE = MergeTree() ORDER BY tuple()`, tableName)))
		defer conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		// Create test data
		timeArray := []time.Duration{
			6*time.Hour + 123456789*time.Nanosecond,
			12*time.Hour + 30*time.Minute + 987654321*time.Nanosecond,
			18 * time.Hour,
		}
		nullableTime := 10*time.Hour + 20*time.Minute + 30*time.Second + 555555555*time.Nanosecond

		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s VALUES (?, ?, ?, ?)", tableName))
		require.NoError(t, err)
		require.NoError(t, batch.Append(timeArray, timeArray, nullableTime, nullableTime))
		require.NoError(t, batch.Send())

		var (
			outArrTime        []time.Duration
			outArrTime64      []time.Duration
			outNullableTime   *time.Duration
			outNullableTime64 *time.Duration
		)

		row := conn.QueryRow(ctx, fmt.Sprintf("SELECT arr_time, arr_time64, nullable_time, nullable_time64 FROM %s", tableName))
		require.NoError(t, row.Scan(&outArrTime, &outArrTime64, &outNullableTime, &outNullableTime64))

		// Verify arrays
		for i, v := range timeArray {
			assert.Equal(t, v.Truncate(time.Second), outArrTime[i])
			assert.Equal(t, v, outArrTime64[i])
		}

		// Verify nullable values
		require.NotNil(t, outNullableTime)
		require.NotNil(t, outNullableTime64)
		assert.Equal(t, nullableTime.Truncate(time.Second), *outNullableTime)
		assert.Equal(t, nullableTime, *outNullableTime64)
	})
}
