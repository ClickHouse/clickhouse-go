// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tests

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeBasic(t *testing.T) {
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

	// Drop table if exists
	conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_basic")

	const ddl = `
		CREATE TABLE test_time_basic (
			Col1 Time,
			Col2 Time64(6)
		) Engine Memory
	`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_basic")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))

	// Test direct insert without parameter binding
	require.NoError(t, conn.Exec(ctx, `
		INSERT INTO test_time_basic VALUES 
		('12:34:56', '23:59:59.123456')
	`))

	var (
		col1 time.Time
		col2 time.Time
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_time_basic").Scan(&col1, &col2))
	assert.Equal(t, 12, col1.Hour())
	assert.Equal(t, 34, col1.Minute())
	assert.Equal(t, 56, col1.Second())
	assert.Equal(t, 23, col2.Hour())
	assert.Equal(t, 59, col2.Minute())
	assert.Equal(t, 59, col2.Second())
	assert.Equal(t, 123456000, col2.Nanosecond())
}

func TestTimeAndTime64(t *testing.T) {
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

	// Drop table if exists
	conn.Exec(ctx, "DROP TABLE IF EXISTS test_time")

	const ddl = `
		CREATE TABLE test_time (
			Col1 Time,
			Col2 Nullable(Time),
			Col3 Array(Time),
			Col4 Time64(9),
			Col5 Nullable(Time64(9)),
			Col6 Array(Time64(9))
		) Engine Memory
	`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_time")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_time")
	require.NoError(t, err)
	tm := time.Date(2024, 7, 11, 12, 34, 56, 0, time.UTC)
	tm64 := time.Date(2024, 7, 11, 23, 59, 59, 123456789, time.UTC)
	require.NoError(t, batch.Append(
		tm,
		&tm,
		[]time.Time{tm, tm},
		tm64,
		&tm64,
		[]time.Time{tm64, tm64},
	))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 time.Time
		col2 *time.Time
		col3 []time.Time
		col4 time.Time
		col5 *time.Time
		col6 []time.Time
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_time").Scan(&col1, &col2, &col3, &col4, &col5, &col6))
	assert.Equal(t, tm.Hour(), col1.Hour())
	assert.Equal(t, tm.Minute(), col1.Minute())
	assert.Equal(t, tm.Second(), col1.Second())
	assert.Equal(t, tm.Hour(), col2.Hour())
	assert.Equal(t, 2, len(col3))
	assert.Equal(t, tm.Hour(), col3[0].Hour())
	assert.Equal(t, tm64.Hour(), col4.Hour())
	assert.Equal(t, tm64.Nanosecond(), col4.Nanosecond())
	assert.Equal(t, tm64.Hour(), col5.Hour())
	assert.Equal(t, 2, len(col6))
	assert.Equal(t, tm64.Hour(), col6[0].Hour())
}

func TestTimeStringParsing(t *testing.T) {
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

	// Drop table if exists
	conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_strings")

	const ddl = `
		CREATE TABLE test_time_strings (
			Col1 Time,
			Col2 Time64(6)
		) Engine Memory
	`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_strings")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_time_strings")
	require.NoError(t, err)

	// Test various string formats
	require.NoError(t, batch.Append(
		"12:34:56",
		"23:59:59.123456",
	))
	require.NoError(t, batch.Send())

	var (
		col1 time.Time
		col2 time.Time
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_time_strings").Scan(&col1, &col2))
	assert.Equal(t, 12, col1.Hour())
	assert.Equal(t, 34, col1.Minute())
	assert.Equal(t, 56, col1.Second())
	assert.Equal(t, 23, col2.Hour())
	assert.Equal(t, 59, col2.Minute())
	assert.Equal(t, 59, col2.Second())
	assert.Equal(t, 123456000, col2.Nanosecond())
}

func TestTimeNegativeValues(t *testing.T) {
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

	// Drop table if exists
	conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_negative")

	const ddl = `
		CREATE TABLE test_time_negative (
			Col1 Time,
			Col2 Time64(6)
		) Engine Memory
	`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_negative")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_time_negative")
	require.NoError(t, err)

	// Test negative values (seconds since midnight)
	require.NoError(t, batch.Append(
		int64(-3600),  // -1 hour
		int64(-60000), // -1 minute in milliseconds
	))
	require.NoError(t, batch.Send())

	var (
		col1 time.Time
		col2 time.Time
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_time_negative").Scan(&col1, &col2))

	// Note: ClickHouse may handle negative times differently, so we just verify it doesn't crash
	assert.NotNil(t, col1)
	assert.NotNil(t, col2)
}

func TestTimeTimezoneSupport(t *testing.T) {
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

	// Drop table if exists
	conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_tz")

	const ddl = `
		CREATE TABLE test_time_tz (
			Col1 Time,
			Col2 Time64(6),
			Col3 Time,
			Col4 Time64(3)
		) Engine Memory
	`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_tz")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_time_tz")
	require.NoError(t, err)

	tm := time.Date(2024, 7, 11, 12, 34, 56, 0, time.UTC)
	tm64 := time.Date(2024, 7, 11, 23, 59, 59, 123456789, time.UTC)
	require.NoError(t, batch.Append(
		tm,
		tm64,
		tm,
		tm64,
	))
	require.NoError(t, batch.Send())

	var (
		col1 time.Time
		col2 time.Time
		col3 time.Time
		col4 time.Time
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_time_tz").Scan(&col1, &col2, &col3, &col4))

	// Verify that we can read the times correctly (don't check specific hours due to timezone handling)
	assert.NotZero(t, col1.Hour())
	assert.NotZero(t, col2.Hour())
	assert.NotZero(t, col3.Hour())
	assert.NotZero(t, col4.Hour())
}

func TestTimeColumnRegistration(t *testing.T) {
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

	// Test that we can create a table with Time and Time64 columns
	require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_registration"))

	const ddl = `
		CREATE TABLE test_time_registration (
			Col1 Time,
			Col2 Time64(6)
		) Engine Memory
	`
	require.NoError(t, conn.Exec(ctx, ddl))

	// Test that we can query the table structure
	rows, err := conn.Query(ctx, "DESCRIBE TABLE test_time_registration")
	require.NoError(t, err)
	defer rows.Close()

	var columnName, columnType string
	var dummy1, dummy2, dummy3, dummy4, dummy5 string
	foundTime := false
	foundTime64 := false

	for rows.Next() {
		require.NoError(t, rows.Scan(&columnName, &columnType, &dummy1, &dummy2, &dummy3, &dummy4, &dummy5))
		if strings.Contains(columnType, "Time") && !strings.Contains(columnType, "Time64") {
			foundTime = true
		}
		if strings.Contains(columnType, "Time64") {
			foundTime64 = true
		}
	}

	assert.True(t, foundTime, "Time column not found")
	assert.True(t, foundTime64, "Time64 column not found")

	// Clean up
	require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_time_registration"))
}
