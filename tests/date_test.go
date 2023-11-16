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
	"database/sql/driver"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestDate(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_date (
				  ID   UInt8
				, Col1 Date
				, Col2 Nullable(Date)
				, Col3 Array(Date)
				, Col4 Array(Nullable(Date))
				, Col5 Date
			    , Col6 Nullable(Date)
				, Col7 Date
			    , Col8 Nullable(Date)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_date")
	}()
	type result struct {
		ColID uint8 `ch:"ID"`
		Col1  time.Time
		Col2  *time.Time
		Col3  []time.Time
		Col4  []*time.Time
		Col5  time.Time
		Col6  *time.Time
		Col7  time.Time
		Col8  *time.Time
	}
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date")
	require.NoError(t, err)
	dateStr := "2022-01-12"
	testStuStr := testStr{
		Col1: dateStr,
	}
	date, err := time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
	require.NoError(t, err)
	require.NoError(t, batch.Append(uint8(1), date, &date, []time.Time{date}, []*time.Time{&date, nil, &date}, dateStr, dateStr, testStuStr, &testStuStr))
	require.NoError(t, batch.Append(uint8(2), date, nil, []time.Time{date}, []*time.Time{nil, nil, &date}, dateStr, dateStr, testStuStr, &testStuStr))
	require.NoError(t, batch.Send())
	var (
		result1 result
		result2 result
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_date WHERE ID = $1", 1).ScanStruct(&result1))
	require.Equal(t, date, result1.Col1)
	assert.Equal(t, "UTC", result1.Col1.Location().String())
	assert.Equal(t, date, *result1.Col2)
	assert.Equal(t, []time.Time{date}, result1.Col3)
	assert.Equal(t, []*time.Time{&date, nil, &date}, result1.Col4)
	assert.Equal(t, date, result1.Col5)
	assert.Equal(t, date, *result1.Col6)
	assert.Equal(t, date, result1.Col7)
	assert.Equal(t, date, *result1.Col8)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_date WHERE ID = $1", 2).ScanStruct(&result2))
	require.Equal(t, date, result2.Col1)
	assert.Equal(t, "UTC", result2.Col1.Location().String())
	require.Nil(t, result2.Col2)
	assert.Equal(t, []time.Time{date}, result2.Col3)
	assert.Equal(t, []*time.Time{nil, nil, &date}, result2.Col4)
}

func TestNullableDate(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_date (
				  Col1 Date
				, Col2 Nullable(Date)
			    , Col3 Nullable(Date)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_date")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date")

	require.NoError(t, err)
	date, err := time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
	var dateNilStr *string = nil
	require.NoError(t, err)
	require.NoError(t, batch.Append(date, date, dateNilStr))
	require.NoError(t, batch.Send())
	var (
		col1 *time.Time
		col2 *time.Time
		col3 *time.Time
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_date").Scan(&col1, &col2, &col3))
	assert.Equal(t, date, *col1)
	assert.Equal(t, date, *col2)
	assert.Nil(t, col3)
	require.NoError(t, conn.Exec(ctx, "TRUNCATE TABLE test_date"))
	batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_date")
	require.NoError(t, err)
	date, err = time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
	require.NoError(t, err)
	require.NoError(t, batch.Append(date, nil, nil))
	require.NoError(t, batch.Send())
	{
		var (
			col1 *time.Time
			col2 *time.Time
			col3 *time.Time
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_date").Scan(&col1, &col2, &col3))
		require.Nil(t, col2)
		assert.Equal(t, date, *col1)
		assert.Equal(t, date.Unix(), col1.Unix())
		assert.Nil(t, col3)
	}
}

func TestColumnarDate(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_date (
			  ID   UInt64
			, Col1 Date
			, Col2 Nullable(Date)
			, Col3 Array(Date)
			, Col4 Array(Nullable(Date))
		    , Col5 Array(Date)
			, Col6 Array(Nullable(Date))
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_date")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date")
	require.NoError(t, err)
	var (
		id       []uint64
		col1Data []time.Time
		col2Data []*time.Time
		col3Data [][]time.Time
		col4Data [][]*time.Time
		col5Data [][]string
		col6Data [][]*string
	)
	dateStr := "2022-01-12"
	date, err := time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
	if !assert.NoError(t, err) {
		return
	}
	for i := 0; i < 1000; i++ {
		id = append(id, uint64(i))
		col1Data = append(col1Data, date)
		if i%2 == 0 {
			col2Data = append(col2Data, &date)
		} else {
			col2Data = append(col2Data, nil)
		}
		col3Data = append(col3Data, []time.Time{
			date, date, date,
		})
		col4Data = append(col4Data, []*time.Time{
			&date, nil, &date,
		})
		col5Data = append(col5Data, []string{
			dateStr, dateStr, dateStr,
		})
		col6Data = append(col6Data, []*string{
			&dateStr, nil, nil,
		})
	}
	{
		require.NoError(t, batch.Column(0).Append(id))
		require.NoError(t, batch.Column(1).Append(col1Data))
		require.NoError(t, batch.Column(2).Append(col2Data))
		require.NoError(t, batch.Column(3).Append(col3Data))
		require.NoError(t, batch.Column(4).Append(col4Data))
		require.NoError(t, batch.Column(5).Append(col5Data))
		require.NoError(t, batch.Column(6).Append(col6Data))
	}
	require.NoError(t, batch.Send())
	var result struct {
		Col1 time.Time
		Col2 *time.Time
		Col3 []time.Time
		Col4 []*time.Time
		Col5 []time.Time
		Col6 []*time.Time
	}
	require.NoError(t, conn.QueryRow(ctx, "SELECT Col1, Col2, Col3, Col4, Col5, Col6 FROM test_date WHERE ID = $1", 11).ScanStruct(&result))
	require.Nil(t, result.Col2)
	assert.Equal(t, date, result.Col1)
	assert.Equal(t, []time.Time{date, date, date}, result.Col3)
	assert.Equal(t, []*time.Time{&date, nil, &date}, result.Col4)
	assert.Equal(t, []time.Time{date, date, date}, result.Col5)
	assert.Equal(t, []*time.Time{&date, nil, nil}, result.Col6)
}

func TestDateFlush(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS date_flush")
	}()
	const ddl = `
		CREATE TABLE date_flush (
			  Col1 Date
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO date_flush")
	require.NoError(t, err)
	vals := [1000]time.Time{}
	var now = time.Now()

	for i := 0; i < 1000; i++ {
		vals[i] = now.Add(time.Duration(i) * time.Hour)
		batch.Append(vals[i])
		batch.Flush()
	}
	batch.Send()
	rows, err := conn.Query(ctx, "SELECT * FROM date_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 time.Time
		require.NoError(t, rows.Scan(&col1))
		assert.Equal(t, vals[i].Format("2016-02-01"), col1.Format("2016-02-01"))
		i += 1
	}
}

func TestDateTZ(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	const ddl = `
		CREATE TABLE date_tz (
		    Col13 Date,
		    Col14 Date
		) Engine MergeTree() ORDER BY tuple()
		`
	conn.Exec(ctx, "DROP TABLE date_tz")
	require.NoError(t, conn.Exec(ctx, ddl))
	require.NoError(t, err)
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO date_tz")
	require.NoError(t, err)
	require.NoError(t, batch.Append(
		"2022-07-20",
		"2022-07-20 +08:00",
	))
	require.NoError(t, err)
	require.NoError(t, batch.Send())
	var (
		col13, col14 time.Time
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM date_tz").Scan(
		&col13,
		&col14,
	))
	// date tests
	col13Expected, err := time.ParseInLocation("2006-01-02", "2022-07-20", time.UTC)
	require.NoError(t, err)
	assert.Equal(t, col13Expected.UTC(), col13)
	col14Expected, err := time.ParseInLocation("2006-01-02", "2022-07-20", time.UTC)
	require.NoError(t, err)
	assert.Equal(t, col14Expected.UTC(), col14)
}

func TestCustomDate(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	const ddl = `
		CREATE TABLE date_custom (
			Col1 DateTime64(3)
	) Engine MergeTree() ORDER BY tuple()
	`
	conn.Exec(ctx, "DROP TABLE date_custom")
	require.NoError(t, conn.Exec(ctx, ddl))
	require.NoError(t, err)
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO date_custom")
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Hour)
	require.NoError(t, batch.Append(now))
	require.NoError(t, batch.Send())
	row := conn.QueryRow(ctx, "SELECT * FROM date_custom")
	var col1 CustomDateTime
	require.NoError(t, row.Scan(&col1))
	require.Equal(t, now, time.Time(col1))
}

func TestDateWithUserLocation(t *testing.T) {
	ctx := context.Background()

	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS date_with_user_location"))
	require.NoError(t, conn.Exec(ctx, `
		CREATE TABLE date_with_user_location (
			Col1 Date
	) Engine MergeTree() ORDER BY tuple()
	`))
	require.NoError(t, conn.Exec(ctx, "INSERT INTO date_with_user_location SELECT toStartOfMonth(toDate('2022-07-12'))"))

	userLocation, _ := time.LoadLocation("Pacific/Pago_Pago")
	queryCtx := clickhouse.Context(ctx, clickhouse.WithUserLocation(userLocation))

	var col1 time.Time
	row := conn.QueryRow(queryCtx, "SELECT * FROM date_with_user_location")
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&col1))

	const dateTimeNoZoneFormat = "2006-01-02T15:04:05"
	assert.Equal(t, "2022-07-01T00:00:00", col1.Format(dateTimeNoZoneFormat))
	assert.Equal(t, userLocation.String(), col1.Location().String())
}

type testDateSerializer struct {
	val time.Time
}

func (c testDateSerializer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testDateSerializer) Scan(src any) error {
	if t, ok := src.(time.Time); ok {
		*c = testDateSerializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testDateSerializer", src)
}

func TestDateValuer(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS date_valuer")
	}()
	const ddl = `
		CREATE TABLE date_valuer (
			  Col1 Date
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO date_valuer")
	require.NoError(t, err)
	vals := [1000]time.Time{}
	var now = time.Now()

	for i := 0; i < 1000; i++ {
		vals[i] = now.Add(time.Duration(i) * time.Hour)
		batch.Append(testDateSerializer{val: vals[i]})
		batch.Flush()
	}
	batch.Send()
	rows, err := conn.Query(ctx, "SELECT * FROM date_valuer")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 time.Time
		require.NoError(t, rows.Scan(&col1))
		assert.Equal(t, vals[i].Format("2016-02-01"), col1.Format("2016-02-01"))
		i += 1
	}
}
