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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestDate32(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 9, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
			CREATE TABLE test_date32 (
				  ID   UInt8
				, Col1 Date32
				, Col2 Nullable(Date32)
				, Col3 Array(Date32)
				, Col4 Array(Nullable(Date32))
			    , Col5 Date32
			    , Col6 Nullable(Date32)
			    , Col7 Array(Date32)
			    , Col8 Array(Nullable(Date32))
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_date32")
	}()
	type result struct {
		ColID uint8 `ch:"ID"`
		Col1  time.Time
		Col2  *time.Time
		Col3  []time.Time
		Col4  []*time.Time
		Col5  time.Time
		Col6  *time.Time
		Col7  []time.Time
		Col8  []*time.Time
	}
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date32")
	require.NoError(t, err)
	var (
		date1, _   = time.Parse("2006-01-02 15:04:05", "2100-01-01 00:00:00")
		date2, _   = time.Parse("2006-01-02 15:04:05", "1925-01-01 00:00:00")
		date3, _   = time.Parse("2006-01-02 15:04:05", "2283-11-11 00:00:00")
		dateStr1   = "2100-01-01"
		dateStr2   = "1925-01-01"
		dateStr3   = "2283-11-11"
		dateStrNil *string
	)
	require.NoError(t, batch.Append(uint8(1), date1, &date2, []time.Time{date2}, []*time.Time{&date2, nil, &date1}, dateStr1, dateStrNil, []string{dateStr1, dateStr2, dateStr3}, []*string{dateStrNil, &dateStr1, dateStrNil}))
	require.NoError(t, batch.Append(uint8(2), date2, nil, []time.Time{date1}, []*time.Time{nil, nil, &date2}, &testStr{Col1: dateStr1}, nil, []string{dateStr1, dateStr2, dateStr3}, []*string{nil, &dateStr1, dateStrNil}))
	require.NoError(t, batch.Append(uint8(3), date3, nil, []time.Time{date3}, []*time.Time{nil, nil, &date3}, &testStr{Col1: dateStr1}, &dateStr1, []string{dateStr1, dateStr2, dateStr3}, []*string{nil, nil, dateStrNil}))
	require.Equal(t, 3, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		result1 result
		result2 result
		result3 result
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_date32 WHERE ID = $1", 1).ScanStruct(&result1))
	require.Equal(t, date1, result1.Col1)
	assert.Equal(t, 2100, date1.Year())
	assert.Equal(t, 1, int(date1.Month()))
	assert.Equal(t, 1, date1.Day())
	assert.Equal(t, "UTC", result1.Col1.Location().String())
	assert.Equal(t, date2, *result1.Col2)
	assert.Equal(t, []time.Time{date2}, result1.Col3)
	assert.Equal(t, []*time.Time{&date2, nil, &date1}, result1.Col4)
	assert.Equal(t, dateStr1, result1.Col5.UTC().Format("2006-01-02"))
	assert.Nil(t, result1.Col6)
	assert.Equal(t, []time.Time{date1, date2, date3}, result1.Col7)
	assert.Equal(t, []*time.Time{nil, &date1, nil}, result1.Col8)

	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_date32 WHERE ID = $1", 2).ScanStruct(&result2))
	require.Equal(t, date2, result2.Col1)
	assert.Equal(t, "UTC", result2.Col1.Location().String())
	require.Nil(t, result2.Col2)
	assert.Equal(t, 1925, date2.Year())
	assert.Equal(t, 1, int(date2.Month()))
	assert.Equal(t, 1, date2.Day())
	assert.Equal(t, []time.Time{date1}, result2.Col3)
	assert.Equal(t, []*time.Time{nil, nil, &date2}, result2.Col4)
	assert.Equal(t, dateStr1, result2.Col5.UTC().Format("2006-01-02"))
	assert.Nil(t, result2.Col6)
	assert.Equal(t, []time.Time{date1, date2, date3}, result2.Col7)
	assert.Equal(t, []*time.Time{nil, &date1, nil}, result2.Col8)

	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_date32 WHERE ID = $1", 3).ScanStruct(&result3))
	require.Equal(t, date3, result3.Col1)
	assert.Equal(t, "UTC", result3.Col1.Location().String())
	require.Nil(t, result3.Col2)
	assert.Equal(t, 2283, date3.Year())
	assert.Equal(t, 11, int(date3.Month()))
	assert.Equal(t, 11, date3.Day())
	assert.Equal(t, []time.Time{date3}, result3.Col3)
	assert.Equal(t, []*time.Time{nil, nil, &date3}, result3.Col4)
	assert.Equal(t, dateStr1, result3.Col5.UTC().Format("2006-01-02"))
	assert.Equal(t, dateStr1, result3.Col6.UTC().Format("2006-01-02"))
	assert.Equal(t, []time.Time{date1, date2, date3}, result3.Col7)
	assert.Equal(t, []*time.Time{nil, nil, nil}, result3.Col8)
}

func TestDate32Extremes(t *testing.T) {
	ctx := context.Background()

	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	dateMin := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	dateMax := time.Date(2299, 12, 31, 0, 0, 0, 0, time.UTC)

	const ddl = `CREATE TABLE test_date32_extremes (min Date32, max Date32) Engine MergeTree() ORDER BY tuple()`
	conn.Exec(ctx, "DROP TABLE IF EXISTS test_date32_extremes")
	require.NoError(t, conn.Exec(ctx, ddl))

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date32_extremes")
	require.NoError(t, err)
	require.NoError(t, batch.Append(dateMin, dateMax))
	require.NoError(t, batch.Send())

	var (
		actualMin time.Time
		actualMax time.Time
	)

	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_date32_extremes").Scan(&actualMin, &actualMax))
	assert.Equal(t, dateMin, actualMin)
	assert.Equal(t, dateMax, actualMax)
}

func TestNullableDate32(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 9, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
			CREATE TABLE test_date32 (
				  Col1 Date32
				, Col2 Nullable(Date32)
				, Col3 Date32
			    , Col4 Nullable(Date32)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_date32")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date32")
	require.NoError(t, err)
	date, err := time.Parse("2006-01-02 15:04:05", "2283-11-11 00:00:00")
	require.NoError(t, err)
	dateStr := "2283-11-11"
	require.NoError(t, batch.Append(date, &date, dateStr, &dateStr))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 *time.Time
		col2 *time.Time
		col3 *time.Time
		col4 *time.Time
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_date32").Scan(&col1, &col2, &col3, &col4))
	assert.Equal(t, date, *col1)
	assert.Equal(t, date, *col2)
	assert.Equal(t, date, *col3)
	assert.Equal(t, date, *col4)
	require.NoError(t, conn.Exec(ctx, "TRUNCATE TABLE test_date32"))
	batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_date32")
	require.NoError(t, err)
	date, err = time.Parse("2006-01-02 15:04:05", "1925-01-01 00:00:00")
	require.NoError(t, err)
	require.NoError(t, batch.Append(date, nil, &date, nil))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	col2 = nil
	col4 = nil
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_date32").Scan(&col1, &col2, &col3, &col4))
	require.Nil(t, col2)
	assert.Equal(t, date, *col1)
	assert.Equal(t, date.Unix(), col1.Unix())
	assert.Equal(t, date, *col3)
	assert.Nil(t, col4)
}

func TestColumnarDate32(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 9, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_date32 (
			  ID   UInt64
			, Col1 Date32
			, Col2 Nullable(Date32)
			, Col3 Array(Date32)
			, Col4 Array(Nullable(Date32))
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_date32")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_date32")
	require.NoError(t, err)
	var (
		id       []uint64
		col1Data []time.Time
		col2Data []*time.Time
		col3Data [][]time.Time
		col4Data [][]*time.Time
	)
	var (
		date1, _ = time.Parse("2006-01-02 15:04:05", "2283-11-11 00:00:00")
		date2, _ = time.Parse("2006-01-02 15:04:05", "1925-01-01 00:00:00")
	)
	for i := 0; i < 1000; i++ {
		id = append(id, uint64(i))
		col1Data = append(col1Data, date1)
		if i%2 == 0 {
			col2Data = append(col2Data, &date2)
		} else {
			col2Data = append(col2Data, nil)
		}
		col3Data = append(col3Data, []time.Time{
			date1, date2, date1,
		})
		col4Data = append(col4Data, []*time.Time{
			&date2, nil, &date1,
		})
	}
	{
		require.NoError(t, batch.Column(0).Append(id))
		require.NoError(t, batch.Column(1).Append(col1Data))
		require.NoError(t, batch.Column(2).Append(col2Data))
		require.NoError(t, batch.Column(3).Append(col3Data))
		require.NoError(t, batch.Column(4).Append(col4Data))
	}
	require.Equal(t, 1000, batch.Rows())
	require.NoError(t, batch.Send())
	var result struct {
		Col1 time.Time
		Col2 *time.Time
		Col3 []time.Time
		Col4 []*time.Time
	}
	require.NoError(t, conn.QueryRow(ctx, "SELECT Col1, Col2, Col3, Col4 FROM test_date32 WHERE ID = $1", 11).ScanStruct(&result))
	require.Nil(t, result.Col2)
	assert.Equal(t, date1, result.Col1)
	assert.Equal(t, []time.Time{date1, date2, date1}, result.Col3)
	assert.Equal(t, []*time.Time{&date2, nil, &date1}, result.Col4)
}

func TestDate32Flush(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	if !CheckMinServerServerVersion(conn, 21, 9, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	require.NoError(t, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS date_32_flush")
	}()
	const ddl = `
		CREATE TABLE date_32_flush (
			  Col1 Date32
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO date_32_flush")
	require.NoError(t, err)
	vals := [1000]time.Time{}
	var now = time.Now()

	for i := 0; i < 1000; i++ {
		vals[i] = now.Add(time.Duration(i) * time.Hour)
		batch.Append(vals[i])
		require.Equal(t, 1, batch.Rows())
		batch.Flush()
	}
	require.Equal(t, 0, batch.Rows())
	batch.Send()
	rows, err := conn.Query(ctx, "SELECT * FROM date_32_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 time.Time
		require.NoError(t, rows.Scan(&col1))
		assert.Equal(t, vals[i].Format("2016-02-01"), col1.Format("2016-02-01"))
		i += 1
	}
}

func TestDate32TZ(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	const ddl = `
		CREATE TABLE date32_tz (
		    Col15 Date32,
		    Col16 Date32
		) Engine MergeTree() ORDER BY tuple()
		`
	conn.Exec(ctx, "DROP TABLE date32_tz")
	require.NoError(t, conn.Exec(ctx, ddl))
	require.NoError(t, err)
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO date32_tz")
	require.NoError(t, err)
	require.NoError(t, batch.Append(
		"2022-07-20",
		"2022-07-20 +08:00",
	))
	require.NoError(t, err)
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col15, col16 time.Time
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM date32_tz").Scan(
		&col15,
		&col16,
	))
	// date32 tests
	col15Expected, err := time.ParseInLocation("2006-01-02", "2022-07-20", time.UTC)
	require.NoError(t, err)
	assert.Equal(t, col15Expected.UTC(), col15)
	col16Expected, err := time.ParseInLocation("2006-01-02", "2022-07-20", time.UTC)
	require.NoError(t, err)
	assert.Equal(t, col16Expected.UTC(), col16)
}

func TestCustomDateTime32(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	const ddl = `
		CREATE TABLE date32_custom (
			Col1 DateTime
	) Engine MergeTree() ORDER BY tuple()
	`
	conn.Exec(ctx, "DROP TABLE date32_custom")
	require.NoError(t, conn.Exec(ctx, ddl))
	require.NoError(t, err)
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO date32_custom")
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Hour)
	require.NoError(t, batch.Append(now))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	row := conn.QueryRow(ctx, "SELECT * FROM date32_custom")
	var col1 CustomDateTime
	require.NoError(t, row.Scan(&col1))
	require.Equal(t, now, time.Time(col1))
}

func TestDate32WithUserLocation(t *testing.T) {
	t.Skip("Date32 decode is broken in this scenario. row.Scan returns '1977-07-01 00:00:00 +0000' instead of '2022-07-01 00:00:00 +0000'. Needs further investigation.")

	ctx := context.Background()

	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS date_with_user_location"))
	require.NoError(t, conn.Exec(ctx, `
		CREATE TABLE date_with_user_location (
			Col1 Date32
	) Engine MergeTree() ORDER BY tuple()
	`))
	require.NoError(t, conn.Exec(ctx, "INSERT INTO date_with_user_location SELECT toDate32(toStartOfMonth(toDate('2022-07-12')))"))

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

type testDate32Serializer struct {
	val time.Time
}

func (c testDate32Serializer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testDate32Serializer) Scan(src any) error {
	if t, ok := src.(time.Time); ok {
		*c = testDate32Serializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testDate32Serializer", src)
}

func TestDate32Valuer(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	if !CheckMinServerServerVersion(conn, 21, 9, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	require.NoError(t, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS date_32_valuer")
	}()
	const ddl = `
		CREATE TABLE date_32_valuer (
			  Col1 Date32
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO date_32_valuer")
	require.NoError(t, err)
	vals := [1000]time.Time{}
	var now = time.Now()

	for i := 0; i < 1000; i++ {
		vals[i] = now.Add(time.Duration(i) * time.Hour)
		batch.Append(testDate32Serializer{val: vals[i]})
		require.Equal(t, 1, batch.Rows())
		batch.Flush()
	}
	require.Equal(t, 0, batch.Rows())
	batch.Send()
	rows, err := conn.Query(ctx, "SELECT * FROM date_32_valuer")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 time.Time
		require.NoError(t, rows.Scan(&col1))
		assert.Equal(t, vals[i].Format("2016-02-01"), col1.Format("2016-02-01"))
		i += 1
	}
}
