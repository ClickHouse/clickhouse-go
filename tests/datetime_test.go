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

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestDateTime(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_datetime (
				  Col1 DateTime
				, Col2 DateTime('Europe/Moscow')
				, Col3 DateTime('Europe/London')
				, Col4 Nullable(DateTime('Europe/Moscow'))
				, Col5 Array(DateTime('Europe/Moscow'))
				, Col6 Array(Nullable(DateTime('Europe/Moscow')))
				, Col7 DateTime
				, Col8 DateTime('Asia/Shanghai')
				, Col9 Nullable(DateTime('Asia/Shanghai'))
				, Col10 Array(DateTime('Asia/Shanghai'))
			    , Col11 DateTime
				, Col12 DateTime
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_datetime")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime")
	require.NoError(t, err)
	datetime := time.Now().Truncate(time.Second)
	iDateTime := datetime.Unix()
	dateTimeStr := datetime.UTC().Format("2006-01-02 15:04:05 +00:00")
	require.NoError(t, batch.Append(
		datetime,
		datetime,
		datetime,
		&datetime,
		[]time.Time{datetime, datetime},
		[]*time.Time{&datetime, nil, &datetime},
		dateTimeStr,
		dateTimeStr,
		&dateTimeStr,
		[]string{dateTimeStr, dateTimeStr},
		&testStr{Col1: dateTimeStr},
		iDateTime,
	))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1  time.Time
		col2  time.Time
		col3  time.Time
		col4  *time.Time
		col5  []time.Time
		col6  []*time.Time
		col7  time.Time
		col8  time.Time
		col9  *time.Time
		col10 []time.Time
		col11 time.Time
		col12 time.Time
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_datetime").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8, &col9, &col10, &col11, &col12))
	assert.Equal(t, datetime.In(time.UTC), col1)
	assert.Equal(t, datetime.Unix(), col2.Unix())
	assert.Equal(t, datetime.Unix(), col3.Unix())
	require.Equal(t, "Europe/Moscow", col2.Location().String())
	assert.Equal(t, "Europe/London", col3.Location().String())
	assert.Equal(t, datetime.Unix(), col4.Unix())
	require.Len(t, col5, 2)
	assert.Equal(t, "Europe/Moscow", col5[0].Location().String())
	assert.Equal(t, "Europe/Moscow", col5[1].Location().String())
	require.Len(t, col6, 3)
	assert.Nil(t, col6[1])
	assert.NotNil(t, col6[0])
	assert.NotNil(t, col6[2])
	assert.Equal(t, datetime.In(time.UTC), col7)
	assert.Equal(t, datetime.Unix(), col8.Unix())
	assert.Equal(t, datetime.Unix(), col9.Unix())
	assert.Equal(t, "Asia/Shanghai", col8.Location().String())
	require.Len(t, col10, 2)
	assert.Equal(t, "Asia/Shanghai", col10[0].Location().String())
	assert.Equal(t, "Asia/Shanghai", col10[1].Location().String())
	assert.Equal(t, datetime.In(time.UTC), col11)
	assert.Equal(t, datetime.In(time.UTC), col12)
}

func TestNullableDateTime(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_datetime (
				  Col1      DateTime
				, Col1_Null Nullable(DateTime)
				, Col2      DateTime('Europe/Moscow')
				, Col2_Null Nullable(DateTime('Europe/Moscow'))
				, Col3      DateTime('Europe/London')
				, Col3_Null Nullable(DateTime('Europe/London'))
			    , Col4      DateTime
			    , Col4_Null Nullable(DateTime)
			    , Col5		DateTime('Asia/Shanghai')
			    , Col5_Null Nullable(DateTime('Asia/Shanghai'))
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_datetime")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime")
	require.NoError(t, err)
	datetime := time.Now().Truncate(time.Second)
	require.NoError(t, batch.Append(datetime, datetime, datetime, datetime, datetime, datetime, datetime, datetime, datetime, datetime))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1     time.Time
		col1Null *time.Time
		col2     time.Time
		col2Null *time.Time
		col3     time.Time
		col3Null *time.Time
		col4     time.Time
		col4Null *time.Time
		col5     time.Time
		col5Null *time.Time
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_datetime").Scan(
		&col1, &col1Null,
		&col2, &col2Null,
		&col3, &col3Null,
		&col4, &col4Null,
		&col5, &col5Null,
	))
	assert.Equal(t, datetime.In(time.UTC), col1)
	assert.Equal(t, datetime.In(time.UTC), *col1Null)
	assert.Equal(t, datetime.Unix(), col2.Unix())
	assert.Equal(t, datetime.Unix(), col2Null.Unix())
	assert.Equal(t, datetime.Unix(), col3.Unix())
	assert.Equal(t, datetime.Unix(), col3Null.Unix())
	assert.Equal(t, datetime.Unix(), col4.Unix())
	assert.Equal(t, datetime.Unix(), col4Null.Unix())
	assert.Equal(t, datetime.Unix(), col5.Unix())
	assert.Equal(t, datetime.Unix(), col5Null.Unix())
	require.NoError(t, conn.Exec(ctx, "TRUNCATE TABLE test_datetime"))
	batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_datetime")
	require.NoError(t, err)
	{
		var (
			datetime               = time.Now().Truncate(time.Second)
			datetimeStr            = datetime.UTC().Format("2006-01-02 15:04:05 +00:00")
			datetimeNilStr *string = nil
		)
		require.NoError(t, batch.Append(datetime, nil, datetime, nil, datetime, nil, datetimeStr, nil, datetimeStr, datetimeNilStr))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Send())
		var (
			col1     time.Time
			col1Null *time.Time
			col2     time.Time
			col2Null *time.Time
			col3     time.Time
			col3Null *time.Time
			col4     time.Time
			col4Null *time.Time
			col5     time.Time
			col5Null *time.Time
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_datetime").Scan(
			&col1, &col1Null,
			&col2, &col2Null,
			&col3, &col3Null,
			&col4, &col4Null,
			&col5, &col5Null,
		))
		require.Nil(t, col1Null)
		assert.Equal(t, datetime.In(time.UTC), col1)
		assert.Equal(t, datetime.Unix(), col1.Unix())
		require.Nil(t, col2Null)
		require.Equal(t, "Europe/Moscow", col2.Location().String())
		assert.Equal(t, datetime.Unix(), col2.Unix())
		assert.Equal(t, datetime.Unix(), col2.Unix())
		require.Nil(t, col3Null)
		require.Equal(t, "Europe/London", col3.Location().String())
		assert.Equal(t, datetime.Unix(), col3.Unix())
		assert.Equal(t, datetime.Unix(), col3.Unix())
		require.Nil(t, col4Null)
		assert.Equal(t, datetime.In(time.UTC), col4)
		assert.Equal(t, datetime.Unix(), col4.Unix())
		require.Nil(t, col5Null)
		require.Equal(t, "Asia/Shanghai", col5.Location().String())
		assert.Equal(t, datetime.Unix(), col5.Unix())
		assert.Equal(t, datetime.Unix(), col5.Unix())
	}
}

func TestColumnarDateTime(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_datetime (
			  ID   UInt64
			, Col1 DateTime
			, Col2 Nullable(DateTime)
			, Col3 Array(DateTime)
			, Col4 Array(Nullable(DateTime))
		    , Col5 Array(DateTime)
		    , Col6 Array(Nullable(DateTime))
			, Col7 DateTime
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_datetime")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime")
	require.NoError(t, err)
	var (
		id       []uint64
		col1Data []time.Time
		col2Data []*time.Time
		col3Data [][]time.Time
		col4Data [][]*time.Time
		col5Data [][]string
		col6Data [][]*string
		col7Data []int64
	)
	var (
		datetime1              = time.Now().Truncate(time.Second)
		datetime2              = time.Now().Truncate(time.Second)
		datetimeStr            = datetime2.UTC().Format("2006-01-02 15:04:05 +00:00")
		datetimeNilStr *string = nil
	)
	for i := 0; i < 1000; i++ {
		id = append(id, uint64(i))
		col1Data = append(col1Data, datetime1)
		if i%2 == 0 {
			col2Data = append(col2Data, &datetime2)
		} else {
			col2Data = append(col2Data, nil)
		}
		col3Data = append(col3Data, []time.Time{
			datetime1, datetime2, datetime1,
		})
		col4Data = append(col4Data, []*time.Time{
			&datetime2, nil, &datetime1,
		})
		col5Data = append(col5Data, []string{
			datetimeStr, datetimeStr, datetimeStr,
		})
		col6Data = append(col6Data, []*string{
			datetimeNilStr, datetimeNilStr, datetimeNilStr,
		})
		col7Data = append(col7Data, datetime1.Unix())
	}
	{
		require.NoError(t, batch.Column(0).Append(id))
		require.NoError(t, batch.Column(1).Append(col1Data))
		require.NoError(t, batch.Column(2).Append(col2Data))
		require.NoError(t, batch.Column(3).Append(col3Data))
		require.NoError(t, batch.Column(4).Append(col4Data))
		require.NoError(t, batch.Column(5).Append(col5Data))
		require.NoError(t, batch.Column(6).Append(col6Data))
		require.NoError(t, batch.Column(7).Append(col7Data))
	}
	require.Equal(t, 1000, batch.Rows())
	require.NoError(t, batch.Send())
	var result struct {
		Col1 time.Time
		Col2 *time.Time
		Col3 []time.Time
		Col4 []*time.Time
		Col5 []time.Time
		Col6 []*time.Time
		Col7 time.Time
	}
	require.NoError(t, conn.QueryRow(ctx, "SELECT Col1, Col2, Col3, Col4, Col5, Col6, Col7 FROM test_datetime WHERE ID = $1", 11).ScanStruct(&result))
	require.Nil(t, result.Col2)
	assert.Equal(t, datetime1.In(time.UTC), result.Col1)
	assert.Equal(t, []time.Time{datetime1.In(time.UTC), datetime2.In(time.UTC), datetime1.In(time.UTC)}, result.Col3)
	dt2UTC := datetime2.In(time.UTC)
	dt1UTC := datetime1.In(time.UTC)
	assert.Equal(t, []*time.Time{&dt2UTC, nil, &dt1UTC}, result.Col4)
	assert.Equal(t, []time.Time{datetime2.In(time.UTC), datetime2.In(time.UTC), datetime2.In(time.UTC)}, result.Col5)
	assert.Equal(t, []*time.Time{nil, nil, nil}, result.Col6)
	assert.Equal(t, datetime1.In(time.UTC), result.Col7)
}

func TestDateTimeFlush(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE datetime_flush")
	}()
	const ddl = `
		CREATE TABLE datetime_flush (
			  Col1 DateTime
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO datetime_flush")
	require.NoError(t, err)
	vals := [1000]time.Time{}
	var now = time.Now()
	for i := 0; i < 1000; i++ {
		vals[i] = now.Add(time.Duration(i) * time.Hour).Truncate(time.Second)
		batch.Append(vals[i])
		require.Equal(t, 1, batch.Rows())
		batch.Flush()
	}
	require.Equal(t, 0, batch.Rows())
	batch.Send()
	rows, err := conn.Query(ctx, "SELECT * FROM datetime_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 time.Time
		require.NoError(t, rows.Scan(&col1))
		assert.Equal(t, vals[i].In(time.UTC), col1)
		i += 1
	}
}

func TestDateTimeTZ(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	const ddl = `
		CREATE TABLE datetime_tz (
			Col7 DateTime,
		    Col8 DateTime('UTC'),
		    Col9 DateTime('Asia/Shanghai'),
		    Col10 DateTime,
		    Col11 DateTime('UTC'),
		    Col12 DateTime('Asia/Shanghai')
		) Engine MergeTree() ORDER BY tuple()
		`
	conn.Exec(ctx, "DROP TABLE datetime_tz")
	require.NoError(t, conn.Exec(ctx, ddl))
	require.NoError(t, err)
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO datetime_tz")
	require.NoError(t, err)
	require.NoError(t, batch.Append(
		"2022-07-20 17:42:48",
		"2022-07-20 17:42:48",
		"2022-07-20 17:42:48",
		"2022-07-20 17:42:48 +08:00",
		"2022-07-20 17:42:48 +08:00",
		"2022-07-20 17:42:48 +08:00",
	))
	require.NoError(t, err)
	require.NoError(t, batch.Send())
	var (
		col7, col8, col9, col10, col11, col12 time.Time
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM datetime_tz").Scan(
		&col7,
		&col8,
		&col9,
		&col10,
		&col11,
		&col12,
	))
	asiaLoc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	// datetime - no tz
	col7Expected, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-07-20 17:42:48", time.Local)
	require.NoError(t, err)
	assert.Equal(t, col7Expected.UTC(), col7)
	col8Expected, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-07-20 17:42:48", time.Local)
	require.NoError(t, err)
	assert.Equal(t, col8Expected.UTC(), col8)
	col9Expected, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-07-20 17:42:48", time.Local)
	require.NoError(t, err)
	assert.Equal(t, col9Expected.In(asiaLoc), col9)
	// datetime - with tz
	col10Expected, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-07-20 17:42:48", asiaLoc)
	require.NoError(t, err)
	assert.Equal(t, col10Expected.UTC(), col10)
	col11Expected, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-07-20 17:42:48", asiaLoc)
	require.NoError(t, err)
	assert.Equal(t, col11Expected.UTC(), col11)
	col12Expected, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-07-20 17:42:48", asiaLoc)
	require.NoError(t, err)
	assert.Equal(t, col12Expected.In(asiaLoc), col12)
}

type CustomDateTime time.Time

func (ct *CustomDateTime) Scan(src any) error {
	if t, ok := src.(time.Time); ok {
		*ct = CustomDateTime(t)
		return nil
	}
	return fmt.Errorf("cannot scan %T into CustomDateTime", src)
}

func TestCustomDateTime(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	const ddl = `
		CREATE TABLE datetime_custom (
			Col1 DateTime
	) Engine MergeTree() ORDER BY tuple()
	`
	conn.Exec(ctx, "DROP TABLE datetime_custom")
	require.NoError(t, conn.Exec(ctx, ddl))
	require.NoError(t, err)
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO datetime_custom")
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, batch.Append(now))
	require.NoError(t, batch.Send())
	row := conn.QueryRow(ctx, "SELECT * FROM datetime_custom")
	var col1 CustomDateTime
	require.NoError(t, row.Scan(&col1))
	require.Equal(t, now, time.Time(col1))
}

type testDateTimeSerializer struct {
	val time.Time
}

func (c testDateTimeSerializer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testDateTimeSerializer) Scan(src any) error {
	if t, ok := src.(time.Time); ok {
		*c = testDateTimeSerializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testDateTimeSerializer", src)
}

func TestDateTimeValuer(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE datetime_valuer")
	}()
	const ddl = `
		CREATE TABLE datetime_valuer (
			  Col1 DateTime
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO datetime_valuer")
	require.NoError(t, err)
	vals := [1000]time.Time{}
	var now = time.Now()
	for i := 0; i < 1000; i++ {
		vals[i] = now.Add(time.Duration(i) * time.Hour).Truncate(time.Second)
		batch.Append(testDateTimeSerializer{val: vals[i]})
		require.Equal(t, 1, batch.Rows())
		batch.Flush()
	}
	require.Equal(t, 0, batch.Rows())
	batch.Send()
	rows, err := conn.Query(ctx, "SELECT * FROM datetime_valuer")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 time.Time
		require.NoError(t, rows.Scan(&col1))
		assert.Equal(t, vals[i].In(time.UTC), col1)
		i += 1
	}
}
