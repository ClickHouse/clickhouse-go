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

func TestDateTime64(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 20, 3, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
			CREATE TABLE test_datetime64 (
				  Col1 DateTime64(3)
				, Col2 DateTime64(9, 'Europe/Moscow')
				, Col3 DateTime64(0, 'Europe/London')
				, Col4 Nullable(DateTime64(3, 'Europe/Moscow'))
				, Col5 Array(DateTime64(3, 'Europe/Moscow'))
				, Col6 Array(Nullable(DateTime64(3, 'Europe/Moscow')))
				, Col7 DateTime64(3) 
				, Col8 DateTime64(6) 
				, Col9 DateTime64(9)
			    , Col10 DateTime64(9)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_datetime64")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime64")
	require.NoError(t, err)
	var (
		datetime1   = time.Now().Truncate(time.Millisecond)
		datetime2   = time.Now().Truncate(time.Nanosecond)
		datetime3   = time.Now().Truncate(time.Second)
		datetimeStu = &testStr{
			Col1: datetime1.UTC().Format("2006-01-02 15:04:05.999 +00:00"),
		}
	)
	require.NoError(t, batch.Append(
		datetime1,
		datetime2,
		datetime3,
		&datetime1,
		[]time.Time{datetime1, datetime1},
		[]*time.Time{&datetime3, nil, &datetime3},
		datetime1.UTC().Format("2006-01-02 15:04:05.999 +00:00"),
		datetime1.UTC().Format("2006-01-02 15:04:05.999 +00:00"),
		datetime1.UTC().Format("2006-01-02 15:04:05.999 +00:00"),
		datetimeStu,
	))
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
		col9  time.Time
		col10 time.Time
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_datetime64").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8, &col9, &col10))
	assert.Equal(t, datetime1.In(time.UTC), col1)
	assert.Equal(t, datetime2.UnixNano(), col2.UnixNano())
	assert.Equal(t, datetime3.UnixNano(), col3.UnixNano())
	require.Equal(t, "Europe/Moscow", col2.Location().String())
	assert.Equal(t, "Europe/London", col3.Location().String())
	assert.Equal(t, datetime1.UnixNano(), col4.UnixNano())
	require.Len(t, col5, 2)
	assert.Equal(t, "Europe/Moscow", col5[0].Location().String())
	assert.Equal(t, "Europe/Moscow", col5[1].Location().String())
	require.Len(t, col6, 3)
	assert.Nil(t, col6[1])
	assert.NotNil(t, col6[0])
	assert.NotNil(t, col6[2])
	assert.Equal(t, datetime1.In(time.UTC), col7)
	assert.Equal(t, datetime1.In(time.UTC), col8)
	assert.Equal(t, datetime1.In(time.UTC), col9)
	assert.Equal(t, datetime1.In(time.UTC), col10)
}

func TestDateTime64AsReference(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 20, 3, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
			CREATE TABLE test_datetime64 (
				Col1      DateTime64(3)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_datetime64")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime64")
	require.NoError(t, err)
	now := time.Now().Unix()
	err = batch.Append(&now)
	assert.NoError(t, err)
	assert.NoError(t, batch.Send())
	batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_datetime64")
	require.NoError(t, err)
	// batch column
	var col1Data []*int64
	var datetime1 = time.Now().Unix()
	for i := 0; i < 1000; i++ {
		col1Data = append(col1Data, &datetime1)
	}
	if err := batch.Column(0).Append(col1Data); !assert.NoError(t, err) {
		return
	}
	assert.NoError(t, batch.Send())
}

func TestNullableDateTime64(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 20, 3, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
			CREATE TABLE test_datetime64 (
				    Col1      DateTime64(3)
				, Col1_Null Nullable(DateTime64(3))
				, Col2      DateTime64(9, 'Europe/Moscow')
				, Col2_Null Nullable(DateTime64(9, 'Europe/Moscow'))
				, Col3      DateTime64(0, 'Europe/London')
				, Col3_Null Nullable(DateTime64(0, 'Europe/London'))
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_datetime64")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime64")
	require.NoError(t, err)
	var (
		datetime1 = time.Now().Truncate(time.Millisecond)
		datetime2 = time.Now().Truncate(time.Nanosecond)
		datetime3 = time.Now().Truncate(time.Second)
	)
	require.NoError(t, batch.Append(datetime1, datetime1, datetime2, datetime2, datetime3, datetime3))
	require.NoError(t, batch.Send())
	var (
		col1     time.Time
		col1Null *time.Time
		col2     time.Time
		col2Null *time.Time
		col3     time.Time
		col3Null *time.Time
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_datetime64").Scan(
		&col1, &col1Null,
		&col2, &col2Null,
		&col3, &col3Null,
	))
	assert.Equal(t, datetime1.In(time.UTC), col1)
	assert.Equal(t, datetime1.In(time.UTC), *col1Null)
	assert.Equal(t, datetime2.UnixNano(), col2.UnixNano())
	assert.Equal(t, datetime2.UnixNano(), col2Null.UnixNano())
	assert.Equal(t, datetime3.UnixNano(), col3.UnixNano())
	assert.Equal(t, datetime3.UnixNano(), col3Null.UnixNano())
	require.NoError(t, conn.Exec(ctx, "TRUNCATE TABLE test_datetime64"))
	batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_datetime64")
	require.NoError(t, err)
	{
		var (
			datetime1 = time.Now().Truncate(time.Millisecond)
			datetime2 = time.Now().Truncate(time.Nanosecond)
			datetime3 = time.Now().Truncate(time.Second)
		)
		require.NoError(t, batch.Append(datetime1, nil, datetime2, nil, datetime3, nil))
		require.NoError(t, batch.Send())
		var (
			col1     time.Time
			col1Null *time.Time
			col2     time.Time
			col2Null *time.Time
			col3     time.Time
			col3Null *time.Time
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_datetime64").Scan(
			&col1, &col1Null,
			&col2, &col2Null,
			&col3, &col3Null,
		))
		require.Nil(t, col1Null)
		assert.Equal(t, datetime1.In(time.UTC), col1)
		require.Nil(t, col2Null)
		require.Equal(t, "Europe/Moscow", col2.Location().String())
		assert.Equal(t, datetime2.UnixNano(), col2.UnixNano())
		require.Nil(t, col3Null)
		require.Equal(t, "Europe/London", col3.Location().String())
		assert.Equal(t, datetime3.UnixNano(), col3.UnixNano())
	}
}

func TestColumnarDateTime64(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 20, 3, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_datetime64 (
			  ID   UInt64
			, Col1 DateTime64(3)
			, Col2 Nullable(DateTime64(3))
			, Col3 Array(DateTime64(3))
			, Col4 Array(Nullable(DateTime64(3)))
			, Col5 DateTime64(3) 
			, Col6 DateTime64(6) 
			, Col7 DateTime64(9)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_datetime64")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime64")
	require.NoError(t, err)
	var (
		id       []uint64
		col1Data []time.Time
		col2Data []*time.Time
		col3Data [][]time.Time
		col4Data [][]*time.Time
		col5Data []string
		col6Data []string
		col7Data []string
	)
	var (
		datetime1 = time.Now().Truncate(time.Millisecond)
		datetime2 = time.Now().Truncate(time.Second)
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

		col5Data = append(col5Data, datetime1.UTC().Format("2006-01-02 15:04:05.999 +00:00"))
		col6Data = append(col6Data, datetime1.UTC().Format("2006-01-02 15:04:05.999 +00:00"))
		col7Data = append(col7Data, datetime1.UTC().Format("2006-01-02 15:04:05.999 +00:00"))
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
	require.NoError(t, batch.Send())
	var result struct {
		Col1 time.Time
		Col2 *time.Time
		Col3 []time.Time
		Col4 []*time.Time
		Col5 time.Time
		Col6 time.Time
		Col7 time.Time
	}
	require.NoError(t, conn.QueryRow(ctx, "SELECT Col1, Col2, Col3, Col4, Col5, Col6, Col7 FROM test_datetime64 WHERE ID = $1", 11).ScanStruct(&result))
	require.Nil(t, result.Col2)
	assert.Equal(t, datetime1.In(time.UTC), result.Col1)
	assert.Equal(t, []time.Time{datetime1.In(time.UTC), datetime2.In(time.UTC), datetime1.In(time.UTC)}, result.Col3)
	dt2UTC := datetime2.In(time.UTC)
	dt1UTC := datetime1.In(time.UTC)
	assert.Equal(t, []*time.Time{&dt2UTC, nil, &dt1UTC}, result.Col4)
	assert.Equal(t, datetime1.In(time.UTC), result.Col5)
	assert.Equal(t, datetime1.In(time.UTC), result.Col6)
	assert.Equal(t, datetime1.In(time.UTC), result.Col7)
}

func TestDateTime64Flush(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS datetime_64_flush")
	}()
	const ddl = `
		CREATE TABLE datetime_64_flush (
			  Col1 DateTime64(3)
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO datetime_64_flush")
	require.NoError(t, err)
	vals := [1000]time.Time{}
	var now = time.Now()
	for i := 0; i < 1000; i++ {
		vals[i] = now.Add(time.Duration(i) * time.Hour).Truncate(time.Millisecond)
		batch.Append(vals[i])
		batch.Flush()
	}
	batch.Send()
	rows, err := conn.Query(ctx, "SELECT * FROM datetime_64_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 time.Time
		require.NoError(t, rows.Scan(&col1))
		assert.Equal(t, vals[i].In(time.UTC), col1)
		i += 1
	}
}

func TestDateTime64TZ(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	const ddl = `
		CREATE TABLE datetime64_tz (
			Id Int64,
			Col1 DateTime64(3),
		    Col2 DateTime64(6, 'UTC'),
		    Col3 DateTime64(9, 'Asia/Shanghai'),
		    Col4 DateTime64(3),
		    Col5 DateTime64(6, 'UTC'),
		    Col6 DateTime64(9, 'Asia/Shanghai')
		) Engine MergeTree() ORDER BY tuple()
		`
	conn.Exec(ctx, "DROP TABLE datetime64_tz")
	require.NoError(t, conn.Exec(ctx, ddl))
	require.NoError(t, err)
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO datetime64_tz")
	require.NoError(t, err)
	require.NoError(t, batch.Append(
		int64(23),
		"2022-07-20 17:42:48.129",
		"2022-07-20 17:42:48.129876",
		"2022-07-20 17:42:48.129876123",
		"2022-07-20 17:42:48.129 +08:00",
		"2022-07-20 17:42:48.129876 +08:00",
		"2022-07-20 17:42:48.129876123 +08:00",
	))
	require.NoError(t, err)
	require.NoError(t, batch.Send())
	var (
		id                                 int64
		col1, col2, col3, col4, col5, col6 time.Time
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM datetime64_tz").Scan(
		&id,
		&col1,
		&col2,
		&col3,
		&col4,
		&col5,
		&col6,
	))
	assert.Equal(t, int64(23), id)
	// datetime64 - no tz
	col1Expected, err := time.ParseInLocation("2006-01-02 15:04:05.999999999", "2022-07-20 17:42:48.129", time.Local)
	require.NoError(t, err)
	assert.Equal(t, col1Expected.UTC(), col1)
	col2Expected, err := time.ParseInLocation("2006-01-02 15:04:05.999999999", "2022-07-20 17:42:48.129876", time.Local)
	require.NoError(t, err)
	assert.Equal(t, col2Expected.UTC(), col2)
	asiaLoc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	col3Expected, err := time.ParseInLocation("2006-01-02 15:04:05.999999999", "2022-07-20 17:42:48.129876123", time.Local)
	require.NoError(t, err)
	assert.Equal(t, col3Expected.In(asiaLoc), col3)
	col4Expected, err := time.ParseInLocation("2006-01-02 15:04:05.999999999", "2022-07-20 17:42:48.129", asiaLoc)
	require.NoError(t, err)
	// datetime64 - with tz
	assert.Equal(t, col4Expected.UTC(), col4)
	col5Expected, err := time.ParseInLocation("2006-01-02 15:04:05.999999999", "2022-07-20 17:42:48.129876", asiaLoc)
	require.NoError(t, err)
	assert.Equal(t, col5Expected.UTC(), col5)
	col6Expected, err := time.ParseInLocation("2006-01-02 15:04:05.999999999", "2022-07-20 17:42:48.129876123", asiaLoc)
	require.NoError(t, err)
	assert.Equal(t, col6Expected.In(asiaLoc), col6)
}

func TestCustomDateTime64(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	const ddl = `
		CREATE TABLE datetime64_custom (
			Col1 DateTime64(3)
	) Engine MergeTree() ORDER BY tuple()
	`
	conn.Exec(ctx, "DROP TABLE datetime64_custom")
	require.NoError(t, conn.Exec(ctx, ddl))
	require.NoError(t, err)
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO datetime64_custom")
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, batch.Append(now))
	require.NoError(t, batch.Send())
	row := conn.QueryRow(ctx, "SELECT * FROM datetime64_custom")
	var col1 CustomDateTime
	require.NoError(t, row.Scan(&col1))
	require.Equal(t, now, time.Time(col1))
}

type testDateTime64Serializer struct {
	val time.Time
}

func (c testDateTime64Serializer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testDateTime64Serializer) Scan(src any) error {
	if t, ok := src.(time.Time); ok {
		*c = testDateTime64Serializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testDateTime64Serializer", src)
}

func TestDateTime64Valuer(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS datetime_64_valuer")
	}()
	const ddl = `
		CREATE TABLE datetime_64_valuer (
			  Col1 DateTime64(3)
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO datetime_64_valuer")
	require.NoError(t, err)
	vals := [1000]time.Time{}
	var now = time.Now()
	for i := 0; i < 1000; i++ {
		vals[i] = now.Add(time.Duration(i) * time.Hour).Truncate(time.Millisecond)
		batch.Append(testDateTime64Serializer{val: vals[i]})
		batch.Flush()
	}
	batch.Send()
	rows, err := conn.Query(ctx, "SELECT * FROM datetime_64_valuer")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 time.Time
		require.NoError(t, rows.Scan(&col1))
		assert.Equal(t, vals[i].In(time.UTC), col1)
		i += 1
	}
}
