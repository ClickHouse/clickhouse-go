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
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/rnbondarenko/clickhouse-go/v2"
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
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_datetime")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime")
	require.NoError(t, err)
	datetime := time.Now().Truncate(time.Second)
	dateTimeStr := datetime.UTC().Format("2006-01-02 15:04:05")
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
		col9  *time.Time
		col10 []time.Time
		col11 time.Time
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_datetime").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8, &col9, &col10, &col11))
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
		conn.Exec(ctx, "DROP TABLE test_datetime")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_datetime")
	require.NoError(t, err)
	datetime := time.Now().Truncate(time.Second)
	require.NoError(t, batch.Append(datetime, datetime, datetime, datetime, datetime, datetime, datetime, datetime, datetime, datetime))
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
			datetimeStr            = datetime.UTC().Format("2006-01-02 15:04:05")
			datetimeNilStr *string = nil
		)
		require.NoError(t, batch.Append(datetime, nil, datetime, nil, datetime, nil, datetimeStr, nil, datetimeStr, datetimeNilStr))
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
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_datetime")
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
	)
	var (
		datetime1              = time.Now().Truncate(time.Second)
		datetime2              = time.Now().Truncate(time.Second)
		datetimeStr            = datetime2.UTC().Format("2006-01-02 15:04:05")
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
	}
	{
		if err := batch.Column(0).Append(id); !assert.NoError(t, err) {
			return
		}
		if err := batch.Column(1).Append(col1Data); !assert.NoError(t, err) {
			return
		}
		if err := batch.Column(2).Append(col2Data); !assert.NoError(t, err) {
			return
		}
		if err := batch.Column(3).Append(col3Data); !assert.NoError(t, err) {
			return
		}
		if err := batch.Column(4).Append(col4Data); !assert.NoError(t, err) {
			return
		}
		if err := batch.Column(5).Append(col5Data); !assert.NoError(t, err) {
			return
		}
		if err := batch.Column(6).Append(col6Data); !assert.NoError(t, err) {
			return
		}
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
	require.NoError(t, conn.QueryRow(ctx, "SELECT Col1, Col2, Col3, Col4, Col5, Col6 FROM test_datetime WHERE ID = $1", 11).ScanStruct(&result))
	require.Nil(t, result.Col2)
	assert.Equal(t, datetime1.In(time.UTC), result.Col1)
	assert.Equal(t, []time.Time{datetime1.In(time.UTC), datetime2.In(time.UTC), datetime1.In(time.UTC)}, result.Col3)
	dt2UTC := datetime2.In(time.UTC)
	dt1UTC := datetime1.In(time.UTC)
	assert.Equal(t, []*time.Time{&dt2UTC, nil, &dt1UTC}, result.Col4)
	assert.Equal(t, []time.Time{datetime2.In(time.UTC), datetime2.In(time.UTC), datetime2.In(time.UTC)}, result.Col5)
	assert.Equal(t, []*time.Time{nil, nil, nil}, result.Col6)
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
		batch.Flush()
	}
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
