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
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

type customUint8 uint8

func (f *customUint8) Scan(src any) error {
	if t, ok := src.(uint8); ok {
		*f = customUint8(t)
		return nil
	}
	return fmt.Errorf("cannot scan %T into customUint8", src)
}

func TestUInt8(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_uint8 (
				  ID   UInt8
				, Col1 UInt8
				, Col2 Nullable(UInt8)
				, Col3 Array(UInt8)
				, Col4 Array(Nullable(UInt8))
				, Col5 UInt8
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_uint8")
	}()
	type result struct {
		ColID uint8 `ch:"ID"`
		Col1  uint8
		Col2  *uint8
		Col3  []uint8
		Col4  []*uint8
		Col5  customUint8
	}
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_uint8")
	require.NoError(t, err)
	data := uint8(42)
	require.NoError(t, err)
	require.NoError(t, batch.Append(uint8(1), data, &data, []uint8{data}, []*uint8{&data, nil, &data}, customUint8(data)))
	require.NoError(t, batch.Append(uint8(2), data, nil, []uint8{data}, []*uint8{nil, nil, &data}, customUint8(data)))
	require.Equal(t, 2, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		result1 result
		result2 result
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_uint8 WHERE ID = $1", 1).ScanStruct(&result1))
	require.Equal(t, data, result1.Col1)
	assert.Equal(t, data, *result1.Col2)
	assert.Equal(t, []uint8{data}, result1.Col3)
	assert.Equal(t, []*uint8{&data, nil, &data}, result1.Col4)
	require.Equal(t, customUint8(data), result1.Col5)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_uint8 WHERE ID = $1", 2).ScanStruct(&result2))
	require.Equal(t, data, result2.Col1)
	require.Nil(t, result2.Col2)
	assert.Equal(t, []uint8{data}, result2.Col3)
	assert.Equal(t, []*uint8{nil, nil, &data}, result2.Col4)
	require.Equal(t, customUint8(data), result2.Col5)
}

func TestColumnarUInt8(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_uint8_c (
			  ID   UInt64
			, Col1 UInt8
			, Col2 Nullable(UInt8)
			, Col3 Array(UInt8)
			, Col4 Array(Nullable(UInt8))
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_uint8_c")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_uint8_c")
	require.NoError(t, err)
	var (
		id       []uint64
		col1Data []uint8
		col2Data []*uint8
		col3Data [][]uint8
		col4Data [][]*uint8
	)
	data := uint8(42)
	for i := 0; i < 1000; i++ {
		id = append(id, uint64(i))
		col1Data = append(col1Data, data)
		if i%2 == 0 {
			col2Data = append(col2Data, &data)
		} else {
			col2Data = append(col2Data, nil)
		}
		col3Data = append(col3Data, []uint8{
			data, data, data,
		})
		col4Data = append(col4Data, []*uint8{
			&data, nil, &data,
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
	}
	require.Equal(t, 1000, batch.Rows())
	require.NoError(t, batch.Send())
	var result struct {
		Col1 uint8
		Col2 *uint8
		Col3 []uint8
		Col4 []*uint8
	}
	require.NoError(t, conn.QueryRow(ctx, "SELECT Col1, Col2, Col3, Col4 FROM test_uint8_c WHERE ID = $1", 11).ScanStruct(&result))
	require.Nil(t, result.Col2)
	assert.Equal(t, data, result.Col1)
	assert.Equal(t, []uint8{data, data, data}, result.Col3)
	assert.Equal(t, []*uint8{&data, nil, &data}, result.Col4)
}

func TestSimpleInt(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 9, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = "CREATE TABLE test_int (`1` Int64) Engine MergeTree() ORDER BY tuple()"
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_int")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_int")
	require.NoError(t, err)
	require.NoError(t, batch.Append(222))
	require.NoError(t, batch.Send())
}

func TestNullableInt(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 9, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = "CREATE TABLE test_int (col1 Int64, col2 Nullable(Int64), col3 Int32, col4 Nullable(Int32), col5 Int16, col6 Nullable(Int16)) Engine MergeTree() ORDER BY tuple()"
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_int")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_int")
	require.NoError(t, err)
	col1Data := sql.NullInt64{Int64: 1, Valid: true}
	col2Data := sql.NullInt64{Int64: 0, Valid: false}
	col3Data := sql.NullInt32{Int32: 2, Valid: true}
	col4Data := sql.NullInt32{Int32: 0, Valid: false}
	col5Data := sql.NullInt16{Int16: 3, Valid: true}
	col6Data := sql.NullInt16{Int16: 0, Valid: false}
	require.NoError(t, batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 sql.NullInt64
		col2 sql.NullInt64
		col3 sql.NullInt32
		col4 sql.NullInt32
		col5 sql.NullInt16
		col6 sql.NullInt16
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_int").Scan(&col1, &col2, &col3, &col4, &col5, &col6))
	require.Equal(t, col1Data, col1)
	require.Equal(t, col2Data, col2)
	require.Equal(t, col3Data, col3)
	require.Equal(t, col4Data, col4)
	require.Equal(t, col5Data, col5)
	require.Equal(t, col6Data, col6)
}

func TestIntFlush(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS int_flush")
	}()
	const ddl = `
		CREATE TABLE int_flush (
			  Col1 UInt8
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO int_flush")
	require.NoError(t, err)
	vals := [1000]uint8{}

	for i := 0; i < 1000; i++ {
		vals[i] = uint8(i)
		require.NoError(t, batch.Append(vals[i]))
		if i%100 == 0 {
			if i == 0 {
				require.Equal(t, 1, batch.Rows())
			} else {
				require.Equal(t, 100, batch.Rows())
			}
			require.NoError(t, batch.Flush())
		}
	}
	require.Equal(t, 99, batch.Rows())
	batch.Send()
	rows, err := conn.Query(ctx, "SELECT * FROM int_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 uint8
		require.NoError(t, rows.Scan(&col1))
		assert.Equal(t, vals[i], col1)
		i += 1
	}
}
