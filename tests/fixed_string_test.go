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
	"crypto/rand"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

type BinFixedString struct {
	data [10]byte
}

func (bin *BinFixedString) MarshalBinary() ([]byte, error) {
	return bin.data[:], nil
}

func (bin *BinFixedString) UnmarshalBinary(b []byte) error {
	copy(bin.data[:], b)
	return nil
}

func TestFixedString(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_fixed_string (
				Col1 FixedString(10)
				, Col2 FixedString(10)
				, Col3 Nullable(FixedString(10))
				, Col4 Array(FixedString(10))
				, Col5 Array(Nullable(FixedString(10)))
			    , Col6 FixedString(12)
			    , Col7 FixedString(10)
				, Col8 FixedString(10)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_fixed_string")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_fixed_string")
	require.NoError(t, err)
	var (
		col1Data = "ClickHouse"
		col2Data = &BinFixedString{}
		col3Data = &col1Data
		col4Data = []string{"ClickHouse", "ClickHouse", "ClickHouse"}
		col5Data = []*string{&col1Data, nil, &col1Data}
		col6Data = "clickhouse"
		col7Data = []byte("clickhouse")
		col8Data = [10]byte{99, 108, 105, 99, 107, 104, 111, 117, 115, 101}
	)
	_, err = rand.Read(col2Data.data[:])
	require.NoError(t, err)
	require.NoError(t, batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data, col7Data, col8Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 string
		col2 BinFixedString
		col3 *string
		col4 []string
		col5 []*string
		col6 string
		col7 []byte
		col8 [10]byte
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_fixed_string").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8))
	assert.Equal(t, col1Data, col1)
	assert.Equal(t, col2Data.data, col2.data)
	assert.Equal(t, col3Data, col3)
	assert.Equal(t, col4Data, col4)
	assert.Equal(t, col5Data, col5)
	assert.Equal(t, col6Data+string([]byte{0, 0}), col6)
	assert.Equal(t, col7Data, col7)
	assert.Equal(t, col8Data, col8)
	rows, err := conn.Query(ctx, "SELECT CAST('RU' AS FixedString(2)) FROM system.numbers_mt LIMIT 10")
	require.NoError(t, err)
	var count int
	for rows.Next() {
		var code string
		if !assert.NoError(t, rows.Scan(&code)) || !assert.Equal(t, "RU", code) {
			return
		}
		count++
	}
	require.Equal(t, 10, count)
	require.NoError(t, rows.Err())
	assert.NoError(t, rows.Close())
}

func TestEmptyFixedString(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_fixed_string_empty (
				Col1 FixedString(2),
				Col2 FixedString(2),
				Col3 FixedString(2),
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_fixed_string_empty")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_fixed_string_empty")
	require.NoError(t, err)
	var (
		col1Data         = ""
		col2Data         = "US"
		col3Data *string = nil
	)
	require.NoError(t, batch.Append(col1Data, col2Data, col3Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 string
		col2 string
		col3 string
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_fixed_string_empty").Scan(&col1, &col2, &col3))
	emptyVal := string([]byte{0, 0})
	assert.Equal(t, emptyVal, col1)
	assert.Equal(t, col2Data, col2)
	assert.Equal(t, emptyVal, col3)
}

func TestOverflowFixedString(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO function null('x FixedString(16)')")
	require.NoError(t, err)

	input := "this is NOT the correct length."

	err = batch.Append(input)
	require.ErrorContains(t, err, "input value with length")
	require.ErrorContains(t, err, "exceeds FixedString(16) capacity")
}

func TestPaddedFixedString(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO function null('x FixedString(16)')")
	require.NoError(t, err)

	input := "str too short"
	err = batch.Append(input)
	require.NoError(t, err)
}

func TestNullableFixedString(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_nullable_fixed_string (
			  Col1 Nullable(FixedString(10))
			, Col2 Nullable(FixedString(10))
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_nullable_fixed_string")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_nullable_fixed_string")
	require.NoError(t, err)
	var (
		col1Data = "ClickHouse"
		col2Data = &BinFixedString{}
	)
	_, err = rand.Read(col2Data.data[:])
	require.NoError(t, err)
	require.NoError(t, batch.Append(col1Data, col2Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 string
		col2 BinFixedString
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_nullable_fixed_string").Scan(&col1, &col2))
	assert.Equal(t, col1Data, col1)
	assert.Equal(t, col2Data.data, col2.data)
	require.NoError(t, conn.Exec(ctx, "TRUNCATE TABLE test_nullable_fixed_string"))
	batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_nullable_fixed_string")
	require.NoError(t, err)
	col1Data = "ClickHouse"
	require.NoError(t, batch.Append(col1Data, nil))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	{
		var (
			col1 *string
			col2 *string
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_nullable_fixed_string").Scan(&col1, &col2))
		require.Nil(t, col2)
		assert.Equal(t, col1Data, *col1)
	}
}

func TestColumnarFixedString(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_fixed_string (
			  Col1 FixedString(10)
			, Col2 FixedString(10)
			, Col3 Nullable(FixedString(10))
			, Col4 Array(FixedString(10))
			, Col5 Array(Nullable(FixedString(10)))
			, Col6 FixedString(10)
			, Col7 FixedString(10)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_fixed_string")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_fixed_string")
	require.NoError(t, err)
	var (
		col1Data = "ClickHouse"
		col2Data = "XXXXXXXXXX"
		col3Data = &col1Data
		col4Data = []string{"ClickHouse", "ClickHouse", "ClickHouse"}
		col5Data = []*string{&col1Data, nil, &col1Data}
		col6Data = []byte("clickhouse")
		col7Data = [10]byte{99, 108, 105, 99, 107, 104, 111, 117, 115, 101}
	)
	require.NoError(t, batch.Column(0).Append([]string{
		col1Data, col1Data, col1Data, col1Data, col1Data,
	}))
	require.NoError(t, batch.Column(1).Append([]string{
		col2Data, col2Data, col2Data, col2Data, col2Data,
	}))
	require.NoError(t, batch.Column(2).Append([]*string{
		col3Data, col3Data, col3Data, col3Data, col3Data,
	}))
	require.NoError(t, batch.Column(3).Append([][]string{
		col4Data, col4Data, col4Data, col4Data, col4Data,
	}))
	require.NoError(t, batch.Column(4).Append([][]*string{
		col5Data, col5Data, col5Data, col5Data, col5Data,
	}))
	require.NoError(t, batch.Column(5).Append([][]byte{
		col6Data, col6Data, col6Data, col6Data, col6Data,
	}))
	require.NoError(t, batch.Column(6).Append([][10]byte{
		col7Data, col7Data, col7Data, col7Data, col7Data,
	}))
	require.Equal(t, 5, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 string
		col2 string
		col3 *string
		col4 []string
		col5 []*string
		col6 []byte
		col7 [10]byte
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_fixed_string LIMIT 1").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7))
	assert.Equal(t, col1Data, col1)
	assert.Equal(t, col2Data, col2)
	assert.Equal(t, col3Data, col3)
	assert.Equal(t, col4Data, col4)
	assert.Equal(t, col5Data, col5)
	assert.Equal(t, col6Data, col6)
	assert.Equal(t, col7Data, col7)
}

func BenchmarkFixedString(b *testing.B) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS benchmark_fixed_string")
	}()
	if err = conn.Exec(ctx, `DROP TABLE IF EXISTS benchmark_fixed_string`); err != nil {
		b.Fatal(err)
	}
	if err = conn.Exec(ctx, `CREATE TABLE benchmark_fixed_string (Col1 UInt64, Col2 FixedString(4)) ENGINE = Null`); err != nil {
		b.Fatal(err)
	}

	const rowsInBlock = 10_000_000

	for n := 0; n < b.N; n++ {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO benchmark_fixed_string")
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < rowsInBlock; i++ {
			if err := batch.Append(uint64(1), "test"); err != nil {
				b.Fatal(err)
			}
		}
		if err = batch.Send(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkColumnarFixedString(b *testing.B) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS benchmark_fixed_string")
	}()
	if err = conn.Exec(ctx, `CREATE TABLE benchmark_fixed_string (Col1 UInt64, Col2 FixedString(4)) ENGINE = Null`); err != nil {
		b.Fatal(err)
	}

	const rowsInBlock = 10_000_000

	var (
		col1 []uint64
		col2 []string
	)
	for n := 0; n < b.N; n++ {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO benchmark_fixed_string")
		if err != nil {
			b.Fatal(err)
		}
		col1 = col1[:0]
		col2 = col2[:0]
		for i := 0; i < rowsInBlock; i++ {
			col1 = append(col1, uint64(1))
			col2 = append(col2, "test")
		}
		if err := batch.Column(0).Append(col1); err != nil {
			b.Fatal(err)
		}
		if err := batch.Column(1).Append(col2); err != nil {
			b.Fatal(err)
		}
		if err = batch.Send(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestFixedStringFlush(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS fixed_string_flush")
	}()
	const ddl = `
		CREATE TABLE fixed_string_flush (
			  Col1 FixedString(10)
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO fixed_string_flush")
	require.NoError(t, err)
	vals := [1000]string{}
	for i := 0; i < 1000; i++ {
		vals[i] = RandIntString(10)
		batch.Append(vals[i])
		batch.Flush()
	}
	batch.Send()
	rows, err := conn.Query(ctx, "SELECT * FROM fixed_string_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 string
		require.NoError(t, rows.Scan(&col1))
		require.Equal(t, vals[i], col1)
		i += 1
	}
	require.Equal(t, 1000, i)
}

func TestFixedStringFromDriverValuerType(t *testing.T) {
	conn, err := GetConnection("native", nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()

	require.NoError(t, err)
	require.NoError(t, conn.Ping(ctx))
	if !CheckMinServerServerVersion(conn, 21, 9, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_fixed_string (
			  	  Col1 FixedString(5)
		        , Col2 FixedString(5)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_fixed_string")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_fixed_string")
	require.NoError(t, err)

	type data struct {
		Col1 string               `ch:"Col1"`
		Col2 testStringSerializer `ch:"Col2"`
	}
	require.NoError(t, batch.AppendStruct(&data{
		Col1: "Value",
		Col2: testStringSerializer{"Value"},
	}))
	require.NoError(t, batch.Send())

	var dest data
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_fixed_string").ScanStruct(&dest))
	assert.Equal(t, "Value", dest.Col1)
	assert.Equal(t, testStringSerializer{"Value"}, dest.Col2)
}

type testFixedStringPtrSerializer struct {
	val string
}

func (c testFixedStringPtrSerializer) Value() (driver.Value, error) {
	return &c.val, nil
}

func (c *testFixedStringPtrSerializer) Scan(src any) error {
	if t, ok := src.(string); ok {
		*c = testFixedStringPtrSerializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testFixedStringPtrSerializer", src)
}

func TestFixedStringFromDriverValuerTypeNonStdReturn(t *testing.T) {
	conn, err := GetConnection("native", nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()

	require.NoError(t, err)
	require.NoError(t, conn.Ping(ctx))
	if !CheckMinServerServerVersion(conn, 21, 9, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_fixed_string (
			  	  Col1 FixedString(5)
		        , Col2 FixedString(5)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_fixed_string")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_fixed_string")
	require.NoError(t, err)

	s := "Value"
	type data struct {
		Col1 string                       `ch:"Col1"`
		Col2 testFixedStringPtrSerializer `ch:"Col2"`
	}
	require.NoError(t, batch.AppendStruct(&data{
		Col1: "Value",
		Col2: testFixedStringPtrSerializer{s},
	}))
	require.NoError(t, batch.Send())

	var dest data
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_fixed_string").ScanStruct(&dest))
	assert.Equal(t, "Value", dest.Col1)
	assert.Equal(t, testFixedStringPtrSerializer{"Value"}, dest.Col2)
}
