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
	"database/sql/driver"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type testStr struct {
	Col1 string
}

func (t testStr) String() string {
	return t.Col1
}

func TestSimpleString(t *testing.T) {
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
		CREATE TABLE test_string (
			  	  Col1 String
		        , Col2 String
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_string")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_string")
	require.NoError(t, err)
	require.NoError(t, batch.Append("A", &testStr{"B"}))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
}

type customStr string

func (s *customStr) Scan(src any) error {
	if t, ok := src.(string); ok {
		*s = customStr(t)
		return nil
	}
	return fmt.Errorf("cannot scan %T into customStr", src)
}

func (s customStr) String() string {
	return string(s)
}

func TestCustomString(t *testing.T) {
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
		CREATE TABLE test_string (
			  	  Col1 String
		        , Col2 String
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_string")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_string")
	require.NoError(t, err)

	type data struct {
		Col1 string    `ch:"Col1"`
		Col2 customStr `ch:"Col2"`
	}
	require.NoError(t, batch.AppendStruct(&data{
		Col1: "A",
		Col2: "B",
	}))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())

	var dest data
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_string").ScanStruct(&dest))
	assert.Equal(t, "A", dest.Col1)
	assert.Equal(t, customStr("B"), dest.Col2)
}

func TestString(t *testing.T) {
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
		CREATE TABLE test_string (
			  Col1 String
			, Col2 Array(String)
			, Col3 Nullable(String)
			, Col4 String
			, Col5 Nullable(String)
      		, Col6 String
		    , Col7 String
		    , Col8 Nullable(String)
		    , Col9 String
		    , Col10 Nullable(String)
			, Col11 Nullable(String)
		) Engine MergeTree() ORDER BY tuple()
	`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_string")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_string")
	require.NoError(t, err)
	col6Data := "D"
	col7Data := time.Now()
	col8Data := &time.Time{}
	col9Data := &testStr{"E"}
	var col10Data testStr
	col11Data := "G"
	require.NoError(t, batch.Append(
		"A",
		[]string{"A", "B", "C"},
		nil,
		sql.NullString{String: "D", Valid: true},
		sql.NullString{Valid: false},
		[]byte(col6Data),
		col7Data,
		col8Data,
		col9Data,
		&col10Data,
		&col11Data,
	))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1  string
		col2  []string
		col3  *string
		col4  sql.NullString
		col5  sql.NullString
		col6  string
		col7  string
		col8  string
		col9  string
		col10 string
		col11 string
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_string").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8, &col9, &col10, &col11))
	require.Nil(t, col3)
	assert.Equal(t, "A", col1)
	assert.Equal(t, []string{"A", "B", "C"}, col2)
	assert.Equal(t, sql.NullString{String: "D", Valid: true}, col4)
	assert.Equal(t, sql.NullString{Valid: false}, col5)
	assert.Equal(t, col6Data, col6)
	assert.Equal(t, col7, col7Data.String())
	assert.Equal(t, col8, col8Data.String())
	assert.Equal(t, col9, col9Data.String())
	assert.Equal(t, col10, col10Data.String())
	assert.Equal(t, "G", col11)
}

func BenchmarkString(b *testing.B) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		conn.Exec(ctx, "DROP TABLE benchmark_string")
	}()

	if err = conn.Exec(ctx, `CREATE TABLE benchmark_string (Col1 UInt64, Col2 String) ENGINE = Null`); err != nil {
		b.Fatal(err)
	}

	const rowsInBlock = 10_000_000

	for n := 0; n < b.N; n++ {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO benchmark_string VALUES")
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

func BenchmarkColumnarString(b *testing.B) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		conn.Exec(ctx, "DROP TABLE benchmark_string")
	}()
	if err = conn.Exec(ctx, `CREATE TABLE benchmark_string (Col1 UInt64, Col2 String) ENGINE = Null`); err != nil {
		b.Fatal(err)
	}

	const rowsInBlock = 10_000_000

	var (
		col1 []uint64
		col2 []string
	)
	for n := 0; n < b.N; n++ {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO benchmark_string VALUES")
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

func TestStringFlush(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE string_flush")
	}()
	const ddl = `
		CREATE TABLE string_flush (
			  Col1 FixedString(10)
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO string_flush")
	require.NoError(t, err)
	vals := [1000]string{}
	for i := 0; i < 1000; i++ {
		vals[i] = RandAsciiString(10)
		batch.Append(vals[i])
		require.Equal(t, 1, batch.Rows())
		batch.Flush()
	}
	require.Equal(t, 0, batch.Rows())
	batch.Send()
	rows, err := conn.Query(ctx, "SELECT * FROM string_flush")
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

type testStringSerializer struct {
	val string
}

func (c testStringSerializer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testStringSerializer) Scan(src any) error {
	if t, ok := src.(string); ok {
		*c = testStringSerializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testStringSerializer", src)
}

func TestStringFromDriverValuerType(t *testing.T) {
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
		CREATE TABLE test_string (
			  	  Col1 String
		        , Col2 String
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_string")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_string")
	require.NoError(t, err)

	type data struct {
		Col1 string               `ch:"Col1"`
		Col2 testStringSerializer `ch:"Col2"`
	}
	require.NoError(t, batch.AppendStruct(&data{
		Col1: "Value",
		Col2: testStringSerializer{"Value"},
	}))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())

	var dest data
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_string").ScanStruct(&dest))
	assert.Equal(t, "Value", dest.Col1)
	assert.Equal(t, testStringSerializer{"Value"}, dest.Col2)
}

type testStringPtrSerializer struct {
	val string
}

func (c testStringPtrSerializer) Value() (driver.Value, error) {
	return &c.val, nil
}

func (c *testStringPtrSerializer) Scan(src any) error {
	if t, ok := src.(string); ok {
		*c = testStringPtrSerializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testStringPtrSerializer", src)
}

func TestStringFromDriverValuerTypeNonStdReturn(t *testing.T) {
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
		CREATE TABLE test_string (
			  	  Col1 String
		        , Col2 String
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_string")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_string")
	require.NoError(t, err)

	type data struct {
		Col1 string                  `ch:"Col1"`
		Col2 testStringPtrSerializer `ch:"Col2"`
	}
	s := "Value"
	require.NoError(t, batch.AppendStruct(&data{
		Col1: s,
		Col2: testStringPtrSerializer{s},
	}))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())

	var dest data
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_string").ScanStruct(&dest))
	assert.Equal(t, "Value", dest.Col1)
	assert.Equal(t, testStringPtrSerializer{s}, dest.Col2)
}
