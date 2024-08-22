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

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestSimpleEnum(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_enum (
				  Col1 Enum  ('hello'   = 1,  'world' = 2)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_enum")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_enum")
	require.NoError(t, err)
	var (
		col1Data = "hello"
	)
	require.NoError(t, batch.Append(
		col1Data,
	))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 string
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_enum").Scan(&col1))
	assert.Equal(t, col1Data, col1)
}

func TestCustomEnum(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_enum (
				  Col1 Enum ('hello'   = 1,  'world' = 2),
				  Col2 Enum8 ('hello'   = 1,  'world' = 2),
				  Col3 Enum16 ('hello'   = 1,  'world' = 2)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_enum")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_enum")
	require.NoError(t, err)
	var (
		col1Data = customStr("hello")
		col2Data = customStr("world")
		col3Data = customStr("hello")
	)
	require.NoError(t, batch.Append(
		col1Data,
		col2Data,
		col3Data,
	))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 customStr
		col2 customStr
		col3 customStr
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_enum").Scan(&col1, &col2, &col3))
	assert.Equal(t, col1Data, col1)
	assert.Equal(t, col2Data, col2)
	assert.Equal(t, col3Data, col3)
}

func TestEnum(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_enum (
				  Col1 Enum  ('hello,hello'   = 1,  'world' = 2)
				, Col2 Enum8 ('click'   = 5,  'house' = 25)
				, Col3 Enum16('house' = 10,   'value' = 50)
				, Col4 Array(Enum8  ('click' = 1, 'house' = 2))
				, Col5 Array(Enum16 ('click' = 1, 'house' = 2))
				, Col6 Array(Nullable(Enum8  ('click' = 1, 'house' = 2)))
				, Col7 Array(Nullable(Enum16 ('click' = 1, 'house' = 2)))
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_enum")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_enum")
	require.NoError(t, err)
	var (
		col1Data = "hello,hello"
		col2Data = "click"
		col3Data = "house"
		col4Data = []string{"click", "house"}
		col5Data = []string{"house", "click"}
		col6Data = []*string{&col2Data, nil, &col3Data}
		col7Data = []*string{&col3Data, nil, &col2Data}
	)
	require.NoError(t, batch.Append(
		col1Data,
		col2Data,
		col3Data,
		col4Data,
		col5Data,
		col6Data,
		col7Data,
	))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 string
		col2 string
		col3 string
		col4 []string
		col5 []string
		col6 []*string
		col7 []*string
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_enum").Scan(
		&col1, &col2, &col3, &col4,
		&col5, &col6, &col7,
	))
	assert.Equal(t, col1Data, col1)
	assert.Equal(t, col2Data, col2)
	assert.Equal(t, col3Data, col3)
	assert.Equal(t, col4Data, col4)
	assert.Equal(t, col5Data, col5)
	assert.Equal(t, col6Data, col6)
	assert.Equal(t, col7Data, col7)
}

func TestNullableEnum(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_enum (
				  Col1 Nullable(Enum  ('hello'   = 1,  'world' = 2))
				, Col2 Nullable(Enum8 ('click'   = 5,  'house' = 25))
				, Col3 Nullable(Enum16('default' = 10, 'value' = 50))
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_enum")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_enum")
	require.NoError(t, err)
	require.NoError(t, batch.Append("hello", "click", "value"))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 string
		col2 string
		col3 string
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_enum").Scan(&col1, &col2, &col3))
	assert.Equal(t, "hello", col1)
	assert.Equal(t, "click", col2)
	assert.Equal(t, "value", col3)
	require.NoError(t, conn.Exec(ctx, "TRUNCATE TABLE test_enum"))
	batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_enum")
	require.NoError(t, err)
	require.NoError(t, batch.Append("hello", nil, "value"))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	{
		var (
			col1 *string
			col2 *string
			col3 *string
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_enum").Scan(&col1, &col2, &col3))
		require.Nil(t, col2)
		assert.Equal(t, "hello", *col1)
		assert.Equal(t, "value", *col3)
	}
}

func TestColumnarEnum(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_enum (
				  Col1 Enum  ('hello'   = 1,  'world' = 2)
				, Col2 Enum8 ('click'   = 5,  'house' = 25)
				, Col3 Enum16('house' = 10,   'value' = 50)
				, Col4 Array(Enum8  ('click' = 1, 'house' = 2))
				, Col5 Array(Enum16 ('click' = 1, 'house' = 2))
				, Col6 Array(Nullable(Enum8  ('click' = 1, 'house' = 2)))
				, Col7 Array(Nullable(Enum16 ('click' = 1, 'house' = 2)))
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_enum")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_enum")
	require.NoError(t, err)
	var (
		col1Data = "hello"
		col2Data = "click"
		col3Data = "house"
		col4Data = []string{"click", "house"}
		col5Data = []string{"house", "click"}
		col6Data = []*string{&col2Data, nil, &col3Data}
		col7Data = []*string{&col3Data, nil, &col2Data}
	)

	require.NoError(t, batch.Column(0).Append([]string{
		col1Data, col1Data, col1Data,
	}))
	require.NoError(t, batch.Column(1).Append([]string{
		col2Data, col2Data, col2Data,
	}))
	require.NoError(t, batch.Column(2).Append([]string{
		col3Data, col3Data, col3Data,
	}))
	require.NoError(t, batch.Column(3).Append([][]string{
		col4Data, col4Data, col4Data,
	}))
	require.NoError(t, batch.Column(4).Append([][]string{
		col5Data, col5Data, col5Data,
	}))
	require.NoError(t, batch.Column(5).Append([][]*string{
		col6Data, col6Data, col6Data,
	}))
	require.NoError(t, batch.Column(6).Append([][]*string{
		col7Data, col7Data, col7Data,
	}))
	require.Equal(t, 3, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 string
		col2 string
		col3 string
		col4 []string
		col5 []string
		col6 []*string
		col7 []*string
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_enum LIMIT 1").Scan(
		&col1, &col2, &col3, &col4,
		&col5, &col6, &col7,
	))
	assert.Equal(t, col1Data, col1)
	assert.Equal(t, col2Data, col2)
	assert.Equal(t, col3Data, col3)
	assert.Equal(t, col4Data, col4)
	assert.Equal(t, col5Data, col5)
	assert.Equal(t, col6Data, col6)
	assert.Equal(t, col7Data, col7)
}

type testEnumSerializer struct {
	val []string
}

func (c testEnumSerializer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testEnumSerializer) Scan(src any) error {
	if t, ok := src.([]string); ok {
		*c = testEnumSerializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testEnum8Serializer", src)
}

func TestEnumValuer(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_enum_valuer (
				  Col1 Enum  ('hello'   = 1,  'world' = 2)
				, Col2 Enum8 ('click'   = 5,  'house' = 25)
				, Col3 Enum16('house' = 10,   'value' = 50)
				, Col4 Array(Enum8  ('click' = 1, 'house' = 2))
				, Col5 Array(Enum16 ('click' = 1, 'house' = 2))
				, Col6 Array(Nullable(Enum8  ('click' = 1, 'house' = 2)))
				, Col7 Array(Nullable(Enum16 ('click' = 1, 'house' = 2)))
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_enum_valuer")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_enum_valuer")
	require.NoError(t, err)
	var (
		col1Data = "hello"
		col2Data = "click"
		col3Data = "house"
		col4Data = []string{"click", "house"}
		col5Data = []string{"house", "click"}
		col6Data = []*string{&col2Data, nil, &col3Data}
		col7Data = []*string{&col3Data, nil, &col2Data}
	)
	require.NoError(t, batch.Append(
		col1Data,
		col2Data,
		col3Data,
		testEnumSerializer{val: col4Data},
		testEnumSerializer{val: col5Data},
		col6Data,
		col7Data,
	))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 string
		col2 string
		col3 string
		col4 []string
		col5 []string
		col6 []*string
		col7 []*string
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_enum_valuer").Scan(
		&col1, &col2, &col3, &col4,
		&col5, &col6, &col7,
	))
	assert.Equal(t, col1Data, col1)
	assert.Equal(t, col2Data, col2)
	assert.Equal(t, col3Data, col3)
	assert.Equal(t, col4Data, col4)
	assert.Equal(t, col5Data, col5)
	assert.Equal(t, col6Data, col6)
	assert.Equal(t, col7Data, col7)
}
