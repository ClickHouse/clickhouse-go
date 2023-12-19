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
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestBool(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 12, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
			CREATE TABLE test_bool (
				  Col1 Bool
				, Col2 Bool
				, Col3 Array(Bool)
				, Col4 Nullable(Bool)
				, Col5 Array(Nullable(Bool))
				, Col6 Bool
				, Col7 Nullable(Bool)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_bool")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_bool")
	require.NoError(t, err)
	var val bool
	require.NoError(t, batch.Append(true, false, []bool{true, false, true}, nil, []*bool{&val, nil, &val}, sql.NullBool{
		Bool:  true,
		Valid: true,
	}, sql.NullBool{
		Bool:  false,
		Valid: false,
	}))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 bool
		col2 bool
		col3 []bool
		col4 *bool
		col5 []*bool
		col6 sql.NullBool
		col7 sql.NullBool
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_bool").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7))
	assert.Equal(t, true, col1)
	assert.Equal(t, false, col2)
	assert.Equal(t, []bool{true, false, true}, col3)
	require.Nil(t, col4)
	assert.Equal(t, []*bool{&val, nil, &val}, col5)
	assert.Equal(t, sql.NullBool{
		Bool:  true,
		Valid: true,
	}, col6)
	assert.Equal(t, sql.NullBool{
		Bool:  false,
		Valid: false,
	}, col7)
}

func TestColumnarBool(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 12, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
			CREATE TABLE test_bool (
				  ID   UInt64
				, Col1 Bool
				, Col2 Bool
				, Col3 Array(Bool)
				, Col4 Nullable(Bool)
				, Col5 Array(Nullable(Bool))
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_bool")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	val := true
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_bool")
	require.NoError(t, err)
	var (
		id   []uint64
		col1 []bool
		col2 []bool
		col3 [][]bool
		col4 []*bool
		col5 [][]*bool
	)
	for i := 0; i < 1000; i++ {
		id = append(id, uint64(i))
		col1 = append(col1, true)
		col2 = append(col2, false)
		col3 = append(col3, []bool{true, false, true})
		col4 = append(col4, nil)
		col5 = append(col5, []*bool{&val, nil, &val})
	}
	{
		require.NoError(t, batch.Column(0).Append(id))
		require.NoError(t, batch.Column(1).Append(col1))
		require.NoError(t, batch.Column(2).Append(col2))
		require.NoError(t, batch.Column(3).Append(col3))
		require.NoError(t, batch.Column(4).Append(col4))
		require.NoError(t, batch.Column(5).Append(col5))
		require.Equal(t, 1000, batch.Rows())
		require.NoError(t, batch.Send())
		var (
			id   uint64
			col1 bool
			col2 bool
			col3 []bool
			col4 *bool
			col5 []*bool
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_bool WHERE ID = $1", 42).Scan(&id, &col1, &col2, &col3, &col4, &col5))
		assert.Equal(t, true, col1)
		assert.Equal(t, false, col2)
		assert.Equal(t, []bool{true, false, true}, col3)
		require.Nil(t, col4)
		assert.Equal(t, []*bool{&val, nil, &val}, col5)
	}
}

func TestBoolFlush(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS bool_flush")
	}()
	const ddl = `
		CREATE TABLE bool_flush (
			  Col1 Bool
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO bool_flush")
	require.NoError(t, err)
	vals := [1000]bool{}
	var src = rand.NewSource(time.Now().UnixNano())
	var r = rand.New(src)

	for i := 0; i < 1000; i++ {
		vals[i] = r.Intn(2) != 0
		require.NoError(t, batch.Append(vals[i]))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Flush())
	}
	require.Equal(t, 0, batch.Rows())
	batch.Send()
	rows, err := conn.Query(ctx, "SELECT * FROM bool_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 bool
		require.NoError(t, rows.Scan(&col1))
		assert.Equal(t, vals[i], col1)
		i += 1
	}
}

type testBoolSerializer struct {
	val bool
}

func (c testBoolSerializer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testBoolSerializer) Scan(src any) error {
	if t, ok := src.(bool); ok {
		*c = testBoolSerializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testBoolSerializer", src)
}

func TestBoolValuer(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS bool_flush")
	}()
	const ddl = `
		CREATE TABLE bool_flush (
			  Col1 Bool
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO bool_flush")
	require.NoError(t, err)
	vals := [1000]bool{}
	var src = rand.NewSource(time.Now().UnixNano())
	var r = rand.New(src)

	for i := 0; i < 1000; i++ {
		vals[i] = r.Intn(2) != 0
		require.NoError(t, batch.Append(testBoolSerializer{val: vals[i]}))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Flush())
	}
	require.Equal(t, 0, batch.Rows())
	batch.Send()
	rows, err := conn.Query(ctx, "SELECT * FROM bool_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 bool
		require.NoError(t, rows.Scan(&col1))
		assert.Equal(t, vals[i], col1)
		i += 1
	}
}

type CustomBool bool

func (cb *CustomBool) Scan(src any) error {
	if t, ok := src.(bool); ok {
		*cb = CustomBool(t)
		return nil
	}
	return fmt.Errorf("cannot scan %T into CustomBool", src)
}

func TestCustomBool(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	const ddl = `
		CREATE TABLE bool_custom (
			  Col1 Bool
		) Engine MergeTree() ORDER BY tuple()
		`
	conn.Exec(ctx, "DROP TABLE bool_custom")
	require.NoError(t, conn.Exec(ctx, ddl))
	require.NoError(t, err)
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO bool_custom")
	require.NoError(t, err)
	require.NoError(t, batch.Append(true))
	require.NoError(t, batch.Send())
	row := conn.QueryRow(ctx, "SELECT * FROM bool_custom")
	var col1 CustomBool
	require.NoError(t, row.Scan(&col1))
	require.Equal(t, true, bool(col1))
}
