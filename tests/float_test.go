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
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestSimpleFloat(t *testing.T) {
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
		CREATE TABLE test_float (
			  Col1 Float32,
			  Col2 Float64,
			  Col3 Nullable(Float64)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_float")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_float")
	require.NoError(t, err)
	require.NoError(t, batch.Append(float32(33.1221), sql.NullFloat64{
		Float64: 34.222,
		Valid:   true,
	}, sql.NullFloat64{
		Float64: 0,
		Valid:   false,
	}))
	require.Equal(t, 1, batch.Rows())
	assert.NoError(t, batch.Send())
	var (
		col1 float32
		col2 sql.NullFloat64
		col3 sql.NullFloat64
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_float").Scan(&col1, &col2, &col3))
	require.Equal(t, float32(33.1221), col1)
	require.Equal(t, sql.NullFloat64{
		Float64: 34.222,
		Valid:   true,
	}, col2)
	require.Equal(t, sql.NullFloat64{
		Float64: 0,
		Valid:   false,
	}, col3)
}

type customFloat32 float32

func (f *customFloat32) Scan(src any) error {
	if t, ok := src.(float32); ok {
		*f = customFloat32(t)
		return nil
	}
	return fmt.Errorf("cannot scan %T into customFloat32", src)
}

type customFloat64 float64

func (f *customFloat64) Scan(src any) error {
	if t, ok := src.(float64); ok {
		*f = customFloat64(t)
		return nil
	}
	return fmt.Errorf("cannot scan %T into customFloat64", src)
}

func TestCustomFloat(t *testing.T) {
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
		CREATE TABLE test_float (
			  Col1 Float32,
			  Col2 Float64
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_float")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_float")
	require.NoError(t, err)
	require.NoError(t, batch.Append(customFloat32(33.1221), customFloat64(22.1)))
	require.Equal(t, 1, batch.Rows())
	assert.NoError(t, batch.Send())
	var (
		col1 customFloat32
		col2 customFloat64
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_float").Scan(&col1, &col2))
	require.Equal(t, customFloat32(33.1221), col1)
	require.Equal(t, customFloat64(22.1), col2)
}

func BenchmarkFloat(b *testing.B) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS benchmark_float")
	}()

	if err = conn.Exec(ctx, `CREATE TABLE benchmark_float (Col1 Float32, Col2 Float64) ENGINE = Null`); err != nil {
		b.Fatal(err)
	}

	const rowsInBlock = 10_000_000

	for n := 0; n < b.N; n++ {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO benchmark_float VALUES")
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < rowsInBlock; i++ {
			if err := batch.Append(float32(122.112), 322.111); err != nil {
				b.Fatal(err)
			}
		}
		if err = batch.Send(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestFixedFloatFlush(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)

	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS fixed_float_flush")
	}()
	const ddl = `
		CREATE TABLE fixed_float_flush (
			  Col1 Float32,
			  Col2 Float64	
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO fixed_float_flush")
	require.NoError(t, err)
	val32s := [1000]float32{}
	val64s := [1000]float64{}
	for i := 0; i < 1000; i++ {
		val32s[i] = rand.Float32()
		val64s[i] = rand.Float64()
		batch.Append(val32s[i], val64s[i])
		require.Equal(t, 1, batch.Rows())
		batch.Flush()
	}
	require.Equal(t, 0, batch.Rows())
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM fixed_float_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 float32
		var col2 float64
		require.NoError(t, rows.Scan(&col1, &col2))
		require.Equal(t, val32s[i], col1)
		require.Equal(t, val64s[i], col2)
		i += 1
	}
	require.Equal(t, 1000, i)
}
