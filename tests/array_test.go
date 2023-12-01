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

func TestSimpleArray(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_array (
			  Col1 Array(String)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_array")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_array")
	require.NoError(t, err)
	var (
		col1Data = []string{"A", "b", "c"}
	)
	for i := 0; i < 10; i++ {
		require.NoError(t, batch.Append(col1Data))
		require.Equal(t, 1, batch.Rows())
		batch.Flush()
	}
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_array")
	require.NoError(t, err)
	for rows.Next() {
		var (
			col1 []string
		)
		require.NoError(t, rows.Scan(&col1))
		assert.Equal(t, col1Data, col1)

	}
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
}

type customArr []customStr

func TestCustomArray(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_array (
			  Col1 Array(Enum ('hello'   = 1,  'world' = 2)),
			  Col2 Array(String)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_array")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_array")
	require.NoError(t, err)
	var (
		col1Data = customArr{"hello", "hello", "world"}
		col2Data = customArr{"a", "b", "c"}
	)
	for i := 0; i < 10; i++ {
		require.NoError(t, batch.Append(col1Data, col2Data))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Flush())
	}
	require.Equal(t, 0, batch.Rows())
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_array")
	require.NoError(t, err)
	for rows.Next() {
		var (
			col1 customArr
			col2 customArr
		)
		require.NoError(t, rows.Scan(&col1, &col2))
		assert.Equal(t, col1Data, col1)
		assert.Equal(t, col2Data, col2)
	}
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
}

func TestInterfaceArray(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_array (
			  Col1 Array(String)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_array")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_array")
	require.NoError(t, err)
	var (
		col1Data = []string{"A", "b", "c"}
	)
	for i := 0; i < 10; i++ {
		require.NoError(t, batch.Append(col1Data))
	}
	require.Equal(t, 10, batch.Rows())
	require.Nil(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_array")
	require.NoError(t, err)
	for rows.Next() {
		var (
			col1 any
		)
		require.NoError(t, rows.Scan(&col1))
		assert.ObjectsAreEqual(col1Data, col1)
	}
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
}

func TestArray(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_array (
			  Col1 Array(String)
			, Col2 Array(Array(UInt32))
			, Col3 Array(Array(Array(DateTime)))
			, Col4 Array(String)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_array")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_array")
	require.NoError(t, err)
	var (
		timestamp = time.Now().Truncate(time.Second).In(time.UTC)
		col1Data  = []string{"A", "b", "c"}
		col2Data  = [][]uint32{
			[]uint32{1, 2},
			[]uint32{3, 87},
			[]uint32{33, 3, 847},
		}
		col3Data = [][][]time.Time{
			[][]time.Time{
				[]time.Time{
					timestamp,
					timestamp,
					timestamp,
					timestamp,
				},
			},
			[][]time.Time{
				[]time.Time{
					timestamp,
					timestamp,
					timestamp,
				},
				[]time.Time{
					timestamp,
					timestamp,
				},
			},
		}
		col4Data = &[]string{"M", "D"}
	)
	for i := 0; i < 10; i++ {
		require.NoError(t, batch.Append(col1Data, col2Data, col3Data, col4Data))
		require.Equal(t, 1, batch.Rows())
		batch.Flush()
	}
	require.Equal(t, 0, batch.Rows())
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_array")
	require.NoError(t, err)
	for rows.Next() {
		var (
			col1 []string
			col2 [][]uint32
			col3 [][][]time.Time
			col4 []string
		)
		require.NoError(t, rows.Scan(&col1, &col2, &col3, &col4))
		assert.Equal(t, col1Data, col1)
		assert.Equal(t, col2Data, col2)
		assert.Equal(t, col3Data, col3)
		assert.Equal(t, col4Data, &col4)

	}
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
}

func TestColumnarArray(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	const ddl = `
		CREATE TABLE test_array (
			  Col1 Array(String)
			, Col2 Array(Array(UInt32))
			, Col3 Array(Array(Array(DateTime)))
			, Col4 Array(String)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_array")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	var (
		timestamp = time.Now().Truncate(time.Second).In(time.UTC)
		col1Data  = []string{"A", "b", "c"}
		col2Data  = [][]uint32{
			[]uint32{1, 2},
			[]uint32{3, 87},
			[]uint32{33, 3, 847},
		}
		col3Data = [][][]time.Time{
			[][]time.Time{
				[]time.Time{
					timestamp,
					timestamp,
					timestamp,
					timestamp,
				},
			},
			[][]time.Time{
				[]time.Time{
					timestamp,
					timestamp,
					timestamp,
				},
				[]time.Time{
					timestamp,
					timestamp,
				},
			},
		}
		col4Data = &[]string{"M", "D"}

		col1DataColArr [][]string
		col2DataColArr [][][]uint32
		col3DataColArr [][][][]time.Time
		col4DataColArr []*[]string
	)

	for i := 0; i < 10; i++ {
		col1DataColArr = append(col1DataColArr, col1Data)
		col2DataColArr = append(col2DataColArr, col2Data)
		col3DataColArr = append(col3DataColArr, col3Data)
		col4DataColArr = append(col4DataColArr, col4Data)
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_array")
	require.NoError(t, err)
	require.NoError(t, batch.Column(0).Append(col1DataColArr))
	require.NoError(t, batch.Column(1).Append(col2DataColArr))
	require.NoError(t, batch.Column(2).Append(col3DataColArr))
	require.NoError(t, batch.Column(3).Append(col4DataColArr))
	require.Equal(t, 10, batch.Rows())
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_array")
	require.NoError(t, err)
	for rows.Next() {
		var (
			col1 []string
			col2 [][]uint32
			col3 [][][]time.Time
			col4 []string
		)
		require.NoError(t, rows.Scan(&col1, &col2, &col3, &col4))
		assert.Equal(t, col1Data, col1)
		assert.Equal(t, col2Data, col2)
		assert.Equal(t, col3Data, col3)
		assert.Equal(t, col4Data, &col4)
	}
	require.NoError(t, rows.Close())
	assert.NoError(t, rows.Err())
}

type testArraySerializer struct {
	val []string
}

func (c testArraySerializer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testArraySerializer) Scan(src any) error {
	if t, ok := src.([]string); ok {
		*c = testArraySerializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testArraySerializer", src)
}

func TestSimpleArrayValuer(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_array_valuer (
			  Col1 Array(String)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_array_valuer")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_array_valuer")
	require.NoError(t, err)
	var (
		col1Data = []string{"A", "b", "c"}
	)
	for i := 0; i < 10; i++ {
		require.NoError(t, batch.Append(testArraySerializer{val: col1Data}))
		require.Equal(t, 1, batch.Rows())
		batch.Flush()
	}
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_array_valuer")
	require.NoError(t, err)
	for rows.Next() {
		var (
			col1 []string
		)
		require.NoError(t, rows.Scan(&col1))
		assert.Equal(t, col1Data, col1)

	}
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
}
