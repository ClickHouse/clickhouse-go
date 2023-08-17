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
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestSimpleNested(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 22, 1, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
			CREATE TABLE test_nested (
				Col1 Nested(
					  Col1_N1 String
				)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_nested")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_nested")
	require.NoError(t, err)
	var (
		col1Data = []string{"1", "2", "3"}
	)
	require.NoError(t, batch.Append(col1Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 []string
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_nested").Scan(&col1))
	assert.Equal(t, col1Data, col1)
}

// this isn't documented behaviour in ClickHouse - i.e. flatten_nested=1 with multiple Nested. Following does work however.
func TestNestedFlattened(t *testing.T) {
	conn, err := GetNativeConnection(clickhouse.Settings{
		"flatten_nested": 1,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 22, 1, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
			CREATE TABLE test_nested (
				Col1 Nested(
					  Col1_N1 UInt8
					, Col2_N1 UInt8
				)
				, Col2 Nested(
					  Col1_N2 UInt8
					, Col2_N2 Nested(
						  Col1_N2_N1 UInt8
						, Col2_N2_N1 UInt8
					)
				)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_nested")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_nested")
	require.NoError(t, err)
	var (
		col1Data = []uint8{1, 2, 3}

		col2Data = []uint8{10, 20, 30}
		col3Data = []uint8{101, 201, 230} // Col2.Col1_N2
		col4Data = [][][]any{
			[][]any{
				[]any{uint8(1), uint8(2)},
			},
			[][]any{
				[]any{uint8(1), uint8(2)},
			},
			[][]any{
				[]any{uint8(1), uint8(2)},
			},
		}
	)
	require.NoError(t, batch.Append(col1Data, col2Data, col3Data, col4Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 []uint8
		col2 []uint8
		col3 []uint8
		col4 [][][]any
	)
	rows := conn.QueryRow(ctx, "SELECT * FROM test_nested")
	require.NoError(t, rows.Scan(&col1, &col2, &col3, &col4))
	assert.Equal(t, col1Data, col1)
	assert.Equal(t, col2Data, col2)
	assert.Equal(t, col3Data, col3)
	assert.Equal(t, col4Data, col4)
}

func TestFlattenedSimpleNested(t *testing.T) {
	conn, err := GetNativeConnection(clickhouse.Settings{
		"flatten_nested": 0,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 22, 1, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
			CREATE TABLE test_nested (
				Col1 Nested(
					  Col1_N1 String
				)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_nested")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_nested")
	require.NoError(t, err)
	var (
		col1Data = []map[string]any{
			{
				"Col1_N1": "1",
			},
			{
				"Col1_N1": "2",
			},
			{
				"Col1_N1": "3",
			},
		}
	)
	require.NoError(t, batch.Append(col1Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 []map[string]any
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_nested").Scan(&col1))
	assert.Equal(t, col1Data, col1)
}

// nested with flatten_nested = 0
func TestNestedUnFlattened(t *testing.T) {
	conn, err := GetNativeConnection(clickhouse.Settings{
		"flatten_nested": 0,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 22, 1, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
			CREATE TABLE test_nested (
				Col1 Nested(
					  Col1_N1 UInt8
					, Col2_N1 UInt8
				)
				, Col2 Nested(
					  Col1_N2 UInt8
					, Col2_N2 Nested(
						  Col1_N2_N1 UInt8
						, Col2_N2_N1 UInt8
					)
				)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_nested")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_nested")
	require.NoError(t, err)
	var (
		col1Data = []map[string]any{
			{
				"Col1_N1": uint8(1),
				"Col2_N1": uint8(20),
			},
			{
				"Col1_N1": uint8(2),
				"Col2_N1": uint8(20),
			},
			{
				"Col1_N1": uint8(3),
				"Col2_N1": uint8(20),
			},
		}
		col2Data = []map[string]any{
			{
				"Col1_N2": uint8(101),
				"Col2_N2": []map[string]any{
					{
						"Col1_N2_N1": uint8(1),
						"Col2_N2_N1": uint8(2),
					},
				},
			},
			{
				"Col1_N2": uint8(201),
				"Col2_N2": []map[string]any{
					{
						"Col1_N2_N1": uint8(3),
						"Col2_N2_N1": uint8(4),
					},
				},
			},
		}
	)
	require.NoError(t, batch.Append(col1Data, col2Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 []map[string]any
		col2 []map[string]any
	)
	rows := conn.QueryRow(ctx, "SELECT * FROM test_nested")
	require.NoError(t, rows.Scan(&col1, &col2))
	assert.Equal(t, col1Data, col1)
	assert.Equal(t, col2Data, col2)
}

func TestNestedFlush(t *testing.T) {
	conn, err := GetNativeConnection(clickhouse.Settings{
		"flatten_nested": 0,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 22, 1, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
			CREATE TABLE test_nested_flush (
				Col1 Nested(
					  Col1_N1 UInt8
					, Col2_N1 UInt8
				)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_nested_flush")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_nested_flush")
	require.NoError(t, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_nested_flush")
	}()
	batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_nested_flush")
	require.NoError(t, err)
	vals := [1000][]map[string]any{}
	for i := 0; i < 1000; i++ {
		vals[i] = []map[string]any{
			{
				"Col1_N1": uint8(i),
				"Col2_N1": uint8(i) + 1,
			},
			{
				"Col1_N1": uint8(i) + 2,
				"Col2_N1": uint8(i) + 3,
			},
			{
				"Col1_N1": uint8(i) + 4,
				"Col2_N1": uint8(i) + 5,
			},
		}
		require.NoError(t, batch.Append(vals[i]))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Flush())
	}
	require.Equal(t, 0, batch.Rows())
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_nested_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 []map[string]any
		require.NoError(t, rows.Scan(&col1))
		require.Equal(t, vals[i], col1)
		i += 1
	}
	require.Equal(t, 1000, i)
}
