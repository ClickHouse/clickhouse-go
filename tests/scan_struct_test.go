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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestQueryRowScanStruct(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()
		require.NoError(t, err)
		var result struct {
			Col1 string `ch:"col1"`
			Col2 uint8  `ch:"col2"`
			Col3 *uint8 `ch:"col3"`
			Col4 *uint8 `ch:"col4"`
		}
		require.NoError(t, conn.QueryRow(ctx, "SELECT 'ABC' AS col1, 42 AS col2, 5 AS col3, NULL AS col4").ScanStruct(&result))
		require.Nil(t, result.Col4)
		assert.Equal(t, "ABC", result.Col1)
		assert.Equal(t, uint8(42), result.Col2)
		assert.Equal(t, uint8(5), *result.Col3)
	})
}

func TestQueryScanStruct(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()
		require.NoError(t, err)
		rows, err := conn.Query(ctx, "SELECT number, 'ABC_' || CAST(number AS String) AS col1, now() AS time FROM system.numbers LIMIT 5")
		require.NoError(t, err)
		var i uint64
		for rows.Next() {
			var result struct {
				Col1 uint64    `ch:"number"`
				Col2 string    `ch:"col1"`
				Col3 time.Time `ch:"time"`
			}
			if assert.NoError(t, rows.ScanStruct(&result)) {
				assert.Equal(t, i, result.Col1)
				assert.Equal(t, fmt.Sprintf("ABC_%d", i), result.Col2)
			}
			i++
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}

func TestSelectScanStruct(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()
		require.NoError(t, err)
		var result []struct {
			Col1 uint64     `ch:"number"`
			Col2 string     `ch:"col1"`
			Col3 *time.Time `ch:"time"`
		}
		require.NoError(t, conn.Select(ctx, &result, "SELECT number, 'ABC_' || CAST(number AS String) AS col1, now() AS time FROM system.numbers LIMIT 5"))
		require.Len(t, result, 5)
		for i, v := range result {
			if assert.NotNil(t, v.Col3) {
				assert.Equal(t, uint64(i), v.Col1)
				assert.Equal(t, fmt.Sprintf("ABC_%d", i), v.Col2)
			}
		}
	})
}

func TestArrayTupleNullableFieldPanic(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		require.NoError(t, err)

		ctx := context.Background()
		require.NoError(t, conn.Exec(ctx, `
		CREATE TABLE test_array_tuple_nullable_field_panic (
		    c1 Int32,
			c2 Array(Tuple(c3 String, c4 Nullable(Int32)))
		) Engine MergeTree() ORDER BY tuple()
	`))
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_array_tuple_nullable_field_panic")
		}()
		type subRowType struct {
			Col3 string `ch:"c3"`
			// THIS FIELD causes PANIC IF IT'S NULLABLE
			Col4 *int32 `ch:"c4"`
		}
		type rowType struct {
			Col1 int32        `ch:"c1"`
			Col2 []subRowType `ch:"c2"`
		}
		nonNull := int32(42)
		element1 := rowType{
			Col1: 1,
			Col2: []subRowType{
				{Col3: "a", Col4: &nonNull},
				{Col3: "b", Col4: nil},
			},
		}

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_array_tuple_nullable_field_panic")
		require.NoError(t, err)
		require.NoError(t, batch.AppendStruct(&element1))
		require.NoError(t, batch.Send())

		rows, err := conn.Query(ctx, "SELECT c1, c2 FROM test_array_tuple_nullable_field_panic")
		require.NoError(t, err)
		var results []rowType
		for rows.Next() {
			var result rowType
			assert.NoError(t, rows.ScanStruct(&result))
			results = append(results, result)
		}
		require.ElementsMatch(t, results, []rowType{element1})
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}

func TestArrayTupleNonNull(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		require.NoError(t, err)

		ctx := context.Background()
		require.NoError(t, conn.Exec(ctx, `
		CREATE TABLE test_array_tuple_non_null (
		    c1 Int32,
			c2 Array(Tuple(c3 String, c4 Int32))
		) Engine MergeTree() ORDER BY tuple()
	`))
		defer func() {
			conn.Exec(ctx, "DROP TABLE IF EXISTS test_array_tuple_non_null")
		}()
		type subRowType struct {
			Col3 string `ch:"c3"`
			Col4 int32  `ch:"c4"`
		}
		type rowType struct {
			Col1 int32        `ch:"c1"`
			Col2 []subRowType `ch:"c2"`
		}
		element1 := rowType{
			Col1: 1,
			Col2: []subRowType{
				{Col3: "a", Col4: 42},
				{Col3: "b", Col4: 52},
			},
		}

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_array_tuple_non_null")
		require.NoError(t, err)
		require.NoError(t, batch.AppendStruct(&element1))
		require.NoError(t, batch.Send())

		rows, err := conn.Query(ctx, "SELECT c1, c2 FROM test_array_tuple_non_null")
		require.NoError(t, err)
		var results []rowType
		for rows.Next() {
			var result rowType
			assert.NoError(t, rows.ScanStruct(&result))
			results = append(results, result)
		}
		require.ElementsMatch(t, results, []rowType{element1})
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}
