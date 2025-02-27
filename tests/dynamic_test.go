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
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var dynamicTestDate, _ = time.Parse(time.RFC3339, "2024-12-13T02:09:30.123Z")

func setupDynamicTest(t *testing.T) driver.Conn {
	SkipOnCloud(t, "cannot modify Dynamic settings on cloud")

	conn, err := GetNativeConnection(clickhouse.Settings{
		"max_execution_time":              60,
		"allow_experimental_dynamic_type": true,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	if !CheckMinServerServerVersion(conn, 24, 8, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version for Dynamic type"))
		return nil
	}

	return conn
}

func TestDynamic(t *testing.T) {
	ctx := context.Background()
	conn := setupDynamicTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_dynamic (
				  c Dynamic                  
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_dynamic"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_dynamic (c)")
	require.NoError(t, err)

	require.NoError(t, batch.Append(clickhouse.NewDynamicWithType(true, "Bool")))
	colInt64 := int64(42)
	require.NoError(t, batch.Append(clickhouse.NewDynamicWithType(colInt64, "Int64")))
	colString := "test"
	require.NoError(t, batch.Append(clickhouse.NewDynamicWithType(colString, "String")))
	require.NoError(t, batch.Append(clickhouse.NewDynamicWithType(dynamicTestDate, "DateTime64(3)")))
	var colNil any = nil
	require.NoError(t, batch.Append(colNil))
	colSliceUInt8 := []uint8{0xA, 0xB, 0xC}
	require.NoError(t, batch.Append(clickhouse.NewDynamicWithType(colSliceUInt8, "Array(UInt8)")))
	colSliceMapStringString := []map[string]string{{"key1": "value1", "key2": "value2"}, {"key3": "value3"}}
	require.NoError(t, batch.Append(clickhouse.NewDynamicWithType(colSliceMapStringString, "Array(Map(String, String))")))
	colMapStringString := map[string]string{"key1": "value1", "key2": "value2"}
	require.NoError(t, batch.Append(clickhouse.NewDynamicWithType(colMapStringString, "Map(String, String)")))
	colMapStringInt64 := map[string]int64{"key1": 42, "key2": 84}
	require.NoError(t, batch.Append(clickhouse.NewDynamicWithType(colMapStringInt64, "Map(String, Int64)")))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_dynamic")
	require.NoError(t, err)

	var row clickhouse.Dynamic

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, true, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colInt64, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colString, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, dynamicTestDate, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colNil, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colSliceUInt8, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colSliceMapStringString, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colMapStringString, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colMapStringInt64, row.Any())
}

func TestDynamicArray(t *testing.T) {
	ctx := context.Background()
	conn := setupDynamicTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_dynamic (
				  c Array(Dynamic)                  
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_dynamic"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_dynamic (c)")
	require.NoError(t, err)

	batch.Append([]clickhouse.Dynamic{
		clickhouse.NewDynamicWithType(int64(42), "Int64"),
		clickhouse.NewDynamicWithType(true, "Bool"),
	})
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_dynamic")
	require.NoError(t, err)

	var arrRow []clickhouse.Dynamic

	require.True(t, rows.Next())
	err = rows.Scan(&arrRow)
	require.NoError(t, err)
	require.Len(t, arrRow, 2)

	require.Equal(t, int64(42), arrRow[0].Any())
	require.Equal(t, true, arrRow[1].Any())
}

func TestDynamicEmptyArray(t *testing.T) {
	ctx := context.Background()
	conn := setupDynamicTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_dynamic (
				  c Array(Dynamic)                  
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_dynamic"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_dynamic (c)")
	require.NoError(t, err)

	batch.Append([]clickhouse.Dynamic{})
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_dynamic")
	require.NoError(t, err)

	var arrRow []clickhouse.Dynamic

	require.True(t, rows.Next())
	err = rows.Scan(&arrRow)
	require.NoError(t, err)
	require.Len(t, arrRow, 0)
}

func TestDynamic_ScanWithType(t *testing.T) {
	ctx := context.Background()
	conn := setupDynamicTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_dynamic (
				  c Dynamic                 
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_dynamic"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_dynamic (c)")
	require.NoError(t, err)

	require.NoError(t, batch.Append(clickhouse.NewDynamicWithType(true, "Bool")))
	require.NoError(t, batch.Append(clickhouse.NewDynamicWithType(int64(42), "Int64")))
	require.NoError(t, batch.Append(nil))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_dynamic")
	require.NoError(t, err)

	var row clickhouse.Dynamic

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, true, row.Any())
	require.Equal(t, "Bool", row.Type())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, int64(42), row.Any())
	require.Equal(t, "Int64", row.Type())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, nil, row.Any())
	require.Equal(t, "", row.Type())
}

func TestDynamic_BatchFlush(t *testing.T) {
	t.Skip(fmt.Errorf("server-side Dynamic bug"))

	ctx := context.Background()
	conn := setupDynamicTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_dynamic (
				  c Dynamic                 
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_dynamic"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_dynamic (c)")
	require.NoError(t, err)

	vals := make([]clickhouse.Dynamic, 0, 1000)
	for i := 0; i < 1000; i++ {
		if i%2 == 0 {
			vals = append(vals, clickhouse.NewDynamicWithType(int64(i), "Int64"))
		} else {
			vals = append(vals, clickhouse.NewDynamicWithType(i%5 == 0, "Bool"))
		}

		require.NoError(t, batch.Append(vals[i]))
		require.NoError(t, batch.Flush())
	}
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_dynamic")
	require.NoError(t, err)

	i := 0
	for rows.Next() {
		var row clickhouse.Dynamic
		err = rows.Scan(&row)
		require.NoError(t, err)

		if i%2 == 0 {
			require.Equal(t, int64(i), row.Any())
			require.Equal(t, "Int64", row.Type())
		} else {
			require.Equal(t, i%5 == 0, row.Any())
			require.Equal(t, "Bool", row.Type())
		}

		i++
	}
}
