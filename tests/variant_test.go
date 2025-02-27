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

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
)

var variantTestDate, _ = time.Parse(time.RFC3339, "2024-12-13T02:09:30.123Z")

func setupVariantTest(t *testing.T) driver.Conn {
	SkipOnCloud(t, "cannot modify Variant settings on cloud")

	conn, err := GetNativeConnection(clickhouse.Settings{
		"max_execution_time":              60,
		"allow_experimental_variant_type": true,
		"allow_suspicious_variant_types":  true,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	if !CheckMinServerServerVersion(conn, 24, 4, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version for Variant type"))
		return nil
	}

	return conn
}

func TestVariant(t *testing.T) {
	ctx := context.Background()
	conn := setupVariantTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_variant (
				  c Variant(
			    	Bool,
			    	Int64,
			    	String,
			    	DateTime64(3),
			    	Array(String),
			    	Array(UInt8),
			    	Array(Map(String, String)),
			    	Map(String, String),
			    	Map(String, Int64),
			    )                  
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_variant"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_variant (c)")
	require.NoError(t, err)

	require.NoError(t, batch.Append(true))
	colInt64 := int64(42)
	require.NoError(t, batch.Append(clickhouse.NewVariantWithType(colInt64, "Int64")))
	colString := "test"
	require.NoError(t, batch.Append(clickhouse.NewVariantWithType(colString, "String")))
	require.NoError(t, batch.Append(clickhouse.NewVariantWithType(variantTestDate, "DateTime64(3)")))
	var colNil any = nil
	require.NoError(t, batch.Append(colNil))
	colSliceString := []string{"a", "b"}
	require.NoError(t, batch.Append(clickhouse.NewVariantWithType(colSliceString, "Array(String)")))
	colSliceUInt8 := []uint8{0xA, 0xB, 0xC}
	require.NoError(t, batch.Append(clickhouse.NewVariantWithType(colSliceUInt8, "Array(UInt8)")))
	colSliceMapStringString := []map[string]string{{"key1": "value1", "key2": "value2"}, {"key3": "value3"}}
	require.NoError(t, batch.Append(colSliceMapStringString))
	colMapStringString := map[string]string{"key1": "value1", "key2": "value2"}
	require.NoError(t, batch.Append(colMapStringString))
	colMapStringInt64 := map[string]int64{"key1": 42, "key2": 84}
	require.NoError(t, batch.Append(colMapStringInt64))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_variant")
	require.NoError(t, err)

	var row clickhouse.Variant

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
	require.Equal(t, variantTestDate, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colNil, row.Any())

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, colSliceString, row.Any())

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

func TestVariantArray(t *testing.T) {
	ctx := context.Background()
	conn := setupVariantTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_variant (
				  c Array(Variant(Int64))                  
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_variant"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_variant (c)")
	require.NoError(t, err)

	batch.Append([]clickhouse.Variant{
		clickhouse.NewVariantWithType(int64(42), "Int64"),
		clickhouse.NewVariantWithType(int64(84), "Int64"),
	})
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_variant")
	require.NoError(t, err)

	var arrRow []clickhouse.Variant

	require.True(t, rows.Next())
	err = rows.Scan(&arrRow)
	require.NoError(t, err)
	require.Len(t, arrRow, 2)

	require.Equal(t, int64(42), arrRow[0].Any())
	require.Equal(t, int64(84), arrRow[1].Any())
}

func TestVariantEmptyArray(t *testing.T) {
	ctx := context.Background()
	conn := setupVariantTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_variant (
				  c Array(Variant(Int64))                  
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_variant"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_variant (c)")
	require.NoError(t, err)

	batch.Append([]clickhouse.Variant{})
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_variant")
	require.NoError(t, err)

	var arrRow []clickhouse.Variant

	require.True(t, rows.Next())
	err = rows.Scan(&arrRow)
	require.NoError(t, err)
	require.Len(t, arrRow, 0)
}

func TestVariant_ScanWithType(t *testing.T) {
	ctx := context.Background()
	conn := setupVariantTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_variant (
				  c Variant(Bool, Int64)                  
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_variant"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_variant (c)")
	require.NoError(t, err)

	require.NoError(t, batch.Append(true))
	require.NoError(t, batch.Append(clickhouse.NewVariantWithType(int64(42), "Int64")))
	require.NoError(t, batch.Append(nil))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_variant")
	require.NoError(t, err)

	var row clickhouse.Variant

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

func TestVariant_BatchFlush(t *testing.T) {
	ctx := context.Background()
	conn := setupVariantTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_variant (
				  c Variant(Bool, Int64)                  
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_variant"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_variant (c)")
	require.NoError(t, err)

	vals := make([]clickhouse.Variant, 0, 1000)
	for i := 0; i < 1000; i++ {
		if i%2 == 0 {
			vals = append(vals, clickhouse.NewVariantWithType(int64(i), "Int64"))
		} else {
			vals = append(vals, clickhouse.NewVariantWithType(i%5 == 0, "Bool"))
		}

		require.NoError(t, batch.Append(vals[i]))
		require.NoError(t, batch.Flush())
	}
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_variant")
	require.NoError(t, err)

	i := 0
	for rows.Next() {
		var row clickhouse.Variant
		err = rows.Scan(&row)

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
