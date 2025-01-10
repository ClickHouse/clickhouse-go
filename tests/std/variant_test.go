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

package std

import (
	"context"
	"database/sql"

	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

var variantTestDate, _ = time.Parse(time.RFC3339, "2024-12-13T02:09:30.123Z")

func setupVariantTest(t *testing.T) *sql.DB {
	conn, err := GetStdOpenDBConnection(clickhouse.Native, nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	if !CheckMinServerVersion(conn, 24, 4, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version for Variant type"))
		return nil
	}

	_, err = conn.ExecContext(context.Background(), "SET allow_experimental_variant_type = 1")
	if err != nil {
		t.Fatal(err)
		return nil
	}

	_, err = conn.ExecContext(context.Background(), "SET allow_suspicious_variant_types = 1")
	if err != nil {
		t.Fatal(err)
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
	_, err := conn.ExecContext(ctx, ddl)
	require.NoError(t, err)
	defer func() {
		_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS test_variant")
		require.NoError(t, err)
	}()

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)

	batch, err := tx.PrepareContext(ctx, "INSERT INTO test_variant (c)")
	require.NoError(t, err)

	_, err = batch.ExecContext(ctx, true)
	require.NoError(t, err)
	colInt64 := int64(42)
	_, err = batch.ExecContext(ctx, clickhouse.NewVariantWithType(colInt64, "Int64"))
	require.NoError(t, err)
	colString := "test"
	_, err = batch.ExecContext(ctx, clickhouse.NewVariantWithType(colString, "String"))
	require.NoError(t, err)
	_, err = batch.ExecContext(ctx, clickhouse.NewVariantWithType(variantTestDate, "DateTime64(3)"))
	require.NoError(t, err)
	var colNil any = nil
	_, err = batch.ExecContext(ctx, colNil)
	require.NoError(t, err)
	colSliceString := []string{"a", "b"}
	_, err = batch.ExecContext(ctx, clickhouse.NewVariantWithType(colSliceString, "Array(String)"))
	require.NoError(t, err)
	colSliceUInt8 := []uint8{0xA, 0xB, 0xC}
	_, err = batch.ExecContext(ctx, clickhouse.NewVariantWithType(colSliceUInt8, "Array(UInt8)"))
	require.NoError(t, err)
	colSliceMapStringString := []map[string]string{{"key1": "value1", "key2": "value2"}, {"key3": "value3"}}
	_, err = batch.ExecContext(ctx, colSliceMapStringString)
	require.NoError(t, err)
	colMapStringString := map[string]string{"key1": "value1", "key2": "value2"}
	_, err = batch.ExecContext(ctx, colMapStringString)
	require.NoError(t, err)
	colMapStringInt64 := map[string]int64{"key1": 42, "key2": 84}
	_, err = batch.ExecContext(ctx, colMapStringInt64)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	rows, err := conn.QueryContext(ctx, "SELECT c FROM test_variant")
	require.NoError(t, err)

	var row chcol.Variant

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

func TestVariant_ScanWithType(t *testing.T) {
	ctx := context.Background()
	conn := setupVariantTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_variant (
				  c Variant(Bool, Int64)                  
			) Engine = MergeTree() ORDER BY tuple()
		`
	_, err := conn.ExecContext(ctx, ddl)
	require.NoError(t, err)
	defer func() {
		_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS test_variant")
		require.NoError(t, err)
	}()

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)

	batch, err := tx.PrepareContext(ctx, "INSERT INTO test_variant (c)")
	require.NoError(t, err)

	_, err = batch.ExecContext(ctx, true)
	require.NoError(t, err)
	_, err = batch.ExecContext(ctx, clickhouse.NewVariantWithType(int64(42), "Int64"))
	require.NoError(t, err)
	_, err = batch.ExecContext(ctx, nil)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	rows, err := conn.QueryContext(ctx, "SELECT c FROM test_variant")
	require.NoError(t, err)

	var row chcol.Variant

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
