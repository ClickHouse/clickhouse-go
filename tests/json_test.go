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
	"encoding/json"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
)

func setupJSONTest(t *testing.T, protocol clickhouse.Protocol) driver.Conn {
	SkipOnCloud(t, "cannot modify JSON settings on cloud")

	conn, err := GetNativeConnection(t, protocol, clickhouse.Settings{
		"max_execution_time":              60,
		"allow_experimental_variant_type": true,
		"allow_experimental_dynamic_type": true,
		"allow_experimental_json_type":    true,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	if !CheckMinServerServerVersion(conn, 24, 8, 0) {
		t.Skip("unsupported clickhouse version for JSON type")
	}

	return conn
}

func TestJSONPaths(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupJSONTest(t, protocol)
		ctx := context.Background()

		const ddl = `
			CREATE TABLE IF NOT EXISTS test_json_paths (
				  c JSON(Name String, Age Int64, KeysNumbers Map(String, Int64), SKIP fake.field)
			) Engine = MergeTree() ORDER BY tuple()
		`
		require.NoError(t, conn.Exec(ctx, ddl))
		defer func() {
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json_paths"))
		}()

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json_paths (c)")
		require.NoError(t, err)

		jsonRow := BuildTestJSONPaths()

		require.NoError(t, batch.Append(jsonRow))
		require.NoError(t, batch.Send())

		rows, err := conn.Query(ctx, "SELECT c FROM test_json_paths")
		require.NoError(t, err)

		var row clickhouse.JSON

		require.True(t, rows.Next())
		err = rows.Scan(&row)
		require.NoError(t, err)

		expectedValuesByPath := jsonRow.ValuesByPath()
		actualValuesByPath := row.ValuesByPath()
		for path, expectedValue := range expectedValuesByPath {
			actualValue, ok := actualValuesByPath[path]
			if !ok {
				t.Fatalf("result JSON is missing path: %s", path)
			}

			// Allow Equal func to compare values without Dynamic wrapper
			if v, ok := expectedValue.(clickhouse.Dynamic); ok {
				expectedValue = v.Any()
			}

			if v, ok := actualValue.(clickhouse.Dynamic); ok {
				actualValue = v.Any()
			}

			require.Equal(t, expectedValue, actualValue)
		}

		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}

func TestJSONArray(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupJSONTest(t, protocol)
		ctx := context.Background()

		const ddl = `
			CREATE TABLE IF NOT EXISTS test_json_array (
				  c Array(JSON)
			) Engine = MergeTree() ORDER BY tuple()
		`
		require.NoError(t, conn.Exec(ctx, ddl))
		defer func() {
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json_array"))
		}()

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json_array (c)")
		require.NoError(t, err)

		arrJsonRow := []*clickhouse.JSON{clickhouse.NewJSON(), BuildTestJSONPaths()}

		require.NoError(t, batch.Append(arrJsonRow))
		require.NoError(t, batch.Send())

		rows, err := conn.Query(ctx, "SELECT c FROM test_json_array")
		require.NoError(t, err)

		var arrRow []*clickhouse.JSON

		require.True(t, rows.Next())
		err = rows.Scan(&arrRow)
		require.NoError(t, err)
		require.Len(t, arrRow, 2)

		actualValuesByPathEmpty := arrRow[0].ValuesByPath()
		for _, actualValue := range actualValuesByPathEmpty {
			// Allow Nil func to compare values without Dynamic wrapper
			if v, ok := actualValue.(clickhouse.Dynamic); ok {
				actualValue = v.Any()
			}

			require.Nil(t, actualValue)
		}

		expectedValuesByPath := arrJsonRow[1].ValuesByPath()
		actualValuesByPath := arrRow[1].ValuesByPath()
		for path, expectedValue := range expectedValuesByPath {
			actualValue, ok := actualValuesByPath[path]
			if !ok {
				t.Fatalf("result JSON is missing path: %s", path)
			}

			// Allow Equal func to compare values without Dynamic wrapper
			if v, ok := expectedValue.(clickhouse.Dynamic); ok {
				expectedValue = v.Any()
			}

			if v, ok := actualValue.(clickhouse.Dynamic); ok {
				actualValue = v.Any()
			}

			require.Equal(t, expectedValue, actualValue)
		}

		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}

func TestJSONEmptyArray(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupJSONTest(t, protocol)
		ctx := context.Background()

		if !CheckMinServerServerVersion(conn, 24, 9, 0) {
			t.Skip("Empty Array(JSON) depends on JSON strings for empty payload")
		}

		const ddl = `
			CREATE TABLE IF NOT EXISTS test_json_empty_array (
				  c Array(JSON)
			) Engine = MergeTree() ORDER BY tuple()
		`
		require.NoError(t, conn.Exec(ctx, ddl))
		defer func() {
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json_empty_array"))
		}()

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json_empty_array (c)")
		require.NoError(t, err)

		var arrJsonRow []*clickhouse.JSON
		require.NoError(t, batch.Append(arrJsonRow))
		require.NoError(t, batch.Send())

		rows, err := conn.Query(ctx, "SELECT c FROM test_json_empty_array")
		require.NoError(t, err)

		var arrRow []*clickhouse.JSON

		require.True(t, rows.Next())
		err = rows.Scan(&arrRow)
		require.NoError(t, err)
		require.Len(t, arrRow, 0)

		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}

func TestJSONStruct(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupJSONTest(t, protocol)
		ctx := context.Background()

		const ddl = `
			CREATE TABLE IF NOT EXISTS test_json_struct (
				  c JSON(Name String, Age Int64, KeysNumbers Map(String, Int64), SKIP fake.field)
			) Engine = MergeTree() ORDER BY tuple()
		`
		require.NoError(t, conn.Exec(ctx, ddl))
		defer func() {
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json_struct"))
		}()

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json_struct (c)")
		require.NoError(t, err)

		inputRow := BuildTestJSONStruct()
		require.NoError(t, batch.Append(inputRow))

		inputRow2 := TestStruct{
			KeysNumbers: map[string]int64{},
			Timestamp:   JSONTestDate,
			Metadata: map[string]interface{}{
				"FieldA": "a",
				"FieldB": "b",
				"FieldC": map[string]interface{}{
					"FieldD": int64(5),
				},
				"FieldE": map[string]interface{}{
					"FieldF": "f",
				},
			},
		}
		require.NoError(t, batch.Append(inputRow2))

		require.NoError(t, batch.Send())

		rows, err := conn.Query(ctx, "SELECT c FROM test_json_struct")
		require.NoError(t, err)

		var row TestStruct

		require.True(t, rows.Next())
		err = rows.Scan(&row)
		require.NoError(t, err)
		// The second row adds a nil value at this path. Update the inputRow for easier deep equal check
		inputRow.Metadata["FieldE"] = map[string]interface{}{
			"FieldF": nil,
		}
		require.Equal(t, inputRow, row)

		var row2 TestStruct

		require.True(t, rows.Next())
		err = rows.Scan(&row2)
		require.NoError(t, err)
		// Init slices for easier comparison
		inputRow2.Tags = make([]string, 0)
		inputRow2.Numbers = make([]int64, 0)
		require.Equal(t, inputRow2, row2)

		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}

func TestJSONFastStruct(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupJSONTest(t, protocol)
		ctx := context.Background()

		const ddl = `
			CREATE TABLE IF NOT EXISTS test_json_fast_struct (
				  c JSON(Name String, Age Int64, KeysNumbers Map(String, Int64), SKIP fake.field)
			) Engine = MergeTree() ORDER BY tuple()
		`
		require.NoError(t, conn.Exec(ctx, ddl))
		defer func() {
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json_fast_struct"))
		}()

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json_fast_struct (c)")
		require.NoError(t, err)

		inputRow := BuildFastTestJSONStruct()
		require.NoError(t, batch.Append(&inputRow))

		require.NoError(t, batch.Send())

		rows, err := conn.Query(ctx, "SELECT c FROM test_json_fast_struct")
		require.NoError(t, err)

		var row FastTestStruct

		require.True(t, rows.Next())
		err = rows.Scan(&row)
		require.NoError(t, err)
		require.Equal(t, inputRow, row)

		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}

func TestJSONString(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupJSONTest(t, protocol)
		ctx := context.Background()

		if !CheckMinServerServerVersion(conn, 24, 10, 0) {
			t.Skip("JSON strings not supported")
		}

		require.NoError(t, conn.Exec(ctx, "SET output_format_native_write_json_as_string=1"))
		require.NoError(t, conn.Exec(ctx, "SET output_format_json_quote_64bit_integers=0"))

		const ddl = `
			CREATE TABLE IF NOT EXISTS test_json_string (
				  c JSON(Name String, Age Int64, KeysNumbers Map(String, Int64), SKIP fake.field)
			) Engine = MergeTree() ORDER BY tuple()
		`
		require.NoError(t, conn.Exec(ctx, ddl))
		defer func() {
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json_string"))
		}()

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json_string (c)")
		require.NoError(t, err)

		inputRow := BuildTestJSONStruct()

		inputRowStr, err := json.Marshal(inputRow)
		require.NoError(t, err)
		require.NoError(t, batch.Append(inputRowStr))
		require.NoError(t, batch.Send())

		rows, err := conn.Query(ctx, "SELECT c FROM test_json_string")
		require.NoError(t, err)

		var row json.RawMessage

		require.True(t, rows.Next())
		err = rows.Scan(&row)
		require.NoError(t, err)

		var rowStruct TestStruct
		err = json.Unmarshal(row, &rowStruct)
		require.NoError(t, err)

		// Re-Marshal to get properties in the same order
		rowStructStr, err := json.Marshal(rowStruct)
		require.NoError(t, err)
		require.Equal(t, inputRowStr, rowStructStr)

		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}

func TestJSON_BatchFlush(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		SkipOnHTTP(t, protocol, "Flush")
		conn := setupJSONTest(t, protocol)
		ctx := context.Background()

		const ddl = `
			CREATE TABLE IF NOT EXISTS test_json_batch_flush (
				  c JSON
			) Engine = MergeTree() ORDER BY tuple()
		`
		require.NoError(t, conn.Exec(ctx, ddl))
		defer func() {
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json_batch_flush"))
		}()

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json_batch_flush (c)")
		require.NoError(t, err)

		vals := make([]*clickhouse.JSON, 0, 1000)
		for i := 0; i < 1000; i++ {
			row := clickhouse.NewJSON()
			if i%2 == 0 {
				row.SetValueAtPath("a", int64(i))
				row.SetValueAtPath("b", i%5 == 0)
			} else {
				row.SetValueAtPath("c", int64(-i))
				row.SetValueAtPath("d", i%5 != 0)
			}

			vals = append(vals, row)
			require.NoError(t, batch.Append(vals[i]))
			require.NoError(t, batch.Flush())
		}
		require.NoError(t, batch.Send())

		rows, err := conn.Query(ctx, "SELECT c FROM test_json_batch_flush")
		require.NoError(t, err)

		i := 0
		for rows.Next() {
			var row clickhouse.JSON
			err = rows.Scan(&row)
			require.NoError(t, err)

			if i%2 == 0 {
				valA, ok := row.ValueAtPath("a")
				require.Equal(t, true, ok)
				_, ok = valA.(clickhouse.Dynamic)
				require.Equal(t, true, ok)

				require.Equal(t, int64(i), valA.(clickhouse.Dynamic).Any())
				require.Equal(t, "Int64", valA.(clickhouse.Dynamic).Type())

				valB, ok := row.ValueAtPath("b")
				require.Equal(t, true, ok)
				_, ok = valB.(clickhouse.Dynamic)
				require.Equal(t, true, ok)

				require.Equal(t, i%5 == 0, valB.(clickhouse.Dynamic).Any())
				require.Equal(t, "Bool", valB.(clickhouse.Dynamic).Type())
			} else {
				valC, ok := row.ValueAtPath("c")
				require.Equal(t, true, ok)
				_, ok = valC.(clickhouse.Dynamic)
				require.Equal(t, true, ok)

				require.Equal(t, int64(-i), valC.(clickhouse.Dynamic).Any())
				require.Equal(t, "Int64", valC.(clickhouse.Dynamic).Type())

				valD, ok := row.ValueAtPath("d")
				require.Equal(t, true, ok)
				_, ok = valD.(clickhouse.Dynamic)
				require.Equal(t, true, ok)

				require.Equal(t, i%5 != 0, valD.(clickhouse.Dynamic).Any())
				require.Equal(t, "Bool", valD.(clickhouse.Dynamic).Type())
			}

			i++
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}

// https://github.com/grafana/clickhouse-datasource/issues/1168
func TestJSONArrayDynamic(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupJSONTest(t, protocol)
		ctx := context.Background()

		rows, err := conn.Query(ctx, `SELECT ['{"x":5}','{"y":6}']::Array(JSON)::Dynamic AS c`)
		require.NoError(t, err)

		require.True(t, rows.Next())
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}

func TestJSONArrayVariant(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupJSONTest(t, protocol)
		ctx := context.Background()

		rows, err := conn.Query(ctx, `SELECT ['{"x":5}','{"y":6}']::Array(JSON)::Variant(Array(JSON)) AS c`)
		require.NoError(t, err)

		require.True(t, rows.Next())
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}

func TestJSONLowCardinalityNullableString(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupJSONTest(t, protocol)
		ctx := context.Background()

		rows, err := conn.Query(ctx, `SELECT '{"x": "test"}'::JSON(x LowCardinality(Nullable(String)))`)
		require.NoError(t, err)

		require.True(t, rows.Next())

		var row clickhouse.JSON
		err = rows.Scan(&row)
		require.NoError(t, err)

		xStr, ok := clickhouse.ExtractJSONPathAs[*string](&row, "x")
		require.True(t, ok)
		require.Equal(t, "test", *xStr)

		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}

func TestJSONNullableObjectScan(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupJSONTest(t, protocol)

		if !CheckMinServerServerVersion(conn, 25, 2, 0) {
			t.Skip("Nullable(JSON) unsupported")
		}

		ctx := context.Background()
		rows, err := conn.Query(ctx, `SELECT '{"x": "test"}'::Nullable(JSON)`)
		require.NoError(t, err)

		require.True(t, rows.Next())

		var row clickhouse.JSON
		err = rows.Scan(&row)
		require.NoError(t, err)

		xStr, ok := clickhouse.ExtractJSONPathAs[string](&row, "x")
		require.True(t, ok)
		require.Equal(t, "test", xStr)

		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}

func TestJSONNullableStringsScan(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn := setupJSONTest(t, protocol)

		if !CheckMinServerServerVersion(conn, 25, 2, 0) {
			t.Skip("Nullable(JSON) unsupported")
		}

		ctx := context.Background()
		require.NoError(t, conn.Exec(ctx, "SET output_format_native_write_json_as_string=1"))
		require.NoError(t, conn.Exec(ctx, "SET output_format_json_quote_64bit_integers=0"))

		rows, err := conn.Query(ctx, `SELECT arrayJoin(['{"x": 1}', '{"x": 2}', '{"x": 3}']::Array(Nullable(JSON)))`)
		require.NoError(t, err)

		var rowStr string
		require.True(t, rows.Next())
		err = rows.Scan(&rowStr)
		require.NoError(t, err)
		require.Equal(t, "{\"x\":1}", rowStr)

		var rowBytes []byte
		require.True(t, rows.Next())
		err = rows.Scan(&rowBytes)
		require.NoError(t, err)
		require.Equal(t, []byte("{\"x\":2}"), rowBytes)

		var rowJSONRawMessage json.RawMessage
		require.True(t, rows.Next())
		err = rows.Scan(&rowJSONRawMessage)
		require.NoError(t, err)
		require.Equal(t, json.RawMessage("{\"x\":3}"), rowJSONRawMessage)

		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
	})
}
